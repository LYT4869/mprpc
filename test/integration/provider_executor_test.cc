#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "friend.pb.h"
#include "mprpccodec.h"
#include "proto/rpc_meta.pb.h"

namespace
{
struct ResponseObservation
{
    mprpc::MprpcErrorCode code = mprpc::MprpcErrorCode::INTERNAL_ERROR;
    double elapsed_ms = 0.0;
};

bool SendAll(int fd, const std::string& bytes)
{
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        const ssize_t count = ::send(
            fd, bytes.data() + sent, bytes.size() - sent, 0);
        if (count < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (count == 0) return false;
        sent += static_cast<std::size_t>(count);
    }
    return true;
}

std::string BuildRequest(uint64_t request_id,
                         uint32_t user_id,
                         uint32_t timeout_ms,
                         const std::string& method = "GetFriendList")
{
    fixbug::GetFriendListRequest request;
    request.set_userid(user_id);
    std::string payload;
    request.SerializeToString(&payload);

    mprpc::MprpcRequestMeta meta;
    meta.set_service_name("FriendServiceRpc");
    meta.set_method_name(method);
    meta.set_timeout_ms(timeout_ms);
    std::string encoded_meta;
    meta.SerializeToString(&encoded_meta);

    mprpc::MprpcHeader header;
    header.request_id = request_id;
    header.message_type = mprpc::MprpcMessageType::REQUEST;
    return mprpc::MprpcCodec::Encode(
        header, mprpc::MprpcCodec::EncodeBody(encoded_meta, payload));
}
} // namespace

int main()
{
    constexpr uint64_t kBlocker = 1;
    constexpr uint64_t kQueuedTimeout = 2;
    constexpr uint64_t kQueueOverflow = 3;
    constexpr uint64_t kFrameworkRequest = 4;

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 1;

    timeval timeout{3, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(18887);
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) != 0) {
        ::close(fd);
        return 1;
    }

    if (!SendAll(fd, BuildRequest(kBlocker, 20000, 2000))) {
        ::close(fd);
        return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto measured_from = std::chrono::steady_clock::now();
    const std::string remaining =
        BuildRequest(kQueuedTimeout, 20001, 100) +
        BuildRequest(kQueueOverflow, 7, 2000) +
        BuildRequest(kFrameworkRequest, 7, 2000, "NotExist");
    if (!SendAll(fd, remaining)) {
        ::close(fd);
        return 1;
    }

    std::unordered_map<uint64_t, ResponseObservation> observations;
    std::string received;
    char buffer[4096];
    while (observations.size() < 4) {
        mprpc::MprpcFrame response;
        std::size_t consumed = 0;
        const auto status =
            mprpc::MprpcCodec::Decode(received, &response, &consumed);
        if (status == mprpc::DecodeStatus::OK) {
            observations.emplace(
                response.header.request_id,
                ResponseObservation{
                    response.header.status_code,
                    std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - measured_from)
                        .count()});
            received.erase(0, consumed);
            continue;
        }
        if (status != mprpc::DecodeStatus::NEED_MORE_DATA) {
            ::close(fd);
            return 1;
        }

        const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count <= 0) {
            if (count < 0 && errno == EINTR) continue;
            std::cerr << "provider executor recv failed: "
                      << std::strerror(errno) << '\n';
            ::close(fd);
            return 1;
        }
        received.append(buffer, static_cast<std::size_t>(count));
    }
    ::close(fd);

    const bool passed =
        observations[kBlocker].code == mprpc::MprpcErrorCode::OK &&
        observations[kQueuedTimeout].code ==
            mprpc::MprpcErrorCode::TIMEOUT &&
        observations[kQueueOverflow].code ==
            mprpc::MprpcErrorCode::SERVER_BUSY &&
        observations[kFrameworkRequest].code ==
            mprpc::MprpcErrorCode::METHOD_NOT_FOUND &&
        observations[kFrameworkRequest].elapsed_ms < 250.0;

    std::cout << (passed ? "PASS" : "FAIL")
              << ": provider queue timeout, overload and Reactor isolation"
              << std::endl;
    return passed ? 0 : 1;
}
