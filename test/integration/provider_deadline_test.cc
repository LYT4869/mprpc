#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

#include "friend.pb.h"
#include "mprpccodec.h"
#include "proto/rpc_meta.pb.h"

namespace
{
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
} // namespace

int main()
{
    constexpr uint64_t kRequestId = 0x1020304050607080ULL;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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

    fixbug::GetFriendListRequest request;
    request.set_userid(9999);
    std::string payload;
    request.SerializeToString(&payload);

    mprpc::MprpcRequestMeta meta;
    meta.set_service_name("FriendServiceRpc");
    meta.set_method_name("GetFriendList");
    meta.set_timeout_ms(100);
    std::string encoded_meta;
    meta.SerializeToString(&encoded_meta);

    mprpc::MprpcHeader header;
    header.request_id = kRequestId;
    header.message_type = mprpc::MprpcMessageType::REQUEST;
    const std::string frame = mprpc::MprpcCodec::Encode(
        header, mprpc::MprpcCodec::EncodeBody(encoded_meta, payload));
    if (!SendAll(fd, frame)) {
        ::close(fd);
        return 1;
    }

    std::string received;
    char buffer[4096];
    while (true) {
        mprpc::MprpcFrame response;
        std::size_t consumed = 0;
        const auto status =
            mprpc::MprpcCodec::Decode(received, &response, &consumed);
        if (status == mprpc::DecodeStatus::OK) {
            const bool passed = response.header.request_id == kRequestId &&
                response.header.message_type ==
                    mprpc::MprpcMessageType::RESPONSE &&
                response.header.status_code ==
                    mprpc::MprpcErrorCode::TIMEOUT;
            std::cout << (passed ? "PASS" : "FAIL")
                      << ": provider-side deadline" << std::endl;
            ::close(fd);
            return passed ? 0 : 1;
        }
        if (status != mprpc::DecodeStatus::NEED_MORE_DATA) {
            ::close(fd);
            return 1;
        }

        const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
        if (count <= 0) {
            if (count < 0 && errno == EINTR) continue;
            std::cerr << "failed to receive deadline response: "
                      << std::strerror(errno) << std::endl;
            ::close(fd);
            return 1;
        }
        received.append(buffer, static_cast<std::size_t>(count));
    }
}
