#include "friend.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

namespace
{
using Clock = std::chrono::steady_clock;

void Touch(const std::filesystem::path& path)
{
    std::ofstream marker(path);
    marker << "ready\n";
}

bool WaitForMarker(const std::filesystem::path& path)
{
    const auto deadline = Clock::now() + std::chrono::seconds(8);
    while (Clock::now() < deadline) {
        if (std::filesystem::exists(path)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

std::string CallIdentity(fixbug::FriendServiceRpc_Stub* stub)
{
    fixbug::GetFriendListRequest request;
    request.set_userid(1);
    fixbug::GetFriendListResponse response;
    MprpcController controller;
    controller.SetTimeoutMs(800);
    stub->GetFriendList(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.friends_size() == 0) {
        return {};
    }
    return response.friends(0);
}

bool ObserveIdentity(fixbug::FriendServiceRpc_Stub* stub,
                     const std::string& expected,
                     std::chrono::milliseconds limit)
{
    const auto deadline = Clock::now() + limit;
    while (Clock::now() < deadline) {
        if (CallIdentity(stub) == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return false;
}

bool WaitForWatchEvent(MprpcChannel* channel, uint64_t previous_count)
{
    const auto deadline = Clock::now() + std::chrono::seconds(2);
    while (Clock::now() < deadline) {
        if (channel->GetMetricsSnapshot().total.discovery_watch_event >
            previous_count) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}
} // namespace

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);
    if (argc < 4) {
        std::cerr << "FAIL: marker directory argument is missing\n";
        return 1;
    }

    std::filesystem::path markers;
    for (int i = 1; i + 1 < argc; ++i) {
        if (std::string(argv[i]) == "--markers") {
            markers = argv[i + 1];
            break;
        }
    }
    if (markers.empty()) {
        std::cerr << "FAIL: --markers was not provided\n";
        return 1;
    }

    MprpcChannel channel;
    fixbug::FriendServiceRpc_Stub stub(&channel);
    if (CallIdentity(&stub) != "18891") {
        std::cerr << "FAIL: Provider A did not answer the warm call\n";
        return 1;
    }
    Touch(markers / "a-warmed");

    if (!WaitForMarker(markers / "b-added")) {
        std::cerr << "FAIL: timed out waiting for Provider B\n";
        return 1;
    }
    if (!ObserveIdentity(&stub, "18892", std::chrono::seconds(2))) {
        std::cerr << "FAIL: Provider B was not discovered before TTL expiry\n";
        return 1;
    }
    uint64_t watch_count =
        channel.GetMetricsSnapshot().total.discovery_watch_event;
    Touch(markers / "b-observed");

    if (!WaitForMarker(markers / "b-removed")) {
        std::cerr << "FAIL: timed out waiting for Provider B removal\n";
        return 1;
    }
    if (!WaitForWatchEvent(&channel, watch_count)) {
        std::cerr << "FAIL: Provider B removal watch was not delivered\n";
        return 1;
    }
    if (!ObserveIdentity(&stub, "18891", std::chrono::seconds(2))) {
        std::cerr << "FAIL: calls did not converge to Provider A\n";
        return 1;
    }
    watch_count =
        channel.GetMetricsSnapshot().total.discovery_watch_event;
    Touch(markers / "a-observed");

    if (!WaitForMarker(markers / "b-restarted")) {
        std::cerr << "FAIL: timed out waiting for Provider B restart\n";
        return 1;
    }
    if (!WaitForWatchEvent(&channel, watch_count)) {
        std::cerr << "FAIL: Provider B restart watch was not delivered\n";
        return 1;
    }
    if (!ObserveIdentity(&stub, "18892", std::chrono::seconds(2))) {
        std::cerr << "FAIL: re-registered watch did not discover Provider B\n";
        return 1;
    }

    const RpcMetricsSnapshot metrics = channel.GetMetricsSnapshot();
    if (metrics.total.discovery_watch_event < 3 ||
        metrics.total.discovery_refresh < 4 ||
        metrics.total.active != 0) {
        std::cerr << "FAIL: discovery metrics were incomplete"
                  << " watch=" << metrics.total.discovery_watch_event
                  << " refresh=" << metrics.total.discovery_refresh
                  << " active=" << metrics.total.active << '\n';
        return 1;
    }

    std::cout << "PASS: Provider membership changed before TTL expiry\n";
    return 0;
}
