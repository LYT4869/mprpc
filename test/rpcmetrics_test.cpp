#include <cassert>
#include <chrono>
#include <atomic>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rpcmetrics.h"

int main()
{
    RpcMetrics metrics;
    constexpr int kThreads = 8;
    constexpr int kCallsPerThread = 1000;
    std::vector<std::thread> workers;
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&metrics, thread] {
            for (int call = 0; call < kCallsPerThread; ++call) {
                const std::string method = thread % 2 == 0
                    ? "Service/Fast" : "Service/Slow";
                metrics.CallStarted(method);
                metrics.CallCompleted(
                    method,
                    call % 10 == 0
                        ? mprpc::MprpcErrorCode::TIMEOUT
                        : mprpc::MprpcErrorCode::OK,
                    std::chrono::milliseconds(call % 1200));
                metrics.Increment(RpcMetricEvent::BytesTransferred, 64,
                                  method);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    const std::string discovery_method = "FriendServiceRpc/GetFriendList";
    metrics.Increment(RpcMetricEvent::DiscoveryWatchEvent, 2,
                      discovery_method);
    metrics.Increment(RpcMetricEvent::DiscoveryRefresh, 3,
                      discovery_method);
    metrics.Increment(RpcMetricEvent::DiscoveryRefreshError, 1,
                      discovery_method);

    const RpcMetricsSnapshot snapshot = metrics.Snapshot();
    const uint64_t expected = kThreads * kCallsPerThread;
    assert(snapshot.total.started == expected);
    assert(snapshot.total.active == 0);
    assert(snapshot.total.success == expected * 9 / 10);
    assert(snapshot.total.timeout == expected / 10);
    assert(snapshot.total.bytes_transferred == expected * 64);
    assert(snapshot.total.latency_buckets.back() == expected);
    assert(snapshot.total.discovery_watch_event == 2);
    assert(snapshot.total.discovery_refresh == 3);
    assert(snapshot.total.discovery_refresh_error == 1);
    assert(snapshot.methods.size() == 3);
    const auto discovery = snapshot.methods.find(discovery_method);
    assert(discovery != snapshot.methods.end());
    assert(discovery->second.discovery_watch_event == 2);
    assert(discovery->second.discovery_refresh == 3);
    assert(discovery->second.discovery_refresh_error == 1);

    const std::string formatted =
        FormatRpcMetrics("test", snapshot);
    assert(formatted.find("component=test") != std::string::npos);
    assert(formatted.find("active=0") != std::string::npos);
    assert(formatted.find("discovery_watch_event=2") !=
           std::string::npos);
    assert(formatted.find("discovery_refresh=3") != std::string::npos);
    assert(formatted.find("discovery_refresh_error=1") !=
           std::string::npos);

    std::atomic<int> reports{0};
    std::ostringstream output;
    RpcMetricsReporter reporter;
    reporter.AddSource("test", [&reports] {
        reports.fetch_add(1, std::memory_order_relaxed);
        return std::string("reporter_test value=1");
    });
    reporter.Start(std::chrono::milliseconds(5), &output);
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    reporter.Stop();
    assert(reports.load(std::memory_order_relaxed) >= 1);
    assert(output.str().find("reporter_test value=1") !=
           std::string::npos);
    return 0;
}
