#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mprpccodec.h"

struct RpcMetricValues
{
    uint64_t started = 0;
    uint64_t active = 0;
    uint64_t success = 0;
    uint64_t timeout = 0;
    uint64_t cancelled = 0;
    uint64_t network_error = 0;
    uint64_t channel_closed = 0;
    uint64_t framework_error = 0;
    uint64_t deadline_exceeded = 0;
    uint64_t client_cancel = 0;
    uint64_t disconnect = 0;
    uint64_t callback_rejected = 0;
    uint64_t queue_rejected = 0;
    uint64_t retry = 0;
    uint64_t retry_exhausted = 0;
    uint64_t crc_failure = 0;
    uint64_t session_conflict = 0;
    uint64_t duplicate_chunk = 0;
    uint64_t bytes_transferred = 0;
    uint64_t discovery_watch_event = 0;
    uint64_t discovery_refresh = 0;
    uint64_t discovery_refresh_error = 0;
    std::array<uint64_t, 11> latency_buckets{};
};

struct RpcMetricsSnapshot
{
    RpcMetricValues total;
    std::unordered_map<std::string, RpcMetricValues> methods;
};

enum class RpcMetricEvent
{
    FrameworkError,
    DeadlineExceeded,
    ClientCancel,
    Disconnect,
    CallbackRejected,
    QueueRejected,
    Retry,
    RetryExhausted,
    CrcFailure,
    SessionConflict,
    DuplicateChunk,
    BytesTransferred,
    DiscoveryWatchEvent,
    DiscoveryRefresh,
    DiscoveryRefreshError,
};

// Component-owned lock-light counters with bounded method labels.
class RpcMetrics
{
public:
    struct Counters;

    static constexpr std::array<uint64_t, 10> kLatencyBoundsMs{
        1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000};

    RpcMetrics();
    ~RpcMetrics();

    RpcMetrics(const RpcMetrics&) = delete;
    RpcMetrics& operator=(const RpcMetrics&) = delete;

    void CallStarted(const std::string& method);
    void CallCompleted(const std::string& method,
                       mprpc::MprpcErrorCode code,
                       std::chrono::steady_clock::duration latency);
    void Increment(RpcMetricEvent event, uint64_t amount = 1,
                   const std::string& method = {});

    RpcMetricsSnapshot Snapshot() const;

private:
    std::shared_ptr<Counters> GetOrCreateMethod(
        const std::string& method);
    std::shared_ptr<Counters> FindMethod(
        const std::string& method) const;

    std::shared_ptr<Counters> total_;
    mutable std::mutex methods_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Counters>> methods_;
};

std::string FormatRpcMetrics(const std::string& component,
                             const RpcMetricsSnapshot& snapshot);

// Periodically writes preformatted component snapshots as structured lines.
class RpcMetricsReporter
{
public:
    using Source = std::function<std::string()>;

    RpcMetricsReporter() = default;
    ~RpcMetricsReporter();

    RpcMetricsReporter(const RpcMetricsReporter&) = delete;
    RpcMetricsReporter& operator=(const RpcMetricsReporter&) = delete;

    void AddSource(std::string name, Source source);
    void Start(std::chrono::milliseconds interval,
               std::ostream* output);
    void Stop();

private:
    void Run();

    std::vector<std::pair<std::string, Source>> sources_;
    std::chrono::milliseconds interval_{0};
    std::ostream* output_ = nullptr;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
    std::thread thread_;
};
