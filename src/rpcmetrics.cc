#include "rpcmetrics.h"

#include <algorithm>
#include <sstream>

struct RpcMetrics::Counters
{
    std::atomic<uint64_t> started{0};
    std::atomic<uint64_t> active{0};
    std::atomic<uint64_t> success{0};
    std::atomic<uint64_t> timeout{0};
    std::atomic<uint64_t> cancelled{0};
    std::atomic<uint64_t> network_error{0};
    std::atomic<uint64_t> channel_closed{0};
    std::atomic<uint64_t> framework_error{0};
    std::atomic<uint64_t> deadline_exceeded{0};
    std::atomic<uint64_t> client_cancel{0};
    std::atomic<uint64_t> disconnect{0};
    std::atomic<uint64_t> callback_rejected{0};
    std::atomic<uint64_t> queue_rejected{0};
    std::atomic<uint64_t> retry{0};
    std::atomic<uint64_t> retry_exhausted{0};
    std::atomic<uint64_t> crc_failure{0};
    std::atomic<uint64_t> session_conflict{0};
    std::atomic<uint64_t> duplicate_chunk{0};
    std::atomic<uint64_t> bytes_transferred{0};
    std::array<std::atomic<uint64_t>, 11> latency_buckets{};
};

namespace
{
void Start(RpcMetrics::Counters* counters)
{
    counters->started.fetch_add(1, std::memory_order_relaxed);
    counters->active.fetch_add(1, std::memory_order_relaxed);
}

void ClassifyCompletion(RpcMetrics::Counters* counters,
                        mprpc::MprpcErrorCode code)
{
    if (code == mprpc::MprpcErrorCode::OK) {
        counters->success.fetch_add(1, std::memory_order_relaxed);
    } else if (code == mprpc::MprpcErrorCode::TIMEOUT) {
        counters->timeout.fetch_add(1, std::memory_order_relaxed);
    } else if (code == mprpc::MprpcErrorCode::CANCELLED) {
        counters->cancelled.fetch_add(1, std::memory_order_relaxed);
    } else if (code == mprpc::MprpcErrorCode::NETWORK_ERROR ||
               code == mprpc::MprpcErrorCode::CONNECTION_CLOSED) {
        counters->network_error.fetch_add(1, std::memory_order_relaxed);
    } else if (code == mprpc::MprpcErrorCode::CHANNEL_CLOSED) {
        counters->channel_closed.fetch_add(1, std::memory_order_relaxed);
    } else {
        counters->framework_error.fetch_add(1, std::memory_order_relaxed);
    }
}

void Complete(RpcMetrics::Counters* counters,
              mprpc::MprpcErrorCode code,
              std::chrono::steady_clock::duration latency)
{
    const uint64_t active =
        counters->active.load(std::memory_order_relaxed);
    if (active != 0) {
        counters->active.fetch_sub(1, std::memory_order_relaxed);
    }
    ClassifyCompletion(counters, code);

    const auto elapsed = std::chrono::duration_cast<
        std::chrono::milliseconds>(latency);
    const uint64_t milliseconds = static_cast<uint64_t>(
        std::max<int64_t>(0, elapsed.count()));
    for (std::size_t i = 0; i < RpcMetrics::kLatencyBoundsMs.size(); ++i) {
        if (milliseconds <= RpcMetrics::kLatencyBoundsMs[i]) {
            counters->latency_buckets[i].fetch_add(
                1, std::memory_order_relaxed);
        }
    }
    counters->latency_buckets.back().fetch_add(
        1, std::memory_order_relaxed);
}

void Increment(RpcMetrics::Counters* counters,
               RpcMetricEvent event, uint64_t amount)
{
    std::atomic<uint64_t>* target = nullptr;
    switch (event) {
    case RpcMetricEvent::FrameworkError:
        target = &counters->framework_error; break;
    case RpcMetricEvent::DeadlineExceeded:
        target = &counters->deadline_exceeded; break;
    case RpcMetricEvent::ClientCancel:
        target = &counters->client_cancel; break;
    case RpcMetricEvent::Disconnect:
        target = &counters->disconnect; break;
    case RpcMetricEvent::CallbackRejected:
        target = &counters->callback_rejected; break;
    case RpcMetricEvent::QueueRejected:
        target = &counters->queue_rejected; break;
    case RpcMetricEvent::Retry:
        target = &counters->retry; break;
    case RpcMetricEvent::RetryExhausted:
        target = &counters->retry_exhausted; break;
    case RpcMetricEvent::CrcFailure:
        target = &counters->crc_failure; break;
    case RpcMetricEvent::SessionConflict:
        target = &counters->session_conflict; break;
    case RpcMetricEvent::DuplicateChunk:
        target = &counters->duplicate_chunk; break;
    case RpcMetricEvent::BytesTransferred:
        target = &counters->bytes_transferred; break;
    }
    target->fetch_add(amount, std::memory_order_relaxed);
}

RpcMetricValues SnapshotCounters(const RpcMetrics::Counters& counters)
{
    RpcMetricValues values;
#define LOAD_COUNTER(name) \
    values.name = counters.name.load(std::memory_order_relaxed)
    LOAD_COUNTER(started);
    LOAD_COUNTER(active);
    LOAD_COUNTER(success);
    LOAD_COUNTER(timeout);
    LOAD_COUNTER(cancelled);
    LOAD_COUNTER(network_error);
    LOAD_COUNTER(channel_closed);
    LOAD_COUNTER(framework_error);
    LOAD_COUNTER(deadline_exceeded);
    LOAD_COUNTER(client_cancel);
    LOAD_COUNTER(disconnect);
    LOAD_COUNTER(callback_rejected);
    LOAD_COUNTER(queue_rejected);
    LOAD_COUNTER(retry);
    LOAD_COUNTER(retry_exhausted);
    LOAD_COUNTER(crc_failure);
    LOAD_COUNTER(session_conflict);
    LOAD_COUNTER(duplicate_chunk);
    LOAD_COUNTER(bytes_transferred);
#undef LOAD_COUNTER
    for (std::size_t i = 0; i < values.latency_buckets.size(); ++i) {
        values.latency_buckets[i] =
            counters.latency_buckets[i].load(std::memory_order_relaxed);
    }
    return values;
}

void AppendValues(std::ostringstream& output,
                  const RpcMetricValues& values)
{
    output << " started=" << values.started
           << " active=" << values.active
           << " success=" << values.success
           << " timeout=" << values.timeout
           << " cancelled=" << values.cancelled
           << " network_error=" << values.network_error
           << " channel_closed=" << values.channel_closed
           << " framework_error=" << values.framework_error
           << " deadline_exceeded=" << values.deadline_exceeded
           << " client_cancel=" << values.client_cancel
           << " disconnect=" << values.disconnect
           << " callback_rejected=" << values.callback_rejected
           << " queue_rejected=" << values.queue_rejected
           << " retry=" << values.retry
           << " retry_exhausted=" << values.retry_exhausted
           << " crc_failure=" << values.crc_failure
           << " session_conflict=" << values.session_conflict
           << " duplicate_chunk=" << values.duplicate_chunk
           << " bytes_transferred=" << values.bytes_transferred;
}
} // namespace

RpcMetrics::RpcMetrics() : total_(std::make_shared<Counters>()) {}
RpcMetrics::~RpcMetrics() = default;

std::shared_ptr<RpcMetrics::Counters> RpcMetrics::GetOrCreateMethod(
    const std::string& method)
{
    if (method.empty()) return {};
    std::lock_guard<std::mutex> lock(methods_mutex_);
    auto& counters = methods_[method];
    if (!counters) counters = std::make_shared<Counters>();
    return counters;
}

std::shared_ptr<RpcMetrics::Counters> RpcMetrics::FindMethod(
    const std::string& method) const
{
    std::lock_guard<std::mutex> lock(methods_mutex_);
    const auto it = methods_.find(method);
    return it == methods_.end() ? nullptr : it->second;
}

void RpcMetrics::CallStarted(const std::string& method)
{
    Start(total_.get());
    if (auto counters = GetOrCreateMethod(method)) Start(counters.get());
}

void RpcMetrics::CallCompleted(
    const std::string& method, mprpc::MprpcErrorCode code,
    std::chrono::steady_clock::duration latency)
{
    Complete(total_.get(), code, latency);
    if (auto counters = FindMethod(method)) {
        Complete(counters.get(), code, latency);
    }
}

void RpcMetrics::Increment(RpcMetricEvent event, uint64_t amount,
                           const std::string& method)
{
    ::Increment(total_.get(), event, amount);
    if (auto counters = GetOrCreateMethod(method)) {
        ::Increment(counters.get(), event, amount);
    }
}

RpcMetricsSnapshot RpcMetrics::Snapshot() const
{
    RpcMetricsSnapshot snapshot;
    snapshot.total = SnapshotCounters(*total_);
    std::lock_guard<std::mutex> lock(methods_mutex_);
    for (const auto& entry : methods_) {
        snapshot.methods.emplace(entry.first,
                                 SnapshotCounters(*entry.second));
    }
    return snapshot;
}

std::string FormatRpcMetrics(const std::string& component,
                             const RpcMetricsSnapshot& snapshot)
{
    std::ostringstream output;
    output << "rpc_metrics component=" << component << " scope=total";
    AppendValues(output, snapshot.total);
    for (const auto& method : snapshot.methods) {
        output << '\n' << "rpc_metrics component=" << component
               << " scope=method method=" << method.first;
        AppendValues(output, method.second);
    }
    return output.str();
}

RpcMetricsReporter::~RpcMetricsReporter() { Stop(); }

void RpcMetricsReporter::AddSource(std::string name, Source source)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!thread_.joinable() && source) {
        sources_.emplace_back(std::move(name), std::move(source));
    }
}

void RpcMetricsReporter::Start(std::chrono::milliseconds interval,
                               std::ostream* output)
{
    if (interval.count() <= 0 || output == nullptr) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (thread_.joinable()) return;
    interval_ = interval;
    output_ = output;
    stopping_ = false;
    thread_ = std::thread(&RpcMetricsReporter::Run, this);
}

void RpcMetricsReporter::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void RpcMetricsReporter::Run()
{
    std::unique_lock<std::mutex> lock(mutex_);
    while (!cv_.wait_for(lock, interval_, [this] { return stopping_; })) {
        const auto sources = sources_;
        std::ostream* output = output_;
        lock.unlock();
        for (const auto& source : sources) {
            *output << source.second() << '\n';
        }
        output->flush();
        lock.lock();
    }
}
