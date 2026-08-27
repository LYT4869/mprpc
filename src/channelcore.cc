#include <charconv>
#include <algorithm>
#include <future>
#include <utility>
#include <muduo/net/InetAddress.h>

#include "zookeeperutil.h"
#include "channelcore.h"
#include "rpcclientruntime.h"
#include "proto/rpc_meta.pb.h"

ChannelCore::ChannelCore()
    : ChannelCore(RpcClientRuntime::Default())
{
}

ChannelCore::ChannelCore(std::shared_ptr<RpcClientRuntime> runtime)
    : runtime_(runtime ? std::move(runtime) : RpcClientRuntime::Default()),
      loop_(runtime_->NextLoop())
{
}

ChannelCore::~ChannelCore()
{
    Shutdown();
}

CallHandle ChannelCore::StartCall(const std::string& service_name,
                const std::string& method_name,
                const std::string& request_payload,
                const CallOptions& options,
                RpcCompletion completion)
{
    RpcCallResult call_result;
    auto state = std::make_shared<CallState>();
    state->completion = std::move(completion);
    state->method_name = service_name + "/" + method_name;
    state->started_at = std::chrono::steady_clock::now();
    metrics_.CallStarted(state->method_name);

    uint64_t request_id = next_request_id_.fetch_add(1);
    state->request_id = request_id;
    call_result.request_id = request_id;

    if (state->completion) {
        state->callback_reservation = runtime_->TryReserveCallback();
        if (!state->callback_reservation) {
            call_result.status_code =
                mprpc::MprpcErrorCode::CALLBACK_REJECTED;
            call_result.error_msg = "RPC callback executor is full";
            RpcCompletion rejected_completion;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->result = call_result;
                state->completed = true;
                rejected_completion = std::move(state->completion);
            }
            state->phase.store(
                CallPhase::Completed, std::memory_order_release);
            metrics_.Increment(
                RpcMetricEvent::CallbackRejected, 1, state->method_name);
            metrics_.CallCompleted(
                state->method_name, call_result.status_code,
                std::chrono::steady_clock::now() - state->started_at);
            state->cv.notify_all();
            // 入场失败发生在调用线程，尚未进入 Reactor 或网络层。
            rejected_completion(state->result);
            return state;
        }
    }

    bool channel_closed = false;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        channel_closed = shutting_down_.load(std::memory_order_acquire);
        // 必须先注册再发送，避免快速响应找不到调用状态。
        pending_calls_[request_id] = state;
    }

    if (channel_closed) {
        call_result.status_code = mprpc::MprpcErrorCode::CHANNEL_CLOSED;
        call_result.error_msg = "RPC channel is closed";
        CompleteCall(request_id, std::move(call_result));
        return state;
    }

    std::string request_frame;
    std::string error_msg;

    // 封装请求帧
    if(!BuildRequestFrame(service_name, method_name, request_payload, request_id, options.timeout_ms, &request_frame, &error_msg)){
        call_result.status_code = mprpc::MprpcErrorCode::SERIALIZE_FAILED;
        call_result.error_msg = std::move(error_msg);
        CompleteCall(request_id, std::move(call_result));
        return state;
    }

    Endpoint endpoint;
    mprpc::MprpcErrorCode status_code = GetEndpoint(
        service_name, method_name, options.affinity_key,
        &endpoint, &error_msg);
    if(status_code != mprpc::MprpcErrorCode::OK)
    {
        call_result.status_code =status_code;
        call_result.error_msg = std::move(error_msg);
        CompleteCall(request_id, std::move(call_result));
        return state;
    }

    state->endpoint_key = endpoint.Key();

    // 发送rpc请求)
    if(options.timeout_ms != 0){
        std::weak_ptr<ChannelCore> weak_self = shared_from_this();

        double timeout_seconds = static_cast<double>(options.timeout_ms) / 1000.0;

        muduo::net::TimerId timer_id = loop_->runAfter(
            timeout_seconds,
            [weak_self, request_id](){
                if(auto self = weak_self.lock()){
                    RpcCallResult timeout_result;
                    timeout_result.request_id = request_id;
                    timeout_result.status_code = mprpc::MprpcErrorCode::TIMEOUT;
                    timeout_result.error_msg = "RPC call timeout!";

                    self->CompleteCall(request_id, std::move(timeout_result));
                }
            });

        std::lock_guard<std::mutex> lock(state->mutex);
        state->timer_id = timer_id;
        state->has_timer = true;
    }

    OutboundFrame outbound;
    outbound.request_id = request_id;
    outbound.message_type = mprpc::MprpcMessageType::REQUEST;
    outbound.bytes = std::move(request_frame);
    outbound.state = state;
    SendFrame(endpoint, std::move(outbound));

    return state;
}
RpcCallResult ChannelCore::WaitCall(const CallHandle& state){
    if(!state){
        RpcCallResult result;
        result.status_code = mprpc::MprpcErrorCode::INTERNAL_ERROR;
        result.error_msg = "Invalid call state!";
        return result;
    }

    std::unique_lock<std::mutex> lock(state->mutex);
    state->cv.wait(
        lock,
        [&state](){
            return state->completed;
        }
    );
    return state->result;
}

bool ChannelCore::CancelCall(uint64_t request_id)
{
    RpcCallResult result;
    result.request_id = request_id;
    result.status_code = mprpc::MprpcErrorCode::CANCELLED;
    result.error_msg = "RPC call cancelled";
    return CompleteCall(request_id, std::move(result));
}

bool ChannelCore::IsInIoThread() const
{
    return loop_ != nullptr && loop_->isInLoopThread();
}

RpcMetricsSnapshot ChannelCore::GetMetricsSnapshot() const
{
    return metrics_.Snapshot();
}

bool ChannelCore::CompleteCall(uint64_t request_id, RpcCallResult result)
{
    std::shared_ptr<CallState> state;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_calls_.find(request_id);
        if(it == pending_calls_.end()){
            return false;
        }
        state = it->second;
        
        // 响应、超时、取消和断线都在这里竞争一次完成权。
        CallPhase expected = CallPhase::Pending;
        if(!state->phase.compare_exchange_strong(expected, CallPhase::Completing)){
            return false;
        }

        pending_calls_.erase(it);
    }
    
    muduo::net::TimerId timer_id;
    bool cancel_timer = false;
    RpcCompletion completion;
    BoundedExecutor::Reservation callback_reservation;
    const mprpc::MprpcErrorCode completion_code = result.status_code;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->has_timer) {
            timer_id = state->timer_id;
            state->has_timer = false;
            cancel_timer = true;
        }
        state->result = std::move(result);
        state->completed = true;
        completion = std::move(state->completion);
        callback_reservation = std::move(state->callback_reservation);
    }

    if (cancel_timer && loop_ != nullptr) {
        loop_->cancel(timer_id);
    }

    state->phase.store(CallPhase::Completed, std::memory_order_release);

    metrics_.CallCompleted(
        state->method_name, completion_code,
        std::chrono::steady_clock::now() - state->started_at);

    state->cv.notify_all();

    if (completion_code == mprpc::MprpcErrorCode::TIMEOUT ||
        completion_code == mprpc::MprpcErrorCode::CANCELLED) {
        PropagateCancellation(state);
    }

    if(completion){
        // 执行用户回调时不能持有 pending 或 state 的锁。
        auto task = [state, completion = std::move(completion)]() mutable {
            completion(state->result);
        };
        if (!runtime_->SubmitCallback(
                std::move(callback_reservation), std::move(task))) {
            // 有 Reservation 却无法提交表示内部生命周期不变量被破坏。
            metrics_.Increment(RpcMetricEvent::CallbackRejected);
            std::terminate();
        }
    }

    return true;
}

void ChannelCore::FailCallsForEndpoint(const std::string& endpoint_key,
                                       mprpc::MprpcErrorCode code,
                                       const std::string& message)
{
    std::vector<uint64_t> request_ids;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (const auto& entry : pending_calls_) {
            if (entry.second->endpoint_key == endpoint_key) {
                request_ids.push_back(entry.first);
            }
        }
    }

    for (uint64_t request_id : request_ids) {
        RpcCallResult result;
        result.request_id = request_id;
        result.status_code = code;
        result.error_msg = message;
        CompleteCall(request_id, std::move(result));
    }
}

void ChannelCore::FailAllPending(mprpc::MprpcErrorCode code,
                                 const std::string& message)
{
    std::vector<uint64_t> request_ids;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        request_ids.reserve(pending_calls_.size());
        for (const auto& entry : pending_calls_) {
            request_ids.push_back(entry.first);
        }
    }

    for (uint64_t request_id : request_ids) {
        RpcCallResult result;
        result.request_id = request_id;
        result.status_code = code;
        result.error_msg = message;
        CompleteCall(request_id, std::move(result));
    }
}

void ChannelCore::Shutdown()
{
    bool expected = false;
    if (!shutting_down_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }

    FailAllPending(mprpc::MprpcErrorCode::CHANNEL_CLOSED,
                   "RPC channel closed");

    if (loop_ != nullptr) {
        if (loop_->isInLoopThread()) {
            for (auto& entry : sessions_) {
                entry.second->client->disconnect();
                entry.second->client->stop();
            }
            sessions_.clear();
        } else {
            auto stopped = std::make_shared<std::promise<void>>();
            std::future<void> done = stopped->get_future();
            loop_->runInLoop([this, stopped] {
                for (auto& entry : sessions_) {
                    entry.second->client->disconnect();
                    entry.second->client->stop();
                }
                sessions_.clear();
                stopped->set_value();
            });
            done.wait();
        }
    }
}
void ChannelCore::SendFrame(Endpoint endpoint, OutboundFrame frame)
{
    if (shutting_down_.load(std::memory_order_acquire)) {
        return;
    }
    // 排队任务可能晚于当前函数执行，因此捕获 shared self。
    auto self = shared_from_this();

    loop_->runInLoop([self, 
                     endpoint = std::move(endpoint), 
                     frame = std::move(frame)]() mutable {
                        self->SendFrameInLoop(std::move(endpoint), std::move(frame));
                     });
}

void ChannelCore::SendFrameInLoop(Endpoint endpoint, OutboundFrame frame)
{
    loop_->assertInLoopThread();

    if (frame.message_type == mprpc::MprpcMessageType::REQUEST) {
        const auto state = frame.state.lock();
        if (!state || state->phase.load(std::memory_order_acquire) !=
                          CallPhase::Pending) {
            return;
        }
    }

    ClientSession* session = GetOrCreateSession(endpoint);
    auto conn = session->client->connection();

    if(conn && conn->connected()){
        conn->send(frame.bytes);
        if (auto state = frame.state.lock()) {
            state->request_sent.store(true, std::memory_order_release);
        }
        return;
    }

    session->waiting_frames.push_back(std::move(frame));

    if(!session->connecting){
        session->connecting = true;
        session->client->connect();
    }

}

void ChannelCore::PropagateCancellation(
    const std::shared_ptr<CallState>& state)
{
    if (!state || state->endpoint_key.empty() || loop_ == nullptr ||
        shutting_down_.load(std::memory_order_acquire)) {
        return;
    }

    const bool request_was_sent =
        state->request_sent.load(std::memory_order_acquire);
    std::weak_ptr<ChannelCore> weak_self = shared_from_this();
    loop_->runInLoop(
        [weak_self, endpoint_key = state->endpoint_key,
         request_id = state->request_id, request_was_sent] {
            if (auto self = weak_self.lock()) {
                self->PropagateCancellationInLoop(
                    endpoint_key, request_id, request_was_sent);
            }
        });
}

void ChannelCore::PropagateCancellationInLoop(
    const std::string& endpoint_key,
    uint64_t request_id,
    bool request_was_sent)
{
    loop_->assertInLoopThread();
    const auto it = sessions_.find(endpoint_key);
    if (it == sessions_.end()) {
        return;
    }

    ClientSession& session = *it->second;
    bool removed_waiting_request = false;
    for (auto frame = session.waiting_frames.begin();
         frame != session.waiting_frames.end();) {
        if (frame->message_type == mprpc::MprpcMessageType::REQUEST &&
            frame->request_id == request_id) {
            frame = session.waiting_frames.erase(frame);
            removed_waiting_request = true;
        } else {
            ++frame;
        }
    }

    if (removed_waiting_request && !request_was_sent) {
        return;
    }

    const auto conn = session.client->connection();
    if (conn && conn->connected()) {
        conn->send(BuildCancelFrame(request_id));
    }
}

std::string ChannelCore::BuildCancelFrame(uint64_t request_id)
{
    mprpc::MprpcHeader header;
    header.request_id = request_id;
    header.message_type = mprpc::MprpcMessageType::CANCEL;
    header.status_code = mprpc::MprpcErrorCode::CANCELLED;
    return mprpc::MprpcCodec::Encode(header, {});
}

ChannelCore::ClientSession* ChannelCore::GetOrCreateSession(const Endpoint& endpoint)
{
    loop_->assertInLoopThread();

    const std::string endpoint_key = endpoint.Key();

    auto it = sessions_.find(endpoint_key);
    if(it != sessions_.end()){
        return it->second.get();
    }

    // 如果会话不存在，则创建一个新的session
    auto session = std::make_unique<ClientSession>();
    muduo::net::InetAddress server_addr(endpoint.ip, endpoint.port);
    
    session->client = std::make_unique<muduo::net::TcpClient>(loop_, server_addr, "MprpcClient" + endpoint_key);
    
    std::weak_ptr<ChannelCore> weak_self = shared_from_this();
    session->client->setConnectionCallback(
        [weak_self, endpoint_key](
            const muduo::net::TcpConnectionPtr& conn){
                if(auto self = weak_self.lock()){
                    self->OnConnection(endpoint_key, conn);
                }
        });
    session->client->setMessageCallback(
        [weak_self](const muduo::net::TcpConnectionPtr& conn,
                    muduo::net::Buffer* buffer,
                    muduo::Timestamp receive_time){
            if(auto self = weak_self.lock()){
                self->OnMessage(conn, buffer, receive_time);
            }                   
    });

    ClientSession* result = session.get();
    sessions_.emplace(endpoint_key, std::move(session));
    return result;
}

void ChannelCore::OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer* buffer, muduo::Timestamp receive_time){
    loop_->assertInLoopThread();

    while(buffer->readableBytes() > 0){
        std::string input(buffer->peek(), buffer->readableBytes());

        mprpc::MprpcFrame response_frame;
        size_t bytes_consumed = 0;
        mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::Decode(input, &response_frame, &bytes_consumed);

        if(decode_status == mprpc::DecodeStatus::NEED_MORE_DATA){
            break;
        }
        if(decode_status != mprpc::DecodeStatus::OK){
            conn->shutdown();
            return;
        }

        buffer->retrieve(bytes_consumed);

        RpcCallResult result = ParseResponseFrame(response_frame);

        CompleteCall(response_frame.header.request_id, std::move(result));
    }
}
void ChannelCore::OnConnection(const std::string& endpoint_key, const muduo::net::TcpConnectionPtr& conn)
{
    loop_->assertInLoopThread();
    auto it = sessions_.find(endpoint_key);
    if(it == sessions_.end()){
        return;
    }

    ClientSession& session = *it->second;
    session.connecting = false;

    if(!conn || !conn->connected()){
        session.waiting_frames.clear();
        InvalidateEndpoint(endpoint_key);
        FailCallsForEndpoint(endpoint_key,
                             mprpc::MprpcErrorCode::CONNECTION_CLOSED,
                             "RPC connection closed");
        RetireDisconnectedSession(endpoint_key);
        return;
    }

    while(!session.waiting_frames.empty()){
        OutboundFrame frame = std::move(session.waiting_frames.front());
        session.waiting_frames.pop_front();
        if (frame.message_type == mprpc::MprpcMessageType::REQUEST) {
            const auto state = frame.state.lock();
            if (!state || state->phase.load(std::memory_order_acquire) !=
                              CallPhase::Pending) {
                continue;
            }
            state->request_sent.store(true, std::memory_order_release);
        }
        conn->send(frame.bytes);
    }
}

void ChannelCore::RetireDisconnectedSession(
    const std::string& endpoint_key)
{
    if (endpoint_key.empty() || loop_ == nullptr ||
        shutting_down_.load(std::memory_order_acquire)) {
        return;
    }

    std::weak_ptr<ChannelCore> weak_self = shared_from_this();
    // 等当前 TcpClient 回调退出后，再延迟销毁 session。
    loop_->queueInLoop([weak_self, endpoint_key] {
        auto self = weak_self.lock();
        if (!self || self->shutting_down_.load(std::memory_order_acquire)) {
            return;
        }

        auto it = self->sessions_.find(endpoint_key);
        if (it == self->sessions_.end()) {
            return;
        }

        const auto connection = it->second->client->connection();
        if (connection && connection->connected()) {
            return;
        }

        it->second->client->stop();
        self->sessions_.erase(it);
    });
}

bool ChannelCore::BuildRequestFrame(const std::string& service_name,
                            const std::string& method_name,
                            const std::string& request_payload,
                            uint64_t request_id,
                            uint32_t timeout_ms,
                            std::string* request_frame,
                            std::string* error_msg)
{
    // 定义请求体meta
    mprpc::MprpcRequestMeta request_meta;
    request_meta.set_service_name(service_name);
    request_meta.set_method_name(method_name);
    request_meta.set_timeout_ms(timeout_ms);

    std::string meta;
    if(!request_meta.SerializeToString(&meta)){
        *error_msg = "MprpcRequestMeta serialize request error!";
        return false;
    }
    std::string body = mprpc::MprpcCodec::EncodeBody(meta, request_payload);

    // 定义rpc请求头
    mprpc::MprpcHeader header;
    header.request_id = request_id;
    header.message_type = mprpc::MprpcMessageType::REQUEST;
    header.status_code = mprpc::MprpcErrorCode::OK;
    header.checksum = 0;

    *request_frame = mprpc::MprpcCodec::Encode(header, body);

    return true;
}

RpcCallResult ChannelCore::ParseResponseFrame(const mprpc::MprpcFrame& response_frame)
{
    RpcCallResult result;
    result.request_id = response_frame.header.request_id;

    if(response_frame.header.message_type != mprpc::MprpcMessageType::RESPONSE){
        result.status_code = mprpc::MprpcErrorCode::BAD_FRAME;
        result.error_msg = "Response message type error!";
        return result;
    }

    // 反序列化rpc调用的相应数据
    mprpc::MprpcBody response_body;
    mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::DecodeBody(response_frame.body, &response_body);

    if(decode_status != mprpc::DecodeStatus::OK){
        result.status_code = mprpc::MprpcErrorCode::DECODE_FAILED;
        result.error_msg = "Decode response body error!";
        return result;
    }

    // 反序列化响应meta
    mprpc::MprpcResponseMeta response_meta;
    if(!response_meta.ParseFromString(response_body.meta)){
        result.status_code = mprpc::MprpcErrorCode::PARSE_ERROR;
        result.error_msg = "Response meta parsing error!";
        return result;
    }

    // 组装返回的响应数据
    result.status_code = response_frame.header.status_code;
    result.error_msg = response_meta.error_msg();
    result.response_payload = std::move(response_body.payload);
    if(result.status_code != mprpc::MprpcErrorCode::OK && result.error_msg.empty()){
        result.error_msg = "RPC response status error!";
    }
    return result;
}
mprpc::MprpcErrorCode ChannelCore::GetEndpoint(const std::string& service_name,
                    const std::string& method_name,
                    const std::string& affinity_key,
                    Endpoint* endpoint,
                    std::string* error_msg)
{
    std::string method_path = "/" + service_name + "/" + method_name;
    const std::string providers_path = method_path + "/providers";
    const auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(discovery_mutex_);
    if (zk_client_ && !zk_client_->IsConnected()) {
        endpoint_cache_.clear();
    }
    auto cached = endpoint_cache_.find(method_path);
    if (cached != endpoint_cache_.end() &&
        cached->second.expires_at > now &&
        !cached->second.endpoints.empty()) {
        EndpointCacheEntry& entry = cached->second;
        if (affinity_key.empty()) {
            *endpoint = entry.endpoints[
                entry.next_index % entry.endpoints.size()];
            ++entry.next_index;
        } else {
            uint64_t hash = 1469598103934665603ULL;
            for (unsigned char c : affinity_key) {
                hash ^= c;
                hash *= 1099511628211ULL;
            }
            *endpoint = entry.endpoints[hash % entry.endpoints.size()];
        }
        return mprpc::MprpcErrorCode::OK;
    }

    if (!zk_client_) {
        zk_client_ = std::make_unique<ZkClient>();
    }
    if (!zk_client_->Start()) {
        *error_msg = "ZooKeeper is unavailable";
        return mprpc::MprpcErrorCode::NETWORK_ERROR;
    }

    std::vector<Endpoint> endpoints;
    const std::vector<std::string> providers =
        zk_client_->GetChildren(providers_path);
    bool invalid_address = false;
    for (const std::string& provider : providers) {
        const std::string host_data =
            zk_client_->GetData((providers_path + "/" + provider).c_str());
        const std::size_t separator = host_data.rfind(':');
        if (separator == std::string::npos || separator == 0 ||
            separator + 1 >= host_data.size()) {
            invalid_address = true;
            continue;
        }

        uint32_t port = 0;
        const std::string port_text = host_data.substr(separator + 1);
        const char* begin = port_text.data();
        const char* end = begin + port_text.size();
        auto [ptr, ec] = std::from_chars(begin, end, port);
        if (ec != std::errc{} || ptr != end || port == 0 || port > 65535) {
            invalid_address = true;
            continue;
        }

        endpoints.push_back(
            Endpoint{host_data.substr(0, separator),
                     static_cast<uint16_t>(port)});
    }

    if (endpoints.empty()) {
        *error_msg = invalid_address
            ? method_path + " has no valid provider address"
            : method_path + " has no available provider";
        return invalid_address
            ? mprpc::MprpcErrorCode::INVALID_ADDRESS
            : mprpc::MprpcErrorCode::SERVICE_NOT_FOUND;
    }

    std::sort(endpoints.begin(), endpoints.end(),
              [](const Endpoint& left, const Endpoint& right) {
                  return left.Key() < right.Key();
              });
    // Provider 快速重启时，同一地址可能短暂存在两个 ZK 节点。
    endpoints.erase(
        std::unique(endpoints.begin(), endpoints.end(),
                    [](const Endpoint& left, const Endpoint& right) {
                        return left.ip == right.ip &&
                               left.port == right.port;
                    }),
        endpoints.end());
    EndpointCacheEntry entry;
    entry.endpoints = std::move(endpoints);
    entry.next_index = 1;
    entry.expires_at = now + std::chrono::seconds(3);
    if (affinity_key.empty()) {
        *endpoint = entry.endpoints.front();
    } else {
        uint64_t hash = 1469598103934665603ULL;
        for (unsigned char c : affinity_key) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        *endpoint = entry.endpoints[hash % entry.endpoints.size()];
    }
    endpoint_cache_[method_path] = std::move(entry);
    return mprpc::MprpcErrorCode::OK;
}

void ChannelCore::InvalidateEndpoint(const std::string& endpoint_key)
{
    std::lock_guard<std::mutex> lock(discovery_mutex_);
    for (auto it = endpoint_cache_.begin(); it != endpoint_cache_.end();) {
        auto& endpoints = it->second.endpoints;
        endpoints.erase(
            std::remove_if(endpoints.begin(), endpoints.end(),
                           [&endpoint_key](const Endpoint& endpoint) {
                               return endpoint.Key() == endpoint_key;
                           }),
            endpoints.end());
        if (endpoints.empty()) {
            it = endpoint_cache_.erase(it);
        } else {
            it->second.expires_at = std::chrono::steady_clock::now();
            ++it;
        }
    }
}
