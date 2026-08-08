#include <charconv>
#include <muduo/net/InetAddress.h>

#include "zookeeperutil.h"
#include "channelcore.h"
#include "proto/rpc_meta.pb.h"

ChannelCore::ChannelCore() : loop_(io_thread_.startLoop()){}

CallHandle ChannelCore::StartCall(const std::string& service_name,
                const std::string& method_name,
                const std::string& request_payload,
                const CallOptions& options,
                RpcCompletion completion)
{
    RpcCallResult call_result;
    auto state = std::make_shared<CallState>();
    state->completion = std::move(completion);

    uint64_t request_id = next_request_id_.fetch_add(1);
    state->request_id = request_id;
    call_result.request_id = request_id;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_calls_[request_id] = state;
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
    mprpc::MprpcErrorCode status_code = GetEndpoint(service_name, method_name, &endpoint, &error_msg);
    if(status_code != mprpc::MprpcErrorCode::OK)
    {
        call_result.status_code =status_code;
        call_result.error_msg = std::move(error_msg);
        CompleteCall(request_id, std::move(call_result));
        return state;
    }
    // 发送rpc请求)
    if(options.timeout_ms != 0){
        std::weak_ptr<ChannelCore> weak_self = shared_from_this();

        double timeout_seconds = static_cast<double>(options.timeout_ms) / 1000.0;

        loop_->runAfter(
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
    }

    SendFrame(endpoint, std::move(request_frame));

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
        
        CallPhase expected = CallPhase::Pending;
        if(!state->phase.compare_exchange_strong(expected, CallPhase::Completing)){
            return false;
        }

        pending_calls_.erase(it);
    }
    
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->result = std::move(result);
        state->completed = true;
    }

    state->phase.store(CallPhase::Completed);

    state->cv.notify_all();

    if(state->completion){
        state->completion(state->result);
    }

    return true;
}
void ChannelCore::SendFrame(Endpoint endpoint, std::string frame)
{
    auto self = shared_from_this();

    loop_->runInLoop([self, 
                     endpoint = std::move(endpoint), 
                     frame = std::move(frame)]() mutable {
                        self->SendFrameInLoop(std::move(endpoint), std::move(frame));
                     });
}

void ChannelCore::SendFrameInLoop(Endpoint endpoint, std::string frame)
{
    loop_->assertInLoopThread();

    ClientSession* session = GetOrCreateSession(endpoint);
    auto conn = session->client->connection();

    if(conn && conn->connected()){
        conn->send(frame);
        return;
    }

    session->waiting_frames.push_back(std::move(frame));

    if(!session->connecting){
        session->connecting = true;
        session->client->connect();
    }

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
        return;
    }

    while(!session.waiting_frames.empty()){
        conn->send(session.waiting_frames.front());
        session.waiting_frames.pop_front();
    }
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
                    Endpoint* endpoint,
                    std::string* error_msg)
{
    // 在zk上查询服务所在的host信息
    ZkClient zkCli;
    zkCli.Start();
    std::string method_path = "/" + service_name + "/" + method_name;
    std::string host_data = zkCli.GetData(method_path.data());
    if(host_data.empty()){
        *error_msg = method_path + " does not exist!";
        return mprpc::MprpcErrorCode::SERVICE_NOT_FOUND;
    }

    const std::size_t separator = host_data.find(":");
    
    if(separator == std::string::npos ||
        separator == 0 || 
        separator + 1 >= host_data.size())
    {
        *error_msg = method_path + " address is invalid!";
        return mprpc::MprpcErrorCode::INVALID_ADDRESS;
    }

    const std::string ip = host_data.substr(0, separator);
    const std::string port_text = host_data.substr(separator + 1);

    uint32_t port = 0;

    const char* begin = port_text.data();
    const char* end = port_text.data() + port_text.size();

    auto [ptr, ec] = std::from_chars(begin, end, port);
    if(ec != std::errc{} ||
        ptr != end ||
        port == 0 ||
        port > 65535)
    {
        *error_msg = method_path + " port is invalid!";
        return mprpc::MprpcErrorCode::INVALID_ADDRESS;
    }

    endpoint->ip = ip;
    endpoint->port = static_cast<uint16_t>(port);
    return mprpc::MprpcErrorCode::OK;
}
