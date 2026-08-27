#include "rpcprovider.h"
#include <algorithm>
#include <string>
#include "mprpcapplication.h"
#include <functional>
#include <utility>
#include <google/protobuf/descriptor.h>
#include <memory>
#include "proto/rpc_meta.pb.h"
#include "mprpccodec.h"
#include "logger.h"

namespace
{
std::size_t LoadPositiveSetting(const std::string& key,
                                std::size_t fallback)
{
    const std::string text =
        MprpcApplication::GetInstance().GetConfig().Load(key);
    if (text.empty()) {
        return fallback;
    }
    try {
        const unsigned long long parsed = std::stoull(text);
        return parsed == 0 ? fallback : static_cast<std::size_t>(parsed);
    } catch (...) {
        return fallback;
    }
}
} // namespace

RpcProvider::RpcProvider()
    : RpcProvider(
          LoadPositiveSetting("rpcproviderbusinessworkers", 4),
          LoadPositiveSetting("rpcproviderbusinesscapacity", 64),
          static_cast<int>(
              LoadPositiveSetting("rpcprovideriothreads", 4)))
{
}

RpcProvider::RpcProvider(std::size_t business_threads,
                         std::size_t max_business_outstanding,
                         int io_threads)
    : business_executor_(std::max<std::size_t>(1, business_threads),
                         std::max<std::size_t>(1,
                                               max_business_outstanding)),
      io_thread_count_(std::max(1, io_threads))
{
}

std::size_t RpcProvider::ActiveCallKeyHash::operator()(
    const ActiveCallKey& key) const noexcept
{
    const std::size_t connection_hash =
        std::hash<std::string>{}(key.connection_name);
    const std::size_t request_hash =
        std::hash<uint64_t>{}(key.request_id);
    return connection_hash ^ (request_hash + 0x9e3779b9U +
                              (connection_hash << 6U) +
                              (connection_hash >> 2U));
}

RpcMetricsSnapshot RpcProvider::GetMetricsSnapshot() const
{
    return metrics_.Snapshot();
}
/*
service_name => service描述；
service描述 
    => service* 记录服务对象
        - method_name => method方法对象；


*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
void RpcProvider::NotifyService(google::protobuf::Service *service){
    RpcProvider::ServiceInfo service_info;

    service_info.m_service = service;
    // 获取服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象service方法的数量
    int methodCnt = pserviceDesc->method_count();

    // std::cout << "service_name: " << service_name << std::endl;
    LOG_INFO("service_name: %s", service_name.data());
    for(int i = 0; i < methodCnt; i++){
        // 获取了服务对象指定下标的服务方法的描述（抽象描述）
        const google::protobuf::MethodDescriptor* pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        // std::cout << "method_name: " << method_name << std::endl;
        LOG_INFO("method_name: %s", method_name.data());
        service_info.m_methodMap.insert({method_name, pmethodDesc});
    }
    m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程嗲用服务
void RpcProvider::Run(){
    std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    muduo::net::InetAddress address(ip, port);

    // 创建TcpServer对象
    muduo::net::TcpServer server(&m_eventLoop, address, "RpcProvider");
    // 绑定连接回调和消息读写回调方法 分离网络代码和业务代码
    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, 
            std::placeholders::_2, std::placeholders::_3));
    //设置muduo库的线程数量
    server.setThreadNum(io_thread_count_);

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc client可以从zk上发现服务
    // session timeout 30s   zkclient API 网络IO线程 1/3 * timeout 时间发送ping消息（心跳消息）   
    ZkClient zkCli;
    if (!zkCli.Start()) {
        std::cerr << "failed to connect to ZooKeeper" << std::endl;
        return;
    }
    const std::string endpoint = ip + ":" + std::to_string(port);
    for(auto &sp : m_serviceMap){
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.data(), nullptr,0, 0);
        for(auto &mp: sp.second.m_methodMap){
            std::string method_path = service_path + "/" + mp.first;
            zkCli.Create(method_path.data(), nullptr, 0, 0);
            std::string providers_path = method_path + "/providers";
            zkCli.Create(providers_path.data(), nullptr, 0, 0);
            const std::string provider_path =
                zkCli.CreateEphemeralSequential(
                    providers_path + "/provider-", endpoint);
            if (provider_path.empty()) {
                std::cerr << "failed to register " << method_path << std::endl;
                return;
            }
        }
    }
    std::cout << "[RpcProvider] start service at ip: " << ip << " port: " << port << std::endl;
    // 启动网络服务
    server.start();
    m_eventLoop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr& conn){
    if(!conn->connected()){
        CancelCallsForConnection(conn);
    }
}

/*
在框架内部， RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型 （protobuf)
service_name method_name args  定义proto的message类型，进行数据头的序列化和反序列化。

*/
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
        muduo::net::Buffer *buffer, muduo::Timestamp time)
{    
    // 一个 Buffer 可能包含半帧，也可能同时包含多个完整帧。
    while(buffer->readableBytes() > 0){
        std::string input(buffer->peek(), buffer->readableBytes());

        mprpc::MprpcFrame rpc_frame;
        size_t bytes_consumed = 0;
        mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::Decode(input, &rpc_frame, &bytes_consumed);  
        if(decode_status == mprpc::DecodeStatus::OK){
            buffer->retrieve(bytes_consumed);
            if (rpc_frame.header.message_type ==
                mprpc::MprpcMessageType::CANCEL) {
                HandleCancelFrame(conn, rpc_frame);
            } else if (rpc_frame.header.message_type ==
                       mprpc::MprpcMessageType::REQUEST) {
                HandleRpcFrame(conn, rpc_frame);
            } else {
                conn->shutdown();
                return;
            }
            continue;
        }

        if(decode_status == mprpc::DecodeStatus::NEED_MORE_DATA) break;

        std::cout << "Decode rpc frame error" << std::endl;
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        conn->shutdown();
        break;
    }
}

void RpcProvider::OnBusinessDone(
    std::shared_ptr<RpcResponseContext> context)
{
    if (!context || !context->connection) {
        return;
    }
    context->dispatch_state.Finish();
    auto* loop = context->connection->getLoop();
    loop->queueInLoop([this, context = std::move(context)] {
        if (context->controller->Failed()) {
            CompleteServerCallInLoop(
                context, context->controller->ErrorCode(),
                context->controller->ErrorText(), true);
        } else {
            CompleteServerCallInLoop(
                context, mprpc::MprpcErrorCode::OK, {}, true);
        }
    });
}

void RpcProvider::CompleteServerCall(
    const std::shared_ptr<RpcResponseContext>& context,
    mprpc::MprpcErrorCode code,
    std::string message,
    bool send_response)
{
    if (!context || !context->connection) {
        return;
    }
    auto* loop = context->connection->getLoop();
    loop->runInLoop(
        [this, context, code, message = std::move(message),
         send_response]() mutable {
            CompleteServerCallInLoop(
                context, code, message, send_response);
        });
}

void RpcProvider::CompleteServerCallInLoop(
    const std::shared_ptr<RpcResponseContext>& context,
    mprpc::MprpcErrorCode code,
    const std::string& message,
    bool send_response)
{
    context->connection->getLoop()->assertInLoopThread();
    bool expected = false;
    if (!context->response_sent.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }

    if (context->has_timer) {
        context->connection->getLoop()->cancel(context->timer_id);
        context->has_timer = false;
    }

    {
        std::lock_guard<std::mutex> lock(active_calls_mutex_);
        active_calls_.erase(
            ActiveCallKey{context->connection_name,
                          context->request_id});
    }

    const auto elapsed =
        std::chrono::steady_clock::now() - context->started_at;

    if (!send_response || !context->connection->connected()) {
        metrics_.CallCompleted(context->method_name, code, elapsed);
        return;
    }

    if (code != mprpc::MprpcErrorCode::OK) {
        metrics_.CallCompleted(context->method_name, code, elapsed);
        SendRpcErrorResponse(context->connection, context->request_id,
                             code, message);
        return;
    }

    std::string response_payload;
    if (!context->response->SerializeToString(&response_payload)) {
        metrics_.CallCompleted(
            context->method_name,
            mprpc::MprpcErrorCode::SERIALIZE_FAILED, elapsed);
        SendRpcErrorResponse(
            context->connection, context->request_id,
            mprpc::MprpcErrorCode::SERIALIZE_FAILED,
            "Serialize RPC response failed");
        return;
    }

    mprpc::MprpcResponseMeta response_meta;
    std::string meta;
    if (!response_meta.SerializeToString(&meta)) {
        metrics_.CallCompleted(
            context->method_name,
            mprpc::MprpcErrorCode::SERIALIZE_FAILED, elapsed);
        context->connection->shutdown();
        return;
    }

    const std::string body =
        mprpc::MprpcCodec::EncodeBody(meta, response_payload);
    mprpc::MprpcHeader header;
    header.request_id = context->request_id;
    header.message_type = mprpc::MprpcMessageType::RESPONSE;
    header.status_code = mprpc::MprpcErrorCode::OK;
    metrics_.CallCompleted(
        context->method_name, mprpc::MprpcErrorCode::OK, elapsed);
    context->connection->send(mprpc::MprpcCodec::Encode(header, body));
}

void RpcProvider::HandleCancelFrame(
    const muduo::net::TcpConnectionPtr& conn,
    const mprpc::MprpcFrame& frame)
{
    if (!frame.body.empty()) {
        conn->shutdown();
        return;
    }

    std::shared_ptr<RpcResponseContext> context;
    {
        std::lock_guard<std::mutex> lock(active_calls_mutex_);
        const auto it = active_calls_.find(
            ActiveCallKey{conn->name(), frame.header.request_id});
        if (it == active_calls_.end()) {
            return;
        }
        context = it->second;
    }

    context->controller->StartCancel();
    context->dispatch_state.CancelQueued();
    metrics_.Increment(
        RpcMetricEvent::ClientCancel, 1, context->method_name);
    CompleteServerCallInLoop(
        context, mprpc::MprpcErrorCode::CANCELLED,
        "RPC call cancelled by client", true);
}

void RpcProvider::CancelCallsForConnection(
    const muduo::net::TcpConnectionPtr& conn)
{
    std::vector<std::shared_ptr<RpcResponseContext>> calls;
    {
        std::lock_guard<std::mutex> lock(active_calls_mutex_);
        for (const auto& entry : active_calls_) {
            if (entry.first.connection_name == conn->name()) {
                calls.push_back(entry.second);
            }
        }
    }

    for (const auto& context : calls) {
        context->controller->StartCancel();
        context->dispatch_state.CancelQueued();
        metrics_.Increment(
            RpcMetricEvent::Disconnect, 1, context->method_name);
        CompleteServerCallInLoop(
            context, mprpc::MprpcErrorCode::CONNECTION_CLOSED,
            "RPC client connection closed", false);
    }
}

void RpcProvider::SendRpcErrorResponse(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::MprpcErrorCode error_code, const std::string& err_msg){
    mprpc::MprpcResponseMeta meta_response;
    meta_response.set_error_msg(err_msg);
    std::string meta;
    if(!meta_response.SerializeToString(&meta)){
        std::cout << "Serialize error response meta failed!" << std::endl;
        conn->shutdown();
        return;
    }

    std::string body = mprpc::MprpcCodec::EncodeBody(meta, "");

    mprpc::MprpcHeader header;
    header.request_id = request_id;
    header.status_code = error_code;
    header.message_type = mprpc::MprpcMessageType::RESPONSE;
    header.checksum = 0;

    std::string frame = mprpc::MprpcCodec::Encode(header, body);
    conn->send(frame);
}

void RpcProvider::HandleRpcFrame(const muduo::net::TcpConnectionPtr& conn, const mprpc::MprpcFrame& frame){

    mprpc::MprpcBody rpc_body;
    mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::DecodeBody(frame.body, &rpc_body);
    if(decode_status != mprpc::DecodeStatus::OK){
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::BAD_FRAME, "Decode rpc body error!");
        return;
    }

    mprpc::MprpcRequestMeta rpc_meta;
    if(!rpc_meta.ParseFromString(rpc_body.meta)){
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::PARSE_ERROR, "Parse rpc meta error!");
        return;
    }

    const std::string& service_name = rpc_meta.service_name();
    const std::string& method_name = rpc_meta.method_name();
    const uint32_t timeout_ms = rpc_meta.timeout_ms();
    const std::string& payload = rpc_body.payload;

    auto it = m_serviceMap.find(service_name);
    if(it == m_serviceMap.end()){
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::SERVICE_NOT_FOUND, service_name + " does not exist!");
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if(mit == it->second.m_methodMap.end()){
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::METHOD_NOT_FOUND, method_name + " does not exist!");
        return;
    }

    google::protobuf::Service *service = it->second.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());
    if(!request->ParseFromString(payload)){
        metrics_.Increment(RpcMetricEvent::FrameworkError);
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::PARSE_ERROR, "Parse request payload error!");
        return;
    }

    auto context = std::make_shared<RpcResponseContext>();
    context->request = std::move(request);
    context->response.reset(service->GetResponsePrototype(method).New());
    context->controller = std::make_shared<MprpcController>();
    context->connection = conn;
    context->connection_name = conn->name();
    context->method_name = service_name + "/" + method_name;
    context->request_id = frame.header.request_id;
    context->started_at = std::chrono::steady_clock::now();
    metrics_.CallStarted(context->method_name);

    const ActiveCallKey key{context->connection_name,
                            context->request_id};
    {
        std::lock_guard<std::mutex> lock(active_calls_mutex_);
        if (!active_calls_.emplace(key, context).second) {
            metrics_.CallCompleted(
                context->method_name, mprpc::MprpcErrorCode::BAD_FRAME,
                std::chrono::steady_clock::now() - context->started_at);
            SendRpcErrorResponse(
                conn, frame.header.request_id,
                mprpc::MprpcErrorCode::BAD_FRAME,
                "Duplicate active request id on connection");
            return;
        }
    }

    if (timeout_ms != 0) {
        const double timeout_seconds =
            static_cast<double>(timeout_ms) / 1000.0;
        std::weak_ptr<RpcResponseContext> weak_context = context;
        context->timer_id = conn->getLoop()->runAfter(
            timeout_seconds, [this, weak_context] {
                if (auto active = weak_context.lock()) {
                    active->controller->StartCancel();
                    active->dispatch_state.CancelQueued();
                    metrics_.Increment(
                        RpcMetricEvent::DeadlineExceeded, 1,
                        active->method_name);
                    CompleteServerCallInLoop(
                        active, mprpc::MprpcErrorCode::TIMEOUT,
                        "RPC server deadline exceeded", true);
                }
            });
        context->has_timer = true;
    }

    auto business_task = [this, context, service, method] {
        if (!context->dispatch_state.TryStart()) {
            return;
        }
        if (context->controller->IsCanceled()) {
            context->dispatch_state.Finish();
            return;
        }

        google::protobuf::Closure* done =
            google::protobuf::NewCallback<
                RpcProvider, std::shared_ptr<RpcResponseContext>>(
                this, &RpcProvider::OnBusinessDone, context);

        service->CallMethod(
            method, context->controller.get(), context->request.get(),
            context->response.get(), done);
    };

    if (!business_executor_.TrySubmit(std::move(business_task))) {
        context->dispatch_state.CancelQueued();
        metrics_.Increment(
            RpcMetricEvent::QueueRejected, 1, context->method_name);
        CompleteServerCallInLoop(
            context, mprpc::MprpcErrorCode::SERVER_BUSY,
            "RPC business queue is full", true);
    }
}
