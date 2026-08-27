#pragma once
#include <google/protobuf/service.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/TimerId.h>
#include "mprpccodec.h"
#include <google/protobuf/descriptor.h>
#include <unordered_map>
#include <string>
#include <vector>
#include <cstddef>
#include "boundedexecutor.h"
#include "mprpccontroller.h"
#include "rpcdispatchstate.h"
#include "rpcmetrics.h"
#include "zookeeperutil.h" 

// RPC 服务端入口，负责服务注册、请求拆帧、方法分发和响应发送。
class RpcProvider
{
public:
    RpcProvider();
    RpcProvider(std::size_t business_threads,
                std::size_t max_business_outstanding,
                int io_threads);
    // 注册一个 Protobuf Service 及其全部方法描述符。
    void NotifyService(google::protobuf::Service *service);

    // 注册 ZooKeeper 节点并启动 Muduo 服务循环。
    void Run();
    RpcMetricsSnapshot GetMetricsSnapshot() const;
private:
    muduo::net::EventLoop m_eventLoop;

    //service服务类型信息；
    struct ServiceInfo{
        google::protobuf::Service *m_service; //保存服务对象
        std::string name;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> m_methodMap; // 保存服务方法
    };
    // 存储注册成功的服务对象和其方法的所有信息；
    std::unordered_map<std::string, ServiceInfo> m_serviceMap;
    struct RpcResponseContext
    {
        std::unique_ptr<google::protobuf::Message> request;
        std::unique_ptr<google::protobuf::Message> response;
        std::shared_ptr<MprpcController> controller;
        muduo::net::TcpConnectionPtr connection;
        std::string connection_name;
        std::string method_name;
        uint64_t request_id;
        std::atomic<bool> response_sent{false};
        RpcDispatchState dispatch_state;
        bool has_timer = false;
        muduo::net::TimerId timer_id;
        std::chrono::steady_clock::time_point started_at =
            std::chrono::steady_clock::now();
    };

    struct ActiveCallKey
    {
        std::string connection_name;
        uint64_t request_id = 0;

        bool operator==(const ActiveCallKey& other) const noexcept
        {
            return connection_name == other.connection_name &&
                   request_id == other.request_id;
        }
    };

    struct ActiveCallKeyHash
    {
        std::size_t operator()(const ActiveCallKey& key) const noexcept;
    };

    std::mutex active_calls_mutex_;
    std::unordered_map<ActiveCallKey,
                       std::shared_ptr<RpcResponseContext>,
                       ActiveCallKeyHash> active_calls_;
    RpcMetrics metrics_;
    BoundedExecutor business_executor_;
    int io_thread_count_ = 4;
    // 处理连接状态变化。
    void OnConnection(const muduo::net::TcpConnectionPtr&);

    // 从连接 Buffer 中解析并分发所有完整请求帧。
    void OnMessage(const muduo::net::TcpConnectionPtr&, muduo::net::Buffer*, muduo::Timestamp);

    // 构造并发送框架错误响应。
    void SendRpcErrorResponse(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::MprpcErrorCode error_code, const std::string& err_msg);

    // 业务完成后回到连接所属 EventLoop 完成响应。
    void OnBusinessDone(std::shared_ptr<RpcResponseContext> context);

    void CompleteServerCall(
        const std::shared_ptr<RpcResponseContext>& context,
        mprpc::MprpcErrorCode code,
        std::string message,
        bool send_response = true);

    void CompleteServerCallInLoop(
        const std::shared_ptr<RpcResponseContext>& context,
        mprpc::MprpcErrorCode code,
        const std::string& message,
        bool send_response);

    void HandleCancelFrame(const muduo::net::TcpConnectionPtr& conn,
                           const mprpc::MprpcFrame& frame);

    void CancelCallsForConnection(
        const muduo::net::TcpConnectionPtr& conn);

    // 解析请求元数据并调用目标 Protobuf Service 方法。
    void HandleRpcFrame(const muduo::net::TcpConnectionPtr& conn, const mprpc::MprpcFrame& frame);
};
