#pragma once
#include <google/protobuf/service.h>
#include <memory>
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/Buffer.h>
#include "mprpccodec.h"
#include <google/protobuf/descriptor.h>
#include <unordered_map>
#include <string>
#include "zookeeperutil.h" 

// RPC 服务端入口，负责服务注册、请求拆帧、方法分发和响应发送。
class RpcProvider
{
public:
    // 注册一个 Protobuf Service 及其全部方法描述符。
    void NotifyService(google::protobuf::Service *service);

    // 注册 ZooKeeper 节点并启动 Muduo 服务循环。
    void Run();
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
        // 保证生成的 response 存活到业务调用 done。
        std::unique_ptr<google::protobuf::Message> response;
        uint64_t request_id;
    };
    // 处理连接状态变化。
    void OnConnection(const muduo::net::TcpConnectionPtr&);

    // 从连接 Buffer 中解析并分发所有完整请求帧。
    void OnMessage(const muduo::net::TcpConnectionPtr&, muduo::net::Buffer*, muduo::Timestamp);

    // 构造并发送框架错误响应。
    void SendRpcErrorResponse(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::MprpcErrorCode error_code, const std::string& err_msg);

    // 业务完成回调：序列化 response 并发送对应 request_id 的响应。
    void SendRpcResponse(muduo::net::TcpConnectionPtr, RpcResponseContext* context);

    // 解析请求元数据并调用目标 Protobuf Service 方法。
    void HandleRpcFrame(const muduo::net::TcpConnectionPtr& conn, const mprpc::MprpcFrame& frame);
};
