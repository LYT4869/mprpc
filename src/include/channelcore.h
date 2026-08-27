#pragma once

#include <cstdint>
#include <string>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <functional>
#include <memory>
#include <unordered_map>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpClient.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/TimerId.h>
#include <muduo/base/Timestamp.h>
#include <deque>
#include <vector>
#include "boundedexecutor.h"
#include "mprpccodec.h"
#include "rpcmetrics.h"

class ZkClient;
class RpcClientRuntime;

// RPC 核心层产出的字节级调用结果，不依赖 Protobuf 消息类型。
struct RpcCallResult
{
    uint64_t request_id = 0;

    mprpc::MprpcErrorCode status_code = mprpc::MprpcErrorCode::OK;

    std::string error_msg;
    std::string response_payload;

    bool Ok() const noexcept
    {
        return status_code == mprpc::MprpcErrorCode::OK && error_msg.empty();
    }
};
using RpcCompletion = std::function<void(const RpcCallResult&)>;

// 单次调用选项；affinity_key 用于有状态请求的稳定路由。
struct CallOptions
{
    uint32_t timeout_ms = 3000;
    std::string affinity_key;
};

enum class CallPhase
{
    Pending,
    Completing,
    Completed
};


// 记录一次在途调用的完成状态，并为同步调用提供等待条件。
struct CallState
{
    uint64_t request_id = 0;
    std::string method_name;
    std::chrono::steady_clock::time_point started_at =
        std::chrono::steady_clock::now();
    // 用于只终止受某条断开连接影响的调用。
    std::string endpoint_key;
    RpcCallResult result;
    std::atomic<CallPhase> phase{CallPhase::Pending};

    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    bool has_timer = false;
    std::atomic<bool> request_sent{false};
    muduo::net::TimerId timer_id;

    RpcCompletion completion;
    BoundedExecutor::Reservation callback_reservation;
};

using CallHandle = std::shared_ptr<CallState>;
// 管理服务发现、长连接、pending calls、超时和响应分发。
class ChannelCore : public std::enable_shared_from_this<ChannelCore>
{
public:
    ChannelCore();
    explicit ChannelCore(std::shared_ptr<RpcClientRuntime> runtime);
    ~ChannelCore();

    ChannelCore(const ChannelCore&) = delete;
    ChannelCore& operator=(const ChannelCore&) = delete;

    // 创建并注册调用状态，随后异步选择连接并发送请求帧。
    CallHandle StartCall(const std::string& service_name,
                   const std::string& method_name,
                   const std::string& request_payload,
                   const CallOptions& options,
                   RpcCompletion completion = {});
    // 阻塞等待调用完成，仅供同步适配层使用。
    RpcCallResult WaitCall(const CallHandle& state);

    // 通过 request_id 取消仍处于 pending 的调用。
    bool CancelCall(uint64_t request_id);

    // 停止本 Core 的网络资源，并完成所有剩余调用。
    void Shutdown();
    bool IsInIoThread() const;
    RpcMetricsSnapshot GetMetricsSnapshot() const;

private:
    struct OutboundFrame
    {
        uint64_t request_id = 0;
        mprpc::MprpcMessageType message_type =
            mprpc::MprpcMessageType::REQUEST;
        std::string bytes;
        std::weak_ptr<CallState> state;
    };

    struct ClientSession
    {
        std::unique_ptr<muduo::net::TcpClient> client;
        // 异步连接建立前，请求帧暂存在这里。
        std::deque<OutboundFrame> waiting_frames;
        bool connecting = false;
    };
    struct Endpoint
    {
        std::string ip;
        uint16_t port = 0;

        std::string Key() const
        {
            return ip + ":" + std::to_string(port);
        }
    };
    struct EndpointCacheEntry
    {
        std::vector<Endpoint> endpoints;
        std::size_t next_index = 0;
        std::chrono::steady_clock::time_point expires_at;
    };

    std::atomic<uint64_t> next_request_id_{1};
    std::unordered_map<uint64_t, std::shared_ptr<CallState>> pending_calls_;
    std::mutex pending_mutex_;
    // Runtime 拥有线程；Core 只借用生命周期内固定不变的 loop_。
    std::shared_ptr<RpcClientRuntime> runtime_;
    muduo::net::EventLoop* loop_ = nullptr;
    // 仅允许在 loop_ 所在线程中增删和读取。
    std::unordered_map<std::string, std::unique_ptr<ClientSession>> sessions_;
    RpcMetrics metrics_;
    std::atomic<bool> shutting_down_{false};
    std::unique_ptr<ZkClient> zk_client_;
    std::mutex discovery_mutex_;
    std::unordered_map<std::string, EndpointCacheEntry> endpoint_cache_;

    void SendFrame(Endpoint endpoint, OutboundFrame frame);

    void SendFrameInLoop(Endpoint endpoint, OutboundFrame frame);

    void PropagateCancellation(const std::shared_ptr<CallState>& state);
    void PropagateCancellationInLoop(
        const std::string& endpoint_key,
        uint64_t request_id,
        bool request_was_sent);

    static std::string BuildCancelFrame(uint64_t request_id);

    ClientSession* GetOrCreateSession(const Endpoint& endpoint);

    void OnConnection(const std::string& endpoint_key, const muduo::net::TcpConnectionPtr& conn);

    // 在 EventLoop 中循环拆帧并按 request_id 分发响应。
    void OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer*, muduo::Timestamp receive_time);

    // 所有响应、超时、取消和断线共用的一次性完成入口。
    bool CompleteCall(uint64_t request_id, RpcCallResult result);

    void FailCallsForEndpoint(const std::string& endpoint_key,
                              mprpc::MprpcErrorCode code,
                              const std::string& message);
    void FailAllPending(mprpc::MprpcErrorCode code,
                        const std::string& message);
    void RetireDisconnectedSession(const std::string& endpoint_key);
    void InvalidateEndpoint(const std::string& endpoint_key);

    bool BuildRequestFrame(const std::string& service_name,
                            const std::string& method_name,
                            const std::string& request_payload,
                            uint64_t request_id,
                            uint32_t timeout_ms,
                            std::string* request_frame,
                            std::string* error_msg);

    RpcCallResult ParseResponseFrame(const mprpc::MprpcFrame& response_frame);

    // 从短期缓存或 ZooKeeper 选择一个可用服务节点。
    mprpc::MprpcErrorCode GetEndpoint(const std::string& service_name,
                        const std::string& method_name,
                        const std::string& affinity_key,
                        Endpoint* endpoint,
                        std::string* error_msg);     
};
