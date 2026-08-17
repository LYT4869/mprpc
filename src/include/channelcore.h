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
#include <muduo/net/EventLoopThread.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpClient.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/TimerId.h>
#include <muduo/base/Timestamp.h>
#include <deque>
#include <vector>
#include "boundedexecutor.h"
#include "mprpccodec.h"

class ZkClient;

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


struct CallState
{
    uint64_t request_id = 0;
    std::string endpoint_key;
    RpcCallResult result;
    std::atomic<CallPhase> phase{CallPhase::Pending};

    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    bool has_timer = false;
    muduo::net::TimerId timer_id;

    RpcCompletion completion;
    
};

using CallHandle = std::shared_ptr<CallState>;
class ChannelCore : public std::enable_shared_from_this<ChannelCore>
{
public:
    ChannelCore();
    ~ChannelCore();

    ChannelCore(const ChannelCore&) = delete;
    ChannelCore& operator=(const ChannelCore&) = delete;

    CallHandle StartCall(const std::string& service_name,
                   const std::string& method_name,
                   const std::string& request_payload,
                   const CallOptions& options,
                   RpcCompletion completion = {});
    RpcCallResult WaitCall(const CallHandle& state);
    bool CancelCall(uint64_t request_id);
    void Shutdown();
    bool IsInIoThread() const;

private:
    struct ClientSession
    {
        std::unique_ptr<muduo::net::TcpClient> client;
        std::deque<std::string> waiting_frames;
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
    muduo::net::EventLoopThread io_thread_;
    muduo::net::EventLoop* loop_ = nullptr;
    std::unordered_map<std::string, std::unique_ptr<ClientSession>> sessions_;
    std::shared_ptr<BoundedExecutor> callback_executor_;
    std::atomic<bool> shutting_down_{false};
    std::unique_ptr<ZkClient> zk_client_;
    std::mutex discovery_mutex_;
    std::unordered_map<std::string, EndpointCacheEntry> endpoint_cache_;

    void SendFrame(Endpoint endpoint, std::string frame);

    void SendFrameInLoop(Endpoint endpoint, std::string frame);

    ClientSession* GetOrCreateSession(const Endpoint& endpoint);

    void OnConnection(const std::string& endpoint_key, const muduo::net::TcpConnectionPtr& conn);

    void OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer*, muduo::Timestamp receive_time);

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

    mprpc::MprpcErrorCode GetEndpoint(const std::string& service_name,
                        const std::string& method_name,
                        const std::string& affinity_key,
                        Endpoint* endpoint,
                        std::string* error_msg);     
};
