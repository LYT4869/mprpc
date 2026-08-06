#pragma once

#include <cstdint>
#include <string>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <functional>
#include <memory>
#include <unordered_map>
#include <muduo/net/EventLoopThread.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpClient.h>
#include <muduo/net/Buffer.h>
#include <muduo/base/Timestamp.h>
#include <deque>
#include "mprpccodec.h"

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

struct CallOptions
{
    uint32_t timeout_ms = 3000;
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
    RpcCallResult result;
    std::atomic<CallPhase> phase{CallPhase::Pending};

    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;

    std::function<void(const RpcCallResult&)> completion;
    
};

class ChannelCore : public std::enable_shared_from_this<ChannelCore>
{
public:
    ChannelCore();
    RpcCallResult StartCall(const std::string& service_name,
                   const std::string& method_name,
                   const std::string& request_payload,
                   const CallOptions& options);


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

    std::atomic<uint64_t> next_request_id_{1};
    std::unordered_map<uint64_t, std::shared_ptr<CallState>> pending_calls_;
    std::mutex pending_mutex_;
    muduo::net::EventLoopThread io_thread_;
    muduo::net::EventLoop* loop_ = nullptr;
    std::unordered_map<std::string, std::unique_ptr<ClientSession>> sessions_;

    void SendFrame(Endpoint endpoint, std::string frame);

    void SendFrameInLoop(Endpoint endpoint, std::string frame);

    ClientSession* GetOrCreateSession(const Endpoint& endpoint);

    void OnConnection(const std::string& endpoint_key, const muduo::net::TcpConnectionPtr& conn);

    void OnMessage(const muduo::net::TcpConnectionPtr& conn, muduo::net::Buffer*, muduo::Timestamp receive_time);

    bool CompleteCall(uint64_t request_id, RpcCallResult result);

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
                        Endpoint* endpoint,
                        std::string* error_msg); 
    
    mprpc::MprpcErrorCode DoSyncRequest(
                        const Endpoint& endpoint,
                        const std::string& request_frame,
                        uint32_t timeout_ms,
                        std::string* response_frame,
                        std::string* error_msg);

    
};