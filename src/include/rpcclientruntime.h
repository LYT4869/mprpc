#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include <muduo/net/EventLoopThread.h>

namespace muduo::net
{
class EventLoop;
}

// 进程级客户端运行时：少量 EventLoop 为多个 ChannelCore 提供 I/O。
class RpcClientRuntime
{
public:
    explicit RpcClientRuntime(std::size_t io_thread_count = 2);
    ~RpcClientRuntime();

    RpcClientRuntime(const RpcClientRuntime&) = delete;
    RpcClientRuntime& operator=(const RpcClientRuntime&) = delete;

    static std::shared_ptr<RpcClientRuntime> Default();

    // 新 Core 通过轮询固定选择一个 EventLoop，之后不再迁移。
    muduo::net::EventLoop* NextLoop() noexcept;
    std::size_t IoThreadCount() const noexcept;

private:
    std::vector<std::unique_ptr<muduo::net::EventLoopThread>> threads_;
    std::vector<muduo::net::EventLoop*> loops_;
    std::atomic<std::size_t> next_loop_{0};
};
