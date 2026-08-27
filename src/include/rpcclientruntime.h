#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include <muduo/net/EventLoopThread.h>

#include "boundedexecutor.h"

namespace muduo::net
{
class EventLoop;
}

// 进程级客户端运行时：少量 EventLoop 为多个 ChannelCore 提供 I/O。
class RpcClientRuntime
{
public:
    explicit RpcClientRuntime(std::size_t io_thread_count = 2,
                              std::size_t callback_thread_count = 2,
                              std::size_t callback_capacity = 1024);
    ~RpcClientRuntime();

    RpcClientRuntime(const RpcClientRuntime&) = delete;
    RpcClientRuntime& operator=(const RpcClientRuntime&) = delete;

    static std::shared_ptr<RpcClientRuntime> Default();

    // 新 Core 通过轮询固定选择一个 EventLoop，之后不再迁移。
    muduo::net::EventLoop* NextLoop() noexcept;
    std::size_t IoThreadCount() const noexcept;

    // 异步 RPC 在发送前预留回调容量，保证完成时能够投递。
    BoundedExecutor::Reservation TryReserveCallback();
    bool SubmitCallback(BoundedExecutor::Reservation reservation,
                        BoundedExecutor::Task task);

private:
    // 声明在 I/O 线程之前，使析构时最后排空回调任务。
    std::shared_ptr<BoundedExecutor> callback_executor_;
    std::vector<std::unique_ptr<muduo::net::EventLoopThread>> threads_;
    std::vector<muduo::net::EventLoop*> loops_;
    std::atomic<std::size_t> next_loop_{0};
};
