#include "rpcclientruntime.h"

#include <algorithm>
#include <string>

RpcClientRuntime::RpcClientRuntime(std::size_t io_thread_count,
                                   std::size_t callback_thread_count,
                                   std::size_t callback_capacity)
    : callback_executor_(std::make_shared<BoundedExecutor>(
          callback_thread_count, callback_capacity))
{
    io_thread_count = std::max<std::size_t>(1, io_thread_count);
    threads_.reserve(io_thread_count);
    loops_.reserve(io_thread_count);

    for (std::size_t index = 0; index < io_thread_count; ++index) {
        auto thread = std::make_unique<muduo::net::EventLoopThread>(
            nullptr, "MprpcClientIo-" + std::to_string(index));
        loops_.push_back(thread->startLoop());
        threads_.push_back(std::move(thread));
    }
}

RpcClientRuntime::~RpcClientRuntime() = default;

std::shared_ptr<RpcClientRuntime> RpcClientRuntime::Default()
{
    static auto runtime = std::make_shared<RpcClientRuntime>(2);
    return runtime;
}

muduo::net::EventLoop* RpcClientRuntime::NextLoop() noexcept
{
    const std::size_t index =
        next_loop_.fetch_add(1, std::memory_order_relaxed) % loops_.size();
    return loops_[index];
}

std::size_t RpcClientRuntime::IoThreadCount() const noexcept
{
    return loops_.size();
}

BoundedExecutor::Reservation RpcClientRuntime::TryReserveCallback()
{
    return callback_executor_->TryReserve();
}

bool RpcClientRuntime::SubmitCallback(
    BoundedExecutor::Reservation reservation,
    BoundedExecutor::Task task)
{
    return callback_executor_->SubmitReserved(
        std::move(reservation), std::move(task));
}
