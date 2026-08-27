#pragma once

#include <atomic>

enum class RpcDispatchPhase
{
    Queued,
    Running,
    Cancelled,
    Finished
};

// 线性化业务开始与排队取消，独立于响应的一次性发送状态。
class RpcDispatchState
{
public:
    bool TryStart() noexcept
    {
        RpcDispatchPhase expected = RpcDispatchPhase::Queued;
        return phase_.compare_exchange_strong(
            expected, RpcDispatchPhase::Running,
            std::memory_order_acq_rel);
    }

    bool CancelQueued() noexcept
    {
        RpcDispatchPhase expected = RpcDispatchPhase::Queued;
        return phase_.compare_exchange_strong(
            expected, RpcDispatchPhase::Cancelled,
            std::memory_order_acq_rel);
    }

    void Finish() noexcept
    {
        phase_.store(RpcDispatchPhase::Finished,
                     std::memory_order_release);
    }

    RpcDispatchPhase Phase() const noexcept
    {
        return phase_.load(std::memory_order_acquire);
    }

private:
    std::atomic<RpcDispatchPhase> phase_{RpcDispatchPhase::Queued};
};
