#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <memory>
#include <thread>
#include <vector>

// 固定线程数、限制 outstanding 数量的非阻塞任务执行器。
class BoundedExecutor
{
public:
    using Task = std::function<void()>;

    explicit BoundedExecutor(std::size_t thread_count,
                             std::size_t max_outstanding);
    ~BoundedExecutor();

    BoundedExecutor(const BoundedExecutor&) = delete;
    BoundedExecutor& operator=(const BoundedExecutor&) = delete;

    // 尝试提交任务；队列已满或执行器关闭时立即返回 false。
    bool TrySubmit(Task task);

    // 关闭执行器；drain 为 true 时先执行完已接收的任务。
    void Shutdown(bool drain = true);

    // 返回执行器生命周期内的累计任务统计。
    uint64_t Accepted() const noexcept;
    uint64_t Rejected() const noexcept;
    uint64_t Completed() const noexcept;
    uint64_t CurrentOutstanding() const noexcept;
    uint64_t PeakOutstanding() const noexcept;

private:
    struct State;
    static void WorkerLoop(const std::shared_ptr<State>& state);

    std::shared_ptr<State> state_;
    std::vector<std::thread> workers_;
};
