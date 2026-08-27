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
private:
    struct State;

public:
    using Task = std::function<void()>;

    // 预占一个 outstanding 槽位；未提交时由析构自动归还。
    class Reservation
    {
    public:
        Reservation() = default;
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;
        ~Reservation();

        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;

        explicit operator bool() const noexcept;

    private:
        friend class BoundedExecutor;
        explicit Reservation(std::shared_ptr<State> state);
        void Release() noexcept;

        std::shared_ptr<State> state_;
    };

    explicit BoundedExecutor(std::size_t thread_count,
                             std::size_t max_outstanding);
    ~BoundedExecutor();

    BoundedExecutor(const BoundedExecutor&) = delete;
    BoundedExecutor& operator=(const BoundedExecutor&) = delete;

    // 尝试提交任务；队列已满或执行器关闭时立即返回 false。
    bool TrySubmit(Task task);

    // 为将来的任务预占容量，失败时返回空 Reservation。
    Reservation TryReserve();

    // 使用已预占的容量提交任务，不再参与容量竞争。
    bool SubmitReserved(Reservation reservation, Task task);

    // 关闭执行器；drain 为 true 时先执行完已接收的任务。
    void Shutdown(bool drain = true);

    // 返回执行器生命周期内的累计任务统计。
    uint64_t Accepted() const noexcept;
    uint64_t Rejected() const noexcept;
    uint64_t Completed() const noexcept;
    uint64_t CurrentOutstanding() const noexcept;
    uint64_t PeakOutstanding() const noexcept;

private:
    static void WorkerLoop(const std::shared_ptr<State>& state);

    std::shared_ptr<State> state_;
    std::vector<std::thread> workers_;
};
