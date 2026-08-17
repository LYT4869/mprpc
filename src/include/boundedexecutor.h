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

class BoundedExecutor
{
public:
    using Task = std::function<void()>;

    explicit BoundedExecutor(std::size_t thread_count,
                             std::size_t max_outstanding);
    ~BoundedExecutor();

    BoundedExecutor(const BoundedExecutor&) = delete;
    BoundedExecutor& operator=(const BoundedExecutor&) = delete;

    bool TrySubmit(Task task);
    void Shutdown(bool drain = true);

    uint64_t Accepted() const noexcept;
    uint64_t Rejected() const noexcept;
    uint64_t Completed() const noexcept;

private:
    struct State;
    static void WorkerLoop(const std::shared_ptr<State>& state);

    std::shared_ptr<State> state_;
    std::vector<std::thread> workers_;
};
