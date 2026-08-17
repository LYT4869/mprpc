#include "boundedexecutor.h"

#include <algorithm>
#include <utility>

struct BoundedExecutor::State
{
    explicit State(std::size_t capacity) : max_outstanding(capacity) {}

    const std::size_t max_outstanding;
    std::deque<Task> tasks;
    std::mutex mutex;
    std::condition_variable task_cv;
    bool stopping = false;
    bool drain = true;
    std::size_t outstanding = 0;
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> rejected{0};
    std::atomic<uint64_t> completed{0};
};

BoundedExecutor::BoundedExecutor(std::size_t thread_count,
                                 std::size_t max_outstanding)
    : state_(std::make_shared<State>(
          std::max<std::size_t>(1, max_outstanding)))
{
    thread_count = std::max<std::size_t>(1, thread_count);
    workers_.reserve(thread_count);
    for (std::size_t i = 0; i < thread_count; ++i) {
        workers_.emplace_back(&BoundedExecutor::WorkerLoop, state_);
    }
}

BoundedExecutor::~BoundedExecutor()
{
    Shutdown(true);
}

bool BoundedExecutor::TrySubmit(Task task)
{
    if (!task || !state_) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        if (state_->stopping ||
            state_->outstanding >= state_->max_outstanding) {
            state_->rejected.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        state_->tasks.push_back(std::move(task));
        ++state_->outstanding;
        state_->accepted.fetch_add(1, std::memory_order_relaxed);
    }
    state_->task_cv.notify_one();
    return true;
}

void BoundedExecutor::Shutdown(bool drain)
{
    const std::shared_ptr<State> state = state_;
    if (!state) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->stopping) {
            state->stopping = true;
            state->drain = drain;
            if (!drain) {
                state->outstanding -= state->tasks.size();
                state->tasks.clear();
            }
        }
    }
    state->task_cv.notify_all();

    const std::thread::id current = std::this_thread::get_id();
    for (auto& worker : workers_) {
        if (!worker.joinable()) {
            continue;
        }
        if (worker.get_id() == current) {
            worker.detach();
        } else {
            worker.join();
        }
    }
    workers_.clear();
    state_.reset();
}

uint64_t BoundedExecutor::Accepted() const noexcept
{
    return state_ ? state_->accepted.load(std::memory_order_relaxed) : 0;
}

uint64_t BoundedExecutor::Rejected() const noexcept
{
    return state_ ? state_->rejected.load(std::memory_order_relaxed) : 0;
}

uint64_t BoundedExecutor::Completed() const noexcept
{
    return state_ ? state_->completed.load(std::memory_order_relaxed) : 0;
}

void BoundedExecutor::WorkerLoop(const std::shared_ptr<State>& state)
{
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->task_cv.wait(lock, [&state] {
                return state->stopping || !state->tasks.empty();
            });

            if (state->tasks.empty()) {
                if (state->stopping) {
                    return;
                }
                continue;
            }
            task = std::move(state->tasks.front());
            state->tasks.pop_front();
        }

        try {
            task();
        } catch (...) {
            // User callbacks must not terminate executor workers.
        }

        state->completed.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            --state->outstanding;
        }
        state->task_cv.notify_all();
    }
}
