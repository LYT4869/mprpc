#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "boundedexecutor.h"

int main()
{
    BoundedExecutor executor(1, 1);

    {
        auto reservation = executor.TryReserve();
        if (!reservation || executor.TryReserve()) {
            std::cerr << "FAIL: reservation did not consume capacity\n";
            return 1;
        }
    }

    auto reservation = executor.TryReserve();
    if (!reservation) {
        std::cerr << "FAIL: abandoned reservation did not release capacity\n";
        return 1;
    }

    std::mutex mutex;
    std::condition_variable cv;
    bool ran = false;
    const bool submitted = executor.SubmitReserved(
        std::move(reservation), [&] {
            {
                std::lock_guard<std::mutex> lock(mutex);
                ran = true;
            }
            cv.notify_one();
        });

    std::unique_lock<std::mutex> lock(mutex);
    const bool completed = cv.wait_for(
        lock, std::chrono::seconds(1), [&] { return ran; });
    lock.unlock();

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (executor.Completed() != 1 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const bool passed = submitted && completed &&
        executor.Accepted() == 1 && executor.Completed() == 1;
    std::cout << (passed ? "PASS" : "FAIL")
              << ": bounded executor reservation semantics\n";
    return passed ? 0 : 1;
}
