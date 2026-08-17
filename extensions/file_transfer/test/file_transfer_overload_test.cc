#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <google/protobuf/stubs/callback.h>

#include "file_transfer_service.h"

namespace
{
struct Batch
{
    std::mutex mutex;
    std::condition_variable cv;
    int completed = 0;
};

struct Call
{
    mprpc::file::BeginUploadRequest request;
    mprpc::file::BeginUploadResponse response;
};

void OnDone(std::shared_ptr<Batch> batch)
{
    {
        std::lock_guard<std::mutex> lock(batch->mutex);
        ++batch->completed;
    }
    batch->cv.notify_all();
}
} // namespace

int main()
{
    using namespace mprpc::file;
    constexpr int kCalls = 128;
    const auto root = std::filesystem::temp_directory_path() /
        ("mprpc_overload_test_" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));

    bool passed = true;
    {
        FileTransferServiceImpl service(
            root, 1, std::chrono::minutes(10),
            std::chrono::minutes(1), 1);
        auto batch = std::make_shared<Batch>();
        std::vector<std::unique_ptr<Call>> calls;
        calls.reserve(kCalls);

        for (int i = 0; i < kCalls; ++i) {
            auto call = std::make_unique<Call>();
            call->request.set_file_name(
                "overload-" + std::to_string(i) + ".bin");
            call->request.set_file_size(0);
            call->request.set_file_sha256(
                "e3b0c44298fc1c149afbf4c8996fb924"
                "27ae41e4649b934ca495991b7852b855");
            call->request.set_preferred_chunk_size(1024);
            service.BeginUpload(
                nullptr, &call->request, &call->response,
                google::protobuf::NewCallback(&OnDone, batch));
            calls.push_back(std::move(call));
        }

        {
            std::unique_lock<std::mutex> lock(batch->mutex);
            passed = batch->cv.wait_for(
                lock, std::chrono::seconds(10),
                [&batch] { return batch->completed == kCalls; });
        }

        int accepted = 0;
        int rejected = 0;
        for (const auto& call : calls) {
            if (call->response.has_result() &&
                call->response.result().code() == FILE_OK) {
                ++accepted;
            } else if (call->response.has_result() &&
                       call->response.result().code() == SERVER_BUSY) {
                ++rejected;
            } else {
                passed = false;
            }
        }
        const ServiceTaskStats saturated_stats = service.GetTaskStats();
        passed = passed && accepted >= 1 && rejected >= 1 &&
            saturated_stats.rejected == static_cast<uint64_t>(rejected);

        auto recovery = std::make_unique<Call>();
        recovery->request.set_file_name("recovered.bin");
        recovery->request.set_file_size(0);
        recovery->request.set_file_sha256(
            "e3b0c44298fc1c149afbf4c8996fb924"
            "27ae41e4649b934ca495991b7852b855");
        recovery->request.set_preferred_chunk_size(1024);
        auto recovery_batch = std::make_shared<Batch>();
        service.BeginUpload(
            nullptr, &recovery->request, &recovery->response,
            google::protobuf::NewCallback(&OnDone, recovery_batch));
        {
            std::unique_lock<std::mutex> lock(recovery_batch->mutex);
            recovery_batch->cv.wait_for(
                lock, std::chrono::seconds(5),
                [&recovery_batch] { return recovery_batch->completed == 1; });
        }
        passed = passed && recovery->response.has_result() &&
            recovery->response.result().code() == FILE_OK;

        const ServiceTaskStats final_stats = service.GetTaskStats();
        std::cout << "accepted=" << final_stats.accepted
                  << " rejected=" << final_stats.rejected
                  << " completed=" << final_stats.completed << '\n';
    }

    std::error_code error;
    std::filesystem::remove_all(root, error);
    passed = passed && !error;
    std::cout << (passed ? "PASS" : "FAIL")
              << ": bounded overload rejection and recovery\n";
    return passed ? 0 : 1;
}
