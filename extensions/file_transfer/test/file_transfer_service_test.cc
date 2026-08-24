#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <zlib.h>

#include <google/protobuf/stubs/callback.h>

#include "file_transfer_service.h"

namespace
{
uint32_t Crc32(const std::string& data)
{
    return static_cast<uint32_t>(::crc32(
        0L, reinterpret_cast<const Bytef*>(data.data()),
        static_cast<uInt>(data.size())));
}

struct TestContext
{
    void Check(bool condition, const std::string& message)
    {
        if (!condition) {
            passed = false;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    bool passed = true;
};

struct CompletionState
{
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    int callback_count = 0;
    std::thread::id callback_thread;
};

class TestRpcController : public google::protobuf::RpcController
{
public:
    void Reset() override { cancelled_ = false; failed_ = false; }
    bool Failed() const override { return failed_; }
    std::string ErrorText() const override { return error_; }
    void StartCancel() override { cancelled_ = true; }
    void SetFailed(const std::string& reason) override
    {
        failed_ = true;
        error_ = reason;
    }
    bool IsCanceled() const override { return cancelled_; }
    void NotifyOnCancel(google::protobuf::Closure* callback) override
    {
        if (cancelled_ && callback != nullptr) callback->Run();
    }

private:
    bool cancelled_ = false;
    bool failed_ = false;
    std::string error_;
};

void RecordCompletion(std::shared_ptr<CompletionState> state)
{
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->completed = true;
        ++state->callback_count;
        state->callback_thread = std::this_thread::get_id();
    }

    state->cv.notify_one();
}

google::protobuf::Closure* NewCompletion(
    const std::shared_ptr<CompletionState>& state)
{
    return google::protobuf::NewCallback(
        &RecordCompletion,
        state);
}

bool WaitForCompletion(
    TestContext& context,
    const std::shared_ptr<CompletionState>& state,
    const std::thread::id& caller_thread,
    const std::string& operation)
{
    std::unique_lock<std::mutex> lock(state->mutex);

    const bool completed = state->cv.wait_for(
        lock,
        std::chrono::seconds(5),
        [&state] {
            return state->completed;
        });

    context.Check(completed,
                  operation + " callback should complete");

    if (!completed) {
        return false;
    }

    context.Check(state->callback_count == 1,
                  operation + " callback should run once");
    context.Check(state->callback_thread != caller_thread,
                  operation + " callback should run on a worker thread");
    return true;
}

std::filesystem::path MakeTestRoot()
{
    const auto suffix =
        std::chrono::steady_clock::now()
            .time_since_epoch()
            .count();

    return std::filesystem::temp_directory_path() /
           ("mprpc_file_service_test_" +
            std::to_string(suffix));
}
} // namespace

int main()
{
    using namespace mprpc::file;

    TestContext context;
    const auto caller_thread = std::this_thread::get_id();
    const auto test_root = MakeTestRoot();

    BeginUploadResponse begin_response;
    UploadChunkResponse first_chunk_response;
    FinishUploadResponse incomplete_finish_response;
    UploadChunkResponse second_chunk_response;
    FinishUploadResponse finish_response;
    BeginUploadResponse abort_begin_response;
    AbortUploadResponse abort_response;
    BeginUploadResponse expired_begin_response;
    UploadChunkResponse expired_chunk_response;

    const auto orphan_path =
        test_root / "temporary" / "startup-orphan.part";
    const auto unrelated_path =
        test_root / "temporary" / "startup-notes.txt";
    std::filesystem::create_directories(test_root / "temporary");
    std::ofstream(orphan_path, std::ios::binary) << "orphan";
    std::ofstream(unrelated_path, std::ios::binary) << "keep";

    {
        FileTransferServiceImpl service(
            test_root,
            2,
            std::chrono::milliseconds(100),
            std::chrono::milliseconds(20));

        context.Check(!std::filesystem::exists(orphan_path),
                      "service startup should remove orphaned part files");
        context.Check(std::filesystem::exists(unrelated_path),
                      "service startup should keep unrelated files");

        {
            TestRpcController cancelled_controller;
            cancelled_controller.StartCancel();
            BeginUploadRequest request;
            request.set_file_name("cancelled.bin");
            request.set_file_size(1);
            request.set_file_sha256(std::string(64, 'a'));
            BeginUploadResponse response;
            auto state = std::make_shared<CompletionState>();
            service.BeginUpload(
                &cancelled_controller, &request, &response,
                NewCompletion(state));
            std::lock_guard<std::mutex> lock(state->mutex);
            context.Check(state->completed && state->callback_count == 1,
                          "cancelled request should complete once");
            context.Check(response.has_result() &&
                              response.result().code() == FILE_CANCELLED,
                          "cancelled request should not enter worker queue");
            context.Check(service.GetTaskStats().accepted == 0 &&
                              service.GetTaskStats().cancelled == 1,
                          "cancelled request should update service metrics");
        }

        auto begin_state = std::make_shared<CompletionState>();
        {
            BeginUploadRequest request;
            request.set_file_name("async-upload.bin");
            request.set_file_size(6);
            request.set_file_sha256(
                "e9c0f8b575cbfcb42ab3b78ecc87efa3"
                "b011d9a5d10b09fa4e96f240bf6a82f5");
            request.set_preferred_chunk_size(4);

            service.BeginUpload(
                nullptr,
                &request,
                &begin_response,
                NewCompletion(begin_state));
        }

        const bool begin_completed = WaitForCompletion(
            context,
            begin_state,
            caller_thread,
            "BeginUpload");

        const bool begin_ok =
            begin_completed &&
            begin_response.has_result() &&
            begin_response.result().code() == FILE_OK;

        context.Check(begin_response.has_result(),
                      "BeginUpload should return FileResult");
        context.Check(begin_ok,
                      "BeginUpload should succeed");
        context.Check(!begin_response.transfer_id().empty(),
                      "BeginUpload should return transfer_id");
        context.Check(begin_response.accepted_chunk_size() == 4,
                      "BeginUpload should return negotiated chunk size");

        if (begin_ok) {
            const std::string transfer_id =
                begin_response.transfer_id();

            auto first_chunk_state =
                std::make_shared<CompletionState>();
            {
                UploadChunkRequest request;
                request.set_transfer_id(transfer_id);
                request.set_offset(0);
                request.set_data("ABCD");
                request.set_data_crc32(Crc32("ABCD"));

                service.UploadChunk(
                    nullptr,
                    &request,
                    &first_chunk_response,
                    NewCompletion(first_chunk_state));
            }

            WaitForCompletion(
                context,
                first_chunk_state,
                caller_thread,
                "first UploadChunk");
            context.Check(
                first_chunk_response.has_result() &&
                    first_chunk_response.result().code() == FILE_OK,
                "first UploadChunk should succeed");
            context.Check(
                first_chunk_response.acknowledged_offset() == 0 &&
                    first_chunk_response.total_received_size() == 4,
                "first UploadChunk should acknowledge offset zero");

            auto incomplete_state =
                std::make_shared<CompletionState>();
            {
                FinishUploadRequest request;
                request.set_transfer_id(transfer_id);

                service.FinishUpload(
                    nullptr,
                    &request,
                    &incomplete_finish_response,
                    NewCompletion(incomplete_state));
            }

            WaitForCompletion(
                context,
                incomplete_state,
                caller_thread,
                "incomplete FinishUpload");
            context.Check(
                incomplete_finish_response.has_result() &&
                    incomplete_finish_response.result().code() ==
                        INVALID_STATE,
                "incomplete FinishUpload should fail");
            context.Check(
                incomplete_finish_response.received_size() == 4,
                "incomplete FinishUpload should report four bytes");

            auto second_chunk_state =
                std::make_shared<CompletionState>();
            {
                UploadChunkRequest request;
                request.set_transfer_id(transfer_id);
                request.set_offset(4);
                request.set_data("EF");
                request.set_data_crc32(Crc32("EF"));

                service.UploadChunk(
                    nullptr,
                    &request,
                    &second_chunk_response,
                    NewCompletion(second_chunk_state));
            }

            WaitForCompletion(
                context,
                second_chunk_state,
                caller_thread,
                "second UploadChunk");
            context.Check(
                second_chunk_response.has_result() &&
                    second_chunk_response.result().code() == FILE_OK,
                "second UploadChunk should succeed");
            context.Check(
                second_chunk_response.acknowledged_offset() == 4 &&
                    second_chunk_response.total_received_size() == 6,
                "second UploadChunk should acknowledge offset four");

            auto finish_state =
                std::make_shared<CompletionState>();
            {
                FinishUploadRequest request;
                request.set_transfer_id(transfer_id);

                service.FinishUpload(
                    nullptr,
                    &request,
                    &finish_response,
                    NewCompletion(finish_state));
            }

            WaitForCompletion(
                context,
                finish_state,
                caller_thread,
                "FinishUpload");
            context.Check(
                finish_response.has_result() &&
                    finish_response.result().code() == FILE_OK,
                "FinishUpload should succeed");
            context.Check(finish_response.received_size() == 6,
                          "FinishUpload should report six bytes");

            const auto final_path =
                test_root / "completed" /
                (transfer_id + "_async-upload.bin");
            std::ifstream input(final_path, std::ios::binary);
            std::string content(
                std::istreambuf_iterator<char>(input),
                std::istreambuf_iterator<char>{});

            context.Check(input.is_open(),
                          "published file should be readable");
            context.Check(content == "ABCDEF",
                          "published file should contain uploaded data");
        }

        auto abort_begin_state =
            std::make_shared<CompletionState>();
        {
            BeginUploadRequest request;
            request.set_file_name("abort-upload.bin");
            request.set_file_size(8);
            request.set_file_sha256(std::string(64, 'a'));
            request.set_preferred_chunk_size(4);

            service.BeginUpload(
                nullptr,
                &request,
                &abort_begin_response,
                NewCompletion(abort_begin_state));
        }

        const bool abort_begin_completed = WaitForCompletion(
            context,
            abort_begin_state,
            caller_thread,
            "abort test BeginUpload");

        if (abort_begin_completed &&
            abort_begin_response.has_result() &&
            abort_begin_response.result().code() == FILE_OK) {
            const std::string transfer_id =
                abort_begin_response.transfer_id();
            const auto temporary_path =
                test_root / "temporary" /
                (transfer_id + ".part");

            auto abort_state =
                std::make_shared<CompletionState>();
            {
                AbortUploadRequest request;
                request.set_transfer_id(transfer_id);

                service.AbortUpload(
                    nullptr,
                    &request,
                    &abort_response,
                    NewCompletion(abort_state));
            }

            WaitForCompletion(
                context,
                abort_state,
                caller_thread,
                "AbortUpload");
            context.Check(
                abort_response.has_result() &&
                    abort_response.result().code() == FILE_OK,
                "AbortUpload should succeed");
            context.Check(!std::filesystem::exists(temporary_path),
                          "AbortUpload should remove temporary file");
        } else {
            context.Check(false,
                          "abort test BeginUpload should succeed");
        }

        auto expired_begin_state =
            std::make_shared<CompletionState>();
        {
            BeginUploadRequest request;
            request.set_file_name("expired-upload.bin");
            request.set_file_size(8);
            request.set_file_sha256(std::string(64, 'a'));
            request.set_preferred_chunk_size(4);

            service.BeginUpload(
                nullptr,
                &request,
                &expired_begin_response,
                NewCompletion(expired_begin_state));
        }

        const bool expired_begin_completed = WaitForCompletion(
            context,
            expired_begin_state,
            caller_thread,
            "expired test BeginUpload");

        if (expired_begin_completed &&
            expired_begin_response.has_result() &&
            expired_begin_response.result().code() == FILE_OK) {
            const std::string transfer_id =
                expired_begin_response.transfer_id();
            const auto expired_path =
                test_root / "temporary" / (transfer_id + ".part");

            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(2);

            while (std::filesystem::exists(expired_path) &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(10));
            }

            context.Check(!std::filesystem::exists(expired_path),
                          "background cleanup should remove expired upload");

            auto expired_chunk_state =
                std::make_shared<CompletionState>();
            {
                UploadChunkRequest request;
                request.set_transfer_id(transfer_id);
                request.set_offset(0);
                request.set_data("ABCD");
                request.set_data_crc32(Crc32("ABCD"));

                service.UploadChunk(
                    nullptr,
                    &request,
                    &expired_chunk_response,
                    NewCompletion(expired_chunk_state));
            }

            WaitForCompletion(
                context,
                expired_chunk_state,
                caller_thread,
                "expired UploadChunk");
            context.Check(
                expired_chunk_response.has_result() &&
                    expired_chunk_response.result().code() ==
                        TRANSFER_NOT_FOUND,
                "expired session should no longer accept chunks");
        } else {
            context.Check(false,
                          "expired test BeginUpload should succeed");
        }
    }

    std::error_code cleanup_error;
    std::filesystem::remove_all(test_root, cleanup_error);
    context.Check(!cleanup_error,
                  "service test directory cleanup should succeed");

    std::cout << (context.passed ? "PASS" : "FAIL")
              << ": FileTransferService async tests\n";

    return context.passed ? 0 : 1;
}
