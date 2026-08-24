#include "parallel_file_uploader.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <utility>
#include <vector>

#include <google/protobuf/stubs/callback.h>
#include <openssl/evp.h>
#include <unistd.h>
#include <zlib.h>

#include "mprpccontroller.h"

namespace mprpc::file
{
namespace
{
constexpr std::size_t kHashReadBufferSize = 64U * 1024U;

std::string GenerateTransferId()
{
    thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution;
    std::ostringstream stream;
    stream << std::hex << std::setfill('0')
           << std::setw(16) << distribution(generator)
           << std::setw(16) << distribution(generator);
    return stream.str();
}

struct EvpMdCtxDeleter
{
    void operator()(EVP_MD_CTX* context) const noexcept
    {
        EVP_MD_CTX_free(context);
    }
};
using EvpMdCtxPtr = std::unique_ptr<EVP_MD_CTX, EvpMdCtxDeleter>;

bool ComputeFileSha256(const std::filesystem::path& path,
                       std::string* digest, std::string* error_message)
{
    std::ifstream input(path, std::ios::binary);
    EvpMdCtxPtr context(EVP_MD_CTX_new());
    if (!input || !context ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        *error_message = "failed to initialize local SHA-256";
        return false;
    }

    std::array<char, kHashReadBufferSize> buffer{};
    while (input) {
        input.read(buffer.data(),
                   static_cast<std::streamsize>(buffer.size()));
        const auto size = input.gcount();
        if (size > 0 &&
            EVP_DigestUpdate(context.get(), buffer.data(),
                             static_cast<std::size_t>(size)) != 1) {
            *error_message = "failed to update local SHA-256";
            return false;
        }
    }
    if (!input.eof()) {
        *error_message = "failed to read local file for SHA-256";
        return false;
    }

    std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context.get(), bytes.data(), &size) != 1) {
        *error_message = "failed to finalize local SHA-256";
        return false;
    }
    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < size; ++i) {
        stream << std::setw(2) << static_cast<unsigned int>(bytes[i]);
    }
    *digest = stream.str();
    return true;
}

template <typename Response>
bool CheckResult(const char* operation, const MprpcController& controller,
                 const Response& response, std::string* error_message)
{
    if (controller.Failed()) {
        *error_message = std::string(operation) + " RPC failed: " +
                         controller.ErrorText();
        return false;
    }
    if (!response.has_result() || response.result().code() != FILE_OK) {
        *error_message = std::string(operation) + " failed: " +
            (response.has_result() ? response.result().error_message()
                                   : "missing business result");
        return false;
    }
    return true;
}

struct PendingChunk
{
    uint32_t index = 0;
    uint32_t attempt = 0;
    std::chrono::steady_clock::time_point ready_at;
};

struct UploadOperation
{
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<PendingChunk> pending;
    uint32_t active = 0;
    uint32_t completed = 0;
    uint32_t total_missing = 0;
    uint32_t retries = 0;
    uint32_t retry_exhausted = 0;
    uint32_t chunk_attempts = 0;
    uint32_t duplicate_chunks = 0;
    uint32_t queue_rejected = 0;
    uint32_t max_in_flight = 0;
    uint64_t bytes_uploaded = 0;
    bool failed = false;
    std::string error_message;
    UploadOptions options;
};

struct ChunkCallContext
{
    // 持有本分片的 Protobuf 对象，直到 done 回调结束。
    MprpcController controller;
    UploadChunkRequest request;
    UploadChunkResponse response;
    uint32_t index = 0;
    uint32_t attempt = 0;
    uint64_t offset = 0;
    std::size_t data_size = 0;
};

bool IsRetryable(mprpc::MprpcErrorCode code)
{
    return code == mprpc::MprpcErrorCode::TIMEOUT ||
           code == mprpc::MprpcErrorCode::NETWORK_ERROR ||
           code == mprpc::MprpcErrorCode::CONNECTION_CLOSED;
}

// 汇总分片结果，决定成功计数、重试入队或终止整个上传。
void OnChunkDone(std::shared_ptr<UploadOperation> operation,
                 std::shared_ptr<ChunkCallContext> call)
{
    std::lock_guard<std::mutex> lock(operation->mutex);
    --operation->active;

    if (operation->failed) {
        operation->cv.notify_all();
        return;
    }

    if (call->controller.Failed()) {
        // 只有瞬时传输错误才会重新放回调度队列。
        if (IsRetryable(call->controller.ErrorCode()) &&
            call->attempt < operation->options.max_retries) {
            const uint32_t multiplier =
                1U << std::min<uint32_t>(call->attempt, 10);
            operation->pending.push_back(PendingChunk{
                call->index, call->attempt + 1,
                std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(
                        operation->options.initial_retry_backoff_ms *
                        multiplier)});
            ++operation->retries;
        } else {
            ++operation->retry_exhausted;
            operation->failed = true;
            operation->error_message = "UploadChunk RPC failed: " +
                                       call->controller.ErrorText();
        }
        operation->cv.notify_all();
        return;
    }

    if (!call->response.has_result() ||
        call->response.result().code() != FILE_OK) {
        if (call->response.has_result() &&
            call->response.result().code() == SERVER_BUSY) {
            ++operation->queue_rejected;
        }
        operation->failed = true;
        operation->error_message = call->response.has_result()
            ? "UploadChunk failed: " +
                  call->response.result().error_message()
            : "UploadChunk returned no business result";
    } else if (call->response.acknowledged_offset() != call->offset) {
        operation->failed = true;
        operation->error_message =
            "UploadChunk returned an unexpected acknowledgement";
    } else {
        ++operation->completed;
        operation->bytes_uploaded += call->data_size;
        if (call->response.duplicate()) {
            ++operation->duplicate_chunks;
        }
    }
    operation->cv.notify_all();
}

bool ReadChunk(int fd, uint64_t offset, std::size_t size,
               std::string* data, std::string* error_message)
{
    data->assign(size, '\0');
    std::size_t read = 0;
    while (read < size) {
        const ssize_t count = ::pread(
            fd, data->data() + read, size - read,
            static_cast<off_t>(offset + read));
        if (count < 0) {
            if (errno == EINTR) continue;
            *error_message = "failed to read local chunk: " +
                             std::string(std::strerror(errno));
            return false;
        }
        if (count == 0) {
            *error_message = "unexpected end of local file";
            return false;
        }
        read += static_cast<std::size_t>(count);
    }
    return true;
}
} // namespace

ParallelFileUploader::ParallelFileUploader() : stub_(&channel_)
{
}

RpcMetricsSnapshot ParallelFileUploader::GetRpcMetricsSnapshot() const
{
    return channel_.GetMetricsSnapshot();
}

UploadFileResult ParallelFileUploader::Upload(
    const std::filesystem::path& local_path,
    std::string remote_file_name, UploadOptions options)
{
    UploadFileResult result;
    options.window_size = std::max<uint32_t>(1, options.window_size);
    options.chunk_timeout_ms = std::max<uint32_t>(1, options.chunk_timeout_ms);

    std::error_code file_error;
    if (!std::filesystem::is_regular_file(local_path, file_error) || file_error) {
        result.error_message = "local path is not a regular file";
        return result;
    }
    const uint64_t file_size = std::filesystem::file_size(local_path, file_error);
    if (file_error) {
        result.error_message = "failed to determine local file size";
        return result;
    }
    if (remote_file_name.empty()) {
        remote_file_name = local_path.filename().string();
    }
    if (options.transfer_id.empty()) {
        options.transfer_id = GenerateTransferId();
    }

    std::string sha256;
    if (!ComputeFileSha256(local_path, &sha256, &result.error_message)) {
        return result;
    }

    BeginUploadRequest begin_request;
    begin_request.set_file_name(std::move(remote_file_name));
    begin_request.set_file_size(file_size);
    begin_request.set_file_sha256(std::move(sha256));
    begin_request.set_preferred_chunk_size(options.preferred_chunk_size);
    begin_request.set_requested_transfer_id(options.transfer_id);
    BeginUploadResponse begin_response;
    MprpcController begin_controller;
    begin_controller.SetTimeoutMs(options.chunk_timeout_ms);
    begin_controller.SetAffinityKey(options.transfer_id);
    stub_.BeginUpload(&begin_controller, &begin_request, &begin_response, nullptr);
    if (!CheckResult("BeginUpload", begin_controller, begin_response,
                     &result.error_message)) {
        if (begin_response.has_result() &&
            begin_response.result().code() == SERVER_BUSY) {
            ++result.queue_rejected;
        }
        return result;
    }
    result.transfer_id = begin_response.transfer_id();
    if (result.transfer_id != options.transfer_id) {
        result.error_message = "BeginUpload returned a different transfer id";
        return result;
    }

    QueryUploadStatusRequest query_request;
    query_request.set_transfer_id(result.transfer_id);
    QueryUploadStatusResponse query_response;
    MprpcController query_controller;
    query_controller.SetTimeoutMs(options.chunk_timeout_ms);
    query_controller.SetAffinityKey(result.transfer_id);
    stub_.QueryUploadStatus(&query_controller, &query_request,
                            &query_response, nullptr);
    if (!CheckResult("QueryUploadStatus", query_controller, query_response,
                     &result.error_message)) {
        if (query_response.has_result() &&
            query_response.result().code() == SERVER_BUSY) {
            ++result.queue_rejected;
        }
        return result;
    }
    if (query_response.file_size() != file_size ||
        query_response.chunk_size() == 0 ||
        query_response.received_bitmap().size() !=
            (query_response.chunk_count() + 7U) / 8U) {
        result.error_message = "server returned inconsistent upload status";
        return result;
    }

    const uint32_t chunk_size = query_response.chunk_size();
    auto operation = std::make_shared<UploadOperation>();
    operation->options = options;
    operation->bytes_uploaded = query_response.received_size();
    const auto now = std::chrono::steady_clock::now();
    for (uint32_t index = 0; index < query_response.chunk_count(); ++index) {
        const bool received =
            (static_cast<unsigned char>(
                 query_response.received_bitmap()[index / 8U]) &
             static_cast<unsigned char>(1U << (index % 8U))) != 0;
        if (!received) {
            operation->pending.push_back(PendingChunk{index, 0, now});
            ++operation->total_missing;
        }
    }

    const int local_fd = ::open(local_path.c_str(), O_RDONLY);
    if (local_fd < 0) {
        result.error_message = "failed to open local file for chunk reads";
        return result;
    }
    std::unique_ptr<int, void(*)(int*)> fd_guard(
        new int(local_fd), [](int* fd) { ::close(*fd); delete fd; });

    // 同时在途的异步分片 RPC 不超过 window_size。
    while (true) {
        PendingChunk pending;
        bool launch = false;
        {
            std::unique_lock<std::mutex> lock(operation->mutex);
            if (operation->failed) break;
            if (operation->completed == operation->total_missing &&
                operation->active == 0) {
                break;
            }

            auto ready = std::min_element(
                operation->pending.begin(), operation->pending.end(),
                [](const PendingChunk& left, const PendingChunk& right) {
                    return left.ready_at < right.ready_at;
                });
            const auto current = std::chrono::steady_clock::now();
            if (operation->active < options.window_size &&
                ready != operation->pending.end() &&
                ready->ready_at <= current) {
                pending = *ready;
                operation->pending.erase(ready);
                ++operation->active;
                ++operation->chunk_attempts;
                operation->max_in_flight = std::max(
                    operation->max_in_flight, operation->active);
                launch = true;
            } else if (ready != operation->pending.end() &&
                       operation->active < options.window_size) {
                const auto ready_at = ready->ready_at;
                operation->cv.wait_until(lock, ready_at);
            } else {
                operation->cv.wait(lock);
            }
        }
        if (!launch) continue;

        const uint64_t offset = static_cast<uint64_t>(pending.index) * chunk_size;
        const std::size_t data_size = static_cast<std::size_t>(
            std::min<uint64_t>(chunk_size, file_size - offset));
        auto call = std::make_shared<ChunkCallContext>();
        call->index = pending.index;
        call->attempt = pending.attempt;
        call->offset = offset;
        call->data_size = data_size;
        std::string data;
        if (!ReadChunk(local_fd, offset, data_size, &data,
                       &result.error_message)) {
            std::lock_guard<std::mutex> lock(operation->mutex);
            --operation->active;
            operation->failed = true;
            operation->error_message = result.error_message;
            operation->cv.notify_all();
            continue;
        }

        call->request.set_transfer_id(result.transfer_id);
        call->request.set_offset(offset);
        call->request.set_data(std::move(data));
        call->request.set_data_crc32(static_cast<uint32_t>(::crc32(
            0L,
            reinterpret_cast<const Bytef*>(call->request.data().data()),
            static_cast<uInt>(call->request.data().size()))));
        call->controller.SetTimeoutMs(options.chunk_timeout_ms);
        call->controller.SetAffinityKey(result.transfer_id);
        // Closure 通过 shared_ptr 持有 operation 和 call。
        google::protobuf::Closure* done = google::protobuf::NewCallback(
            &OnChunkDone, operation, call);
        stub_.UploadChunk(&call->controller, &call->request,
                          &call->response, done);
    }

    {
        std::lock_guard<std::mutex> lock(operation->mutex);
        result.retries = operation->retries;
        result.retry_exhausted = operation->retry_exhausted;
        result.chunk_attempts = operation->chunk_attempts;
        result.duplicate_chunks = operation->duplicate_chunks;
        result.queue_rejected = operation->queue_rejected;
        result.max_in_flight = operation->max_in_flight;
        result.bytes_uploaded = operation->bytes_uploaded;
        if (operation->failed) {
            result.error_message = operation->error_message;
            return result;
        }
    }

    FinishUploadRequest finish_request;
    finish_request.set_transfer_id(result.transfer_id);
    FinishUploadResponse finish_response;
    MprpcController finish_controller;
    finish_controller.SetTimeoutMs(options.chunk_timeout_ms);
    finish_controller.SetAffinityKey(result.transfer_id);
    stub_.FinishUpload(&finish_controller, &finish_request,
                       &finish_response, nullptr);
    if (!CheckResult("FinishUpload", finish_controller, finish_response,
                     &result.error_message)) {
        if (finish_response.has_result() &&
            finish_response.result().code() == SERVER_BUSY) {
            ++result.queue_rejected;
        }
        return result;
    }
    if (finish_response.received_size() != file_size) {
        result.error_message = "FinishUpload returned an unexpected size";
        return result;
    }

    result.ok = true;
    return result;
}
} // namespace mprpc::file
