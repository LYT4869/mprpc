#include "file_transfer_service.h"

#include <algorithm>
#include <iostream>
#include <utility>

namespace mprpc::file
{
FileTransferServiceImpl::FileTransferServiceImpl(
    std::filesystem::path upload_root,
    int worker_threads,
    std::chrono::milliseconds session_timeout,
    std::chrono::milliseconds cleanup_interval,
    std::size_t max_outstanding)
    : manager_(std::move(upload_root)),
      worker_pool_(static_cast<std::size_t>(std::max(1, worker_threads)),
                   max_outstanding),
      session_timeout_(std::max(
          session_timeout,
          std::chrono::milliseconds(1))),
      cleanup_interval_(std::max(
          cleanup_interval,
          std::chrono::milliseconds(1)))
{
    ReportCleanupErrors(manager_.CleanupOrphanedTemporaryFiles());
    cleanup_thread_ = std::thread(
        &FileTransferServiceImpl::CleanupLoop,
        this);
}

FileTransferServiceImpl::~FileTransferServiceImpl()
{
    {
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        stopping_ = true;
    }

    cleanup_cv_.notify_one();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    worker_pool_.Shutdown(true);
}

void FileTransferServiceImpl::BeginUpload(google::protobuf::RpcController* controller,
                    const ::mprpc::file::BeginUploadRequest* request,
                    ::mprpc::file::BeginUploadResponse* response,
                    ::google::protobuf::Closure* done)
{
    // Protobuf request 仅被借用，投递任务前必须复制所需字段。
    std::string file_name = request->file_name();
    uint64_t file_size = request->file_size();
    std::string file_sha256 = request->file_sha256();
    uint32_t preferred_chunk_size = request->preferred_chunk_size();
    std::string requested_transfer_id = request->requested_transfer_id();

    SubmitTask(
        [this,
        file_name = std::move(file_name),
        file_size,
        file_sha256 = std::move(file_sha256),
        requested_transfer_id = std::move(requested_transfer_id),
        preferred_chunk_size,
        response,
        done]{
            auto result = manager_.BeginUpload(
                file_name, file_size, file_sha256,
                preferred_chunk_size, requested_transfer_id);
            FillResult(response->mutable_result(), result.code, result.err_msg);
            if(result.Ok()){
                response->set_transfer_id(result.transfer_id);
                response->set_accepted_chunk_size(result.accepted_chunk_size);
            }

            if(done != nullptr) done->Run();
        }, controller, response->mutable_result(), done
    );
}

void FileTransferServiceImpl::UploadChunk(google::protobuf::RpcController* controller,
                    const ::mprpc::file::UploadChunkRequest* request,
                    ::mprpc::file::UploadChunkResponse* response,
                    ::google::protobuf::Closure* done)
{
    std::string transfer_id = request->transfer_id();
    uint64_t offset = request->offset();
    std::string data(request->data());
    const bool has_crc32 = request->has_data_crc32();
    const uint32_t data_crc32 = request->data_crc32();

    SubmitTask(
        [this,
        transfer_id = std::move(transfer_id),
        offset,
        data = std::move(data),
        has_crc32,
        data_crc32,
        response,
        done]{
            UploadChunkResult result;
            if (!has_crc32) {
                result.code = INVALID_ARGUMENT;
                result.err_msg = "UploadChunk requires data_crc32";
                result.acknowledged_offset = offset;
            } else {
                result = manager_.UploadChunk(
                    transfer_id, offset, data, data_crc32);
            }

            if (result.code == CHECKSUM_MISMATCH) {
                crc_failure_.fetch_add(1, std::memory_order_relaxed);
            } else if (result.code == CHUNK_CONFLICT) {
                session_conflict_.fetch_add(1, std::memory_order_relaxed);
            }
            if (result.duplicate) {
                duplicate_chunk_.fetch_add(1, std::memory_order_relaxed);
            }
            if (result.Ok() && !result.duplicate) {
                bytes_transferred_.fetch_add(
                    data.size(), std::memory_order_relaxed);
            }

            FillResult(response->mutable_result(), result.code, result.err_msg);

            response->set_acknowledged_offset(
                result.acknowledged_offset);
            response->set_total_received_size(
                result.total_received_size);
            response->set_duplicate(result.duplicate);
            

            if(done != nullptr) done->Run();
        }, controller, response->mutable_result(), done
    );
}

void FileTransferServiceImpl::QueryUploadStatus(
    google::protobuf::RpcController* controller,
    const ::mprpc::file::QueryUploadStatusRequest* request,
    ::mprpc::file::QueryUploadStatusResponse* response,
    ::google::protobuf::Closure* done)
{
    std::string transfer_id = request->transfer_id();
    SubmitTask(
        [this, transfer_id = std::move(transfer_id), response, done] {
            const auto result = manager_.QueryUploadStatus(transfer_id);
            FillResult(response->mutable_result(), result.code,
                       result.err_msg);
            if (result.Ok()) {
                response->set_file_size(result.file_size);
                response->set_chunk_size(result.chunk_size);
                response->set_received_size(result.received_size);
                response->set_chunk_count(result.chunk_count);
                response->set_received_bitmap(result.received_bitmap);
            }
            if (done != nullptr) {
                done->Run();
            }
        }, controller, response->mutable_result(), done);
}


void FileTransferServiceImpl::FinishUpload(google::protobuf::RpcController* controller,
                    const ::mprpc::file::FinishUploadRequest* request,
                    ::mprpc::file::FinishUploadResponse* response,
                    ::google::protobuf::Closure* done)
{
    std::string transfer_id = request->transfer_id();

    SubmitTask(
        [this,
        transfer_id = std::move(transfer_id),
        controller,
        response,
        done]{
            auto result = manager_.FinishUpload(
                transfer_id,
                [controller] {
                    return controller != nullptr &&
                           controller->IsCanceled();
                });

            FillResult(response->mutable_result(), result.code, result.err_msg);

            response->set_received_size(result.received_size);

            if(done != nullptr) done->Run();
        }, controller, response->mutable_result(), done
    );
}


void FileTransferServiceImpl::AbortUpload(google::protobuf::RpcController* controller,
                    const ::mprpc::file::AbortUploadRequest* request,
                    ::mprpc::file::AbortUploadResponse* response,
                    ::google::protobuf::Closure* done)
{
    std::string transfer_id = request->transfer_id();

    SubmitTask(
        [this,
        transfer_id = std::move(transfer_id),
        response,
        done]{
            auto result = manager_.AbortUpload(transfer_id);

            FillResult(response->mutable_result(), result.code, result.err_msg);

            if(done != nullptr) done->Run();
        }, controller, response->mutable_result(), done
    );
}

ServiceTaskStats FileTransferServiceImpl::GetTaskStats() const noexcept
{
    return ServiceTaskStats{
        worker_pool_.Accepted(),
        worker_pool_.Rejected(),
        worker_pool_.Completed(),
        worker_pool_.CurrentOutstanding(),
        worker_pool_.PeakOutstanding(),
        cancelled_.load(std::memory_order_relaxed),
        crc_failure_.load(std::memory_order_relaxed),
        session_conflict_.load(std::memory_order_relaxed),
        duplicate_chunk_.load(std::memory_order_relaxed),
        bytes_transferred_.load(std::memory_order_relaxed)};
}

bool FileTransferServiceImpl::SubmitTask(
    BoundedExecutor::Task task,
    google::protobuf::RpcController* controller,
    FileResult* response_result,
    google::protobuf::Closure* done)
{
    const auto finish_cancelled = [response_result, done] {
        FillResult(response_result, FILE_CANCELLED,
                   "file transfer call cancelled");
        if (done != nullptr) {
            done->Run();
        }
    };

    if (controller != nullptr && controller->IsCanceled()) {
        cancelled_.fetch_add(1, std::memory_order_relaxed);
        finish_cancelled();
        return false;
    }

    auto cancellable_task =
        [this, task = std::move(task), controller,
         finish_cancelled]() mutable {
            if (controller != nullptr && controller->IsCanceled()) {
                cancelled_.fetch_add(1, std::memory_order_relaxed);
                finish_cancelled();
                return;
            }
            task();
        };

    if (worker_pool_.TrySubmit(std::move(cancellable_task))) {
        return true;
    }

    // 快速拒绝，避免 Provider I/O 线程阻塞等待队列。
    FillResult(response_result, SERVER_BUSY,
               "file transfer worker queue is full");
    if (done != nullptr) {
        done->Run();
    }
    return false;
}

void FileTransferServiceImpl::FillResult(
    FileResult* response_result,
    FileErrorCode code,
    const std::string& error_message
)
{
    if(response_result == nullptr) return;

    response_result->set_code(code);
    response_result->set_error_message(error_message);
}

// 后台周期检查空闲 Session，不占用 RPC 业务 worker。
void FileTransferServiceImpl::CleanupLoop()
{
    std::unique_lock<std::mutex> lock(cleanup_mutex_);

    while (!stopping_) {
        const bool stopping = cleanup_cv_.wait_for(
            lock,
            cleanup_interval_,
            [this] {
                return stopping_;
            });

        if (stopping) {
            break;
        }

        lock.unlock();
        ReportCleanupErrors(
            manager_.CleanupExpiredSessions(session_timeout_));
        lock.lock();
    }
}

void FileTransferServiceImpl::ReportCleanupErrors(
    const CleanupResult& result) const
{
    for (const auto& error : result.errors) {
        std::cerr << "[FileTransferCleanup] " << error << '\n';
    }
}

} 
