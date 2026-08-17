#pragma once

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>

#include "boundedexecutor.h"
#include "file_transfer.pb.h"
#include "upload_session_manager.h"

namespace mprpc::file
{
struct ServiceTaskStats
{
    uint64_t accepted = 0;
    uint64_t rejected = 0;
    uint64_t completed = 0;
};

class FileTransferServiceImpl final
    : public FileTransferServiceRpc
{
public:
    explicit FileTransferServiceImpl(
        std::filesystem::path upload_root,
        int worker_threads = 4,
        std::chrono::milliseconds session_timeout =
            std::chrono::minutes(10),
        std::chrono::milliseconds cleanup_interval =
            std::chrono::minutes(1),
        std::size_t max_outstanding = 32);

    ~FileTransferServiceImpl() override;

    void BeginUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::BeginUploadRequest* request,
                        ::mprpc::file::BeginUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    void UploadChunk(google::protobuf::RpcController* controller,
                        const ::mprpc::file::UploadChunkRequest* request,
                        ::mprpc::file::UploadChunkResponse* response,
                        ::google::protobuf::Closure* done) override;

    void FinishUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::FinishUploadRequest* request,
                        ::mprpc::file::FinishUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    void AbortUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::AbortUploadRequest* request,
                        ::mprpc::file::AbortUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    void QueryUploadStatus(
        google::protobuf::RpcController* controller,
        const ::mprpc::file::QueryUploadStatusRequest* request,
        ::mprpc::file::QueryUploadStatusResponse* response,
        ::google::protobuf::Closure* done) override;

    ServiceTaskStats GetTaskStats() const noexcept;

private:
    static void FillResult(
        FileResult* response_result,
        FileErrorCode code,
        const std::string& error_message
    );

    void CleanupLoop();
    void ReportCleanupErrors(const CleanupResult& result) const;
    bool SubmitTask(BoundedExecutor::Task task,
                    FileResult* response_result,
                    google::protobuf::Closure* done);

private:
    UploadSessionManager manager_;
    BoundedExecutor worker_pool_;
    std::chrono::milliseconds session_timeout_;
    std::chrono::milliseconds cleanup_interval_;
    std::mutex cleanup_mutex_;
    std::condition_variable cleanup_cv_;
    bool stopping_ = false;
    std::thread cleanup_thread_;
};
}
