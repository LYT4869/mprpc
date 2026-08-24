#pragma once

#include <chrono>
#include <atomic>
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
// 文件业务线程池的累计接收、拒绝和完成计数。
struct ServiceTaskStats
{
    uint64_t accepted = 0;
    uint64_t rejected = 0;
    uint64_t completed = 0;
    uint64_t current_outstanding = 0;
    uint64_t peak_outstanding = 0;
    uint64_t cancelled = 0;
    uint64_t crc_failure = 0;
    uint64_t session_conflict = 0;
    uint64_t duplicate_chunk = 0;
    uint64_t bytes_transferred = 0;
};

// Protobuf 文件服务实现，将磁盘操作投递到有界工作线程池。
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

    // 创建上传会话并协商实际分片大小。
    void BeginUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::BeginUploadRequest* request,
                        ::mprpc::file::BeginUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 校验 CRC32 并提交一个文件分片。
    void UploadChunk(google::protobuf::RpcController* controller,
                        const ::mprpc::file::UploadChunkRequest* request,
                        ::mprpc::file::UploadChunkResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 完成整文件校验并发布文件。
    void FinishUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::FinishUploadRequest* request,
                        ::mprpc::file::FinishUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 主动终止并清理上传会话。
    void AbortUpload(google::protobuf::RpcController* controller,
                        const ::mprpc::file::AbortUploadRequest* request,
                        ::mprpc::file::AbortUploadResponse* response,
                        ::google::protobuf::Closure* done) override;

    // 查询断点续传需要的分片位图和进度。
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
    // 非阻塞投递业务任务，过载时立即返回 SERVER_BUSY。
    bool SubmitTask(BoundedExecutor::Task task,
                    google::protobuf::RpcController* controller,
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
    std::atomic<uint64_t> cancelled_{0};
    std::atomic<uint64_t> crc_failure_{0};
    std::atomic<uint64_t> session_conflict_{0};
    std::atomic<uint64_t> duplicate_chunk_{0};
    std::atomic<uint64_t> bytes_transferred_{0};
};
}
