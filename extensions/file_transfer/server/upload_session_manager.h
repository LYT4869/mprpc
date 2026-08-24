#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "file_transfer.pb.h"
#include "upload_session.h"

namespace mprpc::file
{
struct BeginUploadResult
{
    FileErrorCode code = FILE_OK;
    std::string err_msg;
    std::string transfer_id;
    uint32_t accepted_chunk_size = 0;
    bool Ok() const noexcept { return code == FILE_OK; }
};

struct UploadChunkResult
{
    FileErrorCode code = FILE_OK;
    std::string err_msg;
    uint64_t acknowledged_offset = 0;
    uint64_t total_received_size = 0;
    bool duplicate = false;
    bool Ok() const noexcept { return code == FILE_OK; }
};

struct QueryUploadStatusResult
{
    FileErrorCode code = FILE_OK;
    std::string err_msg;
    uint64_t file_size = 0;
    uint32_t chunk_size = 0;
    uint64_t received_size = 0;
    uint32_t chunk_count = 0;
    std::string received_bitmap;
    bool Ok() const noexcept { return code == FILE_OK; }
};

struct AbortUploadResult
{
    FileErrorCode code = FILE_OK;
    std::string err_msg;
    bool Ok() const noexcept { return code == FILE_OK; }
};

struct FinishUploadResult
{
    FileErrorCode code = FILE_OK;
    std::string err_msg;
    uint64_t received_size = 0;
    bool Ok() const noexcept { return code == FILE_OK; }
};

struct CleanupResult
{
    std::size_t sessions_removed = 0;
    std::size_t files_removed = 0;
    std::vector<std::string> errors;
    bool Ok() const noexcept { return errors.empty(); }
};

// 管理上传会话、随机分片写入、幂等校验和重启恢复。
class UploadSessionManager
{
public:
    explicit UploadSessionManager(std::filesystem::path upload_root);

    // 校验文件信息并创建或恢复指定 transfer_id 的上传会话。
    BeginUploadResult BeginUpload(
        const std::string& file_name,
        uint64_t file_size,
        const std::string& file_sha256,
        uint32_t preferred_chunk_size,
        const std::string& requested_transfer_id = {});

    // 校验并通过 pwrite 将一个分片写入其固定 offset。
    UploadChunkResult UploadChunk(
        const std::string& transfer_id,
        uint64_t offset,
        const std::string& data,
        uint32_t data_crc32);

    // 返回文件参数、已接收字节数和压缩分片位图。
    QueryUploadStatusResult QueryUploadStatus(
        const std::string& transfer_id);

    // 中止会话并删除其临时文件和 sidecar。
    AbortUploadResult AbortUpload(const std::string& transfer_id);

    // 校验分片完整性和 SHA-256，随后原子发布最终文件。
    FinishUploadResult FinishUpload(
        const std::string& transfer_id,
        std::function<bool()> is_cancelled = {});

    std::shared_ptr<UploadSession> FindSession(
        const std::string& transfer_id);
    // 清理长时间无活动的会话及其持久化文件。
    CleanupResult CleanupExpiredSessions(
        std::chrono::steady_clock::duration max_idle);

    // 删除启动时发现的无合法会话归属的临时文件。
    CleanupResult CleanupOrphanedTemporaryFiles();

private:
    static constexpr uint32_t kDefaultChunkSize = 1U * 1024U * 1024U;
    static constexpr uint32_t kMaxChunkSize = 4U * 1024U * 1024U;

    static bool IsValidFileName(const std::string& file_name);
    static bool IsValidTransferId(const std::string& transfer_id);
    static uint32_t NegotiateChunkSize(uint32_t preferred_chunk_size);
    static std::string GenerateTransferId();
    static std::string BuildBitmap(const UploadSession& session);
    static bool RestoreBitmap(const std::string& bitmap,
                              UploadSession* session);

    // 将会话 bitmap 和分片 CRC 原子写入 Protobuf sidecar。
    bool PersistSessionLocked(const UploadSession& session,
                              std::string* error_message);

    // 服务启动时从合法的 .meta 与 .part 文件重建会话。
    void RecoverSessions();
    void RemoveSessionFiles(const UploadSession& session,
                            CleanupResult* result = nullptr);

    std::filesystem::path upload_root_;
    std::filesystem::path temporary_directory_;
    std::filesystem::path completed_directory_;
    std::mutex sessions_mutex_;
    std::unordered_map<std::string, std::shared_ptr<UploadSession>> sessions_;
};
} // namespace mprpc::file
