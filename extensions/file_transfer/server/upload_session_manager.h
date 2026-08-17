#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
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

class UploadSessionManager
{
public:
    explicit UploadSessionManager(std::filesystem::path upload_root);

    BeginUploadResult BeginUpload(
        const std::string& file_name,
        uint64_t file_size,
        const std::string& file_sha256,
        uint32_t preferred_chunk_size,
        const std::string& requested_transfer_id = {});

    UploadChunkResult UploadChunk(
        const std::string& transfer_id,
        uint64_t offset,
        const std::string& data,
        uint32_t data_crc32);

    QueryUploadStatusResult QueryUploadStatus(
        const std::string& transfer_id);
    AbortUploadResult AbortUpload(const std::string& transfer_id);
    FinishUploadResult FinishUpload(const std::string& transfer_id);

    std::shared_ptr<UploadSession> FindSession(
        const std::string& transfer_id);
    CleanupResult CleanupExpiredSessions(
        std::chrono::steady_clock::duration max_idle);
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

    bool PersistSessionLocked(const UploadSession& session,
                              std::string* error_message);
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
