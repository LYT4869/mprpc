#include "upload_session_manager.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <unordered_set>
#include <utility>

#include <openssl/evp.h>
#include <zlib.h>

namespace mprpc::file
{
namespace
{
constexpr std::size_t kHashReadBufferSize = 64U * 1024U;

struct Sha256Result
{
    bool ok = false;
    std::string hex_digest;
    std::string error_msg;
};

struct EvpMdDeleter
{
    void operator()(EVP_MD_CTX* context) const noexcept
    {
        EVP_MD_CTX_free(context);
    }
};

using EvpMdCtxPtr = std::unique_ptr<EVP_MD_CTX, EvpMdDeleter>;

Sha256Result ComputeFileSha256(const std::filesystem::path& path)
{
    Sha256Result result;
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        result.error_msg = "failed to open file for SHA-256";
        return result;
    }

    EvpMdCtxPtr context(EVP_MD_CTX_new());
    if (!context ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        result.error_msg = "failed to initialize SHA-256";
        return result;
    }

    std::array<char, kHashReadBufferSize> buffer{};
    while (input) {
        input.read(buffer.data(),
                   static_cast<std::streamsize>(buffer.size()));
        const std::streamsize bytes_read = input.gcount();
        if (bytes_read > 0 &&
            EVP_DigestUpdate(context.get(), buffer.data(),
                             static_cast<std::size_t>(bytes_read)) != 1) {
            result.error_msg = "failed to update SHA-256";
            return result;
        }
    }
    if (!input.eof()) {
        result.error_msg = "failed to read file for SHA-256";
        return result;
    }

    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(context.get(), digest.data(), &digest_size) != 1) {
        result.error_msg = "failed to finalize SHA-256";
        return result;
    }

    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < digest_size; ++i) {
        stream << std::setw(2)
               << static_cast<unsigned int>(digest[i]);
    }
    result.ok = true;
    result.hex_digest = stream.str();
    return result;
}

bool WriteAllAt(int fd, const char* data, std::size_t size,
                uint64_t offset, std::string* error_message)
{
    // pwrite 不改变共享文件偏移，并且可能只完成部分写入。
    std::size_t written = 0;
    while (written < size) {
        const ssize_t count = ::pwrite(
            fd, data + written, size - written,
            static_cast<off_t>(offset + written));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            *error_message = "pwrite failed: " +
                             std::string(std::strerror(errno));
            return false;
        }
        if (count == 0) {
            *error_message = "pwrite made no progress";
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    return true;
}

bool ReadEqualsAt(int fd, const std::string& expected, uint64_t offset,
                  std::string* error_message)
{
    std::string actual(expected.size(), '\0');
    std::size_t read = 0;
    while (read < actual.size()) {
        const ssize_t count = ::pread(
            fd, actual.data() + read, actual.size() - read,
            static_cast<off_t>(offset + read));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            *error_message = "pread failed: " +
                             std::string(std::strerror(errno));
            return false;
        }
        if (count == 0) {
            *error_message = "unexpected end of temporary file";
            return false;
        }
        read += static_cast<std::size_t>(count);
    }
    return actual == expected;
}

bool WriteFileAtomically(const std::filesystem::path& path,
                         const std::string& data,
                         std::string* error_message)
{
    // 恢复时只会看到旧 sidecar 或完整写入的新 sidecar。
    const std::filesystem::path temporary = path.string() + ".tmp";
    UniqueFd fd(::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644));
    if (!fd.Valid()) {
        *error_message = "failed to open metadata: " +
                         std::string(std::strerror(errno));
        return false;
    }

    std::size_t written = 0;
    while (written < data.size()) {
        const ssize_t count = ::write(
            fd.Get(), data.data() + written, data.size() - written);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            *error_message = "failed to write metadata: " +
                             std::string(std::strerror(errno));
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    if (::fsync(fd.Get()) != 0) {
        *error_message = "failed to sync metadata: " +
                         std::string(std::strerror(errno));
        return false;
    }
    fd.Reset();

    std::error_code rename_error;
    std::filesystem::rename(temporary, path, rename_error);
    if (rename_error) {
        *error_message = "failed to publish metadata: " +
                         rename_error.message();
        return false;
    }
    return true;
}

uint64_t ChunkLength(const UploadSession& session, uint32_t index)
{
    const uint64_t offset = static_cast<uint64_t>(index) * session.chunk_size;
    return std::min<uint64_t>(session.chunk_size,
                              session.expected_size - offset);
}
} // namespace

UploadSessionManager::UploadSessionManager(std::filesystem::path upload_root)
    : upload_root_(std::move(upload_root)),
      temporary_directory_(upload_root_ / "temporary"),
      completed_directory_(upload_root_ / "completed")
{
    std::error_code error;
    std::filesystem::create_directories(temporary_directory_, error);
    error.clear();
    std::filesystem::create_directories(completed_directory_, error);
    RecoverSessions();
}

BeginUploadResult UploadSessionManager::BeginUpload(
    const std::string& file_name, uint64_t file_size,
    const std::string& file_sha256, uint32_t preferred_chunk_size,
    const std::string& requested_transfer_id)
{
    BeginUploadResult result;
    if (!IsValidFileName(file_name)) {
        result.code = INVALID_ARGUMENT;
        result.err_msg = "invalid file name";
        return result;
    }
    if (file_sha256.size() != 64 ||
        !std::all_of(file_sha256.begin(), file_sha256.end(),
                     [](unsigned char c) { return std::isxdigit(c) != 0; })) {
        result.code = INVALID_ARGUMENT;
        result.err_msg = "invalid SHA-256";
        return result;
    }
    if (!requested_transfer_id.empty() &&
        !IsValidTransferId(requested_transfer_id)) {
        result.code = INVALID_ARGUMENT;
        result.err_msg = "invalid requested transfer id";
        return result;
    }
    if (file_size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        result.code = SIZE_EXCEEDED;
        result.err_msg = "file is too large for this platform";
        return result;
    }

    auto session = std::make_shared<UploadSession>();
    session->original_file_name = file_name;
    session->expected_size = file_size;
    session->chunk_size = NegotiateChunkSize(preferred_chunk_size);
    session->expected_sha256 = file_sha256;
    std::transform(session->expected_sha256.begin(),
                   session->expected_sha256.end(),
                   session->expected_sha256.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });

    const uint64_t count = file_size == 0
        ? 0
        : ((file_size - 1) / session->chunk_size) + 1;
    if (count > std::numeric_limits<uint32_t>::max()) {
        result.code = SIZE_EXCEEDED;
        result.err_msg = "file requires too many chunks";
        return result;
    }
    session->chunk_count = static_cast<uint32_t>(count);
    session->chunks.assign(session->chunk_count, ChunkState::Missing);
    session->chunk_crc32.assign(session->chunk_count, 0);

    std::lock_guard<std::mutex> sessions_lock(sessions_mutex_);
    if (!requested_transfer_id.empty()) {
        const auto existing = sessions_.find(requested_transfer_id);
        if (existing != sessions_.end()) {
            std::lock_guard<std::mutex> lock(existing->second->mutex);
            const UploadSession& current = *existing->second;
            if (current.original_file_name != file_name ||
                current.expected_size != file_size ||
                current.expected_sha256 != session->expected_sha256) {
                result.code = CHUNK_CONFLICT;
                result.err_msg = "transfer id belongs to a different upload";
                return result;
            }
            result.transfer_id = current.transfer_id;
            result.accepted_chunk_size = current.chunk_size;
            return result;
        }
        session->transfer_id = requested_transfer_id;
    } else {
        do {
            session->transfer_id = GenerateTransferId();
        } while (sessions_.find(session->transfer_id) != sessions_.end());
    }

    session->temporary_path =
        temporary_directory_ / (session->transfer_id + ".part");
    session->metadata_path =
        temporary_directory_ / (session->transfer_id + ".meta");
    session->final_path = completed_directory_ /
        (session->transfer_id + "_" + file_name);

    session->file.Reset(::open(session->temporary_path.c_str(),
                              O_CREAT | O_EXCL | O_RDWR, 0644));
    // ftruncate 固定逻辑大小，但不保证提前分配磁盘块。
    if (!session->file.Valid() ||
        ::ftruncate(session->file.Get(), static_cast<off_t>(file_size)) != 0) {
        result.code = FILE_IO_ERROR;
        result.err_msg = "failed to create sized temporary file: " +
                         std::string(std::strerror(errno));
        session->file.Reset();
        std::error_code ignored;
        std::filesystem::remove(session->temporary_path, ignored);
        return result;
    }

    std::string persist_error;
    if (!PersistSessionLocked(*session, &persist_error)) {
        result.code = FILE_IO_ERROR;
        result.err_msg = persist_error;
        session->file.Reset();
        std::error_code ignored;
        std::filesystem::remove(session->temporary_path, ignored);
        std::filesystem::remove(session->metadata_path, ignored);
        return result;
    }

    sessions_.emplace(session->transfer_id, session);
    result.transfer_id = session->transfer_id;
    result.accepted_chunk_size = session->chunk_size;
    return result;
}

UploadChunkResult UploadSessionManager::UploadChunk(
    const std::string& transfer_id, uint64_t offset,
    const std::string& data, uint32_t data_crc32)
{
    UploadChunkResult result;
    result.acknowledged_offset = offset;
    const auto session = FindSession(transfer_id);
    if (!session) {
        result.code = TRANSFER_NOT_FOUND;
        result.err_msg = "transfer session not found";
        return result;
    }

    const uint32_t actual_crc32 = static_cast<uint32_t>(
        ::crc32(0L, reinterpret_cast<const Bytef*>(data.data()),
                static_cast<uInt>(data.size())));
    if (actual_crc32 != data_crc32) {
        result.code = CHECKSUM_MISMATCH;
        result.err_msg = "chunk CRC32 mismatch";
        return result;
    }

    uint32_t chunk_index = 0;
    {
        std::lock_guard<std::mutex> lock(session->mutex);
        result.total_received_size = session->received_size;
        if (session->state != UploadState::Active) {
            result.code = INVALID_STATE;
            result.err_msg = "session is not active";
            return result;
        }
        if (session->expected_size == 0 || data.empty()) {
            result.code = INVALID_ARGUMENT;
            result.err_msg = "empty files do not accept chunks";
            return result;
        }
        if (offset >= session->expected_size ||
            offset % session->chunk_size != 0) {
            result.code = INVALID_OFFSET;
            result.err_msg = "offset must identify a chunk boundary";
            return result;
        }

        chunk_index = static_cast<uint32_t>(offset / session->chunk_size);
        const uint64_t expected_length = ChunkLength(*session, chunk_index);
        if (data.size() != expected_length) {
            result.code = INVALID_ARGUMENT;
            result.err_msg = "chunk length does not match its offset";
            return result;
        }

        if (session->chunks[chunk_index] == ChunkState::Writing) {
            result.code = SERVER_BUSY;
            result.err_msg = "chunk is already being written";
            return result;
        }
        if (session->chunks[chunk_index] == ChunkState::Received) {
            // CRC 用于快速筛选，内容比较用于排除碰撞。
            if (session->chunk_crc32[chunk_index] != data_crc32) {
                result.code = CHUNK_CONFLICT;
                result.err_msg = "duplicate chunk has different CRC32";
                return result;
            }
            std::string read_error;
            if (!ReadEqualsAt(session->file.Get(), data, offset, &read_error)) {
                result.code = read_error.empty() ? CHUNK_CONFLICT : FILE_IO_ERROR;
                result.err_msg = read_error.empty()
                    ? "duplicate chunk content conflicts with stored data"
                    : std::move(read_error);
                return result;
            }
            result.duplicate = true;
            return result;
        }
        // 锁内预占分片，耗时的磁盘 I/O 放到锁外执行。
        session->chunks[chunk_index] = ChunkState::Writing;
    }

    std::string write_error;
    if (!WriteAllAt(session->file.Get(), data.data(), data.size(),
                    offset, &write_error)) {
        std::lock_guard<std::mutex> lock(session->mutex);
        if (session->chunks[chunk_index] == ChunkState::Writing) {
            session->chunks[chunk_index] = ChunkState::Missing;
        }
        result.code = FILE_IO_ERROR;
        result.err_msg = std::move(write_error);
        result.total_received_size = session->received_size;
        return result;
    }

    // 将 bitmap 和 sidecar 作为一次串行状态提交。
    std::lock_guard<std::mutex> metadata_lock(session->metadata_mutex);
    std::lock_guard<std::mutex> lock(session->mutex);
    if (session->state != UploadState::Active ||
        session->chunks[chunk_index] != ChunkState::Writing) {
        result.code = INVALID_STATE;
        result.err_msg = "session stopped while writing chunk";
        return result;
    }

    session->chunks[chunk_index] = ChunkState::Received;
    session->chunk_crc32[chunk_index] = data_crc32;
    session->received_size += data.size();
    session->last_activity = std::chrono::steady_clock::now();

    std::string persist_error;
    if (!PersistSessionLocked(*session, &persist_error)) {
        session->chunks[chunk_index] = ChunkState::Missing;
        session->chunk_crc32[chunk_index] = 0;
        session->received_size -= data.size();
        result.code = FILE_IO_ERROR;
        result.err_msg = std::move(persist_error);
        result.total_received_size = session->received_size;
        return result;
    }

    result.total_received_size = session->received_size;
    return result;
}

QueryUploadStatusResult UploadSessionManager::QueryUploadStatus(
    const std::string& transfer_id)
{
    QueryUploadStatusResult result;
    const auto session = FindSession(transfer_id);
    if (!session) {
        result.code = TRANSFER_NOT_FOUND;
        result.err_msg = "transfer session not found";
        return result;
    }

    std::lock_guard<std::mutex> lock(session->mutex);
    result.file_size = session->expected_size;
    result.chunk_size = session->chunk_size;
    result.received_size = session->received_size;
    result.chunk_count = session->chunk_count;
    result.received_bitmap = BuildBitmap(*session);
    return result;
}

FinishUploadResult UploadSessionManager::FinishUpload(
    const std::string& transfer_id,
    std::function<bool()> is_cancelled)
{
    FinishUploadResult result;
    const auto session = FindSession(transfer_id);
    if (!session) {
        result.code = TRANSFER_NOT_FOUND;
        result.err_msg = "transfer session not found";
        return result;
    }

    std::filesystem::path temporary_path;
    std::filesystem::path final_path;
    std::string expected_sha256;
    {
        std::lock_guard<std::mutex> metadata_lock(session->metadata_mutex);
        std::lock_guard<std::mutex> lock(session->mutex);
        result.received_size = session->received_size;
        if (session->state != UploadState::Active) {
            result.code = INVALID_STATE;
            result.err_msg = "session is not active";
            return result;
        }
        if (std::any_of(session->chunks.begin(), session->chunks.end(),
                        [](ChunkState state) {
                            return state != ChunkState::Received;
                        })) {
            result.code = INVALID_STATE;
            result.err_msg = "file has missing chunks";
            return result;
        }
        if (is_cancelled && is_cancelled()) {
            result.code = FILE_CANCELLED;
            result.err_msg = "finish upload cancelled";
            return result;
        }

        // Finishing 阻止新分片进入，哈希和 rename 可在锁外执行。
        session->state = UploadState::Finishing;
        if (::fsync(session->file.Get()) != 0) {
            session->state = UploadState::Failed;
            result.code = FILE_IO_ERROR;
            result.err_msg = "failed to sync temporary file: " +
                             std::string(std::strerror(errno));
            return result;
        }
        temporary_path = session->temporary_path;
        final_path = session->final_path;
        expected_sha256 = session->expected_sha256;
    }

    const auto restore_after_cancel = [&] {
        std::lock_guard<std::mutex> lock(session->mutex);
        session->state = UploadState::Active;
        result.code = FILE_CANCELLED;
        result.err_msg = "finish upload cancelled";
    };

    if (is_cancelled && is_cancelled()) {
        restore_after_cancel();
        return result;
    }

    const Sha256Result sha256 = ComputeFileSha256(temporary_path);
    if (!sha256.ok || sha256.hex_digest != expected_sha256) {
        std::lock_guard<std::mutex> lock(session->mutex);
        session->state = UploadState::Failed;
        result.code = sha256.ok ? CHECKSUM_MISMATCH : FILE_IO_ERROR;
        result.err_msg = sha256.ok
            ? "file SHA-256 mismatch"
            : "failed to compute SHA-256: " + sha256.error_msg;
        return result;
    }

    if (is_cancelled && is_cancelled()) {
        restore_after_cancel();
        return result;
    }

    std::error_code error;
    std::filesystem::rename(temporary_path, final_path, error);
    if (error) {
        std::lock_guard<std::mutex> lock(session->mutex);
        session->state = UploadState::Failed;
        result.code = FILE_IO_ERROR;
        result.err_msg = "failed to publish final file: " + error.message();
        return result;
    }
    std::filesystem::remove(session->metadata_path, error);

    {
        std::lock_guard<std::mutex> lock(session->mutex);
        session->state = UploadState::Completed;
        session->file.Reset();
    }
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        const auto it = sessions_.find(transfer_id);
        if (it != sessions_.end() && it->second == session) {
            sessions_.erase(it);
        }
    }
    return result;
}

AbortUploadResult UploadSessionManager::AbortUpload(
    const std::string& transfer_id)
{
    AbortUploadResult result;
    const auto session = FindSession(transfer_id);
    if (!session) {
        result.code = TRANSFER_NOT_FOUND;
        result.err_msg = "transfer session not found";
        return result;
    }

    {
        std::lock_guard<std::mutex> metadata_lock(session->metadata_mutex);
        std::lock_guard<std::mutex> lock(session->mutex);
        if (session->state == UploadState::Finishing ||
            session->state == UploadState::Completed) {
            result.code = INVALID_STATE;
            result.err_msg = "session cannot be aborted";
            return result;
        }
        if (std::any_of(session->chunks.begin(), session->chunks.end(),
                        [](ChunkState state) {
                            return state == ChunkState::Writing;
                        })) {
            result.code = SERVER_BUSY;
            result.err_msg = "session has chunks being written";
            return result;
        }
        session->state = UploadState::Aborted;
        session->file.Reset();
    }

    std::error_code error;
    std::filesystem::remove(session->temporary_path, error);
    error.clear();
    std::filesystem::remove(session->metadata_path, error);
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        const auto it = sessions_.find(transfer_id);
        if (it != sessions_.end() && it->second == session) {
            sessions_.erase(it);
        }
    }
    return result;
}

std::shared_ptr<UploadSession> UploadSessionManager::FindSession(
    const std::string& transfer_id)
{
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    const auto it = sessions_.find(transfer_id);
    return it == sessions_.end() ? nullptr : it->second;
}

CleanupResult UploadSessionManager::CleanupExpiredSessions(
    std::chrono::steady_clock::duration max_idle)
{
    CleanupResult result;
    std::vector<std::shared_ptr<UploadSession>> candidates;
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (const auto& entry : sessions_) {
            candidates.push_back(entry.second);
        }
    }

    const auto now = std::chrono::steady_clock::now();
    for (const auto& session : candidates) {
        bool expired = false;
        {
            std::lock_guard<std::mutex> metadata_lock(session->metadata_mutex);
            std::lock_guard<std::mutex> lock(session->mutex);
            const bool writing = std::any_of(
                session->chunks.begin(), session->chunks.end(),
                [](ChunkState state) { return state == ChunkState::Writing; });
            if (session->state != UploadState::Active || writing ||
                now - session->last_activity < max_idle) {
                continue;
            }
            session->state = UploadState::Expired;
            session->file.Reset();
            expired = true;
        }
        if (!expired) {
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            const auto it = sessions_.find(session->transfer_id);
            if (it != sessions_.end() && it->second == session) {
                sessions_.erase(it);
                ++result.sessions_removed;
            }
        }
        RemoveSessionFiles(*session, &result);
    }
    return result;
}

CleanupResult UploadSessionManager::CleanupOrphanedTemporaryFiles()
{
    CleanupResult result;
    std::unordered_set<std::string> active;
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (const auto& entry : sessions_) {
            active.insert(entry.second->temporary_path.lexically_normal().string());
            active.insert(entry.second->metadata_path.lexically_normal().string());
        }
    }

    std::error_code iterator_error;
    std::filesystem::directory_iterator iterator(
        temporary_directory_, iterator_error);
    for (const std::filesystem::directory_iterator end;
         !iterator_error && iterator != end; iterator.increment(iterator_error)) {
        std::error_code type_error;
        if (!iterator->is_regular_file(type_error) || type_error) {
            continue;
        }
        const auto path = iterator->path();
        const std::string normalized = path.lexically_normal().string();
        const bool managed_extension = path.extension() == ".part" ||
            path.extension() == ".meta" || path.extension() == ".tmp";
        if (managed_extension && active.find(normalized) == active.end()) {
            std::error_code remove_error;
            if (std::filesystem::remove(path, remove_error)) {
                ++result.files_removed;
            } else if (remove_error) {
                result.errors.push_back("failed to remove orphan " +
                                        path.string() + ": " +
                                        remove_error.message());
            }
        }
    }
    if (iterator_error) {
        result.errors.push_back("failed to scan temporary directory: " +
                                iterator_error.message());
    }
    return result;
}

bool UploadSessionManager::PersistSessionLocked(
    const UploadSession& session, std::string* error_message)
{
    UploadSessionMetadata metadata;
    metadata.set_transfer_id(session.transfer_id);
    metadata.set_original_file_name(session.original_file_name);
    metadata.set_expected_size(session.expected_size);
    metadata.set_chunk_size(session.chunk_size);
    metadata.set_expected_sha256(session.expected_sha256);
    metadata.set_received_bitmap(BuildBitmap(session));
    for (uint32_t crc : session.chunk_crc32) {
        metadata.add_chunk_crc32(crc);
    }

    std::string serialized;
    if (!metadata.SerializeToString(&serialized)) {
        *error_message = "failed to serialize upload metadata";
        return false;
    }
    return WriteFileAtomically(session.metadata_path, serialized,
                               error_message);
}

void UploadSessionManager::RecoverSessions()
{
    std::error_code iterator_error;
    std::filesystem::directory_iterator iterator(
        temporary_directory_, iterator_error);
    for (const std::filesystem::directory_iterator end;
         !iterator_error && iterator != end; iterator.increment(iterator_error)) {
        const std::filesystem::path metadata_path = iterator->path();
        if (metadata_path.extension() != ".meta" ||
            !iterator->is_regular_file()) {
            continue;
        }

        std::ifstream input(metadata_path, std::ios::binary);
        std::string serialized((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
        UploadSessionMetadata metadata;
        auto session = std::make_shared<UploadSession>();
        bool valid = input.is_open() && metadata.ParseFromString(serialized) &&
            IsValidTransferId(metadata.transfer_id()) &&
            metadata_path.stem() == metadata.transfer_id() &&
            IsValidFileName(metadata.original_file_name()) &&
            metadata.chunk_size() > 0 &&
            metadata.chunk_size() <= kMaxChunkSize &&
            metadata.expected_sha256().size() == 64;

        if (valid) {
            session->transfer_id = metadata.transfer_id();
            session->original_file_name = metadata.original_file_name();
            session->expected_size = metadata.expected_size();
            session->chunk_size = metadata.chunk_size();
            session->expected_sha256 = metadata.expected_sha256();
            const uint64_t count = session->expected_size == 0
                ? 0
                : ((session->expected_size - 1) / session->chunk_size) + 1;
            valid = count <= std::numeric_limits<uint32_t>::max();
            session->chunk_count = static_cast<uint32_t>(count);
            session->chunks.assign(session->chunk_count, ChunkState::Missing);
            session->chunk_crc32.assign(session->chunk_count, 0);
            session->temporary_path = temporary_directory_ /
                (session->transfer_id + ".part");
            session->metadata_path = metadata_path;
            session->final_path = completed_directory_ /
                (session->transfer_id + "_" + session->original_file_name);
        }

        std::error_code size_error;
        if (valid &&
            (!std::filesystem::is_regular_file(session->temporary_path) ||
             std::filesystem::file_size(session->temporary_path, size_error) !=
                 session->expected_size || size_error ||
             metadata.chunk_crc32_size() !=
                 static_cast<int>(session->chunk_count) ||
             !RestoreBitmap(metadata.received_bitmap(), session.get()))) {
            valid = false;
        }

        if (valid) {
            // received_size 根据持久化 bitmap 重新计算。
            for (uint32_t i = 0; i < session->chunk_count; ++i) {
                session->chunk_crc32[i] = metadata.chunk_crc32(i);
                if (session->chunks[i] == ChunkState::Received) {
                    session->received_size += ChunkLength(*session, i);
                }
            }
            session->file.Reset(::open(session->temporary_path.c_str(), O_RDWR));
            valid = session->file.Valid();
        }

        if (valid) {
            sessions_.emplace(session->transfer_id, std::move(session));
        } else {
            std::error_code ignored;
            std::filesystem::remove(metadata_path, ignored);
            std::filesystem::remove(
                temporary_directory_ / (metadata_path.stem().string() + ".part"),
                ignored);
        }
    }
}

void UploadSessionManager::RemoveSessionFiles(
    const UploadSession& session, CleanupResult* result)
{
    for (const auto& path : {session.temporary_path, session.metadata_path}) {
        std::error_code error;
        const bool removed = std::filesystem::remove(path, error);
        if (result != nullptr) {
            if (removed) {
                ++result->files_removed;
            } else if (error) {
                result->errors.push_back("failed to remove " + path.string() +
                                         ": " + error.message());
            }
        }
    }
}

std::string UploadSessionManager::BuildBitmap(const UploadSession& session)
{
    std::string bitmap((session.chunk_count + 7U) / 8U, '\0');
    for (uint32_t i = 0; i < session.chunk_count; ++i) {
        if (session.chunks[i] == ChunkState::Received) {
            bitmap[i / 8U] = static_cast<char>(
                static_cast<unsigned char>(bitmap[i / 8U]) |
                static_cast<unsigned char>(1U << (i % 8U)));
        }
    }
    return bitmap;
}

bool UploadSessionManager::RestoreBitmap(
    const std::string& bitmap, UploadSession* session)
{
    if (session == nullptr ||
        bitmap.size() != (session->chunk_count + 7U) / 8U) {
        return false;
    }
    for (uint32_t i = 0; i < session->chunk_count; ++i) {
        const bool received =
            (static_cast<unsigned char>(bitmap[i / 8U]) &
             static_cast<unsigned char>(1U << (i % 8U))) != 0;
        session->chunks[i] = received
            ? ChunkState::Received : ChunkState::Missing;
    }
    return true;
}

bool UploadSessionManager::IsValidFileName(const std::string& file_name)
{
    if (file_name.empty() || file_name == "." || file_name == ".." ||
        file_name.find('/') != std::string::npos ||
        file_name.find('\\') != std::string::npos ||
        file_name.find('\0') != std::string::npos) {
        return false;
    }
    const std::filesystem::path path(file_name);
    return !path.has_parent_path() && !path.is_absolute() &&
           path.filename() == path;
}

bool UploadSessionManager::IsValidTransferId(const std::string& transfer_id)
{
    return transfer_id.size() == 32 &&
        std::all_of(transfer_id.begin(), transfer_id.end(),
                    [](unsigned char c) { return std::isxdigit(c) != 0; });
}

uint32_t UploadSessionManager::NegotiateChunkSize(uint32_t preferred_chunk_size)
{
    return preferred_chunk_size == 0
        ? kDefaultChunkSize
        : std::min(preferred_chunk_size, kMaxChunkSize);
}

std::string UploadSessionManager::GenerateTransferId()
{
    thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution;
    std::ostringstream stream;
    stream << std::hex << std::setfill('0')
           << std::setw(16) << distribution(generator)
           << std::setw(16) << distribution(generator);
    return stream.str();
}
} // namespace mprpc::file
