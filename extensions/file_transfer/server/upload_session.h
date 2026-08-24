#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

#include <unistd.h>

namespace mprpc::file
{
// 独占管理 POSIX 文件描述符，离开作用域时自动 close。
class UniqueFd
{
public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    ~UniqueFd() { Reset(); }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(other.Release()) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept
    {
        if (this != &other) {
            Reset(other.Release());
        }
        return *this;
    }

    int Get() const noexcept { return fd_; }
    bool Valid() const noexcept { return fd_ >= 0; }
    int Release() noexcept
    {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }
    void Reset(int fd = -1) noexcept
    {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

// 一次文件上传会话的生命周期状态。
enum class UploadState
{
    Active,
    Finishing,
    Completed,
    Aborted,
    Failed,
    Expired,
};

// 单个分片的写入状态，Writing 用于并发预占。
enum class ChunkState : uint8_t
{
    Missing,
    Writing,
    Received,
};

// 保存一次上传的文件信息、分片位图、校验值和持久化路径。
struct UploadSession
{
    std::string transfer_id;
    std::string original_file_name;

    std::filesystem::path temporary_path;
    std::filesystem::path metadata_path;
    std::filesystem::path final_path;

    uint64_t expected_size = 0;
    uint64_t received_size = 0;
    uint32_t chunk_size = 0;
    uint32_t chunk_count = 0;
    std::string expected_sha256;

    UniqueFd file;
    std::vector<ChunkState> chunks;
    std::vector<uint32_t> chunk_crc32;

    std::chrono::steady_clock::time_point last_activity =
        std::chrono::steady_clock::now();
    UploadState state = UploadState::Active;

    std::mutex mutex;
    // 串行化 sidecar 替换；同时加锁时必须先获取此锁。
    std::mutex metadata_mutex;

    UploadSession() = default;
    UploadSession(const UploadSession&) = delete;
    UploadSession& operator=(const UploadSession&) = delete;
};
} // namespace mprpc::file
