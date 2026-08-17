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

enum class UploadState
{
    Active,
    Finishing,
    Completed,
    Aborted,
    Failed,
    Expired,
};

enum class ChunkState : uint8_t
{
    Missing,
    Writing,
    Received,
};

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
    std::mutex metadata_mutex;

    UploadSession() = default;
    UploadSession(const UploadSession&) = delete;
    UploadSession& operator=(const UploadSession&) = delete;
};
} // namespace mprpc::file
