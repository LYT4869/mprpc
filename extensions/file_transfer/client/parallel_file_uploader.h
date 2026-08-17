#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "file_transfer.pb.h"
#include "mprpcchannel.h"
#include "upload_types.h"

namespace mprpc::file
{
struct UploadOptions
{
    uint32_t preferred_chunk_size = 1U * 1024U * 1024U;
    uint32_t window_size = 8;
    uint32_t chunk_timeout_ms = 30000;
    uint32_t max_retries = 3;
    uint32_t initial_retry_backoff_ms = 100;
    std::string transfer_id;
};

class ParallelFileUploader
{
public:
    ParallelFileUploader();

    UploadFileResult Upload(
        const std::filesystem::path& local_path,
        std::string remote_file_name = {},
        UploadOptions options = {});

private:
    MprpcChannel channel_;
    FileTransferServiceRpc_Stub stub_;
};
} // namespace mprpc::file
