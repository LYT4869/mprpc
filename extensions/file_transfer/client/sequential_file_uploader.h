#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "file_transfer.pb.h"
#include "mprpcchannel.h"
#include "upload_types.h"

namespace mprpc::file
{
// 按 offset 顺序同步上传分片，作为正确性基线实现。
class SequentialFileUploader
{
public:
    explicit SequentialFileUploader(uint32_t timeout_ms = 30000);

    // 完整执行 Begin、逐块 UploadChunk 和 Finish 流程。
    UploadFileResult Upload(
        const std::filesystem::path& local_path,
        std::string remote_file_name = {},
        uint32_t preferred_chunk_size = 1024U * 1024U);

private:
    void AbortBestEffort(const std::string& transfer_id);

private:
    MprpcChannel channel_;
    FileTransferServiceRpc_Stub stub_;
    uint32_t timeout_ms_;
};
} // namespace mprpc::file
