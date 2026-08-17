#include "sequential_file_uploader.h"

#include <algorithm>
#include <array>
#include <fstream>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <utility>

#include <openssl/evp.h>
#include <zlib.h>

#include "mprpccontroller.h"

namespace mprpc::file
{
namespace
{
constexpr std::size_t kHashReadBufferSize = 64U * 1024U;

std::string GenerateTransferId()
{
    thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution;
    std::ostringstream stream;
    stream << std::hex << std::setfill('0')
           << std::setw(16) << distribution(generator)
           << std::setw(16) << distribution(generator);
    return stream.str();
}

struct EvpMdCtxDeleter
{
    void operator()(EVP_MD_CTX* context) const noexcept
    {
        EVP_MD_CTX_free(context);
    }
};

using EvpMdCtxPtr = std::unique_ptr<EVP_MD_CTX, EvpMdCtxDeleter>;

bool ComputeFileSha256(
    const std::filesystem::path& path,
    std::string* hex_digest,
    std::string* error_message)
{
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        *error_message = "failed to open local file for SHA-256";
        return false;
    }

    EvpMdCtxPtr context(EVP_MD_CTX_new());
    if (!context ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        *error_message = "failed to initialize SHA-256";
        return false;
    }

    std::array<char, kHashReadBufferSize> buffer{};
    while (input) {
        input.read(
            buffer.data(),
            static_cast<std::streamsize>(buffer.size()));

        const std::streamsize bytes_read = input.gcount();
        if (bytes_read > 0 &&
            EVP_DigestUpdate(
                context.get(),
                buffer.data(),
                static_cast<std::size_t>(bytes_read)) != 1) {
            *error_message = "failed to update SHA-256";
            return false;
        }
    }

    if (!input.eof()) {
        *error_message = "failed to read local file for SHA-256";
        return false;
    }

    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(
            context.get(), digest.data(), &digest_size) != 1) {
        *error_message = "failed to finalize SHA-256";
        return false;
    }

    std::ostringstream stream;
    stream << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < digest_size; ++i) {
        stream << std::setw(2)
               << static_cast<unsigned int>(digest[i]);
    }

    *hex_digest = stream.str();
    return true;
}

template <typename Response>
bool CheckRpcAndBusinessResult(
    const std::string& operation,
    const MprpcController& controller,
    const Response& response,
    std::string* error_message)
{
    if (controller.Failed()) {
        *error_message = operation + " RPC failed: " +
                         controller.ErrorText();
        return false;
    }

    if (!response.has_result()) {
        *error_message = operation + " returned no business result";
        return false;
    }

    if (response.result().code() != FILE_OK) {
        *error_message = operation + " failed: " +
                         response.result().error_message();
        return false;
    }

    return true;
}
} // namespace

SequentialFileUploader::SequentialFileUploader(uint32_t timeout_ms)
    : stub_(&channel_), timeout_ms_(timeout_ms)
{
}

UploadFileResult SequentialFileUploader::Upload(
    const std::filesystem::path& local_path,
    std::string remote_file_name,
    uint32_t preferred_chunk_size)
{
    UploadFileResult result;

    std::error_code error;
    if (!std::filesystem::is_regular_file(local_path, error) || error) {
        result.error_message = "local path is not a readable regular file";
        return result;
    }

    const uint64_t file_size =
        std::filesystem::file_size(local_path, error);
    if (error) {
        result.error_message = "failed to get local file size: " +
                               error.message();
        return result;
    }

    if (remote_file_name.empty()) {
        remote_file_name = local_path.filename().string();
    }

    std::string sha256;
    if (!ComputeFileSha256(
            local_path, &sha256, &result.error_message)) {
        return result;
    }

    BeginUploadRequest begin_request;
    const std::string requested_transfer_id = GenerateTransferId();
    begin_request.set_file_name(std::move(remote_file_name));
    begin_request.set_file_size(file_size);
    begin_request.set_file_sha256(std::move(sha256));
    begin_request.set_preferred_chunk_size(preferred_chunk_size);
    begin_request.set_requested_transfer_id(requested_transfer_id);

    BeginUploadResponse begin_response;
    MprpcController begin_controller;
    begin_controller.SetTimeoutMs(timeout_ms_);
    begin_controller.SetAffinityKey(requested_transfer_id);
    stub_.BeginUpload(
        &begin_controller,
        &begin_request,
        &begin_response,
        nullptr);

    if (!CheckRpcAndBusinessResult(
            "BeginUpload",
            begin_controller,
            begin_response,
            &result.error_message)) {
        return result;
    }

    result.transfer_id = begin_response.transfer_id();
    const uint32_t chunk_size = begin_response.accepted_chunk_size();
    if (result.transfer_id.empty() || chunk_size == 0) {
        result.error_message =
            "BeginUpload returned invalid transfer parameters";
        return result;
    }

    std::ifstream input(local_path, std::ios::binary);
    if (!input) {
        result.error_message = "failed to reopen local file";
        AbortBestEffort(result.transfer_id);
        return result;
    }

    std::string chunk(chunk_size, '\0');
    uint64_t offset = 0;
    while (offset < file_size) {
        const uint64_t remaining = file_size - offset;
        const std::size_t bytes_to_read = static_cast<std::size_t>(
            std::min<uint64_t>(remaining, chunk_size));

        input.read(
            chunk.data(),
            static_cast<std::streamsize>(bytes_to_read));

        if (input.gcount() !=
            static_cast<std::streamsize>(bytes_to_read)) {
            result.error_message = "failed to read the next local chunk";
            AbortBestEffort(result.transfer_id);
            return result;
        }

        UploadChunkRequest chunk_request;
        chunk_request.set_transfer_id(result.transfer_id);
        chunk_request.set_offset(offset);
        chunk_request.set_data(chunk.data(), bytes_to_read);
        chunk_request.set_data_crc32(static_cast<uint32_t>(
            ::crc32(0L,
                    reinterpret_cast<const Bytef*>(chunk.data()),
                    static_cast<uInt>(bytes_to_read))));

        UploadChunkResponse chunk_response;
        MprpcController chunk_controller;
        chunk_controller.SetTimeoutMs(timeout_ms_);
        chunk_controller.SetAffinityKey(result.transfer_id);
        stub_.UploadChunk(
            &chunk_controller,
            &chunk_request,
            &chunk_response,
            nullptr);

        if (!CheckRpcAndBusinessResult(
                "UploadChunk",
                chunk_controller,
                chunk_response,
                &result.error_message)) {
            AbortBestEffort(result.transfer_id);
            return result;
        }

        const uint64_t expected_next_offset = offset + bytes_to_read;
        if (chunk_response.acknowledged_offset() != offset ||
            chunk_response.total_received_size() != expected_next_offset) {
            result.error_message =
                "UploadChunk returned an unexpected acknowledgement";
            AbortBestEffort(result.transfer_id);
            return result;
        }

        offset = expected_next_offset;
        result.bytes_uploaded = offset;
    }

    FinishUploadRequest finish_request;
    finish_request.set_transfer_id(result.transfer_id);

    FinishUploadResponse finish_response;
    MprpcController finish_controller;
    finish_controller.SetTimeoutMs(timeout_ms_);
    finish_controller.SetAffinityKey(result.transfer_id);
    stub_.FinishUpload(
        &finish_controller,
        &finish_request,
        &finish_response,
        nullptr);

    if (!CheckRpcAndBusinessResult(
            "FinishUpload",
            finish_controller,
            finish_response,
            &result.error_message)) {
        AbortBestEffort(result.transfer_id);
        return result;
    }

    if (finish_response.received_size() != file_size) {
        result.error_message =
            "FinishUpload returned an unexpected received size";
        return result;
    }

    result.ok = true;
    return result;
}

void SequentialFileUploader::AbortBestEffort(
    const std::string& transfer_id)
{
    if (transfer_id.empty()) {
        return;
    }

    AbortUploadRequest request;
    request.set_transfer_id(transfer_id);

    AbortUploadResponse response;
    MprpcController controller;
    controller.SetTimeoutMs(timeout_ms_);
    controller.SetAffinityKey(transfer_id);
    stub_.AbortUpload(&controller, &request, &response, nullptr);
}
} // namespace mprpc::file
