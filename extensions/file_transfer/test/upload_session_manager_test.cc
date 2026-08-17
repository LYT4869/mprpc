#include <chrono>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include <zlib.h>

#include "upload_session_manager.h"

namespace
{
using namespace mprpc::file;

struct TestContext
{
    void Check(bool condition, const std::string& message)
    {
        if (!condition) {
            passed = false;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    bool passed = true;
};

uint32_t Crc32(const std::string& data)
{
    return static_cast<uint32_t>(::crc32(
        0L, reinterpret_cast<const Bytef*>(data.data()),
        static_cast<uInt>(data.size())));
}

UploadChunkResult Put(UploadSessionManager& manager,
                      const std::string& id, uint64_t offset,
                      const std::string& data)
{
    return manager.UploadChunk(id, offset, data, Crc32(data));
}

std::string ReadAll(const std::filesystem::path& path)
{
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

void TestRandomOrderAndIdempotency(TestContext& context,
                                   const std::filesystem::path& root)
{
    UploadSessionManager manager(root);
    const auto begin = manager.BeginUpload(
        "random.bin", 6,
        "e9c0f8b575cbfcb42ab3b78ecc87efa3"
        "b011d9a5d10b09fa4e96f240bf6a82f5",
        2);
    context.Check(begin.Ok(), "random upload should begin");
    if (!begin.Ok()) return;

    auto session = manager.FindSession(begin.transfer_id);
    context.Check(session && session->file.Valid(),
                  "preallocated file descriptor should be valid");
    context.Check(std::filesystem::file_size(session->temporary_path) == 6,
                  "temporary file should have its logical size set");
    context.Check(std::filesystem::exists(session->metadata_path),
                  "sidecar metadata should be created");

    context.Check(Put(manager, begin.transfer_id, 4, "EF").Ok(),
                  "last chunk should be accepted first");
    context.Check(Put(manager, begin.transfer_id, 0, "AB").Ok(),
                  "first chunk should be accepted second");

    const auto query = manager.QueryUploadStatus(begin.transfer_id);
    context.Check(query.Ok() && query.received_size == 4 &&
                      query.chunk_count == 3 &&
                      query.received_bitmap.size() == 1 &&
                      (static_cast<unsigned char>(query.received_bitmap[0]) & 5U) == 5U,
                  "query should return the received chunk bitmap");

    const auto duplicate = Put(manager, begin.transfer_id, 0, "AB");
    context.Check(duplicate.Ok() && duplicate.duplicate &&
                      duplicate.total_received_size == 4,
                  "identical duplicate chunk should be idempotent");

    const auto conflict = Put(manager, begin.transfer_id, 0, "XY");
    context.Check(conflict.code == CHUNK_CONFLICT,
                  "different duplicate chunk should conflict");

    const auto bad_crc = manager.UploadChunk(
        begin.transfer_id, 2, "CD", Crc32("CD") + 1);
    context.Check(bad_crc.code == CHECKSUM_MISMATCH,
                  "incorrect chunk CRC should be rejected");
    context.Check(Put(manager, begin.transfer_id, 1, "CD").code == INVALID_OFFSET,
                  "unaligned offset should be rejected");
    context.Check(Put(manager, begin.transfer_id, 2, "C").code == INVALID_ARGUMENT,
                  "incorrect chunk length should be rejected");

    context.Check(Put(manager, begin.transfer_id, 2, "CD").Ok(),
                  "missing middle chunk should succeed");
    const auto finish = manager.FinishUpload(begin.transfer_id);
    context.Check(finish.Ok() && finish.received_size == 6,
                  "complete random-order upload should finish");
    context.Check(ReadAll(session->final_path) == "ABCDEF",
                  "published file should preserve logical chunk order");
}

void TestConcurrentChunks(TestContext& context,
                          const std::filesystem::path& root)
{
    constexpr uint32_t kChunkSize = 4;
    constexpr uint32_t kChunkCount = 16;
    std::string content;
    for (uint32_t i = 0; i < kChunkCount; ++i) {
        content.append(kChunkSize, static_cast<char>('A' + i));
    }

    // This test validates random concurrent writes; final SHA is irrelevant here.
    UploadSessionManager manager(root);
    const auto begin = manager.BeginUpload(
        "concurrent.bin", content.size(), std::string(64, 'a'), kChunkSize);
    context.Check(begin.Ok(), "concurrent upload should begin");
    if (!begin.Ok()) return;

    std::vector<UploadChunkResult> results(kChunkCount);
    std::vector<std::thread> workers;
    for (uint32_t reverse = 0; reverse < kChunkCount; ++reverse) {
        const uint32_t index = kChunkCount - reverse - 1;
        workers.emplace_back([&, index] {
            const std::string chunk = content.substr(
                index * kChunkSize, kChunkSize);
            results[index] = Put(manager, begin.transfer_id,
                                 index * kChunkSize, chunk);
        });
    }
    for (auto& worker : workers) worker.join();
    context.Check(std::all_of(results.begin(), results.end(),
                              [](const UploadChunkResult& result) {
                                  return result.Ok();
                              }),
                  "all concurrent chunks should succeed");

    const auto session = manager.FindSession(begin.transfer_id);
    context.Check(session && ReadAll(session->temporary_path) == content,
                  "concurrent pwrite should produce correct file contents");
    const auto query = manager.QueryUploadStatus(begin.transfer_id);
    context.Check(query.received_size == content.size(),
                  "concurrent commits should count every byte once");
    context.Check(manager.AbortUpload(begin.transfer_id).Ok(),
                  "concurrent test upload should remain abortable");
}

void TestRecovery(TestContext& context,
                  const std::filesystem::path& root)
{
    std::string transfer_id;
    {
        UploadSessionManager manager(root);
        const auto begin = manager.BeginUpload(
            "resume.bin", 6,
            "e9c0f8b575cbfcb42ab3b78ecc87efa3"
            "b011d9a5d10b09fa4e96f240bf6a82f5",
            2, "0123456789abcdef0123456789abcdef");
        context.Check(begin.Ok(), "recoverable upload should begin");
        transfer_id = begin.transfer_id;
        context.Check(Put(manager, transfer_id, 4, "EF").Ok(),
                      "recoverable upload should store last chunk");
        context.Check(Put(manager, transfer_id, 0, "AB").Ok(),
                      "recoverable upload should store first chunk");
    }

    UploadSessionManager recovered(root);
    const auto status = recovered.QueryUploadStatus(transfer_id);
    context.Check(status.Ok() && status.received_size == 4,
                  "manager reconstruction should restore bitmap and byte count");
    context.Check(Put(recovered, transfer_id, 2, "CD").Ok(),
                  "client should upload only the missing chunk after restart");
    context.Check(recovered.FinishUpload(transfer_id).Ok(),
                  "resumed upload should finish successfully");
}

void TestEmptyAndFailures(TestContext& context,
                          const std::filesystem::path& root)
{
    UploadSessionManager manager(root);
    const auto empty = manager.BeginUpload(
        "empty.bin", 0,
        "e3b0c44298fc1c149afbf4c8996fb924"
        "27ae41e4649b934ca495991b7852b855",
        1024);
    context.Check(empty.Ok() && manager.FinishUpload(empty.transfer_id).Ok(),
                  "empty file should finish without chunks");

    const auto failed = manager.BeginUpload(
        "write-failure.bin", 4, std::string(64, 'a'), 4);
    auto session = manager.FindSession(failed.transfer_id);
    session->file.Reset();
    context.Check(Put(manager, failed.transfer_id, 0, "ABCD").code == FILE_IO_ERROR,
                  "closed descriptor should report a write failure");
    context.Check(manager.AbortUpload(failed.transfer_id).Ok(),
                  "failed write session should be removable");
}

void TestCorruptMetadataAndCleanup(TestContext& context,
                                   const std::filesystem::path& root)
{
    std::filesystem::path part_path;
    std::filesystem::path metadata_path;
    {
        UploadSessionManager manager(root);
        const auto begin = manager.BeginUpload(
            "corrupt.bin", 4, std::string(64, 'a'), 4);
        const auto session = manager.FindSession(begin.transfer_id);
        part_path = session->temporary_path;
        metadata_path = session->metadata_path;
    }
    std::ofstream(metadata_path, std::ios::binary | std::ios::trunc) << "bad";
    UploadSessionManager recovered(root);
    context.Check(!std::filesystem::exists(metadata_path) &&
                      !std::filesystem::exists(part_path),
                  "corrupt metadata and its part file should be removed");

    const auto active = recovered.BeginUpload(
        "expired.bin", 4, std::string(64, 'a'), 4);
    const auto session = recovered.FindSession(active.transfer_id);
    {
        std::lock_guard<std::mutex> lock(session->mutex);
        session->last_activity = std::chrono::steady_clock::now() -
                                 std::chrono::hours(2);
    }
    const auto cleanup = recovered.CleanupExpiredSessions(std::chrono::hours(1));
    context.Check(cleanup.Ok() && cleanup.sessions_removed == 1 &&
                      cleanup.files_removed == 2,
                  "expired cleanup should remove session, part and metadata");
}
} // namespace

int main()
{
    TestContext context;
    const auto root = std::filesystem::temp_directory_path() /
        ("mprpc_file_test_" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));

    TestRandomOrderAndIdempotency(context, root / "random");
    TestConcurrentChunks(context, root / "concurrent");
    TestRecovery(context, root / "recovery");
    TestEmptyAndFailures(context, root / "failures");
    TestCorruptMetadataAndCleanup(context, root / "cleanup");

    std::error_code error;
    std::filesystem::remove_all(root, error);
    context.Check(!error, "test directory cleanup should succeed");
    std::cout << (context.passed ? "PASS" : "FAIL")
              << ": UploadSessionManager random-write/recovery tests\n";
    return context.passed ? 0 : 1;
}
