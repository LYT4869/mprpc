#include <filesystem>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "mprpcapplication.h"
#include "parallel_file_uploader.h"

namespace
{
struct CommandLine
{
    std::vector<std::string> positional;
    mprpc::file::UploadOptions upload;
    bool valid = true;
};

bool ParseUint32(const std::string& text, uint32_t* value)
{
    try {
        const unsigned long parsed = std::stoul(text);
        if (parsed > std::numeric_limits<uint32_t>::max()) return false;
        *value = static_cast<uint32_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

CommandLine ParseCommandLine(int argc, char** argv)
{
    CommandLine command;
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "-i") {
            ++i;
        } else if (argument == "--transfer-id" && i + 1 < argc) {
            command.upload.transfer_id = argv[++i];
        } else if (argument == "--window" && i + 1 < argc) {
            command.valid = command.valid &&
                ParseUint32(argv[++i], &command.upload.window_size);
        } else if (argument == "--timeout-ms" && i + 1 < argc) {
            command.valid = command.valid &&
                ParseUint32(argv[++i], &command.upload.chunk_timeout_ms);
        } else if (argument == "--max-retries" && i + 1 < argc) {
            command.valid = command.valid &&
                ParseUint32(argv[++i], &command.upload.max_retries);
        } else if (argument == "--chunk-size" && i + 1 < argc) {
            command.valid = command.valid &&
                ParseUint32(argv[++i], &command.upload.preferred_chunk_size);
        } else if (argument.rfind("--", 0) == 0) {
            command.valid = false;
        } else {
            command.positional.push_back(argument);
        }
    }
    return command;
}
} // namespace

int main(int argc, char** argv)
{
    const CommandLine command = ParseCommandLine(argc, argv);
    if (!command.valid || command.positional.empty() ||
        command.positional.size() > 2) {
        std::cerr << "usage: file_transfer_client -i <config> "
                     "[--window N] [--timeout-ms N] [--max-retries N] "
                     "[--transfer-id ID] [--chunk-size N] "
                     "<local-file> [remote-file-name]\n";
        return 1;
    }

    MprpcApplication::Init(argc, argv);
    mprpc::file::ParallelFileUploader uploader;
    const std::string remote_file_name = command.positional.size() == 2
        ? command.positional[1] : std::string{};
    const auto result = uploader.Upload(
        std::filesystem::path(command.positional[0]),
        remote_file_name, command.upload);

    if (!result.ok) {
        std::cerr << "upload failed: " << result.error_message << '\n';
        if (!result.transfer_id.empty()) {
            std::cerr << "transfer_id: " << result.transfer_id << '\n';
        }
        return 1;
    }

    std::cout << "upload succeeded\n"
              << "transfer_id: " << result.transfer_id << '\n'
              << "bytes_uploaded: " << result.bytes_uploaded << '\n'
              << "retries: " << result.retries << '\n'
              << "max_in_flight: " << result.max_in_flight << '\n';
    return 0;
}
