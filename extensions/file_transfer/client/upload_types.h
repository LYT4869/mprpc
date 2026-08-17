#pragma once

#include <cstdint>
#include <string>

namespace mprpc::file
{
struct UploadFileResult
{
    bool ok = false;
    std::string transfer_id;
    uint64_t bytes_uploaded = 0;
    uint32_t retries = 0;
    uint32_t max_in_flight = 0;
    std::string error_message;
};
} // namespace mprpc::file
