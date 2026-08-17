#pragma once

#include <cstdint>
#include <string>

namespace mprpc 
{
constexpr uint32_t MPRPC_MAGIC = 0x19990102;
constexpr uint16_t MPRPC_VERSION = 1;
constexpr uint16_t MPRPC_HEADER_SIZE = 28;
constexpr uint32_t MPRPC_MAX_BODY_SIZE = 64 * 1024 * 1024; // 64MB

enum class MprpcMessageType : uint16_t
{
    REQUEST = 1,
    RESPONSE = 2,
    HEARTBEAT = 3,
};

enum class MprpcErrorCode : uint16_t
{
    OK = 0,

    // codec / frame errors
    INVALID_MAGIC = 1001,
    INVALID_VERSION = 1002,
    FRAME_TOO_LARGE = 1003,
    BAD_FRAME = 1004,
    CHECKSUM_MISMATCH = 1005,
    DECODE_FAILED = 1006,

    // protobuf / parse errors
    PARSE_ERROR = 1101,
    SERIALIZE_FAILED = 1102,

    // service dispatch errors
    SERVICE_NOT_FOUND = 1201,
    METHOD_NOT_FOUND = 1202,

    // transport / client errors
    TIMEOUT = 1301,
    NETWORK_ERROR = 1302,
    INVALID_ADDRESS = 1303,
    CANCELLED = 1304,
    CONNECTION_CLOSED = 1305,
    CHANNEL_CLOSED = 1306,

    // generic framework errors
    INTERNAL_ERROR = 1401,
    IO_THREAD_BLOCKING_CALL = 1402,
    SERVER_BUSY = 1403,
};

enum class DecodeStatus
{
    OK,
    NEED_MORE_DATA,
    BAD_MAGIC,
    INVALID_VERSION,
    FRAME_TOO_LARGE,
    BAD_FRAME,
    CHECKSUM_MISMATCH,
};

struct MprpcHeader
{
    uint32_t magic = MPRPC_MAGIC;
    uint16_t version = MPRPC_VERSION;
    uint16_t header_len = MPRPC_HEADER_SIZE;
    uint32_t body_len = 0;
    uint64_t request_id = 0;
    MprpcMessageType message_type = MprpcMessageType::REQUEST;
    MprpcErrorCode status_code = MprpcErrorCode::OK;
    uint32_t checksum = 0;
};

struct MprpcFrame
{
    MprpcHeader header;
    std::string body;
};

struct MprpcBody
{
    std::string meta;
    std::string payload;
};

class MprpcCodec
{
public:
    static std::string Encode(const MprpcHeader& header, const std::string& body);
    static DecodeStatus Decode(const std::string& input, MprpcFrame* frame, size_t* bytes_consumed); 
    static std::string EncodeBody(const std::string& meta, const std::string& payload);
    static DecodeStatus DecodeBody(const std::string& input, MprpcBody* decoded_body);
private:
    static bool IsValidMessageType(uint16_t message_type);
};
} // namespace mprpc
