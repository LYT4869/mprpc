#include "mprpccodec.h"

namespace mprpc
{
namespace
{
    void AppendUint64(std::string* out, uint64_t value){
        for(int shift = 56; shift >= 0; shift -= 8){
            out->push_back(static_cast<char>((value >> shift) & 0xFF));
        }
    }
    void AppendUint32(std::string* out, uint32_t value){
        for(int shift = 24; shift >= 0; shift -= 8){
            out->push_back(static_cast<char>((value >> shift) & 0xFF));
        }
    }
    void AppendUint16(std::string* out, uint16_t value){
        for(int shift = 8; shift >= 0; shift -= 8){
            out->push_back(static_cast<char>((value >> shift) & 0xFF));
        }
    }
    uint64_t ReadUint64(const char* data){
        uint64_t value = 0;
        for(int i = 0; i < 8; ++i){
            value = static_cast<uint64_t>(value << 8) | static_cast<unsigned char>(data[i]);
        }
        return value;
    }
    uint32_t ReadUint32(const char* data){
        uint32_t value = 0;
        for(int i = 0; i < 4; ++i){
            value = static_cast<uint32_t>(value << 8) | static_cast<unsigned char>(data[i]);
        }
        return value;
    }
    uint16_t ReadUint16(const char* data){
        uint16_t value = 0;
        for(int i = 0; i < 2; ++i){
            value = static_cast<uint16_t>(value << 8) | static_cast<unsigned char>(data[i]);
        }
        return value;
    }
} // namespace

std::string MprpcCodec::Encode(const MprpcHeader& header, const std::string& body){
    if(body.size() > MPRPC_MAX_BODY_SIZE){
        return "";
    }
    
    MprpcHeader encode_header = header;
    encode_header.magic = MPRPC_MAGIC;
    encode_header.version = MPRPC_VERSION;
    encode_header.header_len = MPRPC_HEADER_SIZE;
    encode_header.body_len = static_cast<uint32_t>(body.size());

    std::string output;
    output.reserve(MPRPC_HEADER_SIZE + body.size());

    AppendUint32(&output, encode_header.magic);
    AppendUint16(&output, encode_header.version);
    AppendUint16(&output, encode_header.header_len);
    AppendUint32(&output, encode_header.body_len);
    AppendUint64(&output, encode_header.request_id);
    AppendUint16(&output, static_cast<uint16_t>(encode_header.message_type));
    AppendUint16(&output, static_cast<uint16_t>(encode_header.status_code));
    AppendUint32(&output, encode_header.checksum);

    output.append(body);

    return output;
}
DecodeStatus MprpcCodec::Decode(const std::string& input, MprpcFrame* frame, size_t* bytes_consumed){
    if(frame == nullptr || bytes_consumed == nullptr){
        return DecodeStatus::BAD_FRAME;
    }
    *bytes_consumed = 0;

    if(input.size() < MPRPC_HEADER_SIZE){
        return DecodeStatus::NEED_MORE_DATA;
    }

    MprpcHeader header;
    const char* data = input.data();
    header.magic = ReadUint32(data);
    header.version = ReadUint16(data + 4);
    header.header_len = ReadUint16(data + 6);
    header.body_len = ReadUint32(data + 8);
    header.request_id = ReadUint64(data + 12);
    header.message_type = static_cast<MprpcMessageType>(ReadUint16(data + 20));
    header.status_code = static_cast<MprpcErrorCode>(ReadUint16(data + 22));
    header.checksum = ReadUint32(data + 24);

    if(header.magic != MPRPC_MAGIC){
        return DecodeStatus::BAD_MAGIC;
    }
    if(header.version != MPRPC_VERSION){
        return DecodeStatus::INVALID_VERSION;
    }
    if(header.header_len != MPRPC_HEADER_SIZE){
        return DecodeStatus::BAD_FRAME;
    }
    if(header.body_len > MPRPC_MAX_BODY_SIZE){
        return DecodeStatus::FRAME_TOO_LARGE;
    }
    if(!IsValidMessageType(static_cast<uint16_t>(header.message_type))){
        return DecodeStatus::BAD_FRAME;
    }

    size_t frame_size = MPRPC_HEADER_SIZE + header.body_len;
    if(input.size() < frame_size){
        return DecodeStatus::NEED_MORE_DATA;
    }

    frame->header = header;
    frame->body.assign(data + MPRPC_HEADER_SIZE, header.body_len);
    *bytes_consumed = frame_size;

    return DecodeStatus::OK;

}

std::string MprpcCodec::EncodeBody(const std::string& meta, const std::string& payload){
    std::string body;
    uint32_t meta_len = static_cast<uint32_t>(meta.size());
    body.reserve(4 + meta_len + payload.size());
    
    AppendUint32(&body, meta_len);
    body.append(meta);
    body.append(payload);

    return body;
}
DecodeStatus MprpcCodec::DecodeBody(const std::string& input, MprpcBody* decoded_body){
    if(decoded_body == nullptr){
        return DecodeStatus::BAD_FRAME;
    }

    if(input.size() < 4){
        return DecodeStatus::BAD_FRAME;
    }

    const char* data = input.data();
    uint32_t meta_len = static_cast<uint32_t>(ReadUint32(data));

    if(meta_len > input.size() - 4){
        return DecodeStatus::BAD_FRAME;
    }

    decoded_body->meta.assign(data + 4, meta_len);
    decoded_body->payload.assign(data + 4 + meta_len, input.size() - 4 - meta_len);

    return DecodeStatus::OK;

}

bool MprpcCodec::IsValidMessageType(uint16_t messageType){
    return messageType == static_cast<uint16_t>(MprpcMessageType::REQUEST) ||
            messageType == static_cast<uint16_t>(MprpcMessageType::RESPONSE) ||
            messageType == static_cast<uint16_t>(MprpcMessageType::HEARTBEAT);  
}
} // namespace mprpc