#include "mprpccodec.h"
#include "proto/rpc_meta.pb.h"
#include <cassert>
#include <iomanip>
#include <iostream>
#include <string>

int main(){
    mprpc::MprpcHeader header;
    header.request_id = 0x1122334455667788;
    header.message_type = mprpc::MprpcMessageType::REQUEST;
    header.status_code = mprpc::MprpcErrorCode::OK;
    std::string body = "Hello";

    // 编码测试
    std::string encoded = mprpc::MprpcCodec::Encode(header, body);

    assert(encoded.size() == mprpc::MPRPC_HEADER_SIZE + body.size());

    // for(unsigned char c : encoded){
    //     std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c) << " ";
    // }
    // std::cout << std::dec << std::endl;
    
    // 解码测试
    mprpc::MprpcFrame frame;
    size_t bytes_consumed = 0;
    mprpc::DecodeStatus status = mprpc::MprpcCodec::Decode(encoded, &frame, &bytes_consumed);

    // std::cout << "Decode status: " << static_cast<int>(status) << std::endl;

    assert(status == mprpc::DecodeStatus::OK);
    assert(bytes_consumed == encoded.size());
    assert(frame.header.magic == mprpc::MPRPC_MAGIC);
    assert(frame.header.version == mprpc::MPRPC_VERSION);
    assert(frame.header.header_len == mprpc::MPRPC_HEADER_SIZE);
    assert(frame.header.body_len == body.size());
    assert(frame.header.request_id == header.request_id);
    assert(frame.header.message_type == header.message_type);
    assert(frame.header.status_code == header.status_code);
    assert(frame.body == body);

    // 控制帧沿用固定协议头，通过 request_id 指向待取消请求。
    mprpc::MprpcHeader cancel_header;
    cancel_header.request_id = 42;
    cancel_header.message_type = mprpc::MprpcMessageType::CANCEL;
    cancel_header.status_code = mprpc::MprpcErrorCode::CANCELLED;
    const std::string cancel_encoded =
        mprpc::MprpcCodec::Encode(cancel_header, {});
    mprpc::MprpcFrame cancel_frame;
    size_t cancel_consumed = 0;
    assert(mprpc::MprpcCodec::Decode(
               cancel_encoded, &cancel_frame, &cancel_consumed) ==
           mprpc::DecodeStatus::OK);
    assert(cancel_consumed == mprpc::MPRPC_HEADER_SIZE);
    assert(cancel_frame.header.request_id == 42);
    assert(cancel_frame.header.message_type ==
           mprpc::MprpcMessageType::CANCEL);
    assert(cancel_frame.body.empty());

    const std::string invalid_cancel =
        mprpc::MprpcCodec::Encode(cancel_header, "unexpected");
    cancel_consumed = 99;
    assert(mprpc::MprpcCodec::Decode(
               invalid_cancel, &cancel_frame, &cancel_consumed) ==
           mprpc::DecodeStatus::BAD_FRAME);
    assert(cancel_consumed == 0);

    // 半包测试
    std::string partial_header = encoded.substr(0, 10);
    mprpc::MprpcFrame partial_frame;
    size_t partial_bytes_consumed = 123;
    mprpc::DecodeStatus partial_status = mprpc::MprpcCodec::Decode(partial_header, &partial_frame, &partial_bytes_consumed); 
    
    assert(partial_status == mprpc::DecodeStatus::NEED_MORE_DATA);
    assert(partial_bytes_consumed == 0);

    std::string partial_body = encoded.substr(0, mprpc::MPRPC_HEADER_SIZE + 2);
    partial_bytes_consumed = 123;

    partial_status = mprpc::MprpcCodec::Decode(partial_body, &partial_frame, &partial_bytes_consumed);
    assert(partial_status == mprpc::DecodeStatus::NEED_MORE_DATA);
    assert(partial_bytes_consumed == 0);

    // 粘包测试
    mprpc::MprpcHeader header2;
    header2.request_id = 0x1122334455667789;
    header2.message_type = mprpc::MprpcMessageType::REQUEST;
    header2.status_code = mprpc::MprpcErrorCode::OK;
    std::string body2 = "Hello, World!";
    std::string encoded2 = mprpc::MprpcCodec::Encode(header2, body2);
    std::string sticky_packet = encoded + encoded2;
    mprpc::MprpcFrame frame1, frame2;
    size_t bytes_consumed1 = 0,  bytes_consumed2 = 0;
    mprpc::DecodeStatus status1 = mprpc::MprpcCodec::Decode(sticky_packet, &frame1, &bytes_consumed1);
    assert(status1 == mprpc::DecodeStatus::OK);
    assert(bytes_consumed1 == encoded.size());  
    assert(frame1.header.request_id == header.request_id);
    assert(frame1.body == body);

    mprpc::DecodeStatus status2 = mprpc::MprpcCodec::Decode(sticky_packet.substr(bytes_consumed1), &frame2, &bytes_consumed2);
    assert(status2 == mprpc::DecodeStatus::OK);
    assert(bytes_consumed2 == encoded2.size());
    assert(frame2.header.request_id == header2.request_id);
    assert(frame2.body == body2);

    // 非法包测试
    // CRC32 detects corruption in an otherwise structurally valid header.
    {
        std::string corrupted_header = encoded;
        corrupted_header[19] ^= 0x01;
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        assert(mprpc::MprpcCodec::Decode(
                   corrupted_header, &bad_frame, &bad_bytes_consumed) ==
               mprpc::DecodeStatus::CHECKSUM_MISMATCH);
        assert(bad_bytes_consumed == 0);
    }
    // CRC32 also covers every body byte.
    {
        std::string corrupted_body = encoded;
        corrupted_body.back() ^= 0x01;
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        assert(mprpc::MprpcCodec::Decode(
                   corrupted_body, &bad_frame, &bad_bytes_consumed) ==
               mprpc::DecodeStatus::CHECKSUM_MISMATCH);
    }
    {
        std::string corrupted_checksum = encoded;
        corrupted_checksum[27] ^= 0x01;
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        assert(mprpc::MprpcCodec::Decode(
                   corrupted_checksum, &bad_frame, &bad_bytes_consumed) ==
               mprpc::DecodeStatus::CHECKSUM_MISMATCH);
    }

    // 测试非法魔数
    {
        std::string bad_magic = encoded;
        bad_magic[0] = 0x00; // 修改魔数
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        mprpc::DecodeStatus bad_status = mprpc::MprpcCodec::Decode(bad_magic, &bad_frame, &bad_bytes_consumed);
        assert(bad_status == mprpc::DecodeStatus::BAD_MAGIC);
        assert(bad_bytes_consumed == 0);
    }

    // 测试非法版本号
    {
        std::string bad_version = encoded;
        bad_version[5] = 0x02; // 修改版本号
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        mprpc::DecodeStatus bad_status = mprpc::MprpcCodec::Decode(bad_version, &bad_frame, &bad_bytes_consumed);
        assert(bad_status == mprpc::DecodeStatus::INVALID_VERSION);
        assert(bad_bytes_consumed == 0);
    }

    // 非法header_len
    {
        std::string bad_header_len = encoded;
        bad_header_len[7] = 0x20; // 修改header_len
        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 0;
        mprpc::DecodeStatus bad_status = mprpc::MprpcCodec::Decode(bad_header_len, &bad_frame, &bad_bytes_consumed);
        assert(bad_status == mprpc::DecodeStatus::BAD_FRAME);
        assert(bad_bytes_consumed == 0);
    }
    // 测试非法message_type
    {
        std::string bad_message_type = encoded;
        bad_message_type[21] = 0x09;

        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 999;

        mprpc::DecodeStatus bad_status =
            mprpc::MprpcCodec::Decode(bad_message_type, &bad_frame, &bad_bytes_consumed);

        assert(bad_status == mprpc::DecodeStatus::BAD_FRAME);
        assert(bad_bytes_consumed == 0);
    }   
    // 测试 body_len 超限
    {
        std::string oversized_body = encoded;
        oversized_body[8] = 0x05;
        oversized_body[9] = 0x00;
        oversized_body[10] = 0x00;
        oversized_body[11] = 0x00;

        mprpc::MprpcFrame bad_frame;
        size_t bad_bytes_consumed = 999;

        mprpc::DecodeStatus bad_status =
            mprpc::MprpcCodec::Decode(oversized_body, &bad_frame, &bad_bytes_consumed);

        assert(bad_status == mprpc::DecodeStatus::FRAME_TOO_LARGE);
        assert(bad_bytes_consumed == 0);
    }

    {
        mprpc::MprpcRequestMeta request_meta;
        request_meta.set_service_name("FriendServiceRpc");
        request_meta.set_method_name("GetFriendList");
        request_meta.set_timeout_ms(3000);

        std::string meta_bytes;
        if(!request_meta.SerializeToString(&meta_bytes)){
            std::cerr << "Failed to serialize request_meta" << std::endl;
            return -1;
        }

        std::string user_payload = "serialized-user-payload";
        std::string encoded_body = mprpc::MprpcCodec::EncodeBody(meta_bytes, user_payload);

        mprpc::MprpcBody decoded_body;
        mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::DecodeBody(encoded_body, &decoded_body);
        assert(decode_status == mprpc::DecodeStatus::OK);
        assert(decoded_body.meta == meta_bytes);
        assert(decoded_body.payload == user_payload);

        mprpc::MprpcRequestMeta parsed_meta;
        if(!parsed_meta.ParseFromString(decoded_body.meta)){
            std::cerr << "Failed to parse decoded meta" << std::endl;
            return -1;
        }
        assert(parsed_meta.service_name() == "FriendServiceRpc");
        assert(parsed_meta.method_name() == "GetFriendList");
        assert(parsed_meta.timeout_ms() == 3000);
    }
    {
        mprpc::MprpcRequestMeta request_meta;
        request_meta.set_service_name("FriendServiceRpc");
        request_meta.set_method_name("GetFriendList");
        request_meta.set_timeout_ms(3000);

        std::string meta_bytes;
        if(!request_meta.SerializeToString(&meta_bytes)){
            std::cerr << "Failed to serialize request_meta" << std::endl;
            return -1;
        }

        std::string user_payload = "serialized-user-payload";
        std::string encoded_body = mprpc::MprpcCodec::EncodeBody(meta_bytes, user_payload);
        
        mprpc::MprpcHeader header;
        header.request_id = 1001;
        header.message_type = mprpc::MprpcMessageType::REQUEST;
        header.status_code = mprpc::MprpcErrorCode::OK;

        std::string rpc_frame_bytes = mprpc::MprpcCodec::Encode(header, encoded_body);

        mprpc::MprpcFrame decoded_frame;
        size_t bytes_consumed = 0;
        mprpc::DecodeStatus frame_decode_stats = mprpc::MprpcCodec::Decode(rpc_frame_bytes, &decoded_frame, &bytes_consumed);
        assert(frame_decode_stats == mprpc::DecodeStatus::OK);
        assert(bytes_consumed == rpc_frame_bytes.size());
        assert(decoded_frame.header.request_id == 1001);
        assert(decoded_frame.header.message_type == mprpc::MprpcMessageType::REQUEST);
        assert(decoded_frame.header.status_code == mprpc::MprpcErrorCode::OK);
        assert(decoded_frame.header.body_len == encoded_body.size());
        
        mprpc::MprpcBody decoded_body;
        mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::DecodeBody(decoded_frame.body, &decoded_body);
        assert(decode_status == mprpc::DecodeStatus::OK);
        assert(decoded_body.meta == meta_bytes);
        assert(decoded_body.payload == user_payload);
        
        mprpc::MprpcRequestMeta parsed_meta;
        if(!parsed_meta.ParseFromString(decoded_body.meta)){
            std::cerr << "Failed to parse decoded meta" << std::endl;
            return -1;
        }
        assert(parsed_meta.service_name() == "FriendServiceRpc");
        assert(parsed_meta.method_name() == "GetFriendList");
        assert(parsed_meta.timeout_ms() == 3000);
    }
    std::cout << "codec tests passed" << std::endl;
    return 0;
}
