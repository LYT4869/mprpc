#include "mprpcchannel.h"
#include "mprpccodec.h"
#include "proto/rpc_meta.pb.h"
#include "mprpcapplication.h"
#include "mprpccontroller.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
 #include <netinet/in.h>
#include <errno.h>
#include <string>
#include <unistd.h>
#include <cstring>



/*
header_size + service_name method_name args_size + args
*/
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done)
{
    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name(); // service_name
    std::string method_name = method->name(); // method_name

    // 获取参数的序列化字符串长度args_size
    std::string payload_str;
    if(!request->SerializeToString(&payload_str)){
        controller->SetFailed("Serialize request error!");
        return;
    }

    // 定义请求体meta
    mprpc::MprpcRequestMeta request_meta;
    request_meta.set_service_name(service_name);
    request_meta.set_method_name(method_name);
    request_meta.set_timeout_ms(0);

    std::string meta;
    if(!request_meta.SerializeToString(&meta)){
        controller->SetFailed("MprpcRequestMeta serialize request error!");
        return;
    }
    std::string body = mprpc::MprpcCodec::EncodeBody(meta, payload_str);
    // 定义rpc请求头
    mprpc::MprpcHeader header;
    header.request_id = 1;
    header.message_type = mprpc::MprpcMessageType::REQUEST;
    header.status_code = mprpc::MprpcErrorCode::OK;
    header.checksum = 0;

    std::string send_rpc_str = mprpc::MprpcCodec::Encode(header, body);

    std::cout << "==============================================" << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "meta_size: " << meta.size() << std::endl;
    std::cout << "payload_size: " << payload_str.size() << std::endl;
    std::cout << "frame_size: " << send_rpc_str.size() << std::endl;
    std::cout << "==============================================" << std::endl;

    // 使用tcp编程完成rpc方法的远程调用
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if(clientfd == -1){
        controller->SetFailed("Create socket error! errno: " + std::to_string(errno));
        return;
    }

    // 在zk上查询服务所在的host信息
    ZkClient zkCli;
    zkCli.Start();
    std::string method_path = "/" + service_name + "/" + method_name;
    std::string host_data = zkCli.GetData(method_path.data());
    if(host_data == ""){
        controller->SetFailed(method_path + " does not exist!");
        return;
    }
    int idx = host_data.find(":");
    if(idx == -1){
        controller->SetFailed(method_path + " address is invalid!");
        return;
    }
    std::string ip = host_data.substr(0, idx);
    uint16_t port = atoi(host_data.substr(idx + 1, host_data.size() - idx).data());

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

    // 连接rpc服务节点
    if(connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1){
        controller->SetFailed("Connection error! errno: " + std::to_string(errno));
        return;
    }

    size_t total_sent = 0;
    while(total_sent < send_rpc_str.size()){
        ssize_t n = send(clientfd, send_rpc_str.data() + total_sent, send_rpc_str.size() - total_sent, 0);
        if(n == -1){
            controller->SetFailed("Send error! errno: " + std::to_string(errno));
            close(clientfd);
            return;
        }
        if (n == 0) {
            controller->SetFailed("Send returned 0, connection may be closed.");
            close(clientfd);
            return;
        }
        total_sent += static_cast<size_t>(n);
    }

    
    // 接收rpc请求的响应值
    std::string response_str;
    char recv_buf[1024];

    while(true){
        ssize_t n = recv(clientfd, recv_buf, sizeof(recv_buf), 0);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            controller->SetFailed("Recv error! errno: " + std::to_string(errno));
            close(clientfd);
            return;
        }
        if (n == 0) {
            // 对端关闭连接，说明响应接收完毕
            break;
        }

        response_str.append(recv_buf, n);
    }


    // 反序列化rpc调用的相应数据
    mprpc::MprpcFrame response_frame;
    size_t bytes_consumed = 0;
    mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::Decode(response_str, &response_frame, &bytes_consumed);

    if(decode_status != mprpc::DecodeStatus::OK){
        controller->SetFailed("Decode response frame error!");
        close(clientfd);
        return;
    }
    if(bytes_consumed != response_str.size()){
        controller->SetFailed("Unexpected extra bytes in response frame!");
        close(clientfd);
        return;
    }
    if(response_frame.header.message_type != mprpc::MprpcMessageType::RESPONSE){
        controller->SetFailed("Response message type error!");
        close(clientfd);
        return;
    }


    mprpc::MprpcBody response_body;
    decode_status = mprpc::MprpcCodec::DecodeBody(response_frame.body, &response_body);

    if(decode_status != mprpc::DecodeStatus::OK){
        controller->SetFailed("Decode response body error!");
        close(clientfd);
        return;
    }

    mprpc::MprpcResponseMeta response_meta;
    if(!response_meta.ParseFromString(response_body.meta)){
        controller->SetFailed("Response meta parsing error!");
        close(clientfd);
        return;
    }
    if(response_frame.header.status_code != mprpc::MprpcErrorCode::OK){
        std::string error_msg = response_meta.error_msg();
        if(error_msg.empty()){
            error_msg = "RPC response status error!";
        }
        controller->SetFailed(error_msg);
        close(clientfd);
        return;
    }
    if(!response_meta.error_msg().empty()){
        controller->SetFailed(response_meta.error_msg());
        close(clientfd);
        return;
    }

    if(!response->ParseFromString(response_body.payload)){
        controller->SetFailed("Response payload parsing error!");
        close(clientfd);
        return;
    }

    close(clientfd);
    if (done != nullptr) {
        done->Run();
    }
}
