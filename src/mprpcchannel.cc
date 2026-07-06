#include "mprpcchannel.h"
#include "rpcheader.pb.h"
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
    int args_size = 0;
    std::string args_str;
    if(request->SerializeToString(&args_str)){
        args_size = args_str.size();
        
    }else{
        controller->SetFailed("Serialize request error!");
        return;
    }

    // 定义rpc请求头
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if(rpcHeader.SerializeToString(&rpc_header_str)){
        header_size = rpc_header_str.size();
    }else{
        controller->SetFailed("rpcHeader serialize request error!");
        return;
    }

    //组织待发送的rpc请求的字符串
    std::string send_rpc_str;
    uint32_t net_header_size = htonl(header_size);
    send_rpc_str.insert(0, std::string(reinterpret_cast<char*>(&net_header_size), 4)); // header_size
    send_rpc_str += rpc_header_str; //rpcheader
    send_rpc_str += args_str; //args

    std::cout << "==============================================" << std::endl;
    std::cout << "header_size: " << header_size << std::endl;
    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "args_size: " << args_size << std::endl;
    std::cout << "args_str: " << args_str << std::endl;
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
        controller->SetFailed(method_path + "does not exist!");
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
    if(!response->ParseFromString(response_str)){
        controller->SetFailed("Response parsing error! response_str: " + response_str);
        close(clientfd);
        return;
    }
    close(clientfd);
    if (done != nullptr) {
        done->Run();
    }
}
