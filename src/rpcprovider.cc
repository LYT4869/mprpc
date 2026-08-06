#include "rpcprovider.h"
#include <string>
#include "mprpcapplication.h"
#include <functional>
#include <google/protobuf/descriptor.h>
#include <memory>
#include "proto/rpc_meta.pb.h"
#include "mprpccodec.h"
#include "logger.h"
/*
service_name => service描述；
service描述 
    => service* 记录服务对象
        - method_name => method方法对象；


*/
// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
void RpcProvider::NotifyService(google::protobuf::Service *service){
    RpcProvider::ServiceInfo service_info;

    service_info.m_service = service;
    // 获取服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象service方法的数量
    int methodCnt = pserviceDesc->method_count();

    // std::cout << "service_name: " << service_name << std::endl;
    LOG_INFO("service_name: %s", service_name.data());
    for(int i = 0; i < methodCnt; i++){
        // 获取了服务对象指定下标的服务方法的描述（抽象描述）
        const google::protobuf::MethodDescriptor* pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        // std::cout << "method_name: " << method_name << std::endl;
        LOG_INFO("method_name: %s", method_name.data());
        service_info.m_methodMap.insert({method_name, pmethodDesc});
    }
    m_serviceMap.insert({service_name, service_info});
}

// 启动rpc服务节点，开始提供rpc远程嗲用服务
void RpcProvider::Run(){
    std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    muduo::net::InetAddress address(ip, port);

    // 创建TcpServer对象
    muduo::net::TcpServer server(&m_eventLoop, address, "RpcProvider");
    // 绑定连接回调和消息读写回调方法 分离网络代码和业务代码
    server.setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, 
            std::placeholders::_2, std::placeholders::_3));
    //设置muduo库的线程数量
    server.setThreadNum(4);

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc client可以从zk上发现服务
    // session timeout 30s   zkclient API 网络IO线程 1/3 * timeout 时间发送ping消息（心跳消息）   
    ZkClient zkCli;
    zkCli.Start();
    // service_name为永久性节点 method_name为临时性节点
    for(auto &sp : m_serviceMap){
        // /service_name
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.data(), nullptr,0, 0);
        for(auto &mp: sp.second.m_methodMap){
            // /service_name/method_name
            std::string method_path = service_path + "/" + mp.first;
            // ip + port
            std::string endpoint = ip + ":" + std::to_string(port);
            zkCli.Create(method_path.data(), endpoint.data(), static_cast<int>(endpoint.size()), ZOO_EPHEMERAL);
        }
    }
    std::cout << "[RpcProvider] start service at ip: " << ip << " port: " << port << std::endl;
    // 启动网络服务
    server.start();
    m_eventLoop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr& conn){
    if(!conn->connected()){
        // 和rpc client断开连接了
        conn->shutdown();
    }
}

/*
在框架内部， RpcProvider和RpcConsumer协商好之间通信用的protobuf数据类型 （protobuf)
service_name method_name args  定义proto的message类型，进行数据头的序列化和反序列化。

*/
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
        muduo::net::Buffer *buffer, muduo::Timestamp time)
{    
    while(buffer->readableBytes() > 0){
        std::string input(buffer->peek(), buffer->readableBytes());

        mprpc::MprpcFrame rpc_frame;
        size_t bytes_consumed = 0;
        mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::Decode(input, &rpc_frame, &bytes_consumed);  
        if(decode_status == mprpc::DecodeStatus::OK){
            buffer->retrieve(bytes_consumed);
            HandleRpcFrame(conn, rpc_frame);
            continue;
        }

        if(decode_status == mprpc::DecodeStatus::NEED_MORE_DATA) break;

        std::cout << "Decode rpc frame error" << std::endl;
        conn->shutdown();
        break;
    }
}

//closure回调操作，用于序列化rpc的响应和网络发送。
void RpcProvider::SendRpcResponse(muduo::net::TcpConnectionPtr conn, RpcResponseContext* context){
    std::string response_payload;
    // response进行序列化
    if(!context->response->SerializeToString(&response_payload)){
        std::cout <<"Serialize response_str error!" << std::endl;
        conn->shutdown();
        delete context;
        return;
    }

    mprpc::MprpcResponseMeta response_meta;
    response_meta.set_error_msg("");
    std::string meta;
    if(!response_meta.SerializeToString(&meta)){
        std::cout <<"Serialize response_str error!" << std::endl;
        conn->shutdown();
        delete context;
        return;
    }

    std::string body = mprpc::MprpcCodec::EncodeBody(meta, response_payload);

    mprpc::MprpcHeader header;
    header.request_id = context->request_id;
    header.message_type = mprpc::MprpcMessageType::RESPONSE;
    header.status_code = mprpc::MprpcErrorCode::OK;
    header.checksum = 0;

    std::string frame = mprpc::MprpcCodec::Encode(header, body);

    conn->send(frame);
    conn->shutdown(); // 模拟http的短链接服务，由rpcprovider主动断开连接。
    delete context;
}

void RpcProvider::SendRpcErrorResponse(const muduo::net::TcpConnectionPtr& conn, uint64_t request_id, mprpc::MprpcErrorCode error_code, const std::string& err_msg){
    mprpc::MprpcResponseMeta meta_response;
    meta_response.set_error_msg(err_msg);
    std::string meta;
    if(!meta_response.SerializeToString(&meta)){
        std::cout << "Serialize error response meta failed!" << std::endl;
        conn->shutdown();
        return;
    }

    std::string body = mprpc::MprpcCodec::EncodeBody(meta, "");

    mprpc::MprpcHeader header;
    header.request_id = request_id;
    header.status_code = error_code;
    header.message_type = mprpc::MprpcMessageType::RESPONSE;
    header.checksum = 0;

    std::string frame = mprpc::MprpcCodec::Encode(header, body);
    conn->send(frame);
    conn->shutdown();
}

void RpcProvider::HandleRpcFrame(const muduo::net::TcpConnectionPtr& conn, const mprpc::MprpcFrame& frame){

    mprpc::MprpcBody rpc_body;
    mprpc::DecodeStatus decode_status = mprpc::MprpcCodec::DecodeBody(frame.body, &rpc_body);
    if(decode_status != mprpc::DecodeStatus::OK){
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::BAD_FRAME, "Decode rpc body error!");
        return;
    }

    mprpc::MprpcRequestMeta rpc_meta;
    if(!rpc_meta.ParseFromString(rpc_body.meta)){
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::PARSE_ERROR, "Parse rpc meta error!");
        return;
    }

    const std::string& service_name = rpc_meta.service_name();
    const std::string& method_name = rpc_meta.method_name();
    const uint32_t timeout_ms = rpc_meta.timeout_ms();
    const std::string& payload = rpc_body.payload;

    std::cout << "==============================================" << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "timeout_ms: " << timeout_ms << std::endl;
    std::cout << "payload_size: " << payload.size() << std::endl;
    std::cout << "==============================================" << std::endl;

    auto it = m_serviceMap.find(service_name);
    if(it == m_serviceMap.end()){
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::SERVICE_NOT_FOUND, service_name + " does not exist!");
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if(mit == it->second.m_methodMap.end()){
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::METHOD_NOT_FOUND, method_name + " does not exist!");
        return;
    }

    google::protobuf::Service *service = it->second.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    std::unique_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
    
    
    if(!request->ParseFromString(payload)){
        SendRpcErrorResponse(conn, frame.header.request_id, mprpc::MprpcErrorCode::PARSE_ERROR, "Parse request payload error!");
        return;
    }

    auto response = std::unique_ptr<google::protobuf::Message>(service->GetResponsePrototype(method).New());

    google::protobuf::Message* response_raw = response.get();

    RpcResponseContext* response_context = new RpcResponseContext{std::move(response), frame.header.request_id};
    google::protobuf::Closure* done = google::protobuf::NewCallback<RpcProvider, 
                                                                    muduo::net::TcpConnectionPtr,
                                                                    RpcResponseContext*>(
                                                                        this,
                                                                        &RpcProvider::SendRpcResponse, 
                                                                        conn, 
                                                                        response_context
                                                                    );
    service->CallMethod(method, nullptr, request.get(), response_raw, done);
} 