#include "mprpcchannel.h"
#include "mprpccontroller.h"

MprpcChannel::MprpcChannel() : core_(std::make_shared<ChannelCore>())
{

}

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done)
{
    const google::protobuf::ServiceDescriptor *sd = method->service();

    std::string service_name = sd->name(); // service_name
    std::string method_name = method->name(); // method_name

    // 获取参数的序列化字符串长度args_size
    std::string request_payload;
    if(!request->SerializeToString(&request_payload)){
        controller->SetFailed("Serialize request error!");
        return;
    }

    CallOptions options;
    auto* mprpc_controller = dynamic_cast<MprpcController*>(controller);
    if(mprpc_controller != nullptr){
        options.timeout_ms = mprpc_controller->TimeoutMs();
    }

    std::cout << "==============================================" << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "timeout_ms: " << options.timeout_ms << std::endl;
    std::cout << "==============================================" << std::endl;

    RpcCallResult result = core_->StartCall(service_name, method_name, request_payload, options);

    if(!result.Ok()){
        controller->SetFailed(result.error_msg);
        return;
    }

    if(!response->ParseFromString(result.response_payload)){
        controller->SetFailed("Response payload parsing error!");
        return;
    }
    if (done != nullptr) {
        done->Run();
    }
}
