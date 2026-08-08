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
    if(request == nullptr || !request->SerializeToString(&request_payload)){
        if(controller != nullptr){
            controller->SetFailed("Serialize request failed!");
        }
        if(done != nullptr){
            done->Run();
        }
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

    if(done == nullptr){
        // 同步调用

        CallHandle call = core_->StartCall(service_name, method_name, request_payload, options, {});
        RpcCallResult  result = core_->WaitCall(call);

        FinishProtobufCall(controller, response, nullptr, result);
        return;
    }else{
        // 异步调用
        RpcCompletion completion = [controller, response, done](const RpcCallResult& result){
            FinishProtobufCall(controller, response, done, result);
        };
        core_->StartCall(service_name, method_name, request_payload, options, std::move(completion));
    }
}

void MprpcChannel::FinishProtobufCall(
    google::protobuf::RpcController* controller,
    google::protobuf::Message* response,
    google::protobuf::Closure* done,
    const RpcCallResult& result
)
{
    if(!result.Ok()){
        if(controller != nullptr){
            std::string error_msg = result.error_msg.empty() ? "RPC call failed" : result.error_msg;
            controller->SetFailed(error_msg);
        }
    }else if(response == nullptr){
        if(controller != nullptr){
            controller->SetFailed("RPC response object is null!");
        }
    }else if(!response->ParseFromString(result.response_payload)){
        if(controller != nullptr){
            controller->SetFailed("Response payload parsing error!");
        }
    }

    if (done != nullptr) {
        done->Run();
    }
}