#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcclientruntime.h"

MprpcChannel::MprpcChannel()
    : MprpcChannel(RpcClientRuntime::Default())
{
}

MprpcChannel::MprpcChannel(std::shared_ptr<RpcClientRuntime> runtime)
    : core_(std::make_shared<ChannelCore>(std::move(runtime)))
{
}

MprpcChannel::~MprpcChannel()
{
    if (core_) {
        core_->Shutdown();
    }
}

RpcMetricsSnapshot MprpcChannel::GetMetricsSnapshot() const
{
    return core_ ? core_->GetMetricsSnapshot() : RpcMetricsSnapshot{};
}

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done)
{
    const google::protobuf::ServiceDescriptor *sd = method->service();

    std::string service_name = sd->name(); // service_name
    std::string method_name = method->name(); // method_name

    // 同步完成序列化，返回后调用方即可销毁 request。
    std::string request_payload;
    if(request == nullptr || !request->SerializeToString(&request_payload)){
        if(controller != nullptr){
            auto* typed = dynamic_cast<MprpcController*>(controller);
            if (typed != nullptr) {
                typed->SetFailed(mprpc::MprpcErrorCode::SERIALIZE_FAILED,
                                 "Serialize request failed!");
            } else {
                controller->SetFailed("Serialize request failed!");
            }
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
        options.affinity_key = mprpc_controller->AffinityKey();
    }

    if(done == nullptr){
        // 同步调用

        if (core_->IsInIoThread()) {
            if (controller != nullptr) {
                if (mprpc_controller != nullptr) {
                    mprpc_controller->SetFailed(
                        mprpc::MprpcErrorCode::IO_THREAD_BLOCKING_CALL,
                        "Synchronous RPC cannot run on the I/O thread");
                } else {
                    controller->SetFailed(
                        "Synchronous RPC cannot run on the I/O thread");
                }
            }
            return;
        }

        CallHandle call = core_->StartCall(service_name, method_name, request_payload, options, {});
        if (mprpc_controller != nullptr) {
            std::weak_ptr<ChannelCore> weak_core = core_;
            const uint64_t request_id = call->request_id;
            mprpc_controller->SetCancelHandler([weak_core, request_id] {
                if (auto core = weak_core.lock()) {
                    core->CancelCall(request_id);
                }
            });
            if (call->phase.load(std::memory_order_acquire) ==
                CallPhase::Completed) {
                mprpc_controller->ClearCancelHandler();
            }
        }
        RpcCallResult  result = core_->WaitCall(call);

        if (mprpc_controller != nullptr) {
            mprpc_controller->ClearCancelHandler();
        }

        FinishProtobufCall(controller, response, nullptr, result);
        return;
    }else{
        // 异步调用
        // 这些 Protobuf 对象仅被借用，必须存活到 done 执行结束。
        RpcCompletion completion = [controller, response, done, mprpc_controller](const RpcCallResult& result){
            if (mprpc_controller != nullptr) {
                mprpc_controller->ClearCancelHandler();
            }
            FinishProtobufCall(controller, response, done, result);
        };
        CallHandle call = core_->StartCall(
            service_name, method_name, request_payload, options,
            std::move(completion));
        if (mprpc_controller != nullptr) {
            std::weak_ptr<ChannelCore> weak_core = core_;
            const uint64_t request_id = call->request_id;
            mprpc_controller->SetCancelHandler([weak_core, request_id] {
                if (auto core = weak_core.lock()) {
                    core->CancelCall(request_id);
                }
            });
            if (call->phase.load(std::memory_order_acquire) ==
                CallPhase::Completed) {
                mprpc_controller->ClearCancelHandler();
            }
        }
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
            auto* typed = dynamic_cast<MprpcController*>(controller);
            if (typed != nullptr) {
                typed->SetFailed(result.status_code, error_msg);
            } else {
                controller->SetFailed(error_msg);
            }
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
        // NewCallback 创建的 Closure 可能在 Run() 内自销毁。
        done->Run();
    }
}
