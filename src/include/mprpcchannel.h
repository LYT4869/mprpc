#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include "mprpccontroller.h"
#include "channelcore.h"

// Protobuf RpcChannel 适配层，负责消息序列化和同步/异步调用语义。
class MprpcChannel : public google::protobuf::RpcChannel
{
public:
    MprpcChannel();
    ~MprpcChannel() override;
    // Generated Stub 的统一入口；done 为空表示同步调用，否则为异步调用。
    void CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                        const google::protobuf::Message* request, google::protobuf::Message* response,
                        google::protobuf::Closure* done) override;

    RpcMetricsSnapshot GetMetricsSnapshot() const;

private:
    std::shared_ptr<ChannelCore> core_;
    // 将核心层结果写回 controller/response，并在异步调用中执行 done。
    static void FinishProtobufCall(
        google::protobuf::RpcController* controller,
        google::protobuf::Message* response,
        google::protobuf::Closure* done,
        const RpcCallResult& result
    );
};
