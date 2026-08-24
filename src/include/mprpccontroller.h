#pragma once
#include <string>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>
#include <google/protobuf/service.h>
#include "mprpccodec.h"

// 保存一次 RPC 的错误、超时、路由亲和性和取消状态。
class MprpcController: public google::protobuf::RpcController{
public:
    MprpcController();
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;
    void SetFailed(mprpc::MprpcErrorCode code, const std::string& reason);
    mprpc::MprpcErrorCode ErrorCode() const;
    // 配置单次 RPC 的超时时间。
    void SetTimeoutMs(uint32_t timeout_ms);
    uint32_t TimeoutMs() const;
    // 配置稳定路由键，适用于文件上传等有状态服务。
    void SetAffinityKey(std::string affinity_key);
    std::string AffinityKey() const;
    // 发起取消并执行已注册的框架处理器和用户回调。
    void StartCancel() override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

    void SetCancelHandler(std::function<void()> handler);
    void ClearCancelHandler();
private:
    mutable std::mutex mutex_;
    bool m_failed;  //RPC方法执行过程中的状态
    std::string m_errText; // RPC方法执行过程中的错误信息
    uint32_t m_timeoutMs;
    std::string affinity_key_;
    mprpc::MprpcErrorCode error_code_ = mprpc::MprpcErrorCode::OK;
    bool canceled_ = false;
    std::function<void()> cancel_handler_;
    std::vector<google::protobuf::Closure*> cancel_callbacks_;
};
