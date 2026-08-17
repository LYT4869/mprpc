#pragma once
#include <string>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>
#include <google/protobuf/service.h>
#include "mprpccodec.h"

class MprpcController: public google::protobuf::RpcController{
public:
    MprpcController();
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;
    void SetFailed(mprpc::MprpcErrorCode code, const std::string& reason);
    mprpc::MprpcErrorCode ErrorCode() const;
    void SetTimeoutMs(uint32_t timeout_ms);
    uint32_t TimeoutMs() const;
    void SetAffinityKey(std::string affinity_key);
    std::string AffinityKey() const;
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
