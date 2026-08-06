#pragma once
#include <string>
#include <cstdint>
#include <google/protobuf/service.h>

class MprpcController: public google::protobuf::RpcController{
public:
    MprpcController();
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;
    void SetTimeoutMs(uint32_t timeout_ms);
    uint32_t TimeoutMs() const;
    // 目前未实现具体功能
    void StartCancel() override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;
private:
    bool m_failed;  //RPC方法执行过程中的状态
    std::string m_errText; // RPC方法执行过程中的错误信息
    uint32_t m_timeoutMs;
};