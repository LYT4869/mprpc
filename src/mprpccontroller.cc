#include "mprpccontroller.h"

MprpcController::MprpcController(){
    m_failed = false;
    m_errText = "";
    m_timeoutMs = 3000;
    error_code_ = mprpc::MprpcErrorCode::OK;
    affinity_key_.clear();
}
void MprpcController::Reset(){
    std::lock_guard<std::mutex> lock(mutex_);
    m_failed = false;
    m_errText = "";
    m_timeoutMs = 3000;
    error_code_ = mprpc::MprpcErrorCode::OK;
    affinity_key_.clear();
    canceled_ = false;
    cancel_handler_ = {};
    cancel_callbacks_.clear();
}
bool MprpcController::Failed() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return m_failed;
}
std::string MprpcController::ErrorText() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return m_errText;
}
void MprpcController::SetFailed(const std::string& reason){
    std::lock_guard<std::mutex> lock(mutex_);
    m_failed = true;
    m_errText = reason;
    if (error_code_ == mprpc::MprpcErrorCode::OK) {
        error_code_ = mprpc::MprpcErrorCode::INTERNAL_ERROR;
    }
}

void MprpcController::SetFailed(
    mprpc::MprpcErrorCode code, const std::string& reason)
{
    std::lock_guard<std::mutex> lock(mutex_);
    m_failed = true;
    error_code_ = code;
    m_errText = reason;
}

mprpc::MprpcErrorCode MprpcController::ErrorCode() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return error_code_;
}

void MprpcController::SetTimeoutMs(uint32_t timeout_ms){
    std::lock_guard<std::mutex> lock(mutex_);
    m_timeoutMs = timeout_ms;
}
uint32_t MprpcController::TimeoutMs() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return m_timeoutMs;
}
void MprpcController::SetAffinityKey(std::string affinity_key){
    std::lock_guard<std::mutex> lock(mutex_);
    affinity_key_ = std::move(affinity_key);
}
std::string MprpcController::AffinityKey() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return affinity_key_;
}
void MprpcController::StartCancel(){
    std::function<void()> handler;
    std::vector<google::protobuf::Closure*> callbacks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (canceled_) {
            return;
        }
        canceled_ = true;
        handler = cancel_handler_;
        callbacks.swap(cancel_callbacks_);
    }

    if (handler) {
        handler();
    }
    for (auto* callback : callbacks) {
        if (callback != nullptr) {
            callback->Run();
        }
    }
}
bool MprpcController::IsCanceled() const{
    std::lock_guard<std::mutex> lock(mutex_);
    return canceled_;
}
void MprpcController::NotifyOnCancel(google::protobuf::Closure* callback){
    if (callback == nullptr) {
        return;
    }

    bool run_now = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (canceled_) {
            run_now = true;
        } else {
            cancel_callbacks_.push_back(callback);
        }
    }
    if (run_now) {
        callback->Run();
    }
}

void MprpcController::SetCancelHandler(std::function<void()> handler)
{
    std::function<void()> run_handler;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancel_handler_ = std::move(handler);
        if (canceled_) {
            run_handler = cancel_handler_;
        }
    }
    if (run_handler) {
        run_handler();
    }
}

void MprpcController::ClearCancelHandler()
{
    std::lock_guard<std::mutex> lock(mutex_);
    cancel_handler_ = {};
}
