#include "mprpccontroller.h"

MprpcController::MprpcController(){
    m_failed = false;
    m_errText = "";
    m_timeoutMs = 3000;
}
void MprpcController::Reset(){
    m_failed = false;
    m_errText = "";
    m_timeoutMs = 3000;
}
bool MprpcController::Failed() const{
    return m_failed;
}
std::string MprpcController::ErrorText() const{
    return m_errText;
}
void MprpcController::SetFailed(const std::string& reason){
    m_failed = true;
    m_errText = reason;
}

void MprpcController::SetTimeoutMs(uint32_t timeout_ms){
    m_timeoutMs = timeout_ms;
}
uint32_t MprpcController::TimeoutMs() const{
    return m_timeoutMs;
}
// 目前未实现具体功能
void MprpcController::StartCancel(){
    
}
bool MprpcController::IsCanceled() const{
    return false;
}
void MprpcController::NotifyOnCancel(google::protobuf::Closure* callback){
    
}