#include <iostream>
#include "mprpcapplication.h"
#include "user.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

int main(int argc, char **argv){
    //整个程序启动以后，想使用mprpc框架来享受rpc服务调用，一定需要先调用框架的初始化函数。
    MprpcApplication::Init(argc, argv);
    // 演示调用远程发布的rpc方法Login
    fixbug::UserServiceRpc_Stub stub(new MprpcChannel());
    // rpc方法的请求参数
    fixbug::LoginRequest request;
    request.set_name("Zhang San");
    request.set_pwd("1123456");
    // rpc方法的相应
    fixbug::LoginResponse response;
    // 发起rpc方法的调用 同步的rpc调用过程 MprpcChannel::CallMethod
    MprpcController controller;
    stub.login(&controller, &request, &response, nullptr); // RpcChannel->RpcChannel::callMethod 集中来做所有rpc方法调用的参数序列化和网络发送
    if (controller.Failed()) {
        std::cout << "rpc login failed: " << controller.ErrorText() << std::endl;
        return 0;
    }
    // 一次rpc调用完成，读调用的结果
    if(0 == response.result().errcode()){
        std::cout << "rpc login response success: " << response.sucess() << std::endl;
    }
    else{
        std::cout << "rpc login response error : " << response.result().errmsg() << std::endl;
    }


    // 注册
    fixbug::RegisterRequest regRequest;
    fixbug::RegisterResponse regResponse;
    regRequest.set_id(123);
    regRequest.set_name("Zhang San");
    regRequest.set_pwd("1234567");
    MprpcController regController;
    stub.Register(&regController, &regRequest, &regResponse, nullptr);
    if (regController.Failed()) {
        std::cout << "rpc register failed: " << regController.ErrorText() << std::endl;
        return 0;
    }
    if(0 == regResponse.result().errcode()){
        std::cout << "rpc register response success: " << regResponse.sucess() << std::endl;
    }
    else{
        std::cout << "rpc register response error : " << regResponse.result().errmsg() << std::endl;
    }
    return 0;
}

