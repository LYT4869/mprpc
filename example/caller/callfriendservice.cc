#include <iostream>
#include "mprpcapplication.h"
#include "friend.pb.h"
#include <string>

int main(int argc, char **argv){
    //整个程序启动以后，想使用mprpc框架来享受rpc服务调用，一定需要先调用框架的初始化函数。
    MprpcApplication::Init(argc, argv);
    
    // 演示调用远程发布的rpc方法Login
    fixbug::FriendServiceRpc_Stub stub(new MprpcChannel());
    // rpc方法的请求参数
    fixbug::GetFriendListRequest request;
    request.set_userid(1234);
    fixbug::GetFriendListResponse response;

    MprpcController controller;
    stub.GetFriendList(&controller, &request, &response, nullptr);
    std::vector<std::string> friendList;
    if(!controller.Failed()){
        if(0 == response.result().errcode()){
            std::cout << "rpc register response success!"<< std::endl;
            int size = response.friends_size();
            for(int i = 0; i < size; i++){
                std::cout << "index: " << (i + 1) << " name:" << response.friends(i) << std::endl;
            }
        }
        else{
            std::cout << "rpc register response error : " << response.result().errmsg() << std::endl;
        }
    }else{
        std::cout <<"rpc connection failed! Error info: " << controller.ErrorText() << std::endl;
    }
    return 0;
}

