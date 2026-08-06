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
    for(int i = 0; i < 2; i++){
        fixbug::GetFriendListRequest request;
        request.set_userid(1234);
        fixbug::GetFriendListResponse response;

        MprpcController controller;
        stub.GetFriendList(&controller, &request, &response, nullptr);
        std::vector<std::string> friendList;
        if(controller.Failed()){
            std::cout << "call" << i 
                    <<" failed: "
                    << controller.ErrorText()
                    << std::endl;
            continue;    
        }

        std::cout << "call " << i
                << " sucess, friends: "
                << response.friends_size()
                << std::endl;
    }
    return 0;
}

