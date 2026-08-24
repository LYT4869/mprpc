#include <iostream>
#include <string>
#include "friend.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "logger.h"
#include <chrono>
#include <thread>

// 提供本地方法Login
// 需求：利用框架将本地方法变成rpc远程方法 
class FriendrService : public fixbug::FriendServiceRpc //rpc提供者 ： 发布rpc方法
{
public:
    std::vector<std::string> GetFriendList(uint32_t user_id){
        std::cout <<"do GetFriendList service! userid" << user_id << std::endl;
        std::vector<std::string> friendList = {"Li Si", "Zhao Liu"};
        return friendList;
    }

    void GetFriendList(google::protobuf::RpcController* controller,
                        const ::fixbug::GetFriendListRequest* request,
                        ::fixbug::GetFriendListResponse* response,
                        ::google::protobuf::Closure* done) override
    {
        uint32_t user_id = request->userid();

        if (user_id == 9998) {
            if (controller != nullptr) {
                controller->SetFailed("friend service rejected request");
            }
            if (done != nullptr) {
                done->Run();
            }
            return;
        }

        if(user_id == 9999) {
            std::thread(
                [controller, response, done] {
                    for (int i = 0; i < 20; ++i) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(50));
                        if (controller != nullptr &&
                            controller->IsCanceled()) {
                            std::cout << "SERVER_CANCEL_OBSERVED"
                                      << std::endl;
                            if (done != nullptr) {
                                done->Run();
                            }
                            return;
                        }
                    }
                    response->add_friends("Li Si");
                    response->add_friends("Zhao Liu");
                    auto* result = response->mutable_result();
                    result->set_errcode(0);
                    result->set_errmsg("");
                    if (done != nullptr) {
                        done->Run();
                    }
                })
                .detach();
            return;
        }
        if(user_id >= 10000 &&
            user_id < 10010) {
                int delay_ms = (10009 - user_id) * 50;
                std::thread(
                    [user_id, response, done, delay_ms]{
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(delay_ms)
                        );

                        response->add_friends("user-" + std::to_string(user_id));

                        fixbug::ResultCode *result = response->mutable_result();
                        result->set_errcode(0);
                        result->set_errmsg("");

                        if (done != nullptr) {
                            done->Run();
                        }
                    })
                    .detach();
                return;
            }
        std::vector<std::string> friendList = GetFriendList(user_id);
        for(int i = 0; i < friendList.size(); i++){
            response->add_friends(friendList[i]);
        }
        fixbug::ResultCode *result = response->mutable_result();
        result->set_errcode(0);
        result->set_errmsg("");

        if(done) done->Run();
    }
};

int main(int argc, char **argv){
    //调用框架的初始化操作 provider -i config.conf 
    MprpcApplication::Init(argc, argv);

    //provider是一个rpc网络服务对象
    RpcProvider provider; //用于发布服务的对象

    //把UserService对象发布到rpc节点上。
    provider.NotifyService(new FriendrService());

    // 启动一个rpc服务发布节点，Run以后，进程进入阻塞状态，等待远程的rpc调用请求。
    provider.Run();
    return 0;
}
