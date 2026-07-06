#include <iostream>
#include <string>
#include "user.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
// 提供本地方法Login
// 需求：利用框架将本地方法变成rpc远程方法 
class UserService : public fixbug::UserServiceRpc //rpc提供者 ： 发布rpc方法
{
public:
    bool login(std::string name, std::string pwd){
        std::cout << "doing local service: Login " << std::endl;
        std::cout << "name: " << name << " pwd:" << pwd << std::endl;
        return true;
    }
    bool Register(uint32_t id, std::string name, std::string pwd){
        std::cout << "doing local service: Register" << std::endl;
        std::cout << "id: " << id << " name: " << name << " pwd:" << pwd << std::endl;
        return true;
    }
    /* 
    重写基类UserServiceRpc的虚函数，直接对框架进行调用
    1. caller --> 发起rpc调用请求 Login(LoginRequest) => muduo => callee;
    2. callee --> Login(LoginRequest) => 交到重写的Login方法上
    */
   //发布rpc方法
    void login(::google::protobuf::RpcController* controller, 
                        const ::fixbug::LoginRequest* request, //远端rpc请求
                        ::fixbug::LoginResponse* response, //完成业务后将业务结果填写rpc响应
                        ::google::protobuf::Closure* done) //业务完成后执行回调, 通过回调将响应结果交还给调用方
    {
        //框架给业务上报了请求参数LoginRequest, 业务获取反序列化后的相应数据做本地业务(反序列化已经在框架中完成)
        std::string name = request->name();
        std::string pwd = request->pwd();

        // 做本地业务
        bool login_result = login(name, pwd); 

        //封装响应，写入response: 包括错误码、错误消息、返回值；
        fixbug::ResultCode *code = response->mutable_result();
        code->set_errcode(0);
        code->set_errmsg ("");
        response->set_sucess(login_result);

        // 执行回调操作 执行相应对象数据的序列化和网络发送（由框架完成)
        done->Run();
    }

    void Register(::google::protobuf::RpcController* controller, 
                        const ::fixbug::RegisterRequest* request, //远端rpc请求
                        ::fixbug::RegisterResponse* response, //完成业务后将业务结果填写rpc响应
                        ::google::protobuf::Closure* done) //业务完成后执行回调, 通过回调将响应结果交还给调用方
    {
        uint32_t id = request->id();
        std::string name = request->name();
        std::string pwd = request->pwd();

        bool res = Register(id, name, pwd);
        if(res){
            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("");
        }else{
            response->mutable_result()->set_errcode(1);
            response->mutable_result()->set_errmsg("Register error!");
        }
        response->set_sucess(res);
        done->Run();
    }
};

int main(int argc, char **argv){
    //调用框架的初始化操作 provider -i config.conf 
    MprpcApplication::Init(argc, argv);

    //provider是一个rpc网络服务对象
    RpcProvider provider; //用于发布服务的对象

    //把UserService对象发布到rpc节点上。
    provider.NotifyService(new UserService());

    // 启动一个rpc服务发布节点，Run以后，进程进入阻塞状态，等待远程的rpc调用请求。
    provider.Run();
    return 0;
}