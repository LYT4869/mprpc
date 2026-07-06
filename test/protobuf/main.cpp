#include "test.pb.h"
#include <iostream>
#include <string>
using namespace fixbug;
int main(){
    // LoginResponse rsp;
    // ResultCode *rc = rsp.mutable_result();
    // rc->set_errcode(0);
    // rc->set_errmsg("Login err");

    GetFriendListsResponse rsp;
    ResultCode *rc = rsp.mutable_result();
    rc->set_errcode(0);
    
    User *user1 = rsp.add_users();
    user1->set_name("张三");
    user1->set_age(18);
    user1->set_sex(User::MALE);

    User *user2 = rsp.add_users();
    user2->set_name("李四");
    user2->set_age(17);
    user2->set_sex(User::FEMALE);

    std::cout << rsp.users_size() << std::endl;
}
int main1(){
    //封装了login请求对象的数据
    LoginRequest req;
    req.set_name("Zhang San");
    req.set_pwd("123456");

    //对象数据序列化 -> char*
    std::string send_str;
    if(req.SerializeToString(&send_str)){
        std::cout << send_str << std::endl;
    }

    // 从send_str反序列化一个login请求对象
    LoginRequest reqB;
    if(reqB.ParseFromString(send_str)){
        std::cout << reqB.name() << std::endl;
        std::cout << reqB.pwd() << std::endl;
    }
    return 0;
}