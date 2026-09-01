#include "friend.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

#include <iostream>

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);

    MprpcChannel channel;
    fixbug::FriendServiceRpc_Stub stub(&channel);
    fixbug::GetFriendListRequest request;
    request.set_userid(1234);
    fixbug::GetFriendListResponse response;
    MprpcController controller;
    controller.SetTimeoutMs(2000);

    stub.GetFriendList(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.friends_size() != 2) {
        std::cerr << "FAIL: RPC through re-registered Provider failed: "
                  << controller.ErrorText() << '\n';
        return 1;
    }

    std::cout << "PASS: new client called the registered Provider\n";
    return 0;
}
