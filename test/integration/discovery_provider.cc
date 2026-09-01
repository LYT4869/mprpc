#include "friend.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"

#include <string>

class DiscoveryService final : public fixbug::FriendServiceRpc
{
public:
    explicit DiscoveryService(std::string identity)
        : identity_(std::move(identity))
    {
    }

    void GetFriendList(
        google::protobuf::RpcController*,
        const fixbug::GetFriendListRequest*,
        fixbug::GetFriendListResponse* response,
        google::protobuf::Closure* done) override
    {
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->add_friends(identity_);
        if (done != nullptr) {
            done->Run();
        }
    }

private:
    std::string identity_;
};

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);
    const std::string identity =
        MprpcApplication::GetConfig().Load("rpcserverport");

    RpcProvider provider;
    provider.NotifyService(new DiscoveryService(identity));
    provider.Run();
    return 0;
}
