#include <filesystem>
#include <iostream>
#include <string>

#include "file_transfer_service.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"

namespace
{
std::string FindUploadRoot(int argc, char** argv)
{
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "-i") {
            ++i;
            continue;
        }
        return argument;
    }
    return {};
}
} // namespace

int main(int argc, char** argv)
{
    const std::string upload_root = FindUploadRoot(argc, argv);
    if (upload_root.empty()) {
        std::cerr << "usage: file_transfer_server -i <config> "
                     "<upload-root>\n";
        return 1;
    }

    MprpcApplication::Init(argc, argv);

    mprpc::file::FileTransferServiceImpl service{
        std::filesystem::path(upload_root)};

    RpcProvider provider;
    provider.NotifyService(&service);
    provider.Run();
    return 0;
}
