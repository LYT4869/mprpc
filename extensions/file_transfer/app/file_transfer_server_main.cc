#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

#include "file_transfer_service.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "rpcmetrics.h"

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

std::chrono::milliseconds MetricsInterval()
{
    const std::string configured =
        MprpcApplication::GetInstance().GetConfig().Load(
            "metricsintervalms");
    if (configured.empty()) {
        return std::chrono::seconds(10);
    }
    try {
        return std::chrono::milliseconds(std::stoll(configured));
    } catch (...) {
        return std::chrono::seconds(10);
    }
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

    RpcMetricsReporter reporter;
    reporter.AddSource("provider", [&provider] {
        return FormatRpcMetrics(
            "provider", provider.GetMetricsSnapshot());
    });
    reporter.AddSource("file_service", [&service] {
        const auto stats = service.GetTaskStats();
        std::ostringstream output;
        output << "file_metrics"
               << " accepted=" << stats.accepted
               << " completed=" << stats.completed
               << " queue_rejected=" << stats.rejected
               << " current_outstanding=" << stats.current_outstanding
               << " peak_outstanding=" << stats.peak_outstanding
               << " cancelled=" << stats.cancelled
               << " crc_failure=" << stats.crc_failure
               << " session_conflict=" << stats.session_conflict
               << " duplicate_chunk=" << stats.duplicate_chunk
               << " bytes_transferred=" << stats.bytes_transferred;
        return output.str();
    });
    reporter.Start(MetricsInterval(), &std::cerr);
    provider.Run();
    return 0;
}
