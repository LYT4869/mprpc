#include <iostream>
#include <memory>

#include "channelcore.h"
#include "rpcclientruntime.h"

int main()
{
    auto runtime = std::make_shared<RpcClientRuntime>(1, 1, 1);
    auto occupied = runtime->TryReserveCallback();
    auto core = std::make_shared<ChannelCore>(runtime);

    bool callback_called = false;
    CallOptions options;
    auto call = core->StartCall(
        "UnavailableService", "UnavailableMethod", {}, options,
        [&](const RpcCallResult& result) {
            callback_called = true;
            if (result.status_code !=
                mprpc::MprpcErrorCode::CALLBACK_REJECTED) {
                callback_called = false;
            }
        });

    const RpcMetricsSnapshot metrics = core->GetMetricsSnapshot();
    const bool passed = occupied && callback_called && call->completed &&
        call->phase.load(std::memory_order_acquire) == CallPhase::Completed &&
        call->result.status_code ==
            mprpc::MprpcErrorCode::CALLBACK_REJECTED &&
        metrics.total.started == 1 && metrics.total.active == 0 &&
        metrics.total.callback_rejected == 1;

    core->Shutdown();
    std::cout << (passed ? "PASS" : "FAIL")
              << ": async callback capacity admission\n";
    return passed ? 0 : 1;
}
