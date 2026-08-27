#include <iostream>

#include "rpcdispatchstate.h"

int main()
{
    RpcDispatchState cancelled;
    const bool cancel_won = cancelled.CancelQueued();
    const bool cancelled_never_started = !cancelled.TryStart() &&
        cancelled.Phase() == RpcDispatchPhase::Cancelled;

    RpcDispatchState running;
    const bool worker_won = running.TryStart();
    const bool running_not_retroactively_cancelled =
        !running.CancelQueued() &&
        running.Phase() == RpcDispatchPhase::Running;
    running.Finish();

    const bool passed = cancel_won && cancelled_never_started &&
        worker_won && running_not_retroactively_cancelled &&
        running.Phase() == RpcDispatchPhase::Finished;

    std::cout << (passed ? "PASS" : "FAIL")
              << ": RPC business dispatch state transitions\n";
    return passed ? 0 : 1;
}
