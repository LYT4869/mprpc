#include <iostream>
#include <memory>

#include "rpcclientruntime.h"

int main()
{
    auto runtime = std::make_shared<RpcClientRuntime>(2);

    auto* first = runtime->NextLoop();
    auto* second = runtime->NextLoop();
    auto* third = runtime->NextLoop();
    auto* fourth = runtime->NextLoop();
    auto* fifth = runtime->NextLoop();

    const bool passed = runtime->IoThreadCount() == 2 &&
        first != nullptr && second != nullptr && first != second &&
        first == third && second == fourth && first == fifth;

    std::cout << (passed ? "PASS" : "FAIL")
              << ": client runtime round-robin loop assignment\n";
    return passed ? 0 : 1;
}
