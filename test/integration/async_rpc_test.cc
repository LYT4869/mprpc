#include <chrono>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <thread>

#include <google/protobuf/stubs/callback.h>

#include "friend.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"

struct AsyncContext
{
    MprpcController controller;
    fixbug::GetFriendListResponse response;
    
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    int callback_count = 0;
    std::chrono::steady_clock::time_point callback_time;
};

struct BatchState
{
    std::mutex mutex;
    std::condition_variable cv;

    int completed = 0;
    int failed = 0;
};

struct ConcurrentCallContext
{
    uint32_t expected_user_id = 0;
    
    MprpcController controller;
    fixbug::GetFriendListResponse response;
    
    std::shared_ptr<BatchState> batch;
};

void OnAsyncDone(std::shared_ptr<AsyncContext> context)
{
    {
        std::lock_guard<std::mutex> lock(context->mutex);
        
        context->callback_time = std::chrono::steady_clock::now();

        ++context->callback_count;
        context->completed = true;
    }
    context->cv.notify_one();
}

void OnConcurrentDone(std::shared_ptr<ConcurrentCallContext> context)
{
    bool correct = !context->controller.Failed() &&
                    context->response.friends_size() == 1 &&
                    context->response.friends(0) == 
                    "user-" + std::to_string(context->expected_user_id);
    {
        std::lock_guard<std::mutex> lock(context->batch->mutex);

        ++context->batch->completed;
        
        if(!correct){
            ++context->batch->failed;
        }
    }
    context->batch->cv.notify_one();
}

bool TestAsyncReturnsImmediately(fixbug::FriendServiceRpc_Stub* stub)
{
    fixbug::GetFriendListRequest request;
    request.set_userid(9999);
    
    std::shared_ptr<AsyncContext> context = std::make_shared<AsyncContext>();

    context->controller.SetTimeoutMs(3000);

    google::protobuf::Closure *done = google::protobuf::NewCallback(&OnAsyncDone, context);

    auto start = std::chrono::steady_clock::now();

    stub->GetFriendList(
        &context->controller, 
        &request, 
        &context->response, 
        done);

    auto return_time =
        std::chrono::steady_clock::now();
    auto elapsed = 
        std::chrono::duration_cast<std::chrono::milliseconds>(
            return_time - start
        );
    
    std::unique_lock<std::mutex> lock(context->mutex);

    bool completed = context->cv.wait_for(lock, 
        std::chrono::seconds(4),
        [&context]{
        return context->completed;
    });
    
    bool returned_before_callback  = completed && return_time < context->callback_time;
    
    bool passed = 
        returned_before_callback  &&
        completed &&
        context->callback_count == 1 &&
        !context->controller.Failed() &&
        context->response.friends_size() == 2;

    std::cout
        << "CallMethod elapsed: "
        << elapsed.count()
        << " ms\n"
        << (passed ? "PASS" : "FAIL")
        << ": async returns before callback"
        << std::endl;

    return passed;
}

bool TestAsyncTimeout(fixbug::FriendServiceRpc_Stub* stub){
    fixbug::GetFriendListRequest request;
    request.set_userid(9999);

    auto context = std::make_shared<AsyncContext>();
    context->controller.SetTimeoutMs(200);

    google::protobuf::Closure* done = google::protobuf::NewPermanentCallback(
        &OnAsyncDone,
        context
    );

    stub->GetFriendList(
        &context->controller,
        &request,
        &context->response,
        done
    );

    std::unique_lock<std::mutex> lock(context->mutex);
    bool completed = context->cv.wait_for(
        lock,
        std::chrono::seconds(2),
        [&context]{
            return context->completed;
        }
    );

    if(!completed){
        std::cout
            << "FAIL: timeout callback was not called"
            << std::endl;
        lock.unlock();
        delete done;
        return false;
    }

    bool timeout_reported = 
        context->controller.Failed() &&
        context->controller.ErrorText().find("timeout") != std::string::npos;

    lock.unlock();

     // 等待服务端一秒后的迟到响应到达。
     std::this_thread::sleep_for(
        std::chrono::milliseconds(1200)
     );

     lock.lock();
     int callback_count = context->callback_count;
     bool called_once =
        callback_count == 1;
    lock.unlock();

    delete done;

    bool passed = 
        timeout_reported &&
        called_once;

    std::cout
        << (passed ? "PASS" : "FAIL")
        << ": async timeout, callback_count = "
        << callback_count
        << ", error =\" "
        << context->controller.ErrorText()
        <<"\""
        << std::endl;
    return passed;
}

bool TestConcurrentAsyncCalls(
    fixbug::FriendServiceRpc_Stub* stub)
{ 
    constexpr int kCallCount = 10;
    std::shared_ptr<BatchState> batch = std::make_shared<BatchState>();
    for(int i = 0; i < kCallCount; i++){
        uint32_t expected_user_id = 10000 + i;

        auto context = std::make_shared<ConcurrentCallContext>();
        context->expected_user_id = expected_user_id;
        context->batch = batch;
        context->controller.SetTimeoutMs(3000);

        fixbug::GetFriendListRequest request;
        request.set_userid(expected_user_id);

        google::protobuf::Closure* done = 
            google::protobuf::NewCallback(
                &OnConcurrentDone,
                context
            );
        
        stub->GetFriendList(
            &context->controller,
            &request,
            &context->response,
            done
        );
    }

    std::unique_lock<std::mutex> lock(batch->mutex);
    bool all_completed = batch->cv.wait_for(
        lock,
        std::chrono::seconds(5),
        [&batch]{
            return batch->completed == kCallCount;
        }
    );

    bool passed = 
        all_completed &&
        batch->failed == 0;

    std::cout
        << (passed ? "PASS" : "FAIL")
        << ": concurrent async calls, completed="
        << batch->completed
        << ", failed="
        << batch->failed
        << std::endl;

    return passed;
}

bool TestCancellation(fixbug::FriendServiceRpc_Stub* stub)
{
    fixbug::GetFriendListRequest request;
    request.set_userid(9999);

    auto context = std::make_shared<AsyncContext>();
    context->controller.SetTimeoutMs(3000);
    google::protobuf::Closure* done =
        google::protobuf::NewPermanentCallback(&OnAsyncDone, context);

    stub->GetFriendList(&context->controller, &request,
                        &context->response, done);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    context->controller.StartCancel();

    std::unique_lock<std::mutex> lock(context->mutex);
    const bool completed = context->cv.wait_for(
        lock, std::chrono::seconds(1), [&context] {
            return context->completed;
        });
    lock.unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    lock.lock();
    const bool passed = completed && context->callback_count == 1 &&
        context->controller.IsCanceled() && context->controller.Failed() &&
        context->controller.ErrorText().find("cancel") != std::string::npos;
    lock.unlock();
    delete done;

    std::cout << (passed ? "PASS" : "FAIL")
              << ": cancellation completes exactly once" << std::endl;
    return passed;
}

bool TestChannelShutdown()
{
    auto channel = std::make_unique<MprpcChannel>();
    auto stub = std::make_unique<fixbug::FriendServiceRpc_Stub>(channel.get());

    fixbug::GetFriendListRequest request;
    request.set_userid(9999);
    auto context = std::make_shared<AsyncContext>();
    context->controller.SetTimeoutMs(5000);
    google::protobuf::Closure* done =
        google::protobuf::NewCallback(&OnAsyncDone, context);

    stub->GetFriendList(&context->controller, &request,
                        &context->response, done);
    stub.reset();
    channel.reset();

    std::unique_lock<std::mutex> lock(context->mutex);
    const bool completed = context->cv.wait_for(
        lock, std::chrono::seconds(1), [&context] {
            return context->completed;
        });
    const bool passed = completed && context->callback_count == 1 &&
        context->controller.Failed() &&
        context->controller.ErrorText().find("closed") != std::string::npos;

    std::cout << (passed ? "PASS" : "FAIL")
              << ": channel shutdown completes pending calls" << std::endl;
    return passed;
}

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);

    std::unique_ptr<MprpcChannel> channel = std::make_unique<MprpcChannel>();

    fixbug::FriendServiceRpc_Stub stub(channel.get());

    bool returns_immediately =
        TestAsyncReturnsImmediately(&stub);

    bool timeout_once =
        TestAsyncTimeout(&stub);

    bool concurrent =
        TestConcurrentAsyncCalls(&stub);

    bool cancelled =
        TestCancellation(&stub);

    bool channel_shutdown =
        TestChannelShutdown();

    bool passed =
        returns_immediately &&
        timeout_once &&
        concurrent &&
        cancelled &&
        channel_shutdown;

    return passed ? 0 : 1;
}
