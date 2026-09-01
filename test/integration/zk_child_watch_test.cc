#include "mprpcapplication.h"
#include "zookeeperutil.h"

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);

    ZkClient owner;
    ZkClient watcher;
    if (!owner.Start() || !watcher.Start()) {
        std::cerr << "FAIL: could not connect to ZooKeeper\n";
        return 1;
    }

    const std::string root = "/mprpc-watch-integration";
    const std::string providers_path = root + "/providers";
    owner.Create(root.c_str(), nullptr, 0);
    owner.Create(providers_path.c_str(), nullptr, 0);

    std::mutex mutex;
    std::condition_variable cv;
    int callback_count = 0;
    std::string callback_path;
    watcher.SetChildrenChangedCallback(
        [&](const std::string& path) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                ++callback_count;
                callback_path = path;
            }
            cv.notify_all();
        });

    auto initial = watcher.GetChildrenAndWatch(providers_path);
    if (!initial.Ok() || !initial.children.empty()) {
        std::cerr << "FAIL: initial watched read was not empty\n";
        return 1;
    }

    if (owner.CreateEphemeralSequential(
            providers_path + "/provider-", "127.0.0.1:18891").empty()) {
        std::cerr << "FAIL: could not create first provider\n";
        return 1;
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(1), [&] {
                return callback_count >= 1;
            }) || callback_path != providers_path) {
            std::cerr << "FAIL: first child watch was not delivered\n";
            return 1;
        }
    }

    auto refreshed = watcher.GetChildrenAndWatch(providers_path);
    if (!refreshed.Ok() || refreshed.children.size() != 1) {
        std::cerr << "FAIL: watched refresh did not see first provider\n";
        return 1;
    }

    if (owner.CreateEphemeralSequential(
            providers_path + "/provider-", "127.0.0.1:18892").empty()) {
        std::cerr << "FAIL: could not create second provider\n";
        return 1;
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(1), [&] {
                return callback_count >= 2;
            }) || callback_path != providers_path) {
            std::cerr << "FAIL: re-registered child watch was not delivered\n";
            return 1;
        }
    }

    std::cout << "PASS: one-shot child watch was re-registered\n";
    return 0;
}
