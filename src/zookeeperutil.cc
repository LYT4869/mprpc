#include "zookeeperutil.h"

#include "mprpcapplication.h"

#include <chrono>
#include <iostream>

ZkClient::ZkClient() : m_zhandle(nullptr)
{
}

ZkClient::~ZkClient()
{
    zhandle_t* handle = m_zhandle;
    m_zhandle = nullptr;
    if (handle != nullptr) {
        zookeeper_close(handle);
    }
}

void ZkClient::GlobalWatcher(zhandle_t*, int type, int state,
                             const char*, void* watcher_context)
{
    if (type != ZOO_SESSION_EVENT || watcher_context == nullptr) {
        return;
    }

    auto* client = static_cast<ZkClient*>(watcher_context);
    {
        std::lock_guard<std::mutex> lock(client->mutex_);
        client->connected_ = state == ZOO_CONNECTED_STATE;
        if (state == ZOO_EXPIRED_SESSION_STATE) {
            client->expired_ = true;
        }
    }
    client->connected_cv_.notify_all();
}

bool ZkClient::Start()
{
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    std::unique_lock<std::mutex> lock(mutex_);
    if (m_zhandle != nullptr && connected_) {
        return true;
    }

    if (m_zhandle != nullptr && expired_) {
        zhandle_t* expired_handle = m_zhandle;
        m_zhandle = nullptr;
        expired_ = false;
        lock.unlock();
        zookeeper_close(expired_handle);
        lock.lock();
    }

    if (m_zhandle == nullptr) {
        const std::string host =
            MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
        const std::string port =
            MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
        const std::string connection_string = host + ":" + port;

        m_zhandle = zookeeper_init(connection_string.c_str(),
                                   &ZkClient::GlobalWatcher,
                                   30000, nullptr, this, 0);
        if (m_zhandle == nullptr) {
            std::cerr << "zookeeper_init failed" << std::endl;
            return false;
        }
        expired_ = false;
    }

    const bool connected = connected_cv_.wait_for(
        lock, std::chrono::seconds(10), [this] { return connected_; });
    if (!connected) {
        std::cerr << "zookeeper connection timed out" << std::endl;
    }
    return connected;
}

void ZkClient::Create(const char* path, const char* data,
                      int data_length, int flags)
{
    if (m_zhandle == nullptr || path == nullptr) {
        return;
    }

    const int exists = zoo_exists(m_zhandle, path, 0, nullptr);
    if (exists == ZOK) {
        return;
    }
    if (exists != ZNONODE) {
        std::cerr << "zoo_exists failed: path=" << path
                  << " code=" << exists << std::endl;
        return;
    }

    char created_path[512] = {};
    int created_path_length = sizeof(created_path);
    const int result = zoo_create(m_zhandle, path, data, data_length,
                                  &ZOO_OPEN_ACL_UNSAFE, flags,
                                  created_path, created_path_length);
    if (result != ZOK && result != ZNODEEXISTS) {
        std::cerr << "zoo_create failed: path=" << path
                  << " code=" << result << std::endl;
    }
}

std::string ZkClient::CreateEphemeralSequential(
    const std::string& path_prefix, const std::string& data)
{
    if (m_zhandle == nullptr) {
        return {};
    }

    char created_path[512] = {};
    int created_path_length = sizeof(created_path);
    const int result = zoo_create(
        m_zhandle, path_prefix.c_str(), data.data(),
        static_cast<int>(data.size()), &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL | ZOO_SEQUENCE, created_path, created_path_length);
    if (result != ZOK) {
        std::cerr << "failed to create provider node: path="
                  << path_prefix << " code=" << result << std::endl;
        return {};
    }
    return std::string(created_path);
}

std::string ZkClient::GetData(const char* path)
{
    if (m_zhandle == nullptr || path == nullptr) {
        return {};
    }

    std::vector<char> buffer(4096);
    int length = static_cast<int>(buffer.size());
    const int result = zoo_get(m_zhandle, path, 0, buffer.data(),
                               &length, nullptr);
    if (result != ZOK) {
        return {};
    }
    return std::string(buffer.data(), static_cast<std::size_t>(length));
}

std::vector<std::string> ZkClient::GetChildren(const std::string& path)
{
    std::vector<std::string> children;
    if (m_zhandle == nullptr) {
        return children;
    }

    String_vector values{};
    const int result = zoo_get_children(m_zhandle, path.c_str(), 0, &values);
    if (result != ZOK) {
        return children;
    }

    children.reserve(static_cast<std::size_t>(values.count));
    for (int i = 0; i < values.count; ++i) {
        children.emplace_back(values.data[i]);
    }
    deallocate_String_vector(&values);
    return children;
}

bool ZkClient::IsConnected() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return connected_;
}
