#include "zookeeperutil.h"

#include "mprpcapplication.h"

#include <chrono>
#include <iostream>

ZkClient::ZkClient() : m_zhandle(nullptr)
{
}

ZkClient::~ZkClient()
{
    zhandle_t* handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        children_changed_callback_ = {};
        session_state_callback_ = {};
        handle = m_zhandle;
        m_zhandle = nullptr;
        connected_ = false;
    }
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
    SessionStateCallback callback;
    ZkSessionState session_state = ZkSessionState::Disconnected;
    {
        std::lock_guard<std::mutex> lock(client->mutex_);
        client->connected_ = state == ZOO_CONNECTED_STATE ||
                             state == ZOO_READONLY_STATE;
        if (state == ZOO_EXPIRED_SESSION_STATE) {
            client->expired_ = true;
            session_state = ZkSessionState::Expired;
        } else if (client->connected_) {
            session_state = ZkSessionState::Connected;
        }
        callback = client->session_state_callback_;
    }
    client->connected_cv_.notify_all();
    if (callback) {
        callback(session_state);
    }
}

void ZkClient::ChildWatcher(zhandle_t*, int type, int,
                            const char* path, void* watcher_context)
{
    if (watcher_context == nullptr || path == nullptr ||
        (type != ZOO_CHILD_EVENT && type != ZOO_DELETED_EVENT)) {
        return;
    }

    auto* client = static_cast<ZkClient*>(watcher_context);
    ChildrenChangedCallback callback;
    {
        std::lock_guard<std::mutex> lock(client->mutex_);
        callback = client->children_changed_callback_;
    }
    if (callback) {
        callback(path);
    }
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

    // zookeeper_init 是异步的，需要等待会话 watcher 通知。
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
    // 临时节点负责下线失效实例，顺序节点允许注册多个 Provider。
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
    if (path == nullptr) {
        return {};
    }
    return GetDataResult(path).data;
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

ZkChildrenResult ZkClient::GetChildrenAndWatch(
    const std::string& path)
{
    ZkChildrenResult result;
    if (m_zhandle == nullptr) {
        result.code = ZINVALIDSTATE;
        return result;
    }

    String_vector values{};
    result.code = zoo_wget_children(
        m_zhandle, path.c_str(), &ZkClient::ChildWatcher, this, &values);
    if (!result.Ok()) {
        return result;
    }

    result.children.reserve(static_cast<std::size_t>(values.count));
    for (int i = 0; i < values.count; ++i) {
        result.children.emplace_back(values.data[i]);
    }
    deallocate_String_vector(&values);
    return result;
}

ZkDataResult ZkClient::GetDataResult(const std::string& path)
{
    ZkDataResult result;
    if (m_zhandle == nullptr) {
        result.code = ZINVALIDSTATE;
        return result;
    }

    std::vector<char> buffer(4096);
    int length = static_cast<int>(buffer.size());
    result.code = zoo_get(m_zhandle, path.c_str(), 0, buffer.data(),
                          &length, nullptr);
    if (result.Ok()) {
        result.data.assign(buffer.data(), static_cast<std::size_t>(length));
    }
    return result;
}

void ZkClient::SetChildrenChangedCallback(
    ChildrenChangedCallback callback)
{
    std::lock_guard<std::mutex> lock(mutex_);
    children_changed_callback_ = std::move(callback);
}

void ZkClient::SetSessionStateCallback(SessionStateCallback callback)
{
    std::lock_guard<std::mutex> lock(mutex_);
    session_state_callback_ = std::move(callback);
}
