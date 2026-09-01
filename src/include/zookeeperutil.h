#pragma once

#include <zookeeper/zookeeper.h>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

struct ZkChildrenResult
{
    int code = ZSYSTEMERROR;
    std::vector<std::string> children;

    bool Ok() const noexcept { return code == ZOK; }
};

struct ZkDataResult
{
    int code = ZSYSTEMERROR;
    std::string data;

    bool Ok() const noexcept { return code == ZOK; }
};

enum class ZkSessionState
{
    Connected,
    Disconnected,
    Expired,
};

// 封装 ZooKeeper 会话、节点注册和服务发现所需的读取操作。
class ZkClient
{
public:
    using ChildrenChangedCallback =
        std::function<void(const std::string& providers_path)>;
    using SessionStateCallback =
        std::function<void(ZkSessionState state)>;

    ZkClient();
    ~ZkClient();
    // 建立或复用持久会话，并等待异步连接完成。
    bool Start();

    // 创建普通 znode；节点已存在时视为成功。
    void Create(const char *path, const char *data, int datalen, int state=0); //state = {0: 永久性节点；ZOO_EPHEMERAL：临时性节点}

    // 创建用于 Provider 注册的临时顺序节点。
    std::string CreateEphemeralSequential(const std::string& path_prefix,
                                          const std::string& data);

    // 读取节点数据或直接子节点列表。
    std::string GetData(const char *path);
    std::vector<std::string> GetChildren(const std::string& path);
    // 读取直接子节点并原子注册一次性 child watch。
    ZkChildrenResult GetChildrenAndWatch(const std::string& path);
    ZkDataResult GetDataResult(const std::string& path);
    void SetChildrenChangedCallback(ChildrenChangedCallback callback);
    void SetSessionStateCallback(SessionStateCallback callback);
    bool IsConnected() const;
    // 每创建一个新的 ZooKeeper 会话句柄，本地代数递增一次。
    uint64_t SessionGeneration() const;
private:
    static void GlobalWatcher(zhandle_t* zh, int type, int state,
                              const char* path, void* watcher_context);
    static void ChildWatcher(zhandle_t* zh, int type, int state,
                             const char* path, void* watcher_context);

    // zk的客户端句柄
    zhandle_t *m_zhandle;
    mutable std::mutex mutex_;
    std::condition_variable connected_cv_;
    bool connected_ = false;
    bool expired_ = false;
    uint64_t session_generation_ = 0;
    ChildrenChangedCallback children_changed_callback_;
    SessionStateCallback session_state_callback_;
};
