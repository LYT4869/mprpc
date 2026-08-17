#pragma once

#include <zookeeper/zookeeper.h>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

// 封装的zk客户端类
class ZkClient
{
public:
    ZkClient();
    ~ZkClient();
    // zkclient启动链接zkserver
    bool Start();
    // skServer上根据指定的path创建znode节点
    void Create(const char *path, const char *data, int datalen, int state=0); //state = {0: 永久性节点；ZOO_EPHEMERAL：临时性节点}
    std::string CreateEphemeralSequential(const std::string& path_prefix,
                                          const std::string& data);
    // 根据参数指定的znode节点路径，获取znode节点的值
    std::string GetData(const char *path);
    std::vector<std::string> GetChildren(const std::string& path);
    bool IsConnected() const;
private:
    static void GlobalWatcher(zhandle_t* zh, int type, int state,
                              const char* path, void* watcher_context);

    // zk的客户端句柄
    zhandle_t *m_zhandle;
    mutable std::mutex mutex_;
    std::condition_variable connected_cv_;
    bool connected_ = false;
    bool expired_ = false;
};
