#pragma once

#include <semaphore.h>
#include <zookeeper/zookeeper.h>
#include <string>

// 封装的zk客户端类
class ZkClient
{
public:
    ZkClient();
    ~ZkClient();
    // zkclient启动链接zkserver
    void Start();
    // skServer上根据指定的path创建znode节点
    void Create(const char *path, const char *data, int datalen, int state=0); //state = {0: 永久性节点；ZOO_EPHEMERAL：临时性节点}
    // 根据参数指定的znode节点路径，获取znode节点的值
    std::string GetData(const char *path);
private:
    // zk的客户端句柄
    zhandle_t *m_zhandle;
};