# MPRPC

一个面向学习和面试展示的 C++17 RPC 框架。项目使用 Muduo、Protobuf 和
ZooKeeper 实现同步/异步 RPC、长连接复用、超时取消、服务发现，并在其上实现
支持乱序并发分片、断点续传和完整性校验的大文件上传服务。

## Architecture

```text
Generated Protobuf Stub
        |
        v
MprpcChannel                 Protobuf 参数适配、同步/异步语义
        |
        v
ChannelCore + CallState      request_id、pending、timer、取消、一次完成
        |
        v
Muduo EventLoop + TcpClient  endpoint 长连接、收发、半包/粘包处理
        |
        v
RpcProvider                  解码、服务分发、响应编码
        |
        v
FileTransferService          有界任务队列、SessionManager、磁盘 I/O
```

核心原则：同步和异步共用协议、连接、请求编号、响应分发、超时和错误处理；差别
只在完成后是唤醒条件变量，还是投递用户回调。

## RPC Protocol

协议采用固定 28 字节网络序头部和变长 body。body 是
`4-byte meta_length + protobuf meta + protobuf payload`。

| Offset | Size | Field | Purpose |
|---:|---:|---|---|
| 0 | 4 | magic | 协议识别码 `0x19990102` |
| 4 | 2 | version | 协议版本 |
| 6 | 2 | header_len | 当前固定为 28 |
| 8 | 4 | body_len | body 字节数，最大 64 MiB |
| 12 | 8 | request_id | 并发请求与乱序响应关联 |
| 20 | 2 | message_type | request/response/heartbeat |
| 22 | 2 | status_code | RPC 框架和传输状态 |
| 24 | 4 | checksum | CRC32，计算时该字段视为 0 |

Codec 逐字段编码，不直接发送 C++ struct，因此不受内存对齐和主机字节序影响。
Decoder 在 Buffer 中循环拆帧：数据不足返回 `NEED_MORE_DATA`，完整帧校验 CRC 后
消费，天然处理 TCP 半包和粘包。

## Call Lifecycle

1. `MprpcChannel` 同步序列化 request，借用调用方的 controller/response/done。
2. `ChannelCore::StartCall` 生成 request ID，并在发送前注册 `pending_calls_`。
3. 注册超时 TimerId，通过 ZooKeeper 缓存选择 endpoint，然后复用 TcpClient。
4. 响应、超时、取消、断线和 Channel 关闭都竞争调用 `CompleteCall`。
5. `CallPhase` 的 CAS 保证请求只完成一次；完成时摘除 pending 并取消 TimerId。
6. 同步调用唤醒等待线程；异步回调投递到有界 callback executor，不阻塞 IO 线程。

`RpcChannel::CallMethod` 的异步参数仍遵循 Protobuf 原生所有权：调用方必须保证
controller、response 和 done 活到回调结束。文件上传客户端在内部用共享上下文封装
这些对象，不向业务调用方暴露裸指针生命周期。

## Service Discovery

Provider 使用持久目录和临时顺序实例节点：

```text
/FileTransferServiceRpc
  /UploadChunk
    /providers
      /provider-0000000001 -> 127.0.0.1:8000
      /provider-0000000002 -> 127.0.0.1:8001
```

ChannelCore 维护持久 ZooKeeper 会话、3 秒 endpoint 缓存和轮询索引。无状态 RPC
按轮询选择；文件客户端预生成 transfer ID，并通过 `affinity_key` 稳定哈希到同一
Provider，避免本地 Session 被跨节点拆散。TCP 断线或 ZK 连接丢失会使相关缓存
失效；临时节点让 Provider 异常退出后自动下线。

## File Upload

默认 chunk 为 1 MiB，最大 4 MiB，滑动窗口为 8。服务端 Begin 阶段使用
`open + ftruncate` 创建固定逻辑大小的 `.part` 文件；UploadChunk 要求 offset 位于分片边界，
用 `pwrite` 循环处理 `EINTR` 和部分写入。

```text
Missing -> Writing -> Received
                 | write/persist failure
                 +----------------> Missing

Active -> Finishing -> Completed
   |            |
   +-> Aborted  +-> Failed
   +-> Expired
```

锁内预占 `Writing`，锁外执行磁盘写，锁内提交 `Received`。因此不同分片可以并行，
同一分片不会重复写。收到已完成分片时比较 CRC32 和磁盘内容：一致按幂等成功处理，
不一致返回 `CHUNK_CONFLICT`。

每次成功提交分片后写入 Protobuf sidecar，并通过 `.meta.tmp -> rename` 原子替换。
服务重启先恢复合法 `.meta + .part`，再清理孤儿文件。该实现保证正常进程重启恢复，
不宣称数据库事务、WAL 或断电级一致性。

完整性分三层：

- RPC 帧 CRC32：发现传输帧被篡改。
- 分片 CRC32：写盘前校验，并辅助重复分片判断。
- 文件 SHA-256：Finish 阶段校验完整文件后才原子 rename 发布。

ParallelFileUploader 在 Begin 后查询 bitmap，只调度缺失分片，同时保持不超过窗口大小
的异步 RPC。仅超时和连接错误按 100/200/400 ms 退避重试；参数、CRC 和冲突错误
不会重试。

## Overload Protection

文件服务默认 4 个 worker、最多 32 个 outstanding（执行中加排队中）。`TrySubmit`
失败时立即返回 `SERVER_BUSY`，不会阻塞 RpcProvider 的 IO 线程。执行器支持排空关闭，
并记录 accepted/rejected/completed 计数。

## Build And Run

依赖：CMake 3.28+、C++17、Muduo、Protobuf、ZooKeeper C client、OpenSSL 和 zlib。

```bash
cmake -S . -B build
cmake --build build -j2
```

启动 ZooKeeper：

```bash
/home/lyt4869/package/apache-zookeeper-3.8.6-bin/bin/zkServer.sh start
```

配置示例：

```ini
rpcserverip=127.0.0.1
rpcserverport=8000
zookeeperip=127.0.0.1
zookeeperport=2181
```

启动文件服务和并发上传客户端：

```bash
./bin/file_transfer_server -i ./bin/test.conf /tmp/mprpc_uploads
./bin/file_transfer_client -i ./bin/test.conf --window 8 ./large.bin
```

断点续传可指定之前的 transfer ID：

```bash
./bin/file_transfer_client -i ./bin/test.conf \
  --transfer-id <id> --window 8 ./large.bin
```

## Tests

```bash
ctest --test-dir build --output-on-failure
./test/integration/run_rpc_reliability.sh
./test/integration/run_file_transfer_e2e.sh
```

RPC 可靠性脚本覆盖异步立即返回、乱序并发响应、超时与迟到响应竞争、取消和 Channel
析构。文件 E2E 会真实启动 Provider，执行普通上传、强制中断、进程重启恢复和 SHA-256 对比。
它还会在滑动窗口传输期间重启 Provider，验证连接类错误重试、重复分片幂等和自动
续传。测试产物保留在脚本输出的临时目录，便于排错。

## Benchmark

```bash
./bin/file_transfer_benchmark -i ./bin/test.conf /tmp/mprpc_benchmark
```

2026-08-17 在本机 WSL2、回环网络、Debug 构建上的一次结果：48/48 上传成功。
8 MiB 文件、单上传时，窗口 1 为 12.73 MiB/s，窗口 8 为 17.31 MiB/s；窗口 8、
并发 2 时为 21.91 MiB/s。完整原始结果见
[`docs/benchmark-2026-08-17.csv`](docs/benchmark-2026-08-17.csv)。这些数字用于观察
窗口、并发、fsync 和连接建立成本的趋势，不代表生产环境性能。

## Scope

本项目只实现文件上传，不包含下载、认证、TLS、数据库和跨机器共享 Session。多个
Provider 可通过 ZooKeeper 被发现，但各自管理本地上传目录，不承诺跨节点续传。

面试讲解、核心代码索引和高频追问见
[`docs/interview-guide.md`](docs/interview-guide.md)。
