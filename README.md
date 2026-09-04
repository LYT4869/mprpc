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
RpcClientRuntime             共享 EventLoop 池、共享回调执行器
        |
        v
Muduo EventLoop + TcpClient  endpoint 长连接、收发、半包/粘包处理
        |
        v
RpcProvider Reactor          解码、活动调用、deadline、响应编码
        |
        v
Provider Business Executor   有界排队、业务方法分发
        |
        v
FileTransfer Executor        文件任务限流、SessionManager、磁盘 I/O
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
| 20 | 2 | message_type | request/response/cancel/heartbeat |
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
6. 同步调用唤醒等待线程；异步调用使用发送前预留的容量投递用户回调，不阻塞 IO 线程。

进程级 `RpcClientRuntime` 默认维护 2 个 EventLoop，并以轮询方式将新 `ChannelCore`
固定到其中一个 loop。一个 loop 可以管理多个 Core 的连接，但每个 Core 的 session 始终
由同一 loop 访问。Runtime 还统一持有 2 个回调线程和 1024 个完成槽位；异步调用在
访问服务发现和网络前预留一个槽位，容量不足立即返回 `CALLBACK_REJECTED`。因此已接收
调用完成时必然可投递回调，不会因队列饱和退回 Reactor 线程执行用户代码。

客户端超时或主动取消时，会先在本地竞争一次完成权，再沿原连接尽力发送空 body 的
`CANCEL` 帧。Provider 以“连接身份 + request ID”索引活动调用，并使用相对
`timeout_ms` 注册服务端 deadline。正常完成、deadline、取消和断线共用一次完成入口；
取消属于协作式语义，业务会在入队、开始执行、最终哈希和发布前检查状态，但不会强行中断
正在运行的系统调用，也不保证回滚已经发生的副作用。

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
Provider，避免本地 Session 被跨节点拆散。

客户端通过 `zoo_wget_children` 在读取 Provider 列表的同时注册一次性 Child Watch。
节点增删事件只把对应方法缓存标记为 stale，并通过弱引用投递到 Core 固定的 Muduo
EventLoop；watcher 线程不读取 ZooKeeper，也不直接持有 Core。下一次 RPC 重新读取完整
列表并重新注册 watch。会话断开、过期和重连会让全部缓存失效，防止继续使用 watch 状态
不确定的结果。

服务发现保留三条独立纠正路径：Child Watch 提供低延迟通知，3 秒 TTL 修复丢失通知，
TCP 连接失败淘汰数据面已证实失效的 endpoint。临时节点让 Provider 会话过期后自动
下线；watch 是刷新提示而不是可靠事件日志。

Provider 由独立注册控制线程维护 ZooKeeper 生命周期。普通 `Disconnected` 由 ZooKeeper
客户端尝试恢复原 Session，不重复创建节点；收到 `Expired` 后才创建新 Session，重新创建
持久目录和全部临时顺序 Provider 节点。重注册失败按 100ms 起步、最高 3s 的退避持续重试，
同步 ZooKeeper 操作不会阻塞 Provider 的 Muduo IO EventLoop。

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

Provider 默认使用 4 个业务 worker、最多 64 个 outstanding。Muduo IO 线程只负责拆帧、
活动调用和非阻塞投递；业务方法在线程池执行，队列满立即返回 `SERVER_BUSY`。排队请求若
先超时或被取消，会通过 `Queued -> Cancelled` 状态转换阻止后续业务执行。

文件服务保留第二级执行器，默认 4 个 worker、最多 32 个 outstanding，用于单独限制磁盘、
哈希和文件任务压力。两级池职责不同：Provider 池隔离任意业务入口，文件池控制重资源业务
容量。客户端共享回调执行器则使用发送前容量预留，满载时返回 `CALLBACK_REJECTED`。
所有队列都非阻塞拒绝，并记录 accepted/rejected/completed、当前 outstanding 和峰值水位。

## Observability

ChannelCore、RpcProvider 和文件服务分别持有自己的线程安全指标对象，不依赖全局可变
单例。指标包括 active、成功、超时、取消、网络/框架错误、deadline、过载拒绝、重试、
CRC 失败、有效字节数、服务发现 watch/refresh/error 以及固定桶延迟直方图。支持总量
和按已注册服务方法汇总，不使用 request ID、transfer ID 或文件名等高基数标签。

文件 Provider 默认每 10 秒输出一行 `key=value` 快照，配置
`metricsintervalms=0` 可禁用。快照由多个原子字段独立读取，是近似瞬时视图，不承诺
字段间的事务一致性。

## Build And Run

依赖：CMake 3.28+、C++17、Muduo、Protobuf、ZooKeeper C client、OpenSSL 和 zlib。

```bash
cmake -S . -B build
cmake --build build -j2
```

Debug、Release 和 Sanitizer 产物分别位于各自构建目录，互不覆盖。启用 ASan/UBSan：

```bash
cmake -S . -B build-sanitize -DMPRPC_ENABLE_SANITIZERS=ON
cmake --build build-sanitize -j2
ctest --test-dir build-sanitize --output-on-failure
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
./build/bin/file_transfer_server -i ./bin/test.conf /tmp/mprpc_uploads
./build/bin/file_transfer_client -i ./bin/test.conf --window 8 ./large.bin
```

断点续传可指定之前的 transfer ID：

```bash
./build/bin/file_transfer_client -i ./bin/test.conf \
  --transfer-id <id> --window 8 ./large.bin
```

## Tests

```bash
ctest --test-dir build --output-on-failure
./test/integration/run_rpc_reliability.sh
./test/integration/run_file_transfer_e2e.sh
./test/integration/run_zookeeper_watch.sh
./test/integration/run_provider_reregistration.sh
./test/integration/run_large_file_e2e.sh
```

RPC 可靠性脚本覆盖异步立即返回、乱序并发响应、超时与迟到响应竞争、取消和 Channel
析构，还验证不同连接使用相同 request ID 不冲突。文件 E2E 会真实启动 Provider，执行普通
上传、强制中断、进程重启恢复和 SHA-256 对比。
它还会在滑动窗口传输期间重启 Provider，验证连接类错误重试、重复分片幂等和自动
续传。测试产物保留在脚本输出的临时目录，便于排错。

ZooKeeper Watch 脚本先验证 one-shot child watch 必须显式重新注册，再用一个持久 Channel
依次加入、删除和重启第二个 Provider，要求每次都在 3 秒 TTL 前观察到成员变化，并检查
watch、refresh 和 active-call 指标。

Provider 重注册脚本使用独立 ZooKeeper 实例：先验证短暂断线不会产生重复节点，再强制旧
Session 失效，确认不重启 Provider 进程即可恢复注册，并由全新客户端完成 RPC 调用。

大型文件脚本使用独立 ZooKeeper 和 Provider，依次上传 256 MiB 与 1 GiB 文件，并用外部
`sha256sum` 比较源文件和服务端最终文件；成功后自动清理大文件，失败时保留现场。

当前提交的 Debug、Sanitizer、集成测试、Release 压测和 256 MiB/1 GiB 校验结果汇总见
[2026-09-02 测试基线](docs/test-results-2026-09-02.md)。

## Benchmark

```bash
./test/integration/run_release_benchmark.sh latency
./test/integration/run_release_benchmark.sh window
./test/integration/run_release_benchmark.sh saturation
```

脚本使用独立 `build-release`，正式计时前会让每个 uploader 分别完成 3 次预热，并
采集客户端/Provider 的 `/proc` CPU 与 RSS。2026-09-02 的本机 WSL2 回环结果中，1 MiB
延迟组从并发 1 到 8 的有效吞吐由 28.27 提升到 72.95 MiB/s，但 P99 由 67.27 上升到
193.27 ms。16 MiB 单上传在窗口 8 达到本轮最高的 61.14 MiB/s，窗口 16 为 59.71 MiB/s。
64 MiB 饱和组在并发 8 时有 14/16 次请求被快速 `SERVER_BUSY` 拒绝，说明系统到达有界
队列保护点；该组吞吐只统计成功完成的文件。

环境、命令、解释和原始 CSV 见
[`docs/test-results-2026-09-02.md`](docs/test-results-2026-09-02.md)。这些结果用于选择窗口
和分析容量边界，不代表生产网络或磁盘性能。旧报告保留为历史对照。

补充的 256 MiB 滑动窗口实验见
[`docs/window-size-comparison-2026-09-04.md`](docs/window-size-comparison-2026-09-04.md)，
用于对照文件大小变化后窗口 8/16 的吞吐趋势与在途并发差异。

## Scope

本项目只实现文件上传，不包含下载、认证、TLS、数据库和跨机器共享 Session。多个
Provider 可通过 ZooKeeper 被发现，但各自管理本地上传目录，不承诺跨节点续传。

面试讲解、核心代码索引和高频追问见
[`docs/interview-guide.md`](docs/interview-guide.md)。
