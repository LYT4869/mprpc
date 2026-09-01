# RPC 项目面试指南

本文件用于快速复习。完整的分块问题、参考回答、追问链和八股延伸见
[`interview-question-bank.md`](interview-question-bank.md)。
真实测试数据、性能趋势和面试解读见
[`interview-test-results-2026-08-28.md`](interview-test-results-2026-08-28.md)。

## 简历描述

- 基于 C++17、Muduo、Protobuf 和 ZooKeeper 实现 RPC 框架，设计 28 字节固定协议头，支持半包/粘包拆帧、request ID 并发关联、CRC32 校验和长连接复用。
- 统一同步与异步调用链，通过 `CallState + pending_calls` 管理超时、取消、断线和 Channel 关闭竞争，使用 CAS 保证请求只完成一次；以进程级 Runtime 共享少量 Reactor 与回调线程，并在异步发送前预留回调容量。
- 设计 CANCEL 控制帧和服务端 deadline，以“连接身份 + request ID”管理活动调用，实现客户端到 Provider 的协作式取消传播和迟到完成抑制。
- 实现大文件上传服务，使用 `ftruncate + pwrite + 分片位图` 支持乱序并发上传，通过分片 CRC32、内容比对和 SHA-256 实现幂等重试及端到端完整性校验。
- 使用 Protobuf sidecar 和原子 rename 实现进程重启恢复；客户端以滑动窗口并发发送缺失分片，对可恢复网络错误进行指数退避重试。
- 设计有界 outstanding 和快速过载拒绝，增加按方法延迟直方图与错误分类指标；完成真实中断恢复 E2E 和可复现 Release 压测，基于吞吐、P50/P95/P99、CPU、RSS 与拒绝数分析容量边界。

## 核心代码索引

| Topic | Files |
|---|---|
| 协议编码、拆帧、CRC | `src/mprpccodec.cc`, `src/include/mprpccodec.h` |
| 请求状态、超时、断线 | `src/channelcore.cc`, `src/include/channelcore.h` |
| 客户端共享 IO 与回调运行时 | `src/rpcclientruntime.cc`, `src/include/rpcclientruntime.h` |
| Protobuf 同步/异步适配 | `src/mprpcchannel.cc` |
| 取消和错误语义 | `src/mprpccontroller.cc` |
| Provider 分发、deadline、业务隔离 | `src/rpcprovider.cc`, `src/include/rpcdispatchstate.h` |
| RPC 指标与 reporter | `src/rpcmetrics.cc`, `src/include/rpcmetrics.h` |
| ZooKeeper 服务发现 | `src/zookeeperutil.cc` |
| 有界执行器 | `src/boundedexecutor.cc` |
| 分片状态和 RAII fd | `extensions/file_transfer/server/upload_session.h` |
| pwrite、bitmap、sidecar | `extensions/file_transfer/server/upload_session_manager.cc` |
| 滑动窗口和重试 | `extensions/file_transfer/client/parallel_file_uploader.cc` |
| 故障 E2E | `test/integration/run_file_transfer_e2e.sh` |
| Release 压测 | `extensions/file_transfer/benchmark/file_transfer_benchmark.cc`, `test/integration/run_release_benchmark.sh` |

## 30 个高频问题

1. **为什么不用 `sizeof(Header)` 直接发送？**
   结构体有对齐、填充和主机字节序差异；逐字段网络序编码才是稳定的线协议。

2. **TCP 为什么会出现半包和粘包？**
   TCP 是字节流，没有消息边界；接收次数与发送次数不一一对应，因此协议必须携带长度并在 Buffer 中循环拆帧。

3. **body 为什么不用统一转大端？**
   大端转换针对定长整数；Protobuf 自己定义字节编码，payload 应当作为不透明字节串传输。

4. **request ID 解决了什么？**
   它把乱序响应映射回 pending call，使同一长连接能同时承载多个在途请求。

5. **为什么 pending 必须先注册再发送？**
   回环或高速服务可能在发送函数返回前响应；先发后注册会出现响应找不到状态的竞态。

6. **为什么需要 `CallPhase` 的 CAS？**
   响应、超时、取消和断线可能同时完成一个调用，CAS 让其中只有一条路径获得完成权。

7. **默认 memory order 表示什么？**
   `seq_cst` 给原子操作建立全局一致顺序，但不等于所有普通代码变成串行；互斥数据仍需 mutex。

8. **正常响应后为什么取消 TimerId？**
   避免无意义的迟到 timer 工作；即便 timer 已触发，pending 摘除和 CAS 仍保证不会二次回调。

9. **条件变量是谁在等待？**
   同步调用线程等待 `CallState::completed`；异步调用不等待，但共用同一个完成状态。

10. **为什么 `wait` 要带谓词？**
    条件变量允许虚假唤醒，谓词保证线程只在真实完成后继续。

11. **为什么用户回调不直接跑在 IO 线程？**
    回调可能阻塞或再次发 RPC，直接执行会拖住 Reactor，严重时造成自锁。

    项目让多个 Channel 共享进程级回调池，并在异步请求发送前预留完成槽位。容量不足时
    在调用线程立即返回 `CALLBACK_REJECTED`；已接收请求完成后不再进行第二次容量竞争，
    因而不会出现“队列满后退回 IO 线程执行”的隐蔽阻塞。

12. **Protobuf 异步参数由谁管理生命周期？**
    原生 CallMethod 只借用 controller/response/done；调用方要保证它们活到 done。安全上传器用共享上下文替用户管理。

13. **ChannelCore 为什么用 `enable_shared_from_this`？**
    投递到 IO 线程的任务可能晚于调用函数执行，捕获 shared self 可保证任务执行前对象仍存在；网络回调用 weak_ptr 防止循环引用。

14. **取消与响应同时到达怎么办？**
    两者都调用 `CompleteCall`，只有先成功摘除 pending 并 CAS 的路径生效，后到者被忽略。

    客户端取消还会尽力发送 CANCEL 帧；Provider 上 deadline、取消、断线和业务 done 也竞争
    一次完成权。取消是协作式的，不保证回滚已执行副作用。

15. **ZooKeeper 为什么用临时顺序 Provider 子节点？**
    临时节点随会话消失，顺序节点允许同一方法注册多个实例且名字不冲突。

    普通断线时不创建新 Session；只有 Session Expired、旧临时节点确定失效后，Provider 才由独立注册线程建立新会话并全量重注册，避免阻塞 Reactor。

16. **endpoint 缓存的取舍是什么？**
    缓存减少每次 RPC 的 ZK 开销，但会短暂陈旧；项目用 Child Watch 懒失效，并保留短 TTL、TCP 失败和 ZK 会话事件作为兜底。

    ZooKeeper 的传统 watch 是一次性的，所以每次重新读取 Provider 列表时都要重新注册；watch 回调只投递缓存失效任务，不在 ZK 线程中同步读取。

    文件上传额外用 transfer ID 做一致的 endpoint affinity；否则普通轮询会把有状态分片发到不同 Provider。

17. **为什么使用 `pwrite` 而不是共享 ofstream seek？**
    `pwrite` 显式携带 offset，不修改共享文件游标，适合不同分片并行写入。

18. **`ftruncate` 的价值是什么？**
    Begin 阶段固定文件逻辑大小，让随机 offset 写入有确定边界；它可能创建稀疏文件，若要提前分配磁盘块可使用 `posix_fallocate`。

19. **Missing/Writing/Received 为什么有 Writing？**
    Writing 是锁内预占标记，防止两个线程同时写同一块，同时允许锁外执行慢磁盘 I/O。

20. **received_size 为什么不能只用最大 offset？**
    乱序上传时最大 offset 不代表前面都收到；位图表示完整性，received_size 只统计已确认字节总量。

21. **重复分片为什么还要比较磁盘内容？**
    CRC32 存在碰撞；CRC 快速筛选后再比较内容，可避免冲突数据被当作幂等重试。

22. **sidecar 为什么用临时文件加 rename？**
    原地覆盖崩溃时可能留下半个 Protobuf；同文件系统 rename 是原子的，读者只看到旧版或新版。

23. **当前恢复保证的边界是什么？**
    保证正常进程重启恢复，不保证突然断电时目录项和数据都严格落盘；要提升需目录 fsync、WAL 或数据库事务。

24. **CRC32 和 SHA-256 为什么都需要？**
    CRC32 快且适合每帧/每块发现随机错误；SHA-256 较慢但碰撞抗性强，用于最终文件身份校验。

25. **滑动窗口如何工作？**
    调度器最多保留 N 个 active chunk；每次回调释放一个名额，再从缺失队列或到期重试队列补充。

26. **哪些错误可以重试？**
    超时、网络错误和连接关闭可能是瞬态；参数错误、CRC 错误和内容冲突是确定性业务错误，不应盲目重试。

27. **为什么重试需要退避？**
    服务故障时立即重试会形成同步风暴；100/200/400ms 退避给网络和 Provider 恢复时间。

28. **有界队列保护了什么？**
    它限制执行中加排队任务的内存和延迟，饱和时快速返回 `SERVER_BUSY`，避免 IO 线程被阻塞等待空间。

    Provider 业务池先把任意业务入口与 Reactor 隔离；文件服务的第二级池再限制磁盘、哈希
    等重任务。排队调用若先超时或取消，原子状态机阻止 worker 继续进入业务方法。客户端
    回调池使用预留而不是完成时拒绝，因为已经发出的 RPC 必须保证最终通知调用方。

29. **P50/P95/P99 分别说明什么？**
    P50 表示典型延迟，P95/P99 观察尾延迟；样本少时高分位不稳定，所以项目原始结果明确标注环境和样本数。

30. **下一步最合理的生产化改进是什么？**
    TLS/认证、连接级流控、跨节点共享 Session、目录 fsync/WAL、Prometheus 导出、调用链追踪，以及真实多机网络和独立磁盘压测。
