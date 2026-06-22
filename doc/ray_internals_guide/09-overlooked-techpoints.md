# 第 9 篇 · 易忽略技术点专章（贡献者速查）

> 把散落前 8 篇的 ⚠️ 坑按**主题**重组成一张速查表，再加几条**跨模块的全局心智陷阱**。这是贡献者最值钱的一篇——调 bug、改代码前扫一遍。
> 每条标注出处篇章，需要细节就回去读。

---

## 一、跨模块的全局心智陷阱（最重要，先看这个）

这些不是某一篇的局部坑，而是贯穿整个 Ray、几乎所有新人都会犯的根本性误解：

### 陷阱 A：「GCS 是中心，什么都问它」—— 错

普通 task 的提交、普通对象的数据与生命周期**都不经过 GCS**。GCS 只管控制元数据（节点/Actor/PG/Job/KV/pubsub）。真正管对象的是**去中心化的 owner + 本地 ReferenceCounter**。
→ 验证：GCS 挂了，在跑的 task/actor 调用仍能继续；只有"新建/查全局元数据"才卡。
（第 0 篇 §1、第 3 篇 §6）

### 陷阱 B：「返回值/对象属于执行它的 worker」—— 错

**owner = 创建该 ObjectRef 的进程**（调用方），不是计算它的 worker。`ray.put` 的 owner 是调 put 的进程。owner 在**提交那一刻**就被钉死。
→ 后果：owner 进程一死，它拥有的对象立即 `ObjectLostError`，跟数据实际在哪个节点无关。
（第 0 篇 §4.2、第 1 篇 A.3）

### 陷阱 C：「每个 task 都要问调度器」—— 错

Ray 是 **lease-based**：申请的是"worker 租约"，租约可复用，**多数后续同类 task 直接复用 worker、不经 Raylet 调度器**。actor 方法更是**直连 actor 进程**完全不经调度器。
（第 1 篇 B.2、第 2 篇 B.1）

### 陷阱 D：「调度/资源视图是全局一致的」—— 错

集群资源视图经 ray_syncer 同步、**可能 stale**，`available` 甚至能暂时为负。调度是"基于过期视图的乐观决策 + 失败重试（reject/spillback）纠正"。
（第 1 篇 C.3/C.4、第 4 篇 §2）

### 陷阱 E：「在内核回调里同步等一下没关系」—— 错

CoreWorker/Raylet/GCS 的控制逻辑都在**单个 io_context 串行**。任何回调里的阻塞（同步 RPC、长抢 GIL、重计算）会**冻结整个进程的事件处理** → 心跳超时、cancel 失效。
（第 0 篇 §2、第 5 篇 §2）

### 陷阱 F：「`ray.put(data)` 像写进可靠存储」—— 错

`ray.put` 对象 `is_reconstructable=false`，**owner（常是 Driver）一死就彻底丢**，无法重建。
（第 0 篇 §4.8、第 6 篇）

### 陷阱 G：「零拷贝到处都有」—— 错

零拷贝**只在单节点内**（同机进程共享 Plasma mmap）。跨节点传输必有拷贝，且传输期间双占内存。
（第 6 篇 §2）

---

## 二、按主题的速查表

### 主题 1：所有权与引用计数

| 坑 | 一句话 | 出处 |
|----|--------|------|
| owner=调用方 | 返回值/put 对象的 owner 是创建 ObjectRef 的进程，提交即钉死 | 0§4.2 |
| owner 死则对象丢 | 元数据随 owner 进程消失 → ObjectLostError | 0§4.8 |
| 三个引用计数 | local/submitted/lineage 对应三种生命周期约束，不能合并 | 0§4.3 |
| merge 在前减计数在后 | `UpdateFinishedTaskReferences` 顺序反了会提前删对象 | 0§4.3 坑① |
| 借用协议级联拍平 | B 转借 C，owner 直接监听 C 不经 B；pub/sub 回调在锁外 | 0§4.5 |
| `deduct_local_ref` | 执行期额外 pin 的 ref 回报时要扣，否则误报"还在借" | 0§4.5 坑④ |
| 借用信息丢=内存泄漏 | 回传借用表的消息丢失则对象永不回收，靠存活检测兜底 | 0§4.5 坑⑤ |
| 嵌套删除顺序 | 内层对象不早于外层出 scope；嵌套+跨进程 owner 死会卡 | 0§4.4 坑③ |
| `_owner=` 双重监听 | `foreign_owner_already_monitoring` 防重复，但仍有 race | 0§4.9 |
| `UnsetObjectPrimaryCopy` 不清 locations | 位置缓存可能指向死节点 → 往死节点调度 | 0§4.6 坑⑥ |

### 主题 2：任务提交与调度

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 提交异步跨线程 | `f.remote()` 几乎不阻塞；提交逻辑全在 io_service_ 串行 | 1§A.3 坑④ |
| 内联总预算 | 一个 task 的内联参数共享 10MB 预算，非逐参判断 | 1§A.2 坑② |
| 嵌套 ObjectRef 当依赖 | 容器里的 ref 也被追踪；自定义序列化器会漏 | 1§A.2 坑③ |
| 依赖两处内联 | 小依赖调用方内联，大依赖留 Raylet 拉 | 1§B.1 坑⑤ |
| SchedulingKey 含依赖 ID | 同函数不同数据走不同 worker 队列（数据本地性） | 1§B.2 坑⑥ |
| lease 超时 race | 检查未超时后必须立刻移除 worker，否则推给已归还的 | 1§B.3 坑⑦ |
| spillback 限一跳 | `grant_or_reject` 防无限重定向；但可能紧循环空转 | 1§B.6 坑⑩ |
| `cancelled_tasks_` 清理时机 | 只在依赖解析完成时清，越过该阶段会残留 | 1§B.7 坑⑪ |
| spread_threshold 反直觉 | 利用率 < 阈值 → pack（不是 spread） | 1§C.2 坑⑫ |
| feasible vs available | 前者定 infeasible，后者定可调度；available 可为负 | 1§C.3 坑⑬ |
| scheduling class cap | 单 class 满则指数退避，防单函数刷爆 worker | 1§C.5 坑⑩ |
| pull 满容隐形挡调度 | `object_pulls_queued` 让 IsAvailable=false | 1§C.3 坑⑭ |
| infeasible≠暂时没资源 | 两个不同队列，调卡住先分清 | 1§C.1 坑⑨' |

### 主题 3：Worker 池与资源

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 资源 instance 级 | 选具体 GPU index 写进 resource_mapping，非标量减 | 1§C.6 坑⑮ |
| PG wildcard/indexed | indexed 分配须同步扣 wildcard 同一 instance | 1§C.6 坑⑮ |
| startup_token 防 PID 复用 | 握手按 token 找槽，非 PID | 1§C.7 坑⑯ |
| worker 复用匹配严 | runtime_env_hash/dynamic_options 全一致才复用 | 1§C.7 坑⑰ |
| prestart 异步预热 | 按 backlog/cpu 预启，与当前 lease 不同步 | 1§C.7 坑⑧' |
| 授租前再查 owner 存活 | 申请到取到 worker 间 owner 可能已死 | 1§C.8 坑⑬ |

### 主题 4：执行、结果、取回

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 执行端登记借用 | 入参 ObjectRef 加 local_ref 标 borrowed，执行完回报 | 1§D 坑⑭ |
| 执行/接收分两线程 | 执行不可占 io_service_，否则 cancel/心跳卡 | 1§D 坑⑱ |
| 首次 vs 重试落 plasma | `store_in_plasma_ids` 区分；重试是血缘重算 | 1§E 坑⑮ |
| at-least-once + attempt_number | 普通 task 可能重发，靠 TaskID+attempt 去重 | 1§E |
| 取回先问 owner 位置 | owner 死则位置丢 → ObjectLostError | 1§F 坑⑯ |
| TASK_ARGS 被 GET 饿死 | PullManager 优先级下低优先级长期不激活 | 1§F.1 坑⑱ |
| 长 pin 致 OOM | 正被用的对象临时 pin，长期持有挤占可驱逐空间 | 1§F.2 坑⑲ |

### 主题 5：Actor

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 创建走 GCS 调用走直连 | 注册/调度/重启经 GCS；方法直连不经调度器 | 2§A/B |
| 直连仍需 seqno | FIFO/去重/乱序检测全靠 caller+actor 的 seqno | 2§B.2 坑⑧ |
| 重启 seqno offset 重置 | `caller_starts_at=next_task_reply_position` 重放未确认 | 2§B.2 坑⑨ |
| 缺号卡住后续全部 | 顺序模式缺一个 seqno，后面全等（有超时兜底） | 2§B.3 坑⑩ |
| detached 生命周期解绑 | 不随 creator 死；非 detached 随 owner 死 | 2§A.3 坑③ |
| `get_actor` 返回 weak ref | 不参与引用计数，actor 可能突然被杀 | 2§A.3 坑④ |
| actor 死晚于 del | 释放靠 out-of-scope 回调，滞后几个 GC 周期 | 2§A.3 坑⑤ |
| async actor 一阻塞全死 | 共享 event loop，同步阻塞拖垮所有 coroutine | 2§C.2 坑⑪ |
| 并发组跨调用死锁 | 线程池边界 + 跨组/跨 actor 同步等待 | 2§C.2 坑⑫ |
| max_concurrency>1 放弃顺序 | 并发即乱序执行，有状态逻辑会错 | 2§C.2 坑⑬ |
| max_restarts vs max_task_retries | 进程级 vs 调用级，重启后状态全丢 | 2§C.3 坑⑭ |

### 主题 6：GCS

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 默认内存存储无 HA | `memory` 模式重启即丢全部元数据，生产用 redis | 3§2 坑② |
| Redis 断连 GCS 自杀 | 不重连，靠外部重启；Redis 也要 HA | 3§2 坑③ |
| KV 无 TTL | runtime_env/函数导出堆积，需手动 Del | 3§2 坑④ |
| 健康检查误判不可撤销 | 网络抖动判死 → 强杀 actor，且检测慢几十秒 | 3§3 坑⑤ |
| pub/sub 单 ID 有序 | 跨 ID 无序；订阅 5 分钟不 poll 丢消息 | 3§4 坑⑦⑧ |
| 默认 io_context 过载 | 大量节点失活时拖慢 KV/actor 创建 | 3§1 坑① |

### 主题 7：对象store物理层

| 坑 | 一句话 | 出处 |
|----|--------|------|
| 零拷贝仅单节点 | 跨节点必拷贝，传输期双占内存 | 6§2 坑① |
| fallback 性能悬崖 | /dev/shm 满退磁盘 mmap，慢百倍，非 Linux 磁盘满 SIGBUS | 6§2 坑② |
| Seal 前不可见 | 崩在产出中途留下永不 seal 的对象 | 6§2 坑③ |
| eviction 与 spill 不协调 | LRU 可能先 evict 掉 raylet 要 spill 的对象 | 6§3 坑⑤ |
| chunk 乱序 | TCP 重排，按 offset 组装，慢则半传输撑内存 | 6§4 坑⑥ |
| 位置上报滞后 | 批量上报延迟，pull 可能在位置到达前重试 | 6§5 坑⑧ |
| 主副本 vs 次副本 | owner pin 主副本可 spill；次副本 LRU 自由驱逐 | 6§6 坑⑨ |
| spill 到 S3 静默失败 | config 写错静默禁用 spilling | 6§6 坑⑩ |
| OOM 优先杀可重试 task | kRetriableLIFO，重算代价小的先死 | 6§7 坑⑫ |

### 主题 8：Serve

| 坑 | 一句话 | 出处 |
|----|--------|------|
| long poll 一致性窗口 | 副本数变化期间 proxy 可能看到部分副本 | 7§2.1 坑② |
| Controller 重启 snapshot 重置 | handle 自动追平，窗口内可能发给已下线副本 | 7§2.1 坑③ |
| 两层背压要协调 | max_queued(handle) 与 max_ongoing(副本)，批处理需 ongoing 够大 | 7§3.1 坑⑤ |
| handle 要复用 | 带 long-poll 缓存，频繁新建触发淘汰重连 | 7§3.1 坑⑦ |
| autoscaling 抖动 / 冷启动延迟 | 窗口太小频繁伸缩；0 副本指标上报慢 | 7§4.1 坑⑨⑩ |
| draining 两超时 | wait_loop 等在途请求，timeout 强杀 | 7§4.2 坑⑪ |

### 主题 9：Data

| 坑 | 一句话 | 出处 |
|----|--------|------|
| InputData 不可恢复 | `from_pandas` lineage 不可序列化，block 丢了无法重建 | 8§2.1 坑① |
| 算子融合条件苛刻 | ~9 条，Actor→任何不融；用 stats 看实际 DAG | 8§2.2 坑② |
| owns_blocks + eager_free | 决定中间块何时从对象store删，别关 eager_free | 8§2.4 坑③⑨ |
| min_rows_per_bundle≠batch_size | 前者攒块提交 task，后者喂 UDF；不设前者→task 爆炸 | 8§3.3 坑④ |
| TaskPool vs ActorPool | 无状态 vs 有状态缓存；Actor 不参与融合 | 8§3.3 坑⑤ |
| 物化算子(shuffle)内存突增 | 等全输入到齐，易 spill；大 shuffle 前先 sample | 8§3.4 坑⑥ |
| 两类背压 | 提交背压 vs 输出背压，进度条标志区分 | 8§4.2 坑⑧ |
| preserve_order 内存代价 | 缓存乱序完成的输出等前序，峰值内存高 | 8§4.3 坑⑩ |

---

## 三、调试 Ray 内核的思维顺序（把上面串起来）

遇到现象时，按这个顺序定位往往最快：

1. **task/actor 卡住不执行** → 落在哪个队列？
   - 调用方 `NormalTaskSubmitter` 的 `task_queue`（没租到 worker）？看 backpressure/lease（1§B.4）
   - Raylet `infeasible_tasks_`（资源形状不可行）vs `WAITING_FOR_RESOURCES`（暂时没空）vs scheduling class cap（1§C）
   - actor seqno 缺号（2§B.3）
2. **对象取不到 / ObjectLostError** → owner 还活着吗？（陷阱 B/F）位置缓存指了死节点吗（0§4.6）？spill 到 S3 配置生效吗（6§6）？
3. **内存爆 / OOM** → 哪一级？Plasma evict / spill / worker kill（6§7）；谁在长 pin（1§F.2）；Data 的物化算子或 preserve_order（8§3.4/4.3）；fallback allocation 触发了吗（6§2）。
4. **进程"假死"、心跳超时** → 某个 io_service_ 回调阻塞了吗（陷阱 E）？async actor event loop 被同步代码占住（2§C.2）？
5. **集群级新建操作全卡** → GCS 挂了/Redis 断了吗（陷阱 A、3§6）？

---

## 全指南导航

| 篇 | 主题 |
|----|------|
| [0](./00-mental-model.md) | 心智模型：三进程、线程模型、所有权与引用计数 |
| [1](./01-core-task-execution-path.md) | Core 普通 task 链路 |
| [2](./02-core-actor-call-path.md) | Core Actor 调用链路 |
| [3](./03-module-gcs.md) | 模块：GCS |
| [4](./04-module-raylet.md) | 模块：Raylet |
| [5](./05-module-core-worker.md) | 模块：CoreWorker |
| [6](./06-module-object-store.md) | 模块：对象store + 所有权（深入） |
| [7](./07-serve-request-path.md) | Ray Serve 请求链路 |
| [8](./08-data-streaming-execution.md) | Ray Data 流式执行链路 |
| 9 | 易忽略技术点专章（本篇） |

> 读完九篇，你应当能：在脑子里跑通一次 task/actor/HTTP 请求/数据集的端到端执行；指出每步在哪个进程哪个文件；预判改某处代码会踩哪些坑。欢迎按真实源码持续校正本指南——行号会漂，函数名和设计原理是锚。
