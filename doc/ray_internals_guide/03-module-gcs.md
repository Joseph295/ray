# 第 3 篇 · 模块：GCS（全局控制存储，专家级）

> GCS 是集群的"大脑"——全局唯一的**控制平面**。本篇是静态视角：它由哪些 manager 组成、数据存哪、怎么容错、单点失败影响多大。
> 回忆第 0 篇的铁律：**GCS 管控制元数据（节点/Actor/PG/Job/KV/pubsub），不碰普通对象的数据与生命周期**。

源码：`src/ray/gcs/`。进程：`gcs_server`。

---

## 一、架构：一个进程 + 多个 manager + 存储层

```
┌───────────────────────────── gcs_server 进程 ─────────────────────────────┐
│  gRPC server(线程池)        InternalPubSubHandler(long-poll 订阅)           │
│        │                            │                                       │
│  ┌─────▼────────────────────────────▼──────────────────────────────────┐  │
│  │  默认 io_context (大多数 manager 单线程串行, 无锁)                      │  │
│  │   GcsNodeManager  GcsActorManager  GcsResourceManager                 │  │
│  │   GcsJobManager   GcsPlacementGroupManager  GcsWorkerManager          │  │
│  │   GcsAutoscalerStateManager  GcsHealthCheckManager  GcsKVManager      │  │
│  └────────────────────────────┬─────────────────────────────────────────┘  │
│  另有 3 个独立 io_context: task(GcsTaskManager) / pubsub / ray_syncer        │
│                               │                                             │
│  ┌────────────────────────────▼─────────────────────────────────────────┐ │
│  │  GcsTableStorage (抽象)                                                │ │
│  │   ├─ InMemoryStoreClient (默认!! 重启即丢)                              │ │
│  │   └─ RedisStoreClient    (RAY_gcs_storage=redis, 可持久化恢复)          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
   ▲ 心跳/资源上报          ▲ actor创建/task事件/pubsub        ▲ GetClusterResourceState
 Raylet                  Core Worker                      Autoscaler / Dashboard
```

### manager 职责表

| Manager | 一句话职责 | 持久化表 |
|---------|-----------|---------|
| GcsNodeManager | 节点注册/注销/死亡，维护 `alive_nodes_`/`dead_nodes_`，节点死触发级联失败 | NodeTable |
| GcsHealthCheckManager | **GCS 主动**向各 raylet 发健康检查（pull-based），失败 N 次判死 | — |
| GcsActorManager | actor 注册/重启/死亡、状态机、owner 追踪（见第 2 篇） | ActorTable + ActorTaskSpecTable |
| GcsActorScheduler | 调度 actor creation（直接向 raylet 租 worker） | — |
| GcsResourceManager | 汇总各节点资源视图，供调度/autoscaler | 内存汇总 |
| GcsAutoscalerStateManager | 给 autoscaler 提供集群资源状态快照（含待调度 task、PG 约束） | — |
| GcsPlacementGroupManager | PG 调度与 bundle 分布管理 | PlacementGroupTable |
| GcsJobManager | job 生命周期、完成事件、清理 | JobTable |
| GcsWorkerManager | worker 注册、失败上报、worker delta 事件 | WorkerTable |
| GcsTaskManager | task 事件流（可观测性/调试），仅内存缓冲 | — |
| GcsKVManager | 全局内部 KV（runtime_env / serve checkpoint / 函数代码导出 / cluster_id） | 取决于后端 |
| InternalPubSubHandler | 处理 core_worker/raylet 的订阅与 long-poll | — |

> 🔑 **线程模型**：大多数 manager 跑在**同一个默认 io_context** 上，单线程串行执行（所以 manager 内部无需加锁，异步回调也在同线程）。只有 task 事件、pubsub、ray_syncer 三个独立 io_context（`gcs_server_io_context_policy.h`），避免 task 事件洪流阻塞 actor/node 管理。

> ⚠️ **专家级坑①｜默认 io_context 过载会拖慢一切**：node/actor/job/resource 全在默认 io_context。大规模节点同时失活时，级联失败处理会堆积，连带 KV 读写、actor 创建都被延迟。这是大集群 GCS 卡顿的常见根因。

---

## 二、存储层：默认内存、可选 Redis

```
RAY_gcs_storage = "memory"(默认) → InMemoryStoreClient
                = "redis"        → RedisStoreClient (+ GcsRedisFailureDetector)

表: JobTable / NodeTable / ActorTable / ActorTaskSpecTable / PlacementGroupTable / WorkerTable
启动恢复: GcsInitData::AsyncLoad 并发 GetAll 这些表 → 各 manager 用结果初始化
```

> ⚠️ **专家级坑②｜默认内存存储无 HA，GCS 重启即丢全部元数据**：`memory` 模式下 GCS 一重启，`AsyncLoad` 拿到空表——节点/actor/job 信息全没，集群要靠 raylet 重新注册才能部分恢复，且重启前的 actor 状态未知。**生产要 HA 必须 `RAY_gcs_storage=redis`**。把它当"重启不丢"的存储是新人灾难性误解。

> ⚠️ **专家级坑③｜Redis 断连 = GCS 进程自杀**：`gcs_redis_failure_detector.cc` 周期 ping Redis，连接一断就 `RAY_LOG(FATAL)` 退出进程、**不重连**。设计意图是避免"僵尸 GCS"（连不上存储却还在跑），强制外部（systemd/k8s）快速拉起。所以 Redis 本身也得高可用（Sentinel/Cluster），否则 Redis 单点拖垮 GCS。

> ⚠️ **专家级坑④｜内部 KV 无 TTL**：runtime_env、函数导出、serve checkpoint 等都堆在 KV，**没有过期机制**。长跑集群会持续积累垃圾，最终撑大 GCS 内存。删除必须显式 `Del`。`maximum_gcs_destroyed_actor_cached_count`、`gcs_actor_table_min_duration_ms`、`task_events_max_num_task_in_gcs` 等控制各类缓存上限。

---

## 三、节点生命周期与心跳

```
HandleRegisterNode: alive_nodes_[id]=info; 写 NodeTable; 发 NodeInfo pub/sub; 开始健康检查
GcsHealthCheckManager: 每 health_check_period_ms(~3s) 向 raylet 发健康检查
   连续失败 health_check_failure_threshold(~5) 次 → OnNodeFailure
OnNodeFailure: 移出 alive_nodes_ → dead_nodes_; 写 NodeTable(DEAD); 广播;
   级联: GcsActorManager.OnNodeDeath / PlacementGroupManager / ResourceManager.RemoveNode
```

> ⚠️ **专家级坑⑤｜健康检查误判不可撤销，且检测慢**：网络抖动让健康检查连续超时就判定节点死亡，**无法自动撤销**——该节点上所有 actor 被强杀重调度，即便节点马上恢复也晚了。综合 `initial_delay + threshold × timeout`，最坏要几十秒才检测到真死亡。调 `health_check_failure_threshold`/`timeout` 是在"误判"与"检测延迟"之间权衡。

> ⚠️ **专家级坑⑥｜健康检查无 jitter**：所有节点的检查在固定周期同时触发，N 个节点同时回包可能在某时刻冲击 GCS。大集群需注意这个突发负载（当前无随机抖动）。

---

## 四、pub/sub：单 ID 有序、订阅会过期

```
GcsPublisher 频道: GCS_ACTOR_CHANNEL / GCS_NODE_INFO_CHANNEL / GCS_WORKER_DELTA_CHANNEL /
                   RAY_ERROR_INFO / RAY_LOG / RAY_NODE_RESOURCE_USAGE ...
InternalPubSubHandler: core_worker/raylet 通过 gRPC long-poll 订阅特定 ID
   Subscribe 命令记录订阅关系; Poll 返回该 subscriber 订阅的待发消息(增量 index)
```

> ⚠️ **专家级坑⑦｜pub/sub 只保证单 ID 内有序，不保证跨 ID 全局序**：Actor A 的 msg1→msg2 有序，但 A 与 B 的相对顺序不保证。依赖跨实体顺序的逻辑要自己用 version/timestamp 兜底。

> ⚠️ **专家级坑⑧｜订阅者 5 分钟不 poll 会丢消息**：`subscriber_timeout_ms`(~300s) 内不来 poll，publisher 清掉它的消息缓冲。core_worker 必须周期 poll（通常几秒），否则失联期间的状态变更（如 actor 死亡）收不到。`publish_batch_size`(~5000) 控制积压时分批大小。

---

## 五、资源汇总与 autoscaler

```
GcsResourceManager: 每 gcs_pull_resource_loads_period_milliseconds(~1s) 向各 raylet 拉资源负载
GcsAutoscalerStateManager: 汇总成集群状态(总/可用资源 + 待调度 task + PG 约束 + draining 节点)
   autoscaler 调 GetClusterResourceState 消费
```

---

## 六、容错与单点失败影响

GCS 重启（Redis 模式）：`GcsInitData::AsyncLoad` 从 Redis 恢复表 → 各 manager 重建；raylet 通过 `gcs_service_connect_retries`(~50) 重连；core_worker 订阅 poll 在 `subscriber_timeout_ms` 内缓冲。

**GCS 不可用时谁能继续、谁阻塞：**

| 操作 | GCS 可用 | GCS 不可用 |
|------|---------|-----------|
| 普通 task 调度/执行 | ✓ | ✓（raylet 本地调度，见第1篇） |
| Actor 方法调用 | ✓ | ✓（地址已知，直连，见第2篇） |
| 现有 task/actor 运行 | ✓ | ✓ |
| 新节点注册 | ✓ | ✗ |
| **新 actor 创建** | ✓ | ✗（走 GCS） |
| 新 job 提交 | ✓ | ✗ |
| 资源汇总 / autoscaling | ✓ | ✗ |
| pub/sub 新消息 | ✓ | ✗（丢） |
| KV 读写 | ✓ | ✗ |
| 节点失活检测 | ✓ | ✗ |

> 🔑 **GCS 失败的影响是"控制面冻结、数据面续命"**：已经在跑的 task / actor 调用能继续（因为它们不依赖 GCS 热路径——这正是第 0/1/2 篇去中心化设计的回报）；但任何需要"新建/查询全局元数据"的操作都卡住。理解这个边界，才能判断一次 GCS 抖动到底影响了什么。

---

## 不变量清单

1. `dead_nodes_` 的 key 都曾在 `alive_nodes_` 中；节点地址(ip:port)单 session 内唯一。
2. 每个 actor 最多一个 owner；非 detached actor owner 死则被 GC（第 2 篇）。
3. 大多数 manager 单线程执行，状态变更无需锁（靠 io_context 串行）。
4. 默认内存存储重启数据全失；只有 Redis 模式可恢复。
5. pub/sub 单 ID 有序、跨 ID 无序；订阅超时则丢缓冲。
6. Redis 连接断 → GCS 进程退出（不重连）。

---

## config 速查

| config | 默认 | 含义 |
|--------|------|------|
| `RAY_gcs_storage` | memory | 存储后端 memory/redis |
| `health_check_initial_delay_ms` / `period_ms` / `timeout_ms` / `failure_threshold` | 5000/3000/10000/5 | 节点健康检查 |
| `gcs_rpc_server_reconnect_timeout_s` / `gcs_service_connect_retries` | — / 50 | GCS 客户端重连 |
| `subscriber_timeout_ms` | 300000 | pub/sub 订阅超时 |
| `publish_batch_size` | 5000 | pub/sub 批量 |
| `gcs_pull_resource_loads_period_milliseconds` | 1000 | 拉资源负载周期 |
| `maximum_gcs_destroyed_actor_cached_count` | 100000 | 已销毁 actor 缓存上限 |
| `task_events_max_num_task_in_gcs` | 100000 | task 事件缓冲上限 |
| `gcs_redis_heartbeat_interval_milliseconds` | 100 | GCS ping Redis 周期 |

---

## 本篇三条主线

1. **一个进程 + 多 manager + 抽象存储**：控制面元数据集中在 GCS，manager 多在单一 io_context 串行（无锁但会相互拖累）。
2. **默认内存存储不持久**：`memory` 重启即丢，HA 必须 Redis；Redis 断连 GCS 自杀靠外部重启；KV 无 TTL 要手动清。
3. **失败影响是控制面冻结、数据面续命**：在跑的 task/actor 续命，新建/查询全局元数据全卡——这是去中心化设计的回报与边界。

→ 下一篇：[第 4 篇 · 模块 Raylet](./04-module-raylet.md)
