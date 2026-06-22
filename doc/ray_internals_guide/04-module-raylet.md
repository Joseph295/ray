# 第 4 篇 · 模块：Raylet（节点管理器，专家级）

> Raylet 是**每节点一个**的本地管家。本篇是静态视角，把第 1 篇链路里散落的 Raylet 部件拼成完整模块图。它管三件大事：**本地调度、worker 池、对象（依赖拉取 + spill）**。
> 第 1 篇阶段 C 已逐函数走过调度链路；本篇补"模块结构、组件分工、不变量"。

源码：`src/ray/raylet/`。进程：`raylet`。

---

## 一、模块全景

```
┌─────────────────────────── Raylet 进程 (node_manager.cc) ──────────────────────────┐
│  NodeManager: gRPC 入口总线 (RequestWorkerLease / ReturnWorker / PinObjects / ...)    │
│      │                                                                                │
│  ┌───┴──────────────┐  ┌──────────────────────┐  ┌──────────────────────────────┐   │
│  │ 调度子系统          │  │ Worker 池             │  │ 对象子系统                     │   │
│  │ scheduling/        │  │ worker_pool.cc       │  │                              │   │
│  │  ClusterTaskManager│  │  起进程/握手/复用/回收 │  │ DependencyManager(拉依赖)      │   │
│  │  LocalTaskManager  │  │                      │  │ LocalObjectManager(pin/spill) │   │
│  │  ClusterResource   │  │  runtime_env 经       │  │ ObjectManager(跨节点传输)      │   │
│  │   Scheduler        │  │  agent 创建           │  │ WaitManager(ray.wait)         │   │
│  │  LocalResourceMgr  │  │                      │  │                              │   │
│  └────────────────────┘  └──────────────────────┘  └──────────────────────────────┘   │
│  其它: PlacementGroupResourceManager, AgentManager(dashboard/runtime_env agent),       │
│        WorkerKillingPolicy(OOM), runtime_env_agent_client                              │
└──────────────────────────────────────────────────────────────────────────────────────┘
   ▲ 心跳/资源上报 → GCS        ▲ RequestWorkerLease ← CoreWorker      ◄═► 其它节点 Raylet(对象传输)
```

---

## 二、调度子系统：两层 + 资源记账

第 1 篇阶段 C 已细讲，这里给模块分工：

```
ClusterTaskManager (集群级):
   选节点(hybrid policy) → 本地 / spillback 远程 / infeasible
LocalTaskManager (节点级):
   排队 + 触发依赖 pull + 资源 instance 分配 + scheduling class cap + PopWorker
ClusterResourceScheduler / LocalResourceManager:
   feasible vs available 判定; instance 级资源分配(FixedPoint); PG wildcard/indexed
ClusterResourceManager:
   维护集群资源视图(经 ray_syncer 与各节点/GCS 同步)
```

关键设计点（已在第 1 篇展开，这里只列模块归属）：

- **hybrid policy + `spread_threshold`**（`scheduling/policy/`）：先 pack 后 spread，阈值语义反直觉（第 1 篇坑⑫）。
- **feasible（`total >= req`）vs available（`available - normal_task_resources >= req`）**：前者定 infeasible，后者定可调度（第 1 篇坑⑨'/⑬）。
- **scheduling class cap + 指数退避**（`local_task_manager.cc`）：防单函数刷爆 worker（第 1 篇坑⑩/⑭）。
- **instance 级资源 + PG wildcard/indexed 同步扣减**（第 1 篇坑⑮）。

> ⚠️ **专家级｜调度是"两层 + 视图最终一致"**：ClusterTaskManager 用的集群资源视图是经 ray_syncer 同步来的、可能 stale 的副本（第 1 篇坑⑬ available 可为负、C.4 stale reject）。所以调度本质是"基于过期视图的乐观决策 + 失败重试纠正"，不是全局一致的精确分配。改调度策略必须接受这个前提。

---

## 三、Worker 池：起进程、握手、复用、回收

```
worker_pool.cc:
  PopWorker: FindAndPopIdleWorker(LIFO + WorkerFitsForTask) 命中即用; 否则 StartNewWorker
  StartNewWorker: (需要 runtime_env → GetOrCreateRuntimeEnv 经 agent) → StartWorkerProcess
       startup_token++(全局单调) → 命令行带 token/runtime_env_hash/node_id/worker_type → execvp
       进 pending_registration, MonitorStartingWorkerProcess 超时计时
  握手: worker 反向连 → RegisterWorker(按 token 找槽, 校验 PID) → AnnounceWorkerPort → PushWorker
  回收: idle worker LIFO, idle_worker_killing_time_threshold_ms 超时 kill
  prestart: 按 backlog/available_cpu 异步预热
```

关键点（第 1 篇 C.7 已展开）：startup_token 防 PID 复用（坑⑯）、复用匹配极严含 runtime_env_hash/dynamic_options（坑⑰）、prestart 异步预热（坑⑧'）。

> ⚠️ **专家级｜worker 进程是 Ray 的稀缺资源，启动昂贵**：起进程 + （可能）建 runtime_env（装包/建 venv）可达秒级。worker_pool 的全部复杂度（复用、prestart、严格匹配、idle 回收）都是为了摊薄这个成本。改这块要时刻想"会不会导致进程风暴或 worker 泄漏"。

---

## 四、对象子系统

```
DependencyManager: task 输入对象不在本地 → object_manager_.Pull() 拉取; 就绪回调放行 task
LocalObjectManager: owner pin 的主副本(PinObjectsAndWaitForFree); 内存压力 spill / restore
ObjectManager: 跨节点对象传输(PullManager 拉 / PushManager 推, 分块 + in-flight quota)
WaitManager: 实现 ray.wait 语义
```

这块的深入在第 6 篇。Raylet 视角只需记住：**Raylet 负责把对象"弄到本地"（拉取/恢复）和"腾出内存"（spill/evict），但对象的所有权与位置权威在 owner（第 0 篇）**。Raylet 是执行者，不是所有者。

> ⚠️ **专家级｜内存压力下的三级响应都在 Raylet**：①Plasma LRU evict 未 pin 对象 → ②`LocalObjectManager` spill 到磁盘/外存 → ③还不够则 `WorkerKillingPolicy`（retriable LIFO / group-by-owner）杀 worker。三者协同但分属不同组件，OOM 排查要分清当前在哪一级（第 6 篇展开）。

---

## 五、NodeManager 作为总线

`node_manager.cc` 是所有 gRPC 的入口与各子系统的粘合层：`RequestWorkerLease`（→调度）、`ReturnWorker`、`PinObjectsAndWaitForFree`（→LocalObjectManager）、`CancelTask`、`ReportWorkerBacklog`、PG 相关的 `Prepare/Commit/CancelResourceReserve`、与 GCS 的心跳/资源上报。它本身逻辑薄，重活在各子 manager。

> ⚠️ **专家级｜Raylet 也是单 io_context 串行**：和 GCS 类似，NodeManager 主逻辑在一个 event loop 上跑。一个慢操作（比如同步等某个 RPC、重的对象操作）会阻塞本节点所有调度/对象事件。Raylet 里的回调同样不能阻塞。

---

## 不变量清单

1. 每节点恰好一个 Raylet。
2. `num_busy_workers ≤ active_workers`；一个 worker 同时一个 task（普通 task）。
3. 资源 instance 级分配，all-or-nothing；PG wildcard 分配 ⊇ indexed 分配。
4. startup_token 全局单调，区分 PID 复用；注册按 token 找槽。
5. Raylet 把对象弄到本地/腾内存，但不拥有对象（所有权在 owner）。
6. 集群资源视图是同步来的、可能 stale 的副本。

---

## config 速查

| config | 含义 |
|--------|------|
| `scheduler_spread_threshold`(0.5) | hybrid policy pack→spread 拐点 |
| `worker_lease_timeout_milliseconds` | 租约超时归还 |
| `worker_register_timeout_seconds`(60) | worker 注册握手超时 |
| `worker_maximum_startup_concurrency` | 并发启动 worker 上限 |
| `idle_worker_killing_time_threshold_ms`(60000) | idle worker 回收 |
| `enable_worker_prestart` | 预热开关 |
| `worker_cap_enabled` | scheduling class cap |
| `object_store_memory` | 本节点 Plasma 容量 |
| `automatic_object_spilling_enabled` / `object_spilling_threshold`(0.8) | spill 触发 |
| `max_io_workers`(4) | spill/restore 并发 worker |

---

## 本篇三条主线

1. **三大子系统 + 总线**：NodeManager 作 gRPC 总线，下挂调度（两层 + 资源记账）、worker 池、对象（依赖拉取 + pin/spill + 跨节点传输）。
2. **调度是基于 stale 视图的乐观决策**：集群资源视图同步而来、可能过期，靠失败重试纠正；worker 池的全部复杂度是为了摊薄进程启动成本。
3. **Raylet 是对象的执行者不是所有者**：负责拉取/恢复/腾内存（evict→spill→kill 三级），所有权与位置权威在 owner。Raylet 主逻辑单 io_context 串行，回调不可阻塞。

→ 下一篇：[第 5 篇 · 模块 CoreWorker](./05-module-core-worker.md)
