# 第 5 篇 · 模块：CoreWorker（进程内内核，专家级）

> CoreWorker 是**每个 Ray 进程都内嵌的一段 C++ 内核库**——Driver 和 Worker 都带它。本篇是静态视角：它由哪些组件构成、线程怎么分、它如何同时扮演"任务提交方"和"任务执行方"两个角色。
> 第 0/1/2 篇已动态走过它的提交/执行/引用计数路径；本篇把组件拼成整体。

源码：`src/ray/core_worker/`。

---

## 一、CoreWorker 是什么：一个进程的"Ray 运行时"

每个 Ray 进程（Driver/Worker）启动时由 `CoreWorkerProcess` 创建一个 `CoreWorker` 实例。它把"和 Ray 集群打交道"的一切封装起来：提交任务、接收任务、管理拥有的对象、和 raylet/对象store/GCS 通信。Python 侧 `_raylet.pyx` 通过 Cython 调它。

```
┌──────────────────────── 一个进程 ────────────────────────┐
│ Python 层 (用户代码 + _raylet.pyx Cython 绑定)             │
│        │ submit_task / get / put / ...                     │
│ ┌──────▼─────────────────── CoreWorker (C++) ───────────┐ │
│ │ 提交侧:                          执行侧:                 │ │
│ │  NormalTaskSubmitter            TaskReceiver           │ │
│ │  ActorTaskSubmitter             ActorSchedulingQueue   │ │
│ │  LocalDependencyResolver        (execute → Python)     │ │
│ │  LeasePolicy                                            │ │
│ │                                                         │ │
│ │ 记账与对象:                       通信:                   │ │
│ │  TaskManager(待提交/完成/重试)    raylet client          │ │
│ │  ReferenceCounter(所有权/借用)    gcs client             │ │
│ │  store_provider(plasma + memory) CoreWorkerClient(直连) │ │
│ │  ActorManager(actor 句柄/订阅)    pubsub pub/sub         │ │
│ │  ObjectRecoveryManager(重建)                            │ │
│ └─────────────────────────────────────────────────────────┘ │
│ 线程: io_service_(控制平面) + task_execution_service_(执行)   │
└────────────────────────────────────────────────────────────┘
```

> 🔑 **CoreWorker 是对称的**：同一个实例既是"我作为调用方提交任务"（提交侧），也是"别人把任务推给我执行"（执行侧）。Driver 主要用提交侧，Worker 两侧都用。理解这种对称性，才明白为什么 owner/borrower 协议、引用计数会在"同一个类"里既发又收。

---

## 二、线程模型（第 0 篇 §2 的落点）

```
io_service_ (1 后台线程):
   控制平面。gRPC 回调、RPC server handler、定时任务、
   提交流水线(SubmitTask→依赖解析→OnWorkerIdle→租约回调)、引用计数事件
task_execution_service_ (RunTaskExecutionLoop, 单独线程):
   数据平面。执行用户 task/actor 方法(调 Python, 持 GIL)
调用方线程:
   Python 调 .remote()/.get()，跨 Cython 进 C++，with nogil 释放 GIL
```

> ⚠️ **专家级铁律｜别在 `io_service_` 回调里阻塞**（第 0 篇/第 1 篇反复强调）：提交流水线、引用计数回调、PushTask 回调全在这条线程串行。一处阻塞 → 整个进程 RPC 回复停摆 → 心跳超时、cancel 失效。这是 CoreWorker 改代码的头号事故源。
> ⚠️ **专家级｜执行与控制分线程是为了"跑重 task 时仍能响应控制 RPC"**：若把执行误放到 `io_service_`，cancel/ref-removed/心跳全卡（第 1 篇坑⑱）。

---

## 三、提交侧组件

| 组件 | 职责 | 详见 |
|------|------|------|
| `TaskManager` | `AddPendingTask`（提交即生成返回值 ObjectRef、钉 owner）、`CompletePendingTask`、`FailOrRetryPendingTask`、`submissible_tasks_`（重试用） | 第 1 篇 A.3/E |
| `NormalTaskSubmitter` | 普通 task：SchedulingKey 分组、租约申请/复用、spillback、backpressure、cancel | 第 1 篇 B |
| `ActorTaskSubmitter` | actor 方法：ClientQueue、seqno 排序、直连 PushTask | 第 2 篇 B |
| `LocalDependencyResolver` | 提交前等依赖就绪 + 内联 | 第 1 篇 B.1 |
| `LeasePolicy` | locality-aware 选 raylet | 第 1 篇 B.5 |

核心机制回顾：**提交异步**（`io_service_.post`）、**lease-based 复用**（多数后续 task 不经调度器）、**返回值 owner=调用方**。

---

## 四、执行侧组件

| 组件 | 职责 |
|------|------|
| `TaskReceiver` | 收 `PushTask`，区分 normal/actor，入对应调度队列 |
| `NormalSchedulingQueue` / `ActorSchedulingQueue` / `OutOfOrderActorSchedulingQueue` | 执行端排队：普通 task 直接执行；actor 按 seqno 顺序 / 并发 |
| `CoreWorker::GetAndPinArgsForExecutor` | 取齐参数（从 plasma/memory）、登记借用、pin |
| `CoreWorker::ExecuteTask` | 调 `task_execution_callback` 进 Python 跑用户函数 |
| `ConcurrencyGroupManager` + `thread_pool`/`fiber` | actor 并发：threaded / async（第 2 篇 C） |

> ⚠️ **专家级｜执行端是 borrower 协议的起点**：`GetAndPinArgsForExecutor` 给入参 ObjectRef 加 local_ref 并标 borrowed，执行完 `PopAndClearLocalBorrowers` 把借用表随结果回传给 owner（第 0 篇 §4.5、第 1 篇坑⑭）。"提交侧发借用、执行侧报借用"都在同一个 CoreWorker 类里，是对称性的体现。

---

## 五、记账与对象组件 ★

| 组件 | 职责 | 详见 |
|------|------|------|
| `ReferenceCounter` | 去中心化所有权、三个引用计数、借用协议、嵌套、pin 记录；单锁 `mutex_` | 第 0 篇 §4 |
| `store_provider/plasma_store_provider` | 大对象 `[P]` Plasma 读写、跨节点取回 | 第 6 篇 |
| `store_provider/memory_store` | 小对象 `[M]` 进程内内存；direct return | 第 6 篇 |
| `ActorManager` | 本进程持有的 actor 句柄、订阅 GCS actor 状态、地址缓存 | 第 2 篇 B.1 |
| `ObjectRecoveryManager` | 对象丢失时驱动重建（查位置/重 pin/重提交 task） | 第 0 篇 §4.7 |
| `future_resolver` | 解析对 ObjectRef 的等待 |

> 🔑 **CoreWorker 是 Ray 去中心化设计的物理载体**：所有权记账（ReferenceCounter）、对象重建（ObjectRecoveryManager）、借用协议都内嵌在每个进程里，而非中心服务。这就是第 0 篇"普通对象不经 GCS"的实现处——每个 CoreWorker 自己管自己拥有的对象。

> ⚠️ **专家级坑｜owner 死 = 这个 CoreWorker 的 ReferenceCounter 随进程消失**：因为记账是进程内的，owner 进程崩溃，它拥有对象的位置/引用/借用信息全没 → `ObjectLostError`（第 0 篇 §4.8）。这是去中心化的代价，也是"为什么 Driver 崩了 `ray.put` 对象全丢"的根因。

---

## 六、通信组件

- **raylet client**：申请租约、归还 worker、pin 对象、上报 backlog。
- **gcs client**：actor 创建/查询、KV、订阅。
- **CoreWorkerClient（点对点）**：直接给别的 worker 推 task（普通 task 的 PushNormalTask、actor 方法的 direct call）、查 owner 要对象位置、ref-removed pub/sub。
- **pubsub publisher/subscriber**：对象位置、ref removed、actor 状态等频道。

> ⚠️ **专家级｜CoreWorker 同时是 RPC client 和 server**：它既主动发 RPC（提交/取回），又起 gRPC server 收别人推来的 task / 查询。proc 内 `grpc_service_` 跑在 `io_service_` 上。

---

## 不变量清单

1. 每个进程一个 CoreWorker（`CoreWorkerProcess` 管理）。
2. 控制平面在 `io_service_` 串行，执行在 `task_execution_service_`。
3. 提交侧"提交即生成返回值 ObjectRef、owner=调用方"。
4. `ReferenceCounter` 单锁；borrower pub/sub 回调在锁外。
5. 同一 CoreWorker 既发借用（提交侧）又报借用（执行侧）。
6. owner 的记账随其进程存亡——无 GCS 备份。

---

## 本篇三条主线

1. **进程内内核、双角色对称**：CoreWorker 是每个 Ray 进程内嵌的运行时，同一实例既是任务提交方又是执行方；owner/borrower 协议在它内部既发又收。
2. **双线程模型**：`io_service_`（控制平面，所有内核回调，不可阻塞）+ `task_execution_service_`（执行用户代码）。
3. **去中心化设计的物理载体**：所有权记账、对象重建、借用追踪都内嵌进程内（ReferenceCounter/ObjectRecoveryManager），普通对象因此不经 GCS——代价是 owner 进程死则对象丢。

→ 下一篇：[第 6 篇 · 模块 对象store + 所有权（深入）](./06-module-object-store.md)
