# 第 1 篇 · Core 普通 Task 执行链路（专家级）

> 跟着一次 `y = f.remote(x)` 走完全程，深到能当 maintainer 改它。每步落到**进程 / 线程 / 文件 / 函数**，并给出**不变量、并发竞态、config 旋钮、失败模式**。
> 函数名是稳定锚点，行号随版本漂移；本篇正文不写死行号断言。
> 先读 [第 0 篇](./00-mental-model.md)：三进程角色、**两个 io_context 的线程模型**、所有权与引用计数。本篇大量依赖那里建立的概念。

本篇是**普通 task**（无状态 `@ray.remote` 函数）。Actor 见第 2 篇。

---

## 全景时序图（标注线程）

```
 Driver/调用方                       本地/远程 Raylet         目标 Worker
 ════════════                       ════════════════         ═══════════
 [调用方线程]
   f.remote(x)
   阶段A: remote_function.py → _raylet.pyx(参数分类) → CoreWorker::SubmitTask
          → TaskManager::AddPendingTask ★ 同步生成返回值 ObjectRef y (owner=调用方)
   io_service_.post  ┐ 切到 io_service_ 线程
                     ▼
 [io_service_ 线程]
   阶段B: NormalTaskSubmitter::SubmitTask
          → ResolveDependencies(等 x 就绪+内联)
          → 按 SchedulingKey 入队 → 有空闲租约? 直接推 : RequestNewWorkerIfNeeded
       │ ③ RequestWorkerLease (grant_or_reject = is_spillback)
       │ ═══════════════════════════════════════►┐ [Raylet io 线程]
       │                          阶段C: HandleRequestWorkerLease
       │                          → ClusterTaskManager(选节点: hybrid policy)
       │                            ├ 本地 → LocalTaskManager(依赖pull+资源instance分配+PopWorker)
       │                            ├ 远程 → spillback(retry_at_raylet 或 rejected)
       │                            └ 无可行节点 → infeasible
       │   ◄═════════════════════════════════════┘ reply: worker 地址 + resource_mapping
       │ ④ PushNormalTask 点对点 (租约可复用, 不再经调度器)
       │ ═══════════════════════════════════════════════════════════►┐ [Worker io_service_ 线程]
       │                                      阶段D: TaskReceiver::HandleTask → 入队
       │                                            GetAndPinArgsForExecutor(取参数+登记借用)
       │                                      [Worker execution 线程] CoreWorker::ExecuteTask
       │                                            → task_execution_callback(Python)
       │                                      阶段E: CompletePendingTask 结果写 [M]/[P]
       │   ◄═══════════════════════════════════════════════════════════┘ reply(+borrower表)
       │ [io_service_线程] PushNormalTask 回调: 标记 y 就绪, merge borrower, OnWorkerIdle
       ▼
   阶段F: ray.get(y) → 本地命中 or 问 owner 要位置 → PullManager 分块拉到本地 [P]
```

---

## 阶段 A — 提交：Python → C++ TaskSpec（调用方线程）

### A.1 Python：`remote_function.py::_remote`

填默认选项、注入 tracing、（首次）导出函数体到 GCS、`flatten_args()` 摊平参数，最后调 `worker.core_worker.submit_task(...)` 入 Cython。

> ⚠️ **易忽略①｜函数体只导出一次**：pickle 后的函数经 `function_actor_manager.export()` 推到 GCS KV，执行它的 worker 按需拉取。后续 `f.remote()` 只传**函数描述符**（module/函数名/源码 hash 的轻量 key），不重复传代码。

### A.2 Cython：`_raylet.pyx::prepare_args_internal` —— 参数分类

```
 对每个参数 arg:
   arg 是 ObjectRef?         → 按引用 (TaskArgByReference): 传 ObjectID + owner 地址
   否则序列化得 size:
     size ≤ 100KB 且 本任务累计内联+size ≤ 10MB
        → 按值内联 (TaskArgByValue): 数据塞进 RPC; 提取 arg 内嵌的 ObjectRef 作依赖
     否则(大对象)
        → put 进本地对象store, 退化为按引用
```

阈值（`src/ray/common/ray_config_def.h`）：`max_direct_call_object_size`（单参内联上限，默认 100KB）、`task_rpc_inlined_bytes_limit`（**整个 task 内联总预算**，默认 10MB）。

> ⚠️ **易忽略②｜内联有"总预算"**：一个 task 的多个参数共享 10MB 内联预算，前面的小参数累计吃满后，后面的小参数也会被推进对象store。同样大小的参数，位置不同→传输路径不同→性能不同。

> ⚠️ **易忽略③｜容器里嵌套的 ObjectRef 也被当依赖**：序列化时 `contained_object_refs` 被提取成 `nested_inlined_refs` 登记为依赖，所以 `{"k": ref}` 里的 ref 也会被正确等待。用了不遵守 Ray 协议的自定义序列化器则会漏掉，导致依赖没就绪就执行。

### A.3 C++：`CoreWorker::SubmitTask` → `AddPendingTask`

```
SubmitTask (core_worker.cc):
  task_id = TaskID::ForNormalTask(job, parent, index)
  BuildCommonTaskSpec(): 参数/资源/runtime_env → TaskSpec protobuf (逐个 AddArg)
  SetNormalTaskSpec(); ConsumeAndBuild() → TaskSpecification
  returned_refs = task_manager_->AddPendingTask(caller_address, spec, ...)   ★ 同步
  io_service_.post([spec]{ normal_task_submitter_->SubmitTask(spec); })      ← 异步, 切线程
  return returned_refs → 回到 Python
```

`AddPendingTask`（`task_manager.cc`）：抽依赖（ArgByRef 的 ID + 内联里的 nested refs）；对每个返回值 `AddOwnedObject(return_id, owner=caller_address, is_reconstructable=true, add_local_ref=true)`；`UpdateSubmittedTaskReferences` 把依赖标成"被已提交 task 引用"（同时 ++ submitted 和 lineage 两个计数，见第0篇§4.3）；把 spec 存进 `submissible_tasks_`（留作重试）。

> 🔑 **第一定理**：返回值 ObjectRef 在**执行前、发 Raylet 前**就生成，owner 钉成调用方。这是第0篇"owner=调用方"的代码出处。

> ⚠️ **易忽略④｜提交跨线程异步**：`AddPendingTask` 在调用方线程同步跑（所以 Python 立刻拿到 `y`），但真正的提交 `io_service_.post` 到 `io_service_` 线程。`f.remote()` 几乎不阻塞——不等调度、不等执行。这是 Ray 能瞬间 fan-out 海量 task 的原因，也意味着**后续所有提交逻辑都在 `io_service_` 单线程串行**，受第0篇§2"别在该线程阻塞"约束。

---

## 阶段 B — CoreWorker 侧：依赖解析 + 租约（io_service_ 线程）

文件：`src/ray/core_worker/transport/normal_task_submitter.cc`。

### B.0 并发模型与不变量

`NormalTaskSubmitter` 全部状态由单把 `mu_` 保护：

```
 scheduling_key_entries_[key]:  task_queue / active_workers / num_busy_workers / pending_lease_requests
 worker_to_lease_entry_[addr]:  lease 元数据(超时时间, is_busy, 资源)
 executing_tasks_[task_id]:     正在某 worker 上执行 → worker 地址
 cancelled_tasks_:              已取消待清理的 TaskID
 remote_lease_clients_[raylet]: spillback 用的远程 raylet 连接
```

**核心不变量**：
- `num_busy_workers ≤ active_workers.size()`。
- 一个 worker 同时最多 1 个 task 在飞（`lease_entry.is_busy`）。
- `executing_tasks_` 里每个 TaskID 对应唯一 worker 地址。
- 回调（依赖解析回调、PushTask 回调）进入时**先抢 `mu_` 检查状态再操作**，持锁时间最短，重 RPC 在锁外发。

### B.1 依赖解析 `dependency_resolver.cc`

```
ResolveDependencies:
  抽出所有 ArgByRef 依赖 ObjectID; 无依赖 → 快路径直接回调
  对每个 ID: in_memory_store_.GetAsync()，就绪回调里 obj_dependencies_remaining--
  全就绪 → InlineDependencies(): 把还在内存(未 spill)的依赖内联进 task_spec, 清掉 object_ref 字段
```

> ⚠️ **易忽略⑤｜依赖可能在调用方内联、也可能留给 Raylet 拉**：小的、还在内存的依赖在提交前就内联进 spec；已 spill 到 Plasma 的大依赖保持按引用，留给阶段 C 的 Raylet pull。**同一个依赖在哪一层取决于它当时在哪。**

### B.2 SchedulingKey 分组 + 租约复用

```
SchedulingKey = (SchedulingClass[资源形状+函数描述符], [依赖ObjectID...], ActorID(Nil), RuntimeEnvHash)

有空闲(非 busy)的已租 worker?  是 → OnWorkerIdle() 直接推, 不惊动 Raylet
                               否 → RequestNewWorkerIfNeeded()
```

> 🔑 **第二定理｜Lease-based scheduling**：申请的是"worker 租约"而非"执行一个 task"。一个租到的 worker 能连续执行**同一 SchedulingKey** 的多个 task。**绝大多数后续 task 不经过 Raylet 调度器**，省掉每 task 一次调度 RPC——Ray 低延迟高吞吐的根。新人最大误解就是"每个 task 都问调度器"。

> ⚠️ **易忽略⑥｜SchedulingKey 含依赖 ObjectID**：所以"同一个函数、相同资源、但依赖不同数据"的 task 会走**不同的 worker 队列**，让 Raylet 能按数据本地性选 worker。少了这一维，跨节点数据搬运会变多。

### B.3 OnWorkerIdle 复用循环 + 租约归还（精确条件）

```
OnWorkerIdle(addr, key, was_error):
  应归还? = was_error || worker_exiting || now > lease_expiration_time || queue.empty()
     是 → ReturnWorker(addr)  (从 active_workers 移除, RPC 通知 raylet, 删 lease entry)
     否 → while (!queue.empty() && !lease_entry.is_busy):
            task = queue.front(); pop; is_busy=true; num_busy_workers++
            set_lease_grant_timestamp; PushNormalTask(addr, ...)
```

租约三条归还路径：①正常（队列空，`OnWorkerIdle` 调 `ReturnWorker`）；②**超时**（`now > lease_expiration_time`，`lease_timeout_ms` ≈ `worker_lease_timeout_milliseconds`，兜底防泄漏）；③显式取消。

> ⚠️ **专家级竞态⑦｜lease 超时 race**：多个 task 几乎同时完成、回调交错时，可能 callback#1 通过了"未超时"检查，callback#2 已 `ReturnWorker`，callback#1 再去推任务给已归还的 worker。规避靠"检查通过后立刻把 worker 从 `active_workers` 移除"。改 `OnWorkerIdle` 时极易破坏这个保证。

### B.4 backpressure：限流租约 + 上报 backlog

```
RequestNewWorkerIfNeeded 几道闸:
  pending_lease_requests.size() >= max_pending_lease_requests_per_scheduling_category? → 不发
       (该 config 默认 -1 = 自动设为集群节点数)
  queue 空 → 清理 entry
  queue.size() <= pending_lease_requests.size() → 在途够了, 不发
  否则 lease_policy_->GetBestNodeForTask() 选节点 → RequestWorkerLease RPC
  ReportWorkerBacklogIfNeeded(): backlog = queue.size() - pending_lease.size(), 变化才上报
```

> ⚠️ **易忽略⑧｜backlog 上报驱动 autoscaler**：调用方把"还差多少 worker"作为 backlog 报给本地 raylet，最终影响自动扩缩容。`SubmitTask` 快速入队但 lease 批量返回时，backlog 会尖刺，可能造成 autoscaler 过冲。

### B.5 locality 决策

`lease_policy_->GetBestNodeForTask`（`lease_policy.cc`）：SPREAD → 本地；node affinity → 指定节点；否则 `GetBestNodeIdForTask` 对每个依赖查 `locality_data_provider_.GetLocalityData`，累加各节点本地依赖字节数，选最多的（把计算搬向数据）。

> ⚠️ **易忽略⑨｜locality 决策在调用方做，且只对首次租约生效**：依据是调用方**本地缓存**的位置视图，可能过期（回忆第0篇§4.6：`UnsetObjectPrimaryCopy` 不清 `locations`，缓存可能指向死节点）。spillback/重试退化为直接问本地 raylet，忽略 locality。

### B.6 spillback 终止保证 ★

```
is_spillback = (raylet_address != nullptr)          // 非首次=被重定向来的
RequestWorkerLease(..., grant_or_reject = is_spillback)
```

- 首次请求 `grant_or_reject=false`：目标 raylet 可以 grant、reject、或 **redirect**（返回 `retry_at_raylet_address`）。
- 被重定向后的请求 `grant_or_reject=true`：目标 raylet **只能 grant 或 reject，不能再 redirect**。
- ⇒ **重定向链最多一跳**，杜绝 A→B→C→A 无限 spillback。reject 后回退本地 raylet 重试；RPC 失败也回退本地。

> ⚠️ **专家级坑⑩｜紧 spillback 循环**：虽无死循环，但若本地资源持续不足，会"本地→spillback 远程→远程 reject→回本地→…"高频空转，租约请求频率远高于资源释放速率。根因是 backlog 上报滞后、autoscaler 反应慢。看到 raylet 间 lease RPC 风暴时想到这里。

### B.7 取消路径 `CancelTask`

```
已提交未调度(在 task_queue): 直接 erase, CancelWorkerLeaseIfNeeded, FailPendingTask(TASK_CANCELLED)
在依赖解析中: resolver_.CancelDependencyResolution; 记入 cancelled_tasks_;
             解析完成回调里发现 cancelled → 不入队
已派发执行中(在 executing_tasks_): 向该 worker 发 CancelTask RPC
             force_kill=true → 杀整个 worker; recursive=true → 级联取消子任务
             reply 说"还在跑" → 定时器 async_wait 后递归重试 CancelTask
```

> ⚠️ **专家级坑⑪｜`cancelled_tasks_` 只在依赖解析完成时清理**：若 task 已越过解析阶段，其 TaskID 可能滞留集合中。配合重试会生成新 TaskID，旧 ID 残留。改取消逻辑要注意这个集合的清理时机，否则可能"某些 task 永不重试"。

---

## 阶段 C — Raylet：调度 + 取/启 worker + 授租（Raylet io 线程）

文件：`src/ray/raylet/`。

### C.1 入口与集群级调度

```
node_manager.cc: HandleRequestWorkerLease
  校验 caller 存活; worker_pool_.PrestartWorkers(); cluster_task_manager_->QueueAndScheduleTask()

scheduling/cluster_task_manager.cc:
  QueueAndScheduleTask → 按 scheduling_class 入 tasks_to_schedule_
  ScheduleAndDispatchTasks → GetBestSchedulableNode():
     nil      → infeasible_tasks_ (集群无任何节点能满足资源形状)
     本地节点  → LocalTaskManager
     远程节点  → spillback: grant_or_reject? reject : (AllocateRemoteTaskResources + retry_at_raylet)
```

### C.2 调度策略：hybrid policy 与 `spread_threshold` ★

`GetBestSchedulableNode` 背后是 `scheduling_policy`（`src/ray/raylet/scheduling/policy/`）。默认 **hybrid policy**：先 pack 到阈值、再 spread。

```
节点评分 score:
  critical_util = max(各资源利用率)   // CPU/MEM/对象store 取最紧的
  critical_util < spread_threshold(默认 0.5)?  → score = 0     // PACK: 阈值内不计分, 优先塞满
                                              否则 → score = critical_util  // SPREAD: 按利用率排
选择: 优先 available 节点(按 score 升序); 无 available 且不强制时退到 feasible 节点
```

> ⚠️ **专家级坑⑫｜`spread_threshold` 语义反直觉**：`0.5` 不是"≤50% 才 spread"，而是"**利用率 < 50% 时 score 归零→优先 pack 填满**，≥50% 才按利用率 spread"。所以调度行为会在 ~50% 利用率处发生**突变**。`scheduler_spread_threshold` 可调。

### C.3 feasible vs available —— 两个必须分清的资源概念

```
IsFeasible(req): node.total >= req            // 总资源够吗(忽略当前占用) → 决定 infeasible 与否
IsAvailable(req): (node.available - normal_task_resources) >= req
                  且 不因 object_pulls_queued 满容而拒
```

> ⚠️ **专家级坑⑬｜available 可暂时为负**：`SubtractResourceInstances(allow_going_negative=true)` 允许 CPU 等被 oversubscribe，内部为负、上报时裁剪为 0。于是别处看到的"available=0"内部可能是 -2.5。stale view 下 GCS 调度器可能过于乐观。改资源记账要 `available>=0` 和 `total>=demand` 两层都查。

> ⚠️ **专家级坑⑭｜pull manager 满容会"隐形"挡住本地调度**：`object_pulls_queued=true` 时 `IsAvailable` 返回 false（除非是本地节点用 `ignore_pull_manager_at_capacity`）。大量对象待拉取时，明明有 CPU 也会突然无法本地调度、全往外 spillback。

### C.4 资源视图同步与 stale reject

`LocalResourceManager` 变化 → `OnResourceOrStateChanged` → `ClusterResourceManager` → `syncer::ResourceViewSyncMessage`（version 单调递增）定期广播给其他 raylet + GCS。视图过期会导致：调用方按旧视图选了某节点，该节点其实已满 → `AllocateRemoteTaskResources` 失败 → reject → 调用方重试。相关：`raylet_report_resources_period_milliseconds`、`resource_broadcast_batch_size`。

### C.5 本地任务管理：排队 + 依赖 pull + 资源分配

```
local_task_manager.cc:
  WaitForTaskArgsRequests(): 有依赖不在本地 → DependencyManager::RequestTaskDependencies()(启动 pull)
                             进 waiting_task_queue_; 就绪 → tasks_to_dispatch_[key]
  DispatchScheduledTasksToWorkers():
     fair scheduling: 某 class 占用过多则跳过
     scheduling class cap: running >= cap → 指数退避 cap_interval*2^(running-cap)  (防单函数刷爆)
     AllocateLocalTaskResources(): instance 级分配; 失败 → TrySpillback; 再不行 WAITING_FOR_RESOURCES, break
     PopWorker() → 回调 PoppedWorkerHandler
```

> ⚠️ **易忽略⑩(承上)｜scheduling class cap + 指数退避**：同一 scheduling class 在跑的 task 达上限(≈节点 CPU 数)时推迟派发，等待时间指数增长。防一个会递归产子任务的函数占满 worker 池导致死锁（`worker_cap_enabled`）。资源看着够却卡在派发，常是撞了这个 cap。

> ⚠️ **易忽略⑨'｜infeasible ≠ 暂时没资源**：`infeasible_tasks_`=集群无任何节点满足资源形状（要 100 GPU 但最多 8），挂着等扩容；`tasks_to_schedule_`/`WAITING_FOR_RESOURCES`=形状可行但暂时没空。调 task 卡住先分清落在哪个队列。

### C.6 资源 instance 模型：为什么不是标量减法 ★

`src/ray/common/scheduling/`（`FixedPoint`、`cluster_resource_data.h`、`resource_instance_set`）。

- 资源用 **instance 向量**而非标量。`FixedPoint` 用整数(单位 1/10000)避免浮点误差。
- **unit 资源**(CPU/GPU)：`total=4` → `[1,1,1,1]`；需 2.5 → 分 `[1,1,0.5]`。分数 GPU 必须落到**具体某个 GPU instance** 上，其 index 写进 reply 的 `resource_mapping`，worker 据此设 `CUDA_VISIBLE_DEVICES`。
- `TryAllocate` 贪心：先用满整数 instance，再从"最小够用"的 instance 取分数；**all-or-nothing**，任一资源不够整体失败。

> ⚠️ **专家级坑⑮｜Placement Group 的 wildcard vs indexed 资源**：PG 资源在 raylet 表示为两套——`GPU_<pgid>`(wildcard，PG 内任意 bundle 可用) 和 `GPU_<bundle>_<pgid>`(indexed，特定 bundle)。分配 indexed 时**必须同步在 wildcard 上扣同一个 instance**（保持 `wildcard 分配 ⊇ indexed 分配` 的不变量）。这块原子性靠 raylet 单线程保证，没有显式锁——若将来 raylet 并发化这里要加锁。改 PG 资源记账极易破坏 wildcard/indexed 一致性。

### C.7 取 worker：复用 or 启新进程 + 启动握手 ★

```
worker_pool.cc: PopWorker:
  FindAndPopIdleWorker(): 从 idle_of_all_languages_ 按 LIFO 找; WorkerFitsForTask() 检查
       匹配条件: 未死/未在退出, language, worker_type, root_detached_actor_id 或 job_id,
                 is_gpu, runtime_env_hash, dynamic_options 全部一致
  未命中 → StartNewWorker():
       需 runtime_env → GetOrCreateRuntimeEnv()(可能很慢: 装包/建 venv)
       StartWorkerProcess(): startup_token++ (全局单调), 拼命令行(--startup-token/--runtime-env-hash/
                             --node-id/--worker-type/...), execvp 起进程
       进 pending_registration_requests, MonitorStartingWorkerProcess 起超时计时器
```

启动握手 5 步：起进程(带 startup_token) → worker 反向连 → `RegisterWorker`(raylet 按 startup_token 找到进程槽，校验 PID，标记已注册，取消超时计时器) → `AnnounceWorkerPort` → `PushWorker` 入池或匹配待处理请求。

> ⚠️ **专家级坑⑯｜startup_token 区分 PID 复用**：PID 会被 OS 复用，所以 raylet 用**全局单调递增的 startup_token**而非 PID 标识 worker 启动实例。注册握手按 token 找进程槽。`worker_register_timeout_seconds`(默认 60) 超时未注册则杀进程并重试。

> ⚠️ **易忽略⑫'｜worker 复用匹配极严**：尤其 `runtime_env_hash` 与 `dynamic_options`(如 JVM `-Xmx`) 必须完全一致才能复用。同 job 不同 runtime_env / 不同 JVM 参数的 task **不能共享 worker 进程** → 频繁切换会触发大量进程启动+空闲堆积。

> ⚠️ **易忽略⑧'｜prestart 是异步预热**：`PrestartWorkers` 按 `num_needed ≈ ceil(backlog/available_cpu)*available_cpu` 提前批量起 worker(受 `worker_maximum_startup_concurrency` 上限)，与当前这个 lease 请求不同步。空闲 worker 按 LIFO + `idle_worker_killing_time_threshold_ms`(默认 60s) 回收。这解释了为何有时秒拿 worker、有时要等 `WAITING_FOR_WORKER`。

### C.8 授租：回填 worker 地址

```
PoppedWorkerHandler:
  再查一次 owner 存活(从申请到拿到 worker 之间 owner 可能已死) → 死了就释放资源放弃
  Dispatch():
     normal task    → SetAllocatedInstances()      (task 结束即释放)
     actor creation → SetLifetimeAllocatedInstances()(actor 生命周期持有)
     reply: worker_address(ip/port/worker_id/raylet_id) + resource_mapping(含 instance index)
     leased_workers[worker_id] = worker; send_reply_callback() ══> 回到调用方
```

> ⚠️ **易忽略⑬｜授租前再校验 owner 存活**：从申请到取到 worker 有时间差，期间调用方可能死了。`PoppedWorkerHandler` 再确认一次，避免白占 worker。"执行前最后一刻再校验"是 Ray 的常见模式。

---

## 阶段 D — Worker 执行（Worker 的两条线程）

```
[io_service_ 线程] task_receiver.cc: HandleTask
   normal → normal_scheduling_queue_->Add() (入 deque)
[io_service_ 线程] core_worker.cc: GetAndPinArgsForExecutor
   by-ref 参数: 加本地引用、登记为 borrowed、从 [P]/[M] 取值
   inlined 参数: 提取嵌套 ObjectRef 逐个加本地引用
[execution 线程] core_worker.cc: ExecuteTask
   设 context → options_.task_execution_callback()  ← C++→Python 桥, 跑用户函数(持 GIL)
   返回值 → return_objects
```

> ⚠️ **易忽略⑭｜执行端把入参 ObjectRef 登记为借用**：`GetAndPinArgsForExecutor` 给每个 by-ref 参数加 local_ref 并标 borrowed，且执行期间额外 pin 一个 local_ref（防执行中被 GC）。这正是第0篇§4.5 借用协议执行端起点，也是坑④ `deduct_local_ref` 的由来——执行完回报借用表时要扣掉这个额外 ref。

> ⚠️ **专家级坑⑰｜两条线程的分工不能破坏**：接收/取参数在 `io_service_` 线程，执行用户函数在 execution 线程。正因为执行不占 `io_service_`，worker 跑重 task 时仍能响应 cancel、ref-removed、心跳等 RPC。若把执行误放到 `io_service_` 线程，cancel 就会失效、心跳会超时。

---

## 阶段 E — 结果回传与完成（Worker → 调用方 io_service_ 线程）

```
task_manager.cc: CompletePendingTask (产生结果的进程侧):
  按 store_in_plasma_ids / direct_return_ids 决定每个返回值落 [P] 还是 [M]
     小对象 → [M], 随 PushTask reply 内联回传
     大对象 → [P] PutInLocalPlasmaStore, memory store 只留 OBJECT_IN_PLASMA 标记
  UpdateFinishedTaskReferences / MergeRemoteBorrowers: 合并 worker 回传的 borrower, 释放对依赖的引用
ExecuteTask 收尾: PopAndClearLocalBorrowers 序列化借用表, 随 reply 回调用方

调用方 [io_service_线程] PushNormalTask 回调:
  executing_tasks_ 移除; lease_entry.is_busy=false
  status.ok? → 视 retry_exceptions/可重试性 决定 RetryTaskIfPossible 或 CompletePendingTask
  OnWorkerIdle(): 队列还有同类 task → 复用该 worker 继续推; 否则 ReturnWorker
```

> ⚠️ **易忽略⑮｜首次执行 vs 重试，返回值落 plasma 与否可能不同**：`CompletePendingTask` 用 `store_in_plasma_ids` 区分。重试是血缘重算，只有"之前被 promote 到 plasma 且仍有下游引用"的返回值才需重新落 plasma。调"重试后对象去哪了"会用到。

> ⚠️ **专家级｜普通 task 是 at-least-once**：网络抖动下 PushNormalTask 可能重发；靠 TaskID 全局唯一 + `attempt_number`（每次重试递增，`task_attempt_number()` 可查）在执行端去重/标识。失败时调用方先 `GetTaskFailureCause` 拿失败原因再决定重试或终止；即便失败，只要 worker 没退出仍可被 `OnWorkerIdle` 复用。

---

## 阶段 F — `ray.get(y)` 取回（owner 进程）

```
plasma_store_provider.cc / memory_store:
  1. 本地优先: batch 尝试本地 [M]/[P], 不阻塞
  2. 不在本地: 循环 FetchAndGetFromPlasmaStore(fetch_only=false)
       → 经 Raylet 向 owner 查位置 → PullManager 从有副本节点拉到本地 [P]
  3. direct return 小对象: 可能本就随 reply 进了 [M], 直接命中
```

### F.1 PullManager 分块传输（`src/ray/object_manager/`）★

对象在节点间是**分块(chunk)传输**，pull 端有 quota/优先级：

```
PullManager 优先级: GET_REQUEST > WAIT_REQUEST > TASK_ARGS
quota: RemainingQuota = num_bytes_available - (num_bytes_being_pulled - pinned)
       OverQuota → 暂不激活低优先级 bundle
  GET 请求不受 quota 限制(必须满足用户 ray.get); WAIT/TASK_ARGS 受 quota
pull 重试: 指数退避 next_pull = now + pull_timeout_ms * 2^num_retries, 上限 ~10 次
传输: PushManager 限 max_chunks_in_flight (≈ object_manager_max_bytes_in_flight / chunk_size)
      chunk 经 gRPC 发送; receiver 按 offset 组装, 最后一块到齐才 seal 成完整对象
```

config：`object_manager_max_bytes_in_flight`(默认 ~2GB)、`object_manager_pull_timeout_ms`(默认 ~10s)。

> ⚠️ **专家级坑⑱｜TASK_ARGS 可能被 GET 饿死**：每次内存可用都重排优先级，若 `ray.get` 持续涌入(GET_REQUEST)，最低优先级的 task 参数拉取(TASK_ARGS)可能长期不被激活——表现为"某些 task 的输入一直拉不下来"。靠给 TASK_ARGS 保底带宽缓解。

> ⚠️ **易忽略⑯｜取回先问 owner 要位置**：对象数据可能在任意节点 Plasma。owner 的 `ReferenceCounter.locations` + object directory 维护位置。**owner 死则位置丢 → `ObjectLostError`**（第0篇§4.8）。"取回失败"未必是数据节点问题，可能是 owner 没了。

### F.2 Plasma 驱逐与 fallback（被取回/被引用时的内存侧）

```
对象状态: created → SealObject → sealed(LRU 可驱逐) → BeginObjectAccess → pinned(不可驱逐)
                                                    → EndObjectAccess → 回 sealed
内存满: EvictionPolicy::RequireSpace → ChooseObjectsToEvict(LRU 末尾, 至少腾 ~20%)
       仍不够 → local_object_manager spill 到外存
分配失败: fallback_allocator (如 /dev/shm 不足时退到 /tmp)
```

> ⚠️ **专家级坑⑲｜正被 task 用的对象会被临时 pin**：`BeginObjectAccess` 把对象从 LRU 摘下计入 `pinned_memory_bytes`，`EndObjectAccess` 放回。如果某进程拿了对象却长期不释放(忘记 `del`/长期持有)，它会一直 pin 着、挤占可驱逐空间，诱发别处 OOM。排查 Plasma OOM 时要看谁在长 pin。

---

## 易忽略点总表（本篇速查）

| # | 技术点 | 位置 |
|---|--------|------|
| ① | 函数体只导出一次到 GCS | `remote_function.py` `export` |
| ② | 内联参数有 10MB **总预算** | `_raylet.pyx` `prepare_args_internal` |
| ③ | 容器内嵌 ObjectRef 也被当依赖 | 同上 `nested_inlined_refs` |
| ④ | 提交跨线程异步，几乎不阻塞 | `core_worker.cc` `io_service_.post` |
| ⑤ | 依赖可能调用方内联或 Raylet 拉取 | `dependency_resolver.cc` |
| ⑥ | SchedulingKey 含依赖 ID → 按数据本地性分队 | `normal_task_submitter.cc` |
| ⑦ | **lease 超时 race**：检查后须立即移除 worker | `OnWorkerIdle` |
| ⑧ | backlog 上报驱动 autoscaler；prestart 异步预热 | `ReportWorkerBacklog`/`PrestartWorkers` |
| ⑨ | locality 在调用方算、只首次生效；位置缓存可能指死节点 | `lease_policy.cc` |
| ⑩ | spillback 最多一跳(grant_or_reject)；但可能紧循环空转 | `RequestNewWorkerIfNeeded` |
| ⑪ | `cancelled_tasks_` 只在解析完成时清理 | `CancelTask` |
| ⑫ | **spread_threshold 语义反直觉**(<阈值→pack) | `scheduling/policy/` |
| ⑬ | available 可暂时为负；pull 满容隐形挡调度 | `cluster_resource_data` |
| ⑭ | scheduling class cap + 指数退避防刷爆 | `local_task_manager.cc` |
| ⑮ | 资源 instance 级分配；PG wildcard/indexed 必须同步扣 | `resource_instance_set` |
| ⑯ | startup_token 区分 PID 复用；握手超时杀进程 | `worker_pool.cc` |
| ⑰ | worker 复用要 runtime_env/dynamic_options 全匹配 | `WorkerFitsForTask` |
| ⑱ | 执行/接收分两线程，执行不可占 io_service_ | `task_receiver`/`ExecuteTask` |
| ⑲ | PullManager: TASK_ARGS 可能被 GET 饿死 | `pull_manager.cc` |
| ⑳ | Plasma: 正被用的对象临时 pin，长 pin 致 OOM | `eviction_policy.cc` |

---

## 本篇三条主线

1. **提交即生成、owner 即调用方、提交异步**：`AddPendingTask` 执行前造好返回值 ObjectRef 并钉 owner=调用方；真正提交 post 到 `io_service_` 单线程。
2. **Lease-based scheduling**：申请 worker 租约而非执行单个 task；租约可复用，多数后续同类 task 不经调度器。spillback 靠 `grant_or_reject` 限一跳防死循环。
3. **数据走两层对象store、生命周期靠去中心化引用计数、传输靠分块 pull**：小对象 `[M]` 内联、大对象 `[P]` 分块拉；资源是 instance 级；取回靠 owner 位置表，owner 死则对象丢。

→ 下一篇（待写）：[第 2 篇 · Core Actor 调用链路](./02-core-actor-call-path.md)——有状态、走 GCS 注册、提交队列顺序语义、并发组/async actor。
