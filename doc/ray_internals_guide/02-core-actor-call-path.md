# 第 2 篇 · Core Actor 调用链路（专家级）

> Actor = **有状态**的 `@ray.remote class`。它和普通 task 共用很多基础设施（CoreWorker、对象store、引用计数），但在三件事上根本不同：**注册走 GCS、方法调用直连不经调度器、有顺序/并发语义**。读完你应当能改 actor 相关源码。
> 先读 [第 0 篇](./00-mental-model.md)（线程模型、所有权）与 [第 1 篇](./01-core-task-execution-path.md)（task 链路、lease）。本篇大量做对比。

文件：`python/ray/actor.py`、`src/ray/core_worker/transport/actor_task_submitter.cc`、`src/ray/core_worker/transport/*actor_scheduling_queue*`、`src/ray/gcs/gcs_server/gcs_actor_{manager,scheduler}.cc`。

---

## Actor vs 普通 Task：一张对照表先建立直觉

| 维度 | 普通 Task | Actor |
|------|-----------|-------|
| 状态 | 无状态 | 有状态（worker 进程终身持有实例） |
| 注册 | 不注册 | **创建时注册到 GCS**（actor 表） |
| 调度 | 申请 worker 租约（经 Raylet 调度器） | **creation task 由 GCS 调度**；**方法调用直连 actor 进程，不经调度器** |
| worker 复用 | 同 SchedulingKey 复用 | 一个 worker 绑定一个 actor 实例，终身 |
| 调用顺序 | 无序 | **默认按提交序**（同 caller→同 actor） |
| 并发 | 一个 worker 一次一个 task | `max_concurrency`：threaded / async 两种模型 |
| 生命周期 | task 结束即释放 | 直到所有 handle 释放 / `ray.kill` / owner 死 |
| 容错 | `max_retries` 重算 | `max_restarts` 重启 + `max_task_retries` 重发 |

---

## 全景图：创建走 GCS，调用走直连

```
 ① 创建 (走 GCS, 像一次特殊的调度)
 ┌────────┐  RegisterActor   ┌──────────┐  调度(向某raylet租worker)  ┌──────────┐
 │Creator │ ═══════════════► │   GCS    │ ═════════════════════════► │ Raylet   │
 │CoreWkr │  CreateActor     │ Actor    │                            │ 起 worker │
 └────────┘                  │ Manager  │ ◄═══ worker 起好, 跑 __init__│ 执行      │
      ▲                      │ Scheduler│      报告 ALIVE             └────┬─────┘
      │ actor ALIVE 通知       └──────────┘                                │
      │ (pub/sub) + 地址            ▲                                 ┌────▼──────┐
      └─────────────────────────────┘                                 │Actor 进程 │
                                                                       │(终身持有  │
 ② 方法调用 (直连, 不经 GCS/调度器)                                       │ 实例)     │
 ┌────────┐   PushTask (带 sequence_no)        ┌──────────────────────┤           │
 │Caller  │ ═════════════════════════════════► │ ActorSchedulingQueue  │           │
 │CoreWkr │   actor_task_submitter             │ 按 seq_no 顺序/并发执行 │           │
 └────────┘   地址来自 actor_manager 缓存        └──────────────────────┴───────────┘
```

---

## A. Actor 创建与注册

### A.1 Python → CoreWorker → GCS

```
python/ray/actor.py: ActorClass._remote()
   校验 name/namespace/lifetime(detached?) → core_worker.create_actor()
   返回 ActorHandle (持 actor_id, owner_address, max_restarts, 各 method 的 ActorMethod 包装)
   ActorHandle.__del__ → core_worker.remove_actor_handle_reference()   ← 引用计数钩子

actor_task_submitter.cc: SubmitActorCreationTask()
   ResolveDependencies(creation task 的依赖, 通常空)
   actor_creator_.AsyncCreateActor() → gcs_client.Actors().AsyncCreateActor(spec, cb)  (异步 RPC)
   完成 → task_finisher_.CompletePendingTask()
```

> 🔑 **actor creation task 是一个"特殊 task"**：它有 TaskSpec、有返回（actor handle），但执行它的 worker **终身绑定**成这个 actor 实例，不归还租约。这是"有状态"的物理实现。

### A.2 GCS 侧：注册、状态机、调度

```
gcs_actor_manager.cc:
   RegisterActor: 由 TaskSpec 构造 GcsActor, 初始状态 DEPENDENCIES_UNREADY
   依赖就绪 → PENDING_CREATION → 交给 GcsActorScheduler

gcs_actor_scheduler.cc: Schedule()
   向 cluster_task_manager 提交 creation task → 选 raylet → RequestWorkerLease
   worker 起好、__init__ 跑完、报告 → 状态 ALIVE, 写回 actor 地址
   通过 pub/sub 把 ALIVE + 地址广播给所有持 handle 的 caller
```

Actor 状态机：

```
 DEPENDENCIES_UNREADY ──依赖就绪──► PENDING_CREATION ──worker起好/__init__完──► ALIVE
                                                                              │
                            ┌── max_restarts 未超且可恢复 ──► RESTARTING ◄────┤(死亡)
                            │                                    │            │
                            └────────────── ALIVE ◄──────────────┘            ▼
                                                                            DEAD
```

> ⚠️ **易忽略①｜Actor 注册/调度走 GCS，是因为它需要"跨进程稳定的身份"**：普通对象去中心化（owner 记账），但 actor 的位置、状态、重启需要一个所有 caller 都能查询/订阅的权威——这正是 GCS 的职责。对比第 0 篇：**普通对象不经 GCS，actor 经 GCS**，根因就是"身份是否需要跨进程稳定存在"。

> ⚠️ **易忽略②｜两种 actor 调度模式**：默认 actor creation 由 GCS 调度（GcsActorScheduler 直接向 raylet 租 worker，**creation task 不进 raylet 的普通调度队列**）。`gcs_actor_scheduling_enabled` 等 config 控制细节。改调度别把 actor 当普通 task 找。

### A.3 owner / creator / GCS 三者的角色

- **creator**：调 `Actor.remote()` 的进程。
- **owner**：记录在 `actor_table_data.owner_address`，**创建后冻结不变**（即使 handle 被序列化转发多次）。非 detached 时 owner = creator。
- **GCS**：actor 的注册表、状态权威、重启决策者。

> ⚠️ **易忽略③｜detached actor 的生命周期与 owner 解绑**：`lifetime="detached"` 的 actor **不随 creator 死亡而死**，可被 `ray.get_actor(name, namespace)` 重新获取。但 `owner_address` 仍记着 creator（用于 traceback/记账），别误以为 detached 就和 owner 完全无关。非 detached actor 则：**owner 进程死 → GCS 监测到 → 杀 actor**（`GenOwnerDiedCause`）。

> ⚠️ **易忽略④｜`ray.get_actor()` 返回 weak ref handle**：它**不参与分布式引用计数**。所以你拿着一个 weak handle，actor 仍可能因为别的（强）持有者全部释放而被杀，调用时突然 `ActorDiedError`。强弱 handle 的区别是 actor 生命周期 bug 高发区。

> ⚠️ **易忽略⑤｜actor 真正被杀的时机晚于 `del handle`**：handle 释放靠 `ObjectID::ForActorHandle` 的 out-of-scope 回调通知 GCS，可能滞后几个 GC 周期。写测试时 `del a; gc.collect()` 后立刻断言 actor 已死会 flaky。

---

## B. Actor 方法调用链路

### B.1 Direct actor call：为什么不经 Raylet

```
python/ray/actor.py: ActorMethod._remote() → ActorHandle._actor_method_call()
   构造 TaskSpec(actor_id, method, sequence_no) → core_worker.submit_actor_task()

actor_task_submitter.cc: SubmitTask()
   取/建该 actor 的 ClientQueue
   send_pos = task_spec.ActorCounter()              ← caller 维护的递增序号
   queue.actor_submit_queue->Emplace(send_pos, spec)
   ResolveDependencies(异步) → MarkDependencyResolved → SendPendingTasks()
   SendPendingTasks: 按序 pop 就绪 task → 直接 PushTask RPC 给 actor worker
```

> 🔑 **方法调用直连 actor 进程，不经 Raylet 调度器**。因为目标 worker 已知（actor 绑定在固定 worker 上），调度器无事可做；直连避免它在高频调用下成为瓶颈。这与第 1 篇"普通 task 要申请租约"形成鲜明对比。

> ⚠️ **易忽略⑥｜地址解析靠订阅 GCS actor 状态**：actor 地址缓存在 `ClientQueue.address`，来源是 `actor_manager.cc::SubscribeActorState` 订阅的 GCS pub/sub。actor 重启换了 worker，caller 通过该订阅更新地址（`ConnectActor`/`DisconnectActor`）。

> ⚠️ **易忽略⑦｜actor 没 ALIVE 时调用会排队而非报错**：只要状态非 DEAD（含 DEPENDENCIES_UNREADY/PENDING_CREATION），`SubmitTask` 就接受并入 submit queue，等 `ConnectActor`（GCS 通知 ALIVE 带地址）后才真正发送。这就是"actor 还没起好你就能 `a.foo.remote()`"的实现。

### B.2 顺序保证与序列号去重 ★

每个 **caller→actor** 维护一个单调递增的 `sequence_no`（来自 `ActorCounter`）。

```
sequential_actor_submit_queue.h:
   requests: map<seqno, (TaskSpec, sent?)>
   next_send_position       // 下一个该发的 seqno
   next_task_reply_position // 下一个该收到 reply 的 seqno
   caller_starts_at         // actor 重启后的 base offset

PopNextTaskToSend(): 只弹出 seqno == next_send_position 的 task
   ⇒ 即便后面的 task 依赖先就绪, 也必须等前面的先发出 ⇒ 保证 FIFO
```

去重协议：

```
 网络断 → caller 重连 actor (next_send_position 不变) → 重发同 seqno 的 task
 actor 端 (ActorSchedulingQueue): 记录已执行的 seqno
   收到已执行过的 seqno → 返回缓存结果, 不重复执行   ← 幂等
```

> ⚠️ **易忽略⑧｜"直连不经调度器"≠"不需要顺序号"**：很多人以为直连 RPC 就天然有序。其实 FIFO 完全靠 **caller 端 submit queue 的 seqno 排序** + **actor 端按 seqno 执行/去重**。seqno 同时承担三职：①保证同一 caller 的调用按提交序执行；②网络重传去重；③乱序检测。

> ⚠️ **易忽略⑨｜actor 重启后的 seqno offset 重置**：重启后新实例从头计数，caller 把 `caller_starts_at` 设为 `next_task_reply_position`，未确认的 task 以"减去 offset 后的 seqno"重放给新实例。若 `next_task_reply_position` 跟踪出错，重放会错乱。这是重启恢复路径最微妙处。

### B.3 顺序队列 vs 乱序队列

- `max_concurrency == 1`（默认）：caller 用 `SequentialActorSubmitQueue`，actor 端 `ActorSchedulingQueue` 严格按 `next_seq_no_` 执行，缺号则缓存等待。
- `max_concurrency > 1`：caller 用 `OutOfOrderActorSubmitQueue`（就绪即发，不等前序），actor 端 `OutOfOrderActorSchedulingQueue` 并发执行。

> ⚠️ **易忽略⑩｜缺号会卡住后续全部调用**：顺序模式下 actor 端只执行 `seqno == next_seq_no_` 的，若某个 seqno 永远没到（caller 崩溃/逻辑 bug），后续 task 全卡。有 `kMaxReorderWaitSeconds`(≈30s) 的重排等待上限兜底超时。调"actor 调用莫名卡住"先查 seqno 连续性。

---

## C. Actor 端执行与并发模型

### C.1 两种并发：threaded（线程池）vs async（fiber/asyncio）

```
concurrency_group_manager.cc / thread_pool.cc / fiber.h

threaded actor (默认):
   BoundedExecutor (boost thread pool, max_concurrency 个线程)
   方法 Post 到线程池并发执行

async actor (定义了 async def 方法):
   FiberState: boost fiber + Python asyncio event loop
   FiberRateLimiter(max_concurrency) 信号量限制并发 fiber 数
   默认 max_concurrency 大得多(异步场景 ~1000)
```

### C.2 并发组(concurrency groups)

```
@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class A:
   @ray.method(concurrency_group="io")
   def fetch(self): ...

ConcurrencyGroupManager<ExecutorType>:
   name_to_executor_index_: group 名 → 独立 executor(线程池/fiber 组)
   default_executor_: 未指定 group 的方法用
   GetExecutor(group, fd): 按方法的 group 路由到对应 executor
```

把方法隔离到不同线程池/fiber 组，避免相互阻塞（例如把慢 IO 方法和快计算方法分开）。

> ⚠️ **专家级坑⑪｜async actor 里一个 fiber 阻塞会拖垮整个 actor**：FiberState 共享一个 event loop。若某个 `async def` 方法里写了**同步阻塞**（`time.sleep` 而非 `await asyncio.sleep`、同步重计算、同步 IO），event loop 被占住，其他所有 coroutine 都无法调度。async actor "假死"几乎都是这个原因。

> ⚠️ **专家级坑⑫｜并发组跨调用死锁**：group G1 的方法 A 调用别的 actor 的方法 B，B 又回调当前 actor 的 group G1 方法 C。若 G1 的线程/并发额度已被 A 占满，C 永远拿不到执行槽 → 死锁。并发组制造了"线程池边界"，跨组/跨 actor 的同步等待要极其小心。

> ⚠️ **专家级坑⑬｜`max_concurrency>1` 与顺序语义互斥**：一旦开并发，actor 端按依赖就绪乱序执行，"调用顺序=执行顺序"的假设不再成立。依赖顺序的有状态逻辑（如累加器）在并发 actor 上会出错。

### C.3 死亡与重启

```
可重启? = num_restarts < max_restarts(-1=无限) 且 死因可恢复(非 owner 死, 除非 detached)
重启时:
   caller 收 GCS 死亡通知 → DisconnectActor → 保留 next_task_reply_position
   新实例起好 → ConnectActor → 重放未确认 task
   已发未回的 task: 看 max_task_retries (<=0 立即失败; >0 重发新 seqno; -1 无限)
```

> ⚠️ **易忽略⑭｜`max_restarts`(进程级) 与 `max_task_retries`(调用级) 是两回事**：前者控制 actor 进程重启几次，后者控制单个方法调用在 actor 重启后重发几次。重启后**所有内存状态丢失**（新实例从 `__init__` 重来），重发的方法在新状态上执行——若方法假设了之前累积的状态，结果会不一致。

> ⚠️ **易忽略⑮｜RPC 重试与 task 重试分层**：网络层 RPC 失败重试由 gcs/rpc client 负责；应用层方法重试由 `max_task_retries` 控制。设了很大的 `max_task_retries` 不代表网络抖动一定被自动救回——RPC 可能在 task 真正提交前就失败。

---

## 不变量清单

1. **actor_id 终身不变**：重启后仍是同一 actor_id（新实例、新状态）。
2. **owner_address 创建后冻结**，序列化转发不改变。
3. **seqno per (caller, actor) 单调递增**，retry 用新 seqno（除重连重发用原 seqno）。
4. 顺序模式下 actor 端只执行 `seqno == next_seq_no_`，缺号则缓存。
5. 一个 worker 进程终身绑定一个 actor 实例（不归还租约）。
6. 方法调用走直连 RPC，**仅创建/重启/状态变更**才碰 GCS。
7. detached actor 独立于 owner 生命周期；非 detached 随 owner 死。

---

## config 速查

| config | 含义 |
|--------|------|
| `max_restarts`（per actor） | actor 进程最大重启次数（-1 无限） |
| `max_task_retries`（per call/actor） | actor 重启后方法重发次数 |
| `max_concurrency`（per actor） | 并发度；threaded 默认 1，async 默认大值 |
| `max_pending_calls`（per handle） | 单 handle 允许的在途调用上限，超出抛 `PendingCallsLimitExceeded` |
| `concurrency_groups`（per actor） | 方法到独立 executor 的映射 |
| `gcs_actor_scheduling_enabled` | actor 调度走 GCS vs raylet |
| `actor_excess_queueing_warn_threshold`(~5000) | pending 调用过多告警 |
| `kMaxReorderWaitSeconds`(~30) | actor 端等待缺失 seqno 的上限 |

---

## 本篇三条主线

1. **创建走 GCS、调用走直连**：actor 需要跨进程稳定身份 → 注册/调度/重启由 GCS 管；但方法调用直连 actor 进程不经调度器，靠 `actor_manager` 订阅的地址。
2. **顺序与去重全靠 seqno**：直连不等于天然有序；caller submit queue + actor scheduling queue 用单调 seqno 保证 FIFO、去重、乱序检测；重启靠 offset 重放。
3. **两种并发模型 + 并发组**：threaded(线程池) / async(fiber+asyncio)；并发组隔离线程池但带来跨调用死锁风险；`max_concurrency>1` 放弃顺序语义。

→ 下一篇：[第 3 篇 · 模块 GCS](./03-module-gcs.md)
