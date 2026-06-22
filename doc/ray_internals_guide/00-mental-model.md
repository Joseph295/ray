# 第 0 篇 · Ray 内核心智模型（专家级）

> 读懂后面所有链路的前提。如果只读一篇，读这篇。读完你应当能像通读过源码的 maintainer 一样，在脑子里跑通"一个对象从生到死、谁在记账、并发下谁拿锁、进程死了会怎样"。

本篇装四样东西：

1. **三种进程角色** —— 谁执行、谁调度、谁管全局元数据。
2. **每个 Ray 进程的线程模型** —— 几个 io_context、哪个线程跑什么、和 Python GIL 的边界。**这是后面所有"为什么要 post 到事件循环""回调跑在哪个线程"的答案。**
3. **对象store的两层结构 + 主副本(primary copy) / spilling / 恢复**。
4. **所有权(ownership)模型与分布式引用计数** —— Ray 的核心设计，含借用协议状态机、删除协议、血缘重建。新人最容易当黑盒，专家最常改。

---

## 1. 三种进程角色

一个 Ray 集群里跑着三类进程：

```
                    ┌──────────────────────────────────────────────┐
                    │                     GCS                        │
                    │            (Global Control Store)              │
                    │   全局唯一(可 HA)。控制平面元数据，不是数据。   │
                    │   节点表/资源视图/Actor 注册表/Job/PG/KV/pubsub │
                    │   进程: gcs_server   源码: src/ray/gcs/        │
                    └───────▲───────────────────────▲────────────────┘
                            ║ 心跳/注册/订阅          ║
              ┌─────────────╨──────────┐  ┌──────────╨─────────────┐
              │      Node A             │  │      Node B            │
              │ ┌────────────────────┐  │  │ ┌────────────────────┐ │
              │ │  Raylet (1个/节点)  │◄─┼──┼─►  Raylet (1个/节点)  │ │
              │ │ 本地调度+worker池+   │  │  │ │ 同左                │ │
              │ │ 对象依赖拉取+spill   │  │  │ │                    │ │
              │ │ src/ray/raylet/    │  │  │ │                    │ │
              │ └─────▲──────▲───────┘  │  │ └────────▲───────────┘ │
              │  ┌────╨──┐ ┌─╨─────┐    │  │     ┌────╨────┐        │
              │  │Driver │ │Worker │... │  │     │ Worker  │ ...    │
              │  │┌Core─┐│ │┌Core─┐│    │  │     │┌Core───┐│        │
              │  ││Wkr  ││ ││Wkr  ││    │  │     ││Wkr    ││        │
              │  │└─────┘│ │└─────┘│    │  │     │└───────┘│        │
              │  └───────┘ └───────┘    │  │     └─────────┘        │
              │  ┌──────────────────┐   │  │  ┌──────────────────┐  │
              │  │ Plasma 共享内存   │   │  │  │ Plasma 共享内存   │  │
              │  │ 对象store(每节点) │   │  │  │ 对象store(每节点) │  │
              │  └──────────────────┘   │  │  └──────────────────┘  │
              └─────────────────────────┘  └────────────────────────┘
```

- **Driver / Worker**：跑用户代码的进程，**都内嵌一个 `CoreWorker`**（C++，`src/ray/core_worker/`）。Driver 是入口进程，Worker 是 Ray 拉起来执行 task/actor 的进程；在内核眼里二者规则一致——都能拥有对象、都能被借用、都能提交任务。
- **Raylet**：每节点恰好一个（`src/ray/raylet/`）。管本地调度+worker 池（`worker_pool.cc`、`local_task_manager.cc`、`scheduling/`）、对象依赖拉取（`dependency_manager.cc`+object manager）、本地 Plasma 的 pin/spill/evict（`local_object_manager.cc`）。
- **GCS**：全局一个（`src/ray/gcs/gcs_server/`，可 HA）。**控制平面**：节点表、集群资源视图、Actor 注册表与调度、Job、Placement Group、内部 KV、pub/sub。

> ⚠️ **易忽略｜GCS 是控制平面，不是数据平面，也不是普通对象的引用计数中心**：普通 task 的提交、普通对象的数据与生命周期**根本不经过 GCS**。把 GCS 当成"什么都问它"的中心，是新人头号错误心智模型。真正管对象生命周期的是 §3 的去中心化所有权。GCS 只在 Actor、PG、节点成员、对象**位置目录**(object directory) 等控制元数据上是权威。

---

## 2. 每个 Ray 进程的线程模型 ★ 后面一切并发问题的地基

`CoreWorker` 不是单线程。理解它的线程划分，才能解释后面"为什么提交要异步 post""回调在哪个线程跑""为什么不能在回调里阻塞"。

```
 ┌──────────────────────── 一个 Worker/Driver 进程 ────────────────────────┐
 │                                                                          │
 │  ① io_service_  (1 个后台线程跑 io_service_.run())                        │
 │     职责: 所有 gRPC 客户端回调、RPC server handler、定时任务、            │
 │           任务提交流水线(SubmitTask→依赖解析→OnWorkerIdle→租约回调)       │
 │     => “控制平面线程”。几乎所有内核逻辑都在这条线程上串行执行。            │
 │                                                                          │
 │  ② task_execution_service_  (RunTaskExecutionLoop 单独线程)               │
 │     职责: 真正执行用户 task / actor 方法 (调 Python)                       │
 │     => “数据平面线程”。执行用户函数、拿 GIL 都在这里。                     │
 │                                                                          │
 │  ③ 调用方线程 (Python 主线程等)                                           │
 │     f.remote() 在这里被调用，持有 GIL；调进 C++ 后 `with nogil` 释放 GIL  │
 │                                                                          │
 │  ReferenceCounter: 一把 absl::Mutex mutex_ 保护全部引用计数状态           │
 └──────────────────────────────────────────────────────────────────────────┘
```

几条要记死的规则：

- **提交流水线全在 `io_service_` 线程串行跑**。`CoreWorker::SubmitTask` 在调用方线程里只做"生成返回值 ObjectRef"这点同步工作，然后 `io_service_.post(...)` 把真正的提交（依赖解析、租约、push）丢到 `io_service_` 线程异步执行。
  > ⚠️ **专家级坑｜不要在 `io_service_` 回调里做任何阻塞操作**。依赖解析回调、租约回调、PushTask 回调全在这条线程。一旦某个回调里同步等待（比如长时间抢 GIL、同步 RPC），**整个进程的 RPC 回复处理都会卡住**，外部表现是"driver 提交卡死 / worker 心跳超时"。这是 Ray 内核改代码最常见的事故源。
- **执行用户函数在 `task_execution_service_` 线程**，通过 `options_.task_execution_callback()` 这个 C++→Python 桥进入；GIL 在这里被获取。控制平面线程(`io_service_`)与执行线程分离，是为了"正在跑一个重 task 时，仍能响应 RPC（比如 cancel、ref removed、心跳）"。
- **`ReferenceCounter` 用单把 `mutex_` 保护所有状态**（`reference_count.{h,cc}`）。50+ 个方法都在锁内。但有一个关键例外见 §3.4：**borrower 释放的 pub/sub 回调刻意在锁外执行**，否则会和持锁路径死锁。

---

## 3. 对象store的两层结构 + 主副本与 spilling

函数返回值、`ray.put` 的值统称**对象**，用 `ObjectRef` 引用，按大小自动选层：

```
  小对象 (序列化 ≤ max_direct_call_object_size, 默认 100KB)
    ─────────────────────────►  [M] In-Memory Store
                                 进程内 C++ 内存 (core_worker/store_provider/memory_store/)
                                 随 RPC 内联回传, 不进 Plasma

  大对象 (> 阈值)
    ─────────────────────────►  [P] Plasma 共享内存
                                 每节点一份, 同机跨进程零拷贝
                                 由 Raylet 管 pin/spill/evict, 内存压力下 spill 到磁盘/外存
```

### 3.1 主副本(primary copy)与位置目录

- 一个对象可能在多个节点有副本，但 owner 的 `ReferenceCounter` 记录一个**主副本**所在 raylet：`Reference.pinned_at_raylet_id`。**只有 owner 能设置/维护这个字段**。
- 主副本由 owner 通过"pin"协议钉在某个 raylet（见 §3.5），保证它在还有人引用时不被驱逐。
- 对象的**位置**（哪些节点有副本）通过 GCS/Raylet 的 object directory 维护，并经 pub/sub（`WORKER_OBJECT_LOCATIONS_CHANNEL`）推给订阅者。`ray.get` 找对象就靠它。

### 3.2 spilling vs eviction vs reconstruction —— 专家必须分清的三件事

新人常把它们混为一谈。它们是三条不同的路径：

| | 谁触发 | 干什么 | 关键代码 |
|---|---|---|---|
| **Eviction(驱逐)** | Raylet 的 Plasma（内存满） | 把**未 pin** 的对象按 LRU 从共享内存丢掉 | `object_manager/plasma/eviction_policy.cc` |
| **Spilling(溢出)** | Raylet `local_object_manager` | 内存压力下把对象**搬到外存**(磁盘/S3)，内存里留位置信息 | `local_object_manager.cc` |
| **Reconstruction(重建)** | **Owner** 的 `ObjectRecoveryManager` | 对象彻底丢了，**重新执行产生它的 task** 算出新值 | `object_recovery_manager.cc` |

- spilling 后对象**还能 restore 回来**（值没丢，只是换了介质）；reconstruction 是**值没了、靠血缘重算**。
- pinned 的对象不会被 evict。被 task 当输入正在用的对象会 `BeginObjectAccess` 临时 pin，用完 `EndObjectAccess` 放回 LRU。

---

## 4. 所有权模型与分布式引用计数 ★ 核心基石

**如果你只深入理解一个机制，选它。** 文件：`src/ray/core_worker/reference_count.{h,cc}`。

### 4.1 问题与 Ray 的答案

分布式下一个对象被多进程、多节点引用，**何时能安全删除？谁记账？**

- 中心化方案：全局服务记所有引用 → 简单但瓶颈+单点。
- **Ray：去中心化所有权**。**每个对象有唯一 owner 进程**，owner 用本地 `ReferenceCounter` 独立记账，**GCS 不参与普通对象引用计数**。

### 4.2 谁是 owner —— 反直觉的关键

> **owner = 创建这个 ObjectRef 的进程，不是计算/存储它的进程。**

```
   Driver: y = f.remote(x)
     │  提交那一刻 (task_manager.cc::AddPendingTask)：
     │  立即生成返回值 ObjectRef y，owner_address = 调用方 Driver 自己
     │  调 reference_counter_.AddOwnedObject(y, ..., caller_address, is_reconstructable=true)
     ▼
   ┌──────────┐   y 的 owner 永远是 Driver        ┌──────────────┐
   │  Driver  │ ◄────────────────────────────────│ Worker on B  │
   │ owner of │   即使 f 在 B 执行、结果 [P] 落 B，│ 执行 f, 结果   │
   │   y      │   y 的所有权/记账仍在 Driver       │ 落 B Plasma   │
   └──────────┘                                   └──────────────┘
```

- `task_manager.cc::AddPendingTask` 在**任务还没执行、还没发给 Raylet** 时就为返回值生成 `ObjectRef` 并把 owner 钉成 `caller_address`。Python 拿到的 `y` 此刻已存在，只是数据未就绪。
- `ray.put(obj)` 同理，owner 是执行 put 的进程；但 **`ray.put` 对象 `is_reconstructable=false`**（无血缘，无法重算），task 返回值 `is_reconstructable=true`。这个差别决定了 owner 死后/血缘驱逐后谁能恢复。

### 4.3 owner 记什么账：`Reference` 结构与三个引用计数

```
struct Reference {                          // reference_count.h，一个对象的全部记账
  bool owned_by_us;                         // 我是不是 owner
  optional<rpc::Address> owner_address;     // borrower 才填 owner 地址; owner 自己留空
  size_t local_ref_count;                   // ① 本进程内的 Python/C++ 引用数
  size_t submitted_task_ref_count;          // ② 已提交、待执行、把它当参数的 task 数
  size_t lineage_ref_count;                 // ③ 把它当血缘依赖、可能重算的 task 数
  NestedReferenceCount nested;              // 嵌套: contained_in_owned / _borrowed_ids / contains
  BorrowInfo borrow;                        // borrowers 集合 + stored_in_objects
  flat_hash_set<NodeID> locations;          // owner 追踪: 副本在哪些节点
  optional<NodeID> pinned_at_raylet_id;     // 主副本 pin 在哪个 raylet
  bool spilled; string spilled_url; NodeID spilled_node_id;  // spill 信息
  bool is_reconstructable; bool lineage_evicted;             // 可重建? 血缘已驱逐?
};
```

**为什么是三个计数而不是一个？** 它们对应**三种不同的生命周期约束**，无法合并：

| 计数 | 语义 | ++ 时机 | -- 时机 |
|------|------|---------|---------|
| `local_ref_count` | **应用级**：Python 变量作用域 | `AddLocalReference` / `AddOwnedObject(add_local_ref=true)` | `RemoveLocalReference` |
| `submitted_task_ref_count` | **执行级**：还有 task 等它当输入 | `UpdateSubmittedTaskReferences`（提交带它的 task） | `RemoveSubmittedTaskReferences`（task 完成或参数被内联） |
| `lineage_ref_count` | **重建级**：还有可重试 task 把它当血缘依赖 | 同提交时一起 ++ | task 最终不可重试 / 血缘被驱逐 时 -- |

对象"还在用吗"由组合判断：

```
RefCount() = local_ref_count + submitted_task_ref_count + nested.contained_in_owned.size()

OutOfScope()  ⇔  RefCount()==0  且 无 borrower  且 无 stored_in_objects 嵌套牵连
ShouldDelete()⇔  OutOfScope()  且 (开启血缘pin时: lineage_ref_count==0)
```

> ⚠️ **专家级坑①｜`UpdateFinishedTaskReferences` 里顺序不能反**：task 完成时必须**先 `MergeRemoteBorrowers()` 合并 worker 回传的新借用者，再 `RemoveSubmittedTaskReferences()` 减计数**。反了会出现"计数先归零→对象被删→再来合并借用者"的悬空，代码里有显式注释警告这一点。改这块务必保持顺序。

> ⚠️ **专家级坑②｜`nested()`/`borrow()` 惰性初始化**：这两个子结构是按需 new 的指针，访问 `nested().contained_in_owned` 这种 getter 可能**顺手分配一个空结构**。判空要先查底层指针非空，否则在热路径上制造无谓分配。

### 4.4 嵌套对象：`ray.put([inner_ref])` 的记账与删除顺序

当一个对象**包含**另一个 ObjectRef：

```
 outer = ray.put([inner_ref])   (假设 inner 也是我拥有的)
   owner 侧 AddNestedObjectIdsInternal(outer, [inner], 我自己):
     outer.nested.contains      += inner
     inner.nested.contained_in_owned += outer
   ⇒ inner 的 RefCount() 现在含 contained_in_owned.size()，即【outer 撑着 inner 不被删】

 删除顺序不变量:
   只有 outer.RefCount()==0 时, DeleteReferenceInternal(outer) 才会
   递归把 inner.contained_in_owned 里的 outer 抹掉, 进而可能删 inner。
   ⇒ inner 永远不会早于 outer 出 scope。
```

**跨进程嵌套**（对象里含 ObjectRef，被传到别的进程）走 borrowed 路径：`inner.borrow.stored_in_objects[outer] = 远端 owner 地址`，并对 inner 启动 `WaitForRefRemoved(..., contained_in_id=outer)`，远端释放时回报"是因为 outer 被删了"。

> ⚠️ **专家级坑③｜嵌套对象的 owner 死了会 cascade 卡住**：内层 ObjectRef 的 owner 进程若崩溃，自动级联删除无法完成，可能造成对象 GC 死锁。嵌套 + 跨进程所有权是 refcount bug 的高发区。

### 4.5 借用(borrowing)协议状态机 ★

把一个 `ObjectRef` 当参数传给别的 task，执行那个 task 的进程就成了 **borrower**。owner 必须知道"还有谁在借"才敢删。完整状态机：

```
 A 拥有 obj。A 传给 task B，B 又传给 task C
 ┌─────┐                ┌─────┐                ┌─────┐
 │  A  │  obj 作参数 ──► │  B  │  obj 作参数 ──► │  C  │
 │owner│                │借用者│                │借用者│
 └──▲──┘                └──┬──┘                └─────┘
    │                      │
 (1) B 收到 obj 作参数: AddBorrowedObject(obj, owner=A)  [owned_by_us=false]
 (2) B 把 obj 传给 C 提交: submitted_task_ref_count++
 (3) B 执行完: GetAndClearLocalBorrowersInternal() 序列化自己的借用表
       (含 “我把 obj 转给了 C” 的 stored_in_objects 信息)
       随 task 结果回传给 A
    │
 (4) A 收: MergeRemoteBorrowers(obj, from=B, table)
       发现 C 也在借 → borrowers.insert(C)
       对 C 启动 WaitForRefRemoved(obj, C)   ← 直接监听 C, 不再经 B
 (5) A 通过 pub/sub 订阅 WORKER_REF_REMOVED_CHANNEL, key=obj, 目标=C
 (6) C 的 local_ref 归零: 通过同一 channel publish, 把它自己的(可能更深的)借用表回传
 (7) A 收到 publish → CleanupBorrowersOnRefRemoved → borrowers.erase(C)
       borrowers 空且 RefCount()==0 ⇒ 删除 obj
```

关键点：

- **级联借用直接拍平**：B 转借给 C 后，A 学会后**直接监听 C**，不需要 B 当中继。这避免了借用链上任何中间进程死亡导致整条链失联。
- **pub/sub 用的是 core_worker 自带的 publisher/subscriber**，channel = `WORKER_REF_REMOVED_CHANNEL`，消息体 `WorkerRefRemovedMessage` 携带新借用者表。
- **回调在 `mutex_` 锁外执行**：`WaitForRefRemoved` 的 `message_published_callback` / `publisher_failed_callback` 不在 refcount 锁内；`CleanupBorrowersOnRefRemoved` 自己重新抢锁。这是为了避免"持锁 → 触发 publish → 又要抢锁"的重入死锁。

> ⚠️ **专家级坑④｜`PopAndClearLocalBorrowers` 的 `deduct_local_ref`**：task 执行期间，传入的借用对象被额外 pin 了一个 local_ref（防止执行中被 GC）。回报借用表时必须 `deduct_local_ref=true`，用 `RefCount() > 1` 而非 `> 0` 判断"是否还在借"。漏掉就会把"其实已经不借了"误报成"还在借"，对象永不回收。

> ⚠️ **专家级坑⑤｜borrower 信息丢失=内存泄漏**：整套协议依赖消息可达。若 A↔B 回传借用表时网络中断，A 可能永远不知道 C 在借 → obj 无法回收。Ray 靠进程存活检测兜底（`publisher_failed_callback` 在对端死亡时触发，按"无新借用者"清理）。

### 4.6 owner 删除对象 + pin / WaitForObjectEviction 协议

owner 这边 `RefCount()` 归零并通过 `ShouldDelete()` 后：

```
 DeleteReferenceInternal:
   - 还有 borrower? → 不删, 只标记 out of scope
   - 还有 contains 的内层? → 递归解开 contained_in_owned 再试删内层
   - ShouldDelete() (含 lineage_ref_count==0)? →
        EraseReference():
          object_info_publisher_->PublishFailure(OBJECT_LOCATIONS_CHANNEL, obj)
          从 object_id_refs_ 抹除, 触发 on_object_ref_delete 回调
          ⇒ 通知持有主副本的 raylet 真正释放 Plasma 内存
```

- **谁 pin 主副本**：owner 把对象的主副本"钉"在某个 raylet（通过 raylet 的 `PinObjectsAndWaitForFree` / owner 端 `UpdateObjectPinnedAtRaylet` 记 `pinned_at_raylet_id`）。raylet 持有 pin 期间不驱逐该对象，并在对象需要释放时通过等待机制(WaitForObjectEviction/Free 语义)回调 owner。
- **节点失败时 unpin**：`ResetObjectsOnRemovedNode` 发现某对象的 `pinned_at_raylet_id` 或 `spilled_node_id` 落在挂掉的节点 → `UnsetObjectPrimaryCopy` 清 pin，若对象还在 scope 就推进 `objects_to_recover_` 走重建。

> ⚠️ **专家级坑⑥｜`UnsetObjectPrimaryCopy` 不清 `locations`**：它只清 pin 与 spill 信息，**旧的 location 仍留在 `locations` 集合里**。于是 `GetLocalityData` 可能仍返回已死节点，导致后续调度/取回被路由到死节点，再靠失败重试纠正。调试"为什么老往死节点调度"时想到这里。

### 4.7 血缘重建(lineage reconstruction)

```
 对象丢失(主副本所在节点挂了, 又没 spill 副本):
   ObjectRecoveryManager::RecoverObject(obj):
     1. object_lookup_() 问 GCS/directory 还有没有别处副本
     2. 有副本 → PinExistingObjectCopy() 重新 pin, 更新 pinned_at_raylet_id
     3. 无副本 → 检查 is_reconstructable && !lineage_evicted ?
          可重建 → task_resubmitter_.ResubmitTask(产生它的 task)
                   先递归 RecoverObject(该 task 的每个依赖)
          不可重建 → 标记失败, ray.get 抛 ObjectLostError
```

- `lineage_ref_count` 的存在就是为了"任务完成后仍把它的 spec 钉住，万一要重算"。**内存压力下** `EvictLineage()` 按 FIFO 驱逐**可重建对象**的血缘（`reconstructable_owned_objects_` 队列），驱逐后置 `lineage_evicted=true`，该对象从此**不可重建**，再丢就是 `OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED`。
- 相关 config：`max_lineage_bytes`（血缘内存预算）。

### 4.8 owner 死亡 —— 去中心化的代价

> **owner 进程一死，它拥有的所有对象的元数据(位置、引用集合、blineage)随之消失 → 这些对象立即不可恢复 → 等它们的 task 抛 `ObjectLostError`。**

- 普通对象**无 GCS 备份**。task 返回值若血缘还在、依赖还在，理论上能重建，但**重建也是 owner 驱动的**——owner 没了就没人驱动。
- `ray.put` 对象 `is_reconstructable=false`，owner（常是 Driver）一死彻底丢。⚠️ 新人常以为 `ray.put(data)` 像写进可靠存储；其实 Driver 崩了所有 put 对象全没。
- 这正是 **Actor 用完全不同机制**的原因：Actor 由 **GCS** 管注册表与重建（见第 2 篇），因为 Actor 的生命周期需要跨进程稳定存在。

### 4.9 `ray.put(value, _owner=other)` 的所有权指派

正常 `ray.put` owner 是调用者。`_owner=` 可把所有权直接指派给另一个进程：

- 本地临时创建者用 `AddBorrowedObject(..., foreign_owner_already_monitoring=true)` 标记，立即把所有权交给目标进程（它 `AddOwnedObject` 接管）。
- `foreign_owner_already_monitoring=true` 的作用：告诉本地"目标 owner 已经在监听这个对象了"，所以 `GetAndClearLocalBorrowersInternal` **不要再把它当新借用者回报**，避免双重监听。
- ⚠️ 但若该对象同时又作为 task 参数传递，可能仍收到两份监听（来自父 task + 来自外部 owner），代码里有 TODO 标注这块仍有 race 余地。这是 `_owner` 参数高级用法的暗坑。

### 4.10 关键不变量清单（背下来）

1. `RefCount() = local + submitted + contained_in_owned.size()`。
2. `borrowers` 非空 ⇒ 对象不可删。
3. `contained_in_owned` 中的内层对象，只能在外层删除后才删 ⇒ 内层不早于外层出 scope。
4. `pinned_at_raylet_id` 只能由 owner 设置/维护。
5. 血缘被驱逐(`lineage_evicted=true`)后，即便原 `is_reconstructable=true` 也变不可重建。
6. `WaitForRefRemoved` 的 pub/sub 回调一律在 `mutex_` 锁外执行。
7. `UpdateFinishedTaskReferences`：先 merge borrower，后减 submitted 计数。

---

## 5. 把三件事串起来：一次 `f.remote()` 的鸟瞰

下一篇逐函数展开，这里先给"每步在哪个进程、哪条线程"的鸟瞰：

```
 Driver 进程                         Raylet (本节点)         Worker 进程 (可能异节点)
 ───────────                         ─────────────           ─────────────────────
 [调用方线程] f.remote(x)
   │ ① 同步: 生成返回值 ObjectRef y (owner=Driver)
   │   io_service_.post → 转入 io_service_ 线程 ↓
 [io_service_线程]
   │ ② 依赖解析(等 x 就绪+内联)
   │ ③ 申请 worker 租约 ═══════════► HandleRequestWorkerLease
   │                                  集群调度→本地调度→取/启 worker
   │   ◄══════════════════════════════ reply: worker 地址 + 资源映射
   │ ④ PushNormalTask 点对点 ════════════════════════════► [io_service_线程] HandleTask
   │   (租约可复用, 不再经调度器)                            入队→取参数(pin)
   │                                                       [execution线程] ExecuteTask
   │                                                         → 调 Python 用户函数
   │                                          ⑤ 结果写 [M]小/[P]大
   │   ◄═══════════════════════════════════════════════════ CompletePendingTask
   │ ⑥ y 标记就绪, 唤醒 ray.get(y); merge borrower
   ▼
 ray.get(y) → 本地有直接拿; 无则问 owner 要位置再从 [P] 拉
```

注意 ③ 与 ④：③ 是问 Raylet **要一个 worker 的租约**，④ 是拿到租约后**直接点对点把任务推给那个 worker**；同类后续任务复用同一租约、**不再惊动调度器**——这是下一篇重头戏 lease-based scheduling。

---

## 本篇要带走的话

1. 三进程：**Worker/Driver**(带 CoreWorker，跑代码)、**Raylet**(每节点，本地调度+对象拉取+spill)、**GCS**(全局，控制元数据)。GCS 不碰普通对象生命周期。
2. 每进程**两个 io_context**：`io_service_`(控制平面，所有内核回调串行)、`task_execution_service_`(执行用户函数)。**别在 `io_service_` 回调里阻塞**。
3. 对象两层：小对象 `[M]`、大对象 `[P]`；分清 **evict / spill / reconstruct** 三条路。
4. **每个对象有唯一 owner = 创建该 ObjectRef 的进程**，用本地 `ReferenceCounter`(单锁) 去中心化记账；三个引用计数对应三种生命周期约束。
5. 借用协议靠**点对点 pub/sub** 追踪、级联拍平、锁外回调；**owner 死则对象 `ObjectLostError`**。

→ 继续：[第 1 篇 · Core 普通 Task 执行链路](./01-core-task-execution-path.md)
