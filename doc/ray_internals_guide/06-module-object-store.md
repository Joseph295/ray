# 第 6 篇 · 模块：对象store + 所有权（深入，专家级）

> 第 0 篇讲了**所有权层**（owner 记账、引用计数、借用）；本篇讲**物理层**——对象在共享内存里怎么放、怎么跨节点传、内存满了怎么 spill/evict，以及物理层如何与所有权层衔接。读完你应当能调内存 bug、设计自定义 spill 后端。
> 先读第 0 篇 §3/§4。

源码：`src/ray/object_manager/plasma/`（共享内存）、`src/ray/object_manager/`（跨节点）、`src/ray/raylet/local_object_manager.cc`（pin/spill）。

---

## 一、三层结构与对象全生命周期

```
 所有权层(每进程 CoreWorker):  ReferenceCounter —— 谁拥有、谁借用、副本在哪、主副本 pin 在哪
        │ (PinObjectsAndWaitForFree / 位置上报 / 删除通知)
        ▼
 节点管理层(每节点 Raylet):  LocalObjectManager(pin/spill/restore) + ObjectManager(跨节点传输)
        │
        ▼
 物理存储层(每节点):  Plasma 共享内存 store(mmap) + spill 到磁盘/S3
```

对象一生：

```
 创建(owner worker)         → PlasmaClient.Create → 写共享内存 → Seal(变可见, 不可变)
 上报位置                    → ObjectManager → OwnershipBasedObjectDirectory → 通知 owner
 owner pin 主副本            → raylet LocalObjectManager.PinObjectsAndWaitForFree(持 RayObject*)
 别的节点要 → Pull          → PullManager 分块从有副本节点拉到本地 Plasma
 内存压力 → evict / spill   → Plasma LRU 驱逐未 pin 的; 或 spill 主副本到磁盘/S3
 丢失 → restore / reconstruct→ 从 spill 还原; 或 owner 驱动血缘重建(第0篇§4.7)
 RefCount→0 → 删除           → owner 通知 raylet free → Plasma 释放 + 清 spill 文件
```

---

## 二、Plasma 共享内存 store

```
plasma/store.cc: PlasmaStore (单线程, absl::Mutex 保护全部状态, 跑在 raylet 一侧线程)
   UNIX domain socket 连接各进程; PlasmaClient(plasma/client.cc) 是客户端
对象状态机(plasma/common.h):
   PLASMA_CREATED(已分配可写, 对他人不可见) → Seal → PLASMA_SEALED(不可变可见, 引用计数生效)
零拷贝:
   Create 返回 mmap 内的指针, 应用直接写; 别的进程 Get 拿到同一 mmap 的 fd+offset → 同机零拷贝
分配器 plasma_allocator.cc:
   主分配: /dev/shm (dlmalloc, 64B 粒度)
   fallback: 主池满 → 退到磁盘 mmap 文件(慢 10~100x)
```

> ⚠️ **专家级坑①｜零拷贝只在单节点内**：同机不同进程经 Plasma 共享 mmap 是真零拷贝；**跨节点传输一定有拷贝**（拉到 `ObjectBufferPool` 再写入本地 Plasma），传输期间该对象同时占 buffer + plasma 两份内存。"Ray 零拷贝"别外推到跨节点。

> ⚠️ **专家级坑②｜fallback allocation 是性能悬崖**：主池(/dev/shm)满了退到磁盘 mmap，每次创建对象都要 fsync，慢几十上百倍。监控 `fallback_allocated` 标志——一旦为真说明 spill 已迫在眉睫。**非 Linux 上磁盘满会 SIGBUS 直接崩**。

> ⚠️ **专家级坑③｜Seal 前对象对他人不可见**：`PLASMA_CREATED` 状态只有创建者能写，`Get` 要等 `Seal`。位置也是 Seal 后才上报。调"对象一直拉不到"先确认它 seal 了没（task 崩在产出中途会留下永不 seal 的对象）。

> ⚠️ **专家级坑④｜fd 去重**：同一 mmap 文件里的多个对象共享一个 fd（`mmap_table_`/`dedup_fd_table_`），每个唯一 backing file 只 mmap 一次。改 client 端内存管理要注意这个去重，否则重复 mmap 或错误 unmap。

---

## 三、驱逐（eviction）：LRU

```
plasma/eviction_policy.cc:
   pinned(ref_count>0, 在用) 不可驱逐; unpinned(ref_count==0) 进 LRU
   RequireSpace(size): 内存不够 → ChooseObjectsToEvict(LRU 末尾, 至少腾 ~20%)
   BeginObjectAccess(被 task 用) 临时 pin; EndObjectAccess 放回 LRU
```

> ⚠️ **专家级坑⑤｜eviction 与 spilling 不协调**：Plasma 的 LRU 驱逐不知道 raylet 正打算 spill 某对象，可能把它先 evict 掉，导致 raylet spill 失败、内存压力缓解不了。`is_plasma_object_spillable()`（要求 plasma refcount==1，即只被 raylet pin）做了部分协调。这是内存管理最微妙的交界。

---

## 四、ObjectManager：跨节点传输

```
object_manager.cc: ObjectManager (多线程: RPC service 线程 + 主 io_context)
   Pull(refs): PullManager 入队(带优先级) → 查目录知位置 → 向源节点发 Pull 请求
   源节点 HandlePush: 读对象 → 切 chunk(object_chunk_size ~64MB) → PushManager 限流逐块发
   收端 HandlePush(chunk): 写 ObjectBufferPool → 全 chunk 到齐 → 写 Plasma → Seal
```

### PullManager 配额与优先级

```
pull_manager.cc:
   优先级: GET_REQUEST > WAIT_REQUEST > TASK_ARGS
   配额: RemainingQuota = available - (being_pulled - pinned); OverQuota 则不激活低优先级
   GET 不受配额(必须满足 ray.get); 重试指数退避(pull_timeout_ms * 2^n)
```

### PushManager 流控

```
push_manager.cc:
   max_chunks_in_flight ≈ max_bytes_in_flight / object_chunk_size
   每对象独立 chunk 计数(防 TCP 队头阻塞); 同(dest,object) 重复推被抑制(去重)
```

> ⚠️ **专家级坑⑥｜chunk 可能乱序到达**：TCP 重排导致 chunk 乱序，收端按 offset 缓冲组装；网络慢时整个对象卡在 buffer 直到最后一块到齐 → 大量半传输对象会撑大内存。
> ⚠️ **专家级坑⑦｜TASK_ARGS 会被 GET 饿死**（第 1 篇坑⑲）：`ray.get` 持续涌入时最低优先级的 task 参数拉取长期不激活。

---

## 五、对象目录：owner 驱动的位置真相

```
ownership_based_object_directory.cc:
   设计原则: 对象 owner 是位置的唯一权威(非 gossip/最终一致)
   非 owner 订阅 owner 获取位置变更; ReportObjectAdded/Spilled 批量上报给 owner
   owner 的 ReferenceCounter.locations 与此同步; 位置变更经 pub/sub 推订阅者
```

> 🔑 **位置目录是 owner-based 的**：谁拥有对象，谁就是"它在哪些节点"的权威。这把第 0 篇的去中心化所有权延伸到了位置追踪——不需要全局位置服务，问 owner 即可。
> ⚠️ **专家级坑⑧｜位置信息滞后**：上报是批量的（`kMaxObjectReportBatchSize`），可能延迟几十~上百 ms。PullManager 可能在位置上报到达前就重试，期间 `pending_creation=true` 会让 pull 挂起。"刚产出的对象拉不到"常是这个窗口。

---

## 六、本地对象管理：pin 主副本 + spill

```
local_object_manager.cc:
  PinObjectsAndWaitForFree(owner 请求): 持 RayObject*(共享内存 buffer) 防驱逐;
       订阅 WORKER_OBJECT_EVICTION 频道, owner 发释放 → ReleaseFreedObject;
       owner_dead_callback: owner 崩了立即释放, 防泄漏
  spill 触发: 内存 > object_spilling_threshold(0.8) 或 plasma OOM 回调
       SpillObjectsOfSize: 选 is_plasma_object_spillable 的(refcount==1) → objects_pending_spill_
       IO worker 写磁盘/S3; min_spilling_size 凑批; max_fused_object_count 融合多对象进一文件
       完成 → ReportObjectSpilled 通知 owner(更新 spilled_url/node)
  restore: PullManager 发现只有 spilled 副本 → AsyncRestoreSpilledObject 从 url 读回 Plasma
  并发: 最多 max_io_workers(4) 个 IO worker 同时 spill/restore
```

> ⚠️ **专家级坑⑨｜primary copy 与 secondary copy**：owner pin 的是**主副本**（可被 spill 到磁盘/S3，但 spill 后主副本逻辑上仍归 owner 追踪）；其他节点的是 secondary（被 LRU 自由驱逐）。主副本一旦 spill，secondary 位置可能失效——PullManager 必须先看 `spilled_url` 再去问 secondary 节点。混淆主/次副本是恢复逻辑 bug 的高发点。
> ⚠️ **专家级坑⑩｜spill 到 S3 的配置静默失败**：`object_spilling_config`（JSON，指定 filesystem/S3 后端）写错会**静默禁用 spilling 而不报错**；S3 失败也不杀 worker，而是让 pull 超时。凭据走环境变量不在 config 里。配 spill 后端要单独验证生效。
> ⚠️ **专家级坑⑪｜owner 死时 pin 的对象可能泄漏**：`owner_dead_callback` 兜底释放，但有竞态窗口；plasma refcount>0 的对象在该窗口内不会被驱逐。

---

## 七、内存压力的三级响应

```
内存紧张:
  ① Plasma LRU evict 未 pin 的对象 (eviction_policy.cc)
  ② LocalObjectManager spill 主副本到磁盘/S3 (automatic_object_spilling_enabled)
  ③ 仍不够 → WorkerKillingPolicy 杀 worker
       kRetriableLIFO(后进先杀, 默认) / kGroupByOwner(按 owner 减少波及) / kRetriableFIFO
       触发: 系统内存 > memory_usage_threshold(0.95) 且 free < min_memory_free_bytes
```

> ⚠️ **专家级坑⑫｜OOM kill 优先杀可重试任务**：`kRetriableLIFO` 优先杀最近启动的可重试 task（重算代价小）。不可重试的 task/actor 更"抗杀"。理解这个优先级才能解释"为什么 OOM 时是某些 worker 被杀"。`oom_grace_period_s` 控制抛 OOM error 前的宽限。

---

## 不变量清单

1. `ref_count==0 ⇔ 可驱逐`；`ref_count>0 ⇒ pin，不可驱逐`（Plasma 与所有权层须一致）。
2. 对象必须 `Seal` 后才可见、才上报位置。
3. owner 持主副本 pin；主副本可 spill 但仍归 owner 追踪；secondary 被 LRU 自由驱逐。
4. 跨节点传输必有拷贝（buffer + plasma 双占）；零拷贝仅单节点。
5. 位置目录 owner-based，权威在 owner，上报批量滞后。
6. chunk 可乱序，按 offset 组装，最后一块触发 Seal。

---

## config 速查

| config | 默认 | 含义 |
|--------|------|------|
| `object_store_memory` | ~30% RAM | Plasma 容量 |
| `object_spilling_config` | "" | spill 后端 JSON（filesystem/S3） |
| `automatic_object_spilling_enabled` | true | 自动 spill |
| `object_spilling_threshold` | 0.8 | spill 触发水位 |
| `max_io_workers` | 4 | spill/restore 并发 |
| `min_spilling_size` | 100MB | 单批最小 spill |
| `max_fused_object_count` | 2000 | 单文件融合对象数 |
| `object_chunk_size` | 64MB | 跨节点分块 |
| `max_bytes_in_flight` | ~500MB | 在途推送上限 |
| `pull_timeout_ms` | 10000~ | pull 重试 |
| `memory_usage_threshold` | 0.95 | OOM kill 水位 |
| `oom_grace_period_s` | 60 | 抛 OOM 前宽限 |

---

## 本篇三条主线

1. **三层衔接**：所有权层（owner 记账）→ 节点管理层（pin/spill/传输）→ 物理层（Plasma mmap/spill 文件）。owner 是位置与生命周期的权威，Raylet 是执行者。
2. **物理细节藏着大坑**：零拷贝仅单节点、fallback 是性能悬崖、Seal 前不可见、eviction 与 spilling 不协调、chunk 乱序、位置上报滞后。
3. **内存压力三级响应 + 主/次副本**：evict→spill→kill worker；owner 持主副本（可 spill）、其他节点是可自由驱逐的 secondary，恢复要先看 spilled_url。

→ 下一篇：[第 9 篇 · 易忽略技术点专章](./09-overlooked-techpoints.md)
