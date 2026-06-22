# 第 8 篇 · Ray Data 流式执行链路（专家级）

> Ray Data 是建在 Core 之上的**数据处理层**。本篇主线：**一个 `dataset.map(...).iter_batches()` 如何被编译成计划、流式地在 Core task/actor 上执行**，以及它如何**重度依赖对象引用计数管理海量中间块**。
> 先读 [第 0 篇](./00-mental-model.md)的对象store与引用计数——Data 是 Ray 引用计数机制最重的"用户"。

源码：`python/ray/data/_internal/`。三层：**逻辑计划 → 物理计划(含算子融合) → 流式执行引擎**。

---

## 一、为什么是流式执行：先建立直觉

传统批处理("bulk")：算子 A 把**所有**输出物化，再喂给 B。峰值内存 = 整个中间结果。

Ray Data 的**流式执行(streaming)**：算子 DAG 同时在跑，A 产出几个 block 就立刻流给 B 消费、并**及时释放**已消费的 block。峰值内存 ≈ 背压预算，与数据总量解耦。

```
  bulk:    A: ████████████(全部)  →  B: ████████████   峰值=全量
  stream:  A: ██..██..██  ⇅ 背压  →  B: ██..██..██     峰值≈在途窗口
           (A 产出即释放, B 消费即销毁, 内存恒定)
```

这套机制的两个支柱：**RefBundle 所有权语义**（明确谁负责释放中间块）+ **多级背压**（下游慢就让上游停产）。

---

## 二、计划编译：Dataset API → 逻辑计划 → 物理计划

### 2.1 惰性逻辑计划

```
python/ray/data/_internal/logical/:
   dataset.map(fn) / .filter() / .map_batches() 只构建 LogicalOperator DAG, 不执行
   LogicalOperator: input_dependencies(上游), estimated_num_outputs(), is_lineage_serializable()
   真正触发执行: iter_batches() / take() / write_*() / materialize()
```

> ⚠️ **易忽略①｜lineage 可序列化性决定能否容错**：`LogicalOperator` 必须 `is_lineage_serializable()` 才能跨进程传递用于故障重算。**`from_pandas()` 等 `InputData` 算子不可序列化**（数据是凭空给的，无法重现）→ 这种 dataset 的 block 丢了**无法重建**。生产里要可恢复就用 `read_*()`（Read 算子可重放），别用 `from_pandas`。

### 2.2 优化规则 + 算子融合 ★

```
_internal/logical/rules/ (按依赖顺序应用):
   set_read_parallelism (定读并行度), limit_pushdown(limit 下推提前终止),
   inherit_batch_format, randomize_blocks, configure_map_task_memory,
   operator_fusion ★
```

**算子融合**把相邻算子合成一个 task，省掉中间 block 物化到对象store。融合条件（`operator_fusion.py`，须同时满足）：

```
1. 都是 Map 类(或 Map + shuffle)
2. 计算策略兼容: Task→Task 或 Task→Actor; 【不允许 Actor→Task 或 Actor→Actor】
3. UDF 兼容: callable class 须同类同构造参数
4. remote args 兼容: 无 _ray_remote_args_fn, scheduling_strategy 相同
5. 上游 get_additional_split_factor()==1 (没额外拆块)
```

> ⚠️ **易忽略②｜融合条件有 ~9 条，很多 map 链其实没融合**：尤其 `Actor→任何` 不融（避免嵌套 actor）、有动态 `_ray_remote_args_fn` 不融、上游有额外拆分不融。融合与否直接决定中间 block 数量与对象store压力。用 `ds.stats()`/explain 看实际物理 DAG 是否融合，别想当然。

### 2.3 逻辑 → 物理

```
_internal/planner/planner.py: 后序遍历 LogicalOperator DAG → PhysicalOperator
   Read              → 读算子
   MapBatches/MapRows→ TaskPoolMapOperator 或 ActorPoolMapOperator (按 compute 策略)
   Filter/Project    → TaskPoolMapOperator
   RandomShuffle/Repartition(shuffle=True) → AllToAllOperator (物化型)
   InputData         → InputDataBuffer
   Union/Zip         → N 元算子
```

### 2.4 Block 与 RefBundle

- **Block**：一段 Arrow/Pandas 表，存在对象store里(一个 `ObjectRef[Block]`)。带 `BlockMetadata`(必含 `size_bytes`，用于内存预算)。
- **RefBundle**（`execution/interfaces/ref_bundle.py`）：算子间传递的单位 = 一组 `(ObjectRef[Block], metadata)` + `owns_blocks` 标志。**不可变**。

```
@dataclass(frozen-ish)
class RefBundle:
   blocks: ((ObjectRef[Block], BlockMetadata), ...)
   owns_blocks: bool   # 我是否拥有这些 block 的释放权
```

> ⚠️ **易忽略③｜`owns_blocks` 决定谁能销毁、何时省内存**：task 新产出的 block `owns_blocks=True`；预先存在的(读源/`from_*`) `owns_blocks=False`；融合后 = 所有输入的 AND。下游消费完调 `destroy_if_owned()`，**仅当 `owns_blocks=True` 且 `DataContext.eager_free=True` 才真正从对象store删除**，否则等 Python GC + Ray 引用计数。这就是 Data 把对象store内存压住的关键。

---

## 三、流式执行引擎

### 3.1 拓扑与执行入口

```
plan.py: execute_to_iterator() → StreamingExecutor.execute(physical_dag)
   build_streaming_topology(dag, options):  后序遍历建 Topology = {PhysicalOperator: OpState}
       OpState: inqueues(= 上游的 outqueue, 共享同一对象!) / outqueue(线程安全 OpBufferQueue) / 进度条
       op.start(options): 分配资源、初始化
   start(): 启动后台调度线程
   返回 StreamIterator 供用户迭代
```

> 🔑 **关键结构**：上游算子的 `outqueue` 就是下游算子的 `inqueue`（同一个 Python 对象引用）。block 通过这些线程安全队列在算子间流动。调度线程与用户消费线程并发访问，靠 `OpBufferQueue`(deque+lock) 同步。

### 3.2 调度主循环

```
streaming_executor.py: 后台调度线程循环:
  1. process_completed_tasks(): ray.wait(所有在途 task, timeout=0.1)
        每个就绪 task → on_data_ready(): 从 streaming_gen 读出 block → 塞进算子 outqueue
  2. update_operator_states(): 上游完成且 outqueue 空 → input_done/all_inputs_done/mark_completed 回调
  3. select_operator_to_run(): 过滤出 eligible 算子(未完成 + inqueue 有数据 + 资源够 + 未被背压)
  4. while 有可运行算子: dispatch_next_task() (inqueue.pop → op.add_input → 提交 task)
  5. 全部 completed → 退出
```

### 3.3 算子如何提交 task

```
map_operator.py: add_input(bundle):
   _block_ref_bundler.add_bundle(bundle)         ← 把小块攒到 min_rows_per_bundle
   攒够 → _add_bundled_input():
       TaskPoolMapOperator: gen = _map_task.options(动态 remote args).remote(transformer, ctx, *block_refs)
       DataOpTask(gen, on_data_ready_cb, on_done_cb)   ← 用 streaming generator task
```

task 是 **streaming generator**：逐个 yield `(block_ref, metadata)`，调度线程 `on_data_ready` 非阻塞地读出来。`_generator_backpressure_num_objects` 限制 generator 缓冲块数。

> ⚠️ **易忽略④｜`min_rows_per_bundle` 与 `batch_size` 不是一回事**：`map_batches(fn, batch_size=1024, min_rows_per_bundle=10000)` —— `batch_size` 是喂给 UDF 的每批行数；`min_rows_per_bundle` 是 bundler 攒够多少行才提交一个 task。输入块很小又不设 `min_rows_per_bundle` → 每个小块一个 task → 调度开销爆炸。

> ⚠️ **易忽略⑤｜TaskPool vs ActorPool 的本质区别**：`TaskPoolMapOperator` 无状态、立即提交、单 task 失败只影响自己；`ActorPoolMapOperator` 有状态、要等 actor 启动、可缓存(如模型权重)，但 actor 崩溃影响更大。GPU 推理/需缓存模型用 ActorPool，无状态 IO 用 TaskPool(默认)。`Actor→任何`不参与融合(见坑②)。

### 3.4 物化型算子（Shuffle/Sort）

```
base_physical_operator.py: AllToAllOperator
   add_input → 攒进 _input_buffer
   all_inputs_done → 等全部输入到齐, 调 _bulk_fn(shuffle/sort 调度) 产出 _output_buffer
```

> ⚠️ **易忽略⑥｜物化型算子打破流式、内存会突增**：sort/shuffle 必须等**所有**输入到齐才能算 → 上游 100GB 全堆在对象store等它，极易触发 spill。大 shuffle 前先 `limit()`/`sample()` 验证；数据 > 集群内存×2 会 spill 拖慢。shuffle 块大小自动升到 `target_shuffle_max_block_size`(~1GB)，pull-based vs push-based 两种策略各有对象store开销。

---

## 四、资源与背压 ★

### 4.1 ResourceManager：内存预算

```
execution/resource_manager.py:
   每调度迭代 update_usages(): 估算每个算子的 object_store_memory 占用
       = pending task 输出 + 算子 outqueue + 下游 inqueue
   对象store内存上限 = 集群内存 × fraction(默认约 0.25~0.5, 视是否启用 reservation allocator)
   ReservationOpResourceAllocator(默认开): 给每个 eligible 算子预留一份预算 + 共享池分摊
       reserved = reservation_ratio(0.5) × global_limit / num_eligible_ops
```

> ⚠️ **易忽略⑦｜预留式分配防"上游撑爆下游/下游饿死上游"**：每个算子保底预留 + 共享池竞争。`IdleDetector` 检测某算子长时间(~10s)无输出就强行放行至少一个 task，避免小集群死锁。`op_resource_reservation_ratio` / `override_object_store_memory_limit_fraction` 是调内存的旋钮。

### 4.2 两类背压

```
backpressure_policy/:
  ① task 提交背压: can_submit_new_task()==False
        原因: 算子预算 < 增量任务资源 → select_operator_to_run 不选它
  ② task 输出背压: max_bytes_to_read==0
        原因: 下游消费太慢 → 任务可提交但不读它的 output, generator 缓冲挂起
```

> ⚠️ **易忽略⑧｜两类背压要分清**："任务提交不出去"和"任务在跑但 output 读不出来"是两个不同维度。进度条的 `[backpressured:...]` 标志能区分。调"为什么算子不跑了"先确认是哪类背压。

> ⚠️ **易忽略⑨｜`eager_free` 别关**：默认 `DataContext.eager_free=True`，消费完立即从对象store删 owned block。关掉就依赖 Python GC，长链流式下 GC 延迟会让对象store堆满→spill→IO 瓶颈。内存有问题应调内存 fraction，而非关 eager_free。

### 4.3 容错与统计

block 丢失靠 lineage 重算（前提：算子可序列化，见坑①）。`stats.py` 收集 spill/restore 字节、各算子耗时。

> ⚠️ **易忽略⑩｜`preserve_order=True` 的内存代价**：保序会把乱序完成的 task 输出**缓存住**直到前序 task 都完成（`_OrderedOutputQueue`），峰值内存 = 最大未完成输出。只在写文件等必须保序时开。

---

## 不变量清单

1. `RefBundle` 不可变；`owns_blocks=True` 的 bundle 只能被一个算子 `destroy`。
2. 每个 `PhysicalOperator` 在 topology 中恰好一个 `OpState`；上游 outqueue === 下游 inqueue（同对象）。
3. `RefBundle` 创建时 `size_bytes` 必须已知（内存预算依赖它）。
4. 融合仅 `TaskPool→TaskPool` 或 `TaskPool→ActorPool`；`Actor→任何`不融。
5. `AllToAllOperator` 全输入到齐前不产出任何 block。
6. `OpBufferQueue` 线程安全（调度线程 + 消费线程并发）。
7. lineage 可恢复要求算子 `is_lineage_serializable()`；`InputData` 不满足。

---

## config 速查（`DataContext`）

| config | 默认 | 含义 |
|--------|------|------|
| `target_max_block_size` | 128MB | 块大小↔并行度 |
| `target_shuffle_max_block_size` | 1GB | shuffle 块大小 |
| `read_op_min_num_blocks` | 200 | 读最少块数 |
| `eager_free` | True | 消费后立即释放 owned block |
| `op_resource_reservation_enabled` | True | 预留式资源分配 |
| `op_resource_reservation_ratio` | 0.5 | 预留比例 |
| `override_object_store_memory_limit_fraction` | None | 对象store内存预算比例 |
| `execution_options.preserve_order` | False | 保序（有内存代价） |
| `scheduling_strategy` | SPREAD | task 默认调度策略 |
| `max_errored_blocks` | 0 | 容错允许的错误块数 |

---

## 本篇三条主线

1. **三段编译 + 算子融合**：Dataset API 惰性建逻辑计划 → 优化(融合省中间物化) → 物理算子 DAG。融合条件苛刻，决定对象store压力。
2. **流式执行 + RefBundle 所有权**：算子 DAG 并发跑，block 经线程安全队列流动；`owns_blocks` + `eager_free` 让中间块即产即销，峰值内存与数据量解耦——Data 是 Ray 引用计数最重的用户。
3. **多级背压撑住内存**：ResourceManager 预留式分内存预算，task 提交背压 + 输出背压两维控制上游产速；物化型算子(shuffle)和 `preserve_order` 是内存突增点。

→ 下一篇：[第 9 篇 · 易忽略技术点专章](./09-overlooked-techpoints.md)
