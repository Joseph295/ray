# Ray 内核入门指南（贡献者视角）

> 面向**想读懂并修改 Ray 源码**的新人。不是使用手册，而是"打开发动机盖看里面怎么转"的导览。
> 所有链路都落到 **进程 / 线程 / 文件 / 函数** 级别，并给出**设计原理、不变量、并发竞态、失败模式、config 旋钮、易忽略的坑**——目标是：**读完即等于一位资深 maintainer 通读过这部分源码**。

---

## 这份指南怎么读

我们用两种视角交叉讲同一个系统：

- **链路（动态视角）**：跟着一个"请求"从发起走到执行、结果取回，每一步都告诉你在哪个进程、哪个文件、哪个函数。这是"导游路线"。
- **模块（静态视角）**：链路上经过的每个组件，单独展开它的职责、设计取舍、关键数据结构和坑。这是"景点详解"。

建议**先走完一遍链路建立端到端心智模型，再回头逐个钻模块**。直接从某个 `.cc` 文件读起，几乎一定会迷路。

---

## 目录与阅读顺序

| 篇 | 文件 | 内容 | 状态 |
|----|------|------|------|
| 0 | [`00-mental-model.md`](./00-mental-model.md) | **心智模型**：三种进程角色、线程模型、对象store、**所有权(ownership)与分布式引用计数**这块基石 | ✅ |
| 1 | [`01-core-task-execution-path.md`](./01-core-task-execution-path.md) | **Core 普通 Task 链路**：`f.remote()` 从 Python 到执行、结果回传的全程 | ✅ |
| 2 | [`02-core-actor-call-path.md`](./02-core-actor-call-path.md) | **Core Actor 调用链路**：Actor 注册走 GCS、直连调用、提交队列顺序语义、并发组 | ✅ |
| 3 | [`03-module-gcs.md`](./03-module-gcs.md) | **模块：GCS**（全局控制存储）manager 分工、存储层、pub/sub、容错 | ✅ |
| 4 | [`04-module-raylet.md`](./04-module-raylet.md) | **模块：Raylet**（节点管理器）调度、worker pool、对象子系统 | ✅ |
| 5 | [`05-module-core-worker.md`](./05-module-core-worker.md) | **模块：CoreWorker**（进程内内核）提交/执行双角色、引用计数 | ✅ |
| 6 | [`06-module-object-store.md`](./06-module-object-store.md) | **模块：对象store + 所有权** 深入（plasma、跨节点传输、spilling、恢复） | ✅ |
| 7 | [`07-serve-request-path.md`](./07-serve-request-path.md) | **Ray Serve 在线请求链路**：HTTP → proxy → router → replica + long poll | ✅ |
| 8 | [`08-data-streaming-execution.md`](./08-data-streaming-execution.md) | **Ray Data 流式执行链路**：逻辑/物理计划 → streaming executor → 算子 | ✅ |
| 9 | [`09-overlooked-techpoints.md`](./09-overlooked-techpoints.md) | **易忽略技术点专章**：按主题汇总全部坑 + 跨模块心智陷阱 + 调试思维顺序 | ✅ |

> 全部 9 篇已完成，专家深度（设计原理 + 不变量 + 并发 + 失败模式 + config + 坑），统一带 ASCII 时序图/架构图。

---

## 阅读约定

- **文件路径**以仓库根为基准，例如 `src/ray/core_worker/core_worker.cc`。
- **行号会随版本漂移**，所以每处都同时给出**函数名**作为稳定锚点——以函数名为准，行号只是帮你快速定位的参考（基于撰写时的 master）。
- **关键符号定位表**：[`SYMBOLS.md`](./SYMBOLS.md) 列出本指南引用的全部关键函数/类的当前 `文件:行号`（已对真实源码校验存在）。行号过期时，在仓库根目录重跑 `bash doc/ray_internals_guide/tools/locate_symbols.sh > doc/ray_internals_guide/SYMBOLS.md` 一键刷新。
- 图例：
  - `══>` 进程间 RPC（gRPC）
  - `──>` 进程内函数调用
  - `· · >` 异步回调 / 事件
  - `[P]` 表示数据落在 Plasma 共享内存对象store
  - `[M]` 表示数据落在进程内 in-memory store（小对象）
- ⚠️ 标记的段落是**贡献者易忽略的技术点**。
