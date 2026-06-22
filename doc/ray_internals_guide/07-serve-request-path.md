# 第 7 篇 · Ray Serve 在线请求链路（专家级）

> Serve 是建在 Ray Core 之上的**在线推理服务层**。本篇主线：**一个 HTTP 请求如何被处理**，以及 **Serve 如何复用 Core 的 actor / 命名空间 / pub/sub 把一堆 actor 编排成一个可伸缩的 Web 服务**。
> 先读 [第 2 篇 Actor](./02-core-actor-call-path.md)：Serve 的 Controller/Proxy/Replica 全是 actor，请求最终落成一次 actor 方法调用。

源码：`python/ray/serve/_private/`。Serve 是 Python 实现，但它对 Core 的用法（detached actor、命名 actor、long-poll pub/sub、对象引用流式传递）才是看点。

---

## 一、组件全景：一个 Controller + 多 Proxy + 多 Replica

```
                    ┌──────────────────────────────────────────────┐
                    │  ServeController  (detached named actor, 唯一) │
                    │  controller.py                                 │
                    │  - 控制循环(reconcile) 每 0.1s                  │
                    │  - DeploymentStateManager / ApplicationState   │
                    │  - EndpointState(路由表) / Autoscaling 决策      │
                    │  - LongPollHost: 状态版本化分发                  │
                    │  - checkpoint 到 GCS 内部 KV                     │
                    └───────▲──────────────────────▲─────────────────┘
              long poll 拉取 │ (DEPLOYMENT_TARGETS,  │ long poll
              路由表/副本表/配置│  ROUTE_TABLE, CONFIG) │
              ┌───────────────┴──────┐      ┌────────┴───────────────┐
              │ Proxy (每节点, actor) │      │ DeploymentHandle/Router │
              │ proxy.py             │      │ (proxy 内 or 用户进程内)  │
              │ HTTP/gRPC 入口        │      │ router.py + 副本调度       │
              └──────────┬───────────┘      └────────────┬───────────┘
                         │ 选 deployment, 经 handle         │ 选副本(Power-of-2)
                         └──────────────┬──────────────────┘
                                        ▼
                          ┌──────────────────────────────┐
                          │ Replica (用户 deployment, actor)│
                          │ replica.py 包装用户 callable    │
                          │ 维护 num_ongoing_requests 队列   │
                          └──────────────────────────────┘
```

- **Controller**：唯一有状态中心（detached named actor）。挂了从 GCS KV checkpoint 恢复，恢复期间数据面靠缓存继续跑。
- **Proxy / Replica**：都是 detached actor。这样 **Controller 重启不会级联杀掉它们**——这是"控制面/数据面解耦"的物理实现。
- **Router / DeploymentHandle**：**不是 actor**，是进程内的内存状态机（带 long-poll 客户端缓存）。可在 proxy 内或任意用户进程内创建。

> 🔑 **Serve 的核心设计**：最小中心化（只有一个 Controller）+ pull-based 软状态分发（long poll）+ 局部最优调度（power-of-two）。Controller 永远不在请求热路径上。

---

## 二、控制面：long poll 与 reconcile 循环

### 2.1 long poll：版本化的状态推送

Serve 不让客户端轮询查询，也不用主动 push 风暴，而是 **long poll**（`long_poll.py`）：

```
LongPollHost (在 Controller):
   snapshot_ids: {key → version}          每个 key 一个版本号
   object_snapshots: {key → 最新状态}
LongPollClient (在 Proxy/Handle):
   带着"我已知的各 key version" 调 listen_for_change()
   Host 比对:
     client 的 version 落后  → 立即返回最新快照
     version 已最新          → 挂起(asyncio.Event), 超时(随机 30~60s)再返回
   Controller notify_changed(key) → version++ → 唤醒所有等待该 key 的 client
```

四个关键命名空间：`DEPLOYMENT_TARGETS`(运行中的副本列表)、`ROUTE_TABLE`(路由→deployment)、`DEPLOYMENT_CONFIG`(副本数/并发等)、`GLOBAL_LOGGING_CONFIG`。

> ⚠️ **易忽略①｜为什么是 long poll 而非 RPC 查询或 pub/sub**：副本/handle 可能成千上万，每次状态变化都 push 会流量爆炸；纯轮询又浪费。long poll 是折中：客户端控速、变化才返回、超时随机化防 thundering herd。

> ⚠️ **易忽略②｜long poll 的一致性窗口**：客户端读到新状态与 Controller 下一轮 reconcile 之间有窗口（≤ 控制循环周期 ~0.1s + 网络）。"num_replicas 1→3" 时，proxy 可能先看到 2 个新副本、第 3 个还在启动。数据面必须容忍"部分副本可用"的临时态，靠请求重试兜底。

> ⚠️ **易忽略③｜Controller 重启后 snapshot_id 重置**：Controller 崩溃恢复后版本号从随机值重新计数。客户端记的是旧版本号，下次 `listen_for_change` 会**立即收到全量最新快照**自动追平。但窗口期内 handle 可能把请求发给已下线副本 → 需重试。

### 2.2 reconcile 循环：像 k8s controller

```
deployment_state.py: 控制循环每 CONTROL_LOOP_INTERVAL_S(~0.1s):
   for each deployment:
      target = 期望(num_replicas/version)   实际 = 运行中副本
      实际 < 目标 → _upscale: 经 deployment_scheduler 起新副本(actor)
      实际 > 目标 → _downscale: 优雅停掉最老副本
      到健康检查周期 → 对每副本 actor.check_health.remote()
      version 不符(rolling update) → 停老版本副本, 起新版本
```

`application_state.py` 负责"YAML/config → 部署"，`endpoint_state.py` 维护"路由前缀 → deployment"映射并经 long poll 广播。状态都 checkpoint 到 **GCS 内部 KV**（持久、跨 Ray 重启存活）。

> ⚠️ **易忽略④｜为什么 checkpoint 到 GCS KV 而非对象store**：对象store的对象会随 actor 崩溃被清理；而 Controller 要在"没有任何 actor 存活"时也能恢复，所以必须用持久的 GCS KV。

---

## 三、数据面：一个 HTTP 请求的完整链路

```
[client HTTP]
  │
  ▼ proxy.py: proxy_request()  (Uvicorn/ASGI event loop)
  │   _get_response_handler_info():
  │     proxy_router.py: match_route(path) → deployment_id   (按前缀长度降序匹配)
  │     取该 deployment 的 DeploymentHandle
  │     生成 request_id, 设置 request context
  │
  ▼ router.py: Router.assign_request()
  │   _metrics_manager.wrap_request_assignment(): 检查 max_queued_requests 背压(超则 BackPressureError)
  │   _resolve_request_arguments(): 并发解析参数里的 ObjectRef
  │   _replica_scheduler.choose_replica_for_request()
  │
  ▼ replica_scheduler/ (Power-of-Two Choices)
  │   随机取 2 个候选副本 → 各发 get_num_ongoing_requests 探测(带 deadline ~0.1s)
  │   副本回 (queue_len, accepted= queue<max_ongoing_requests)
  │   选 queue_len 较小且 accepted 的; 都满 → 指数退避重试(0,0.05,...,1.0s)
  │
  ▼ replica.py: 一次 actor 方法调用 (这就是第2篇的 direct actor call!)
  │   handle_request / handle_request_with_rejection(流式, 首条是 ReplicaQueueLengthInfo 系统消息)
  │   replica 内: inc num_ongoing → 跑用户 callable → 记 metrics → dec num_ongoing
  │
  ▼ proxy_response_generator.py: 流式读结果(ObjectRefGenerator), 处理超时/客户端断开
  │
  ▼ http_util.py: 转 ASGI message → HTTP response body
[client 收到响应]
```

### 3.1 副本调度：Power-of-Two Choices

> 🔑 不用 round-robin，用 **power-of-two**：随机挑 2 个副本、问各自队列长度、选短的那个。数学上已证明接近最优负载均衡，且只需 2 次探测、O(1) 决策、负载感知。

> ⚠️ **易忽略⑤｜两层背压**：`max_queued_requests`(handle 层，超了直接 `BackPressureError`，防无限堆积) 和 `max_ongoing_requests`(副本层，power-of-two 拒绝队列满的副本)。两个数要协调：批处理(`@serve.batch`)时 `max_ongoing_requests` 必须 ≥ 预期最大 batch 并发，否则 batch handler 会自己把自己卡死。

> ⚠️ **易忽略⑥｜高负载下请求可能 hang 到 1s 才报背压**：两个候选都满时 scheduler 指数退避重试，最坏等到 ~1s 才返回 `BackPressureError`。调 `max_ongoing_requests` 与 `request_timeout_s` 要权衡这点。

> ⚠️ **易忽略⑦｜handle 是带 long-poll 缓存的内存对象，要复用**：`DeploymentHandle`/`Router` 缓存路由表与副本表，靠 long poll 更新。共享缓存有上限(`MAX_CACHED_HANDLES` ~100)，频繁新建 handle 会触发淘汰、反复重建 long poll 连接。handle 应在应用级缓存复用。

### 3.2 流式响应与背压

副本通过 `ObjectRefGenerator` 流式产出，`proxy_response_generator.py` 逐条读、检查超时（`request_timeout_s`）和客户端断开。带 rejection 的流式调用第一条消息是系统消息 `ReplicaQueueLengthInfo`，让 handle 在副本临时满时秒级改投他人。

> ⚠️ **易忽略⑧｜响应必须以状态结尾**：所有路径（正常/错误/超时）都必须 yield 最终 status，proxy 有断言保证。改流式路径时漏掉某条错误分支的 final status 会让请求挂死。

---

## 四、伸缩与容错

### 4.1 Autoscaling

```
autoscaling_state.py:
   replica/handle 周期上报 queue 指标(metrics_interval_s ~10s)到 Controller
   policy 用 look_back_period_s(~30s) 窗口均值 / target_ongoing_requests 算目标副本数
   reconcile 循环据此 upscale/downscale
```

> ⚠️ **易忽略⑨｜autoscaling 抖动**：窗口太小、平滑因子不当会导致副本数频繁增减、反复启停。`look_back_period_s` / `smoothing_factor` 要调。
> ⚠️ **易忽略⑩｜冷启动从 0 副本扩容的延迟**：0 副本时 handle 指标默认 ~10s 才上报，autoscaler 收不到 queued 请求就不扩容。有"检测到 0 副本且有积压立即推送"的优化路径(scaled-to-zero optimized push)缓解，但仍是冷启动延迟来源。

### 4.2 容错

- **Replica 崩溃**：handle 收 `ActorDiedError`；reconcile 循环检测副本缺失、起新副本（1~2 个控制周期）。
- **Controller 崩溃**：long poll 超时、proxy 短暂 503；Controller 从 GCS KV checkpoint 恢复（app/deployment/endpoint/logging 状态），扫描命名 actor 重建副本状态，恢复期延迟广播(`RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S` ~10s)避免只看到部分副本。
- **Proxy 崩溃**：Controller 的 ProxyStateManager 健康检查发现后重启。

> ⚠️ **易忽略⑪｜优雅下线(draining)的两个超时**：缩容/更新停副本时，副本等 `graceful_shutdown_wait_loop_s` 直到没有在途请求才真停，超过 `graceful_shutdown_timeout_s` 则强杀。设太短会杀掉在途请求，太长会拖慢缩容。
> ⚠️ **易忽略⑫｜checkpoint 非一致性快照**：副本启动时刻与 checkpoint 时刻有 gap，恢复后 `target_num_replicas` 可能与实际副本数不符，要经 RECOVERING→RUNNING 同步。

---

## 不变量清单

1. 集群同时只有一个运行中的 Controller（named + detached 保证）。
2. 同一 ReplicaID 不同时跑多个实例（actor name 强制）。
3. `target_num_replicas` 与 `version` 同时更新（单一 `DeploymentTargetState`）。
4. 每个路由前缀只指向一个 deployment（`endpoint_state` 覆盖式更新）。
5. 副本队列深度 ≤ `max_ongoing_requests`；handle 队列 ≤ `max_queued_requests`。
6. 响应流必须以最终 status 结尾。
7. Controller 永不在请求热路径上；数据面靠 long-poll 缓存运行。

---

## config 速查（部分）

| config | 默认 | 含义 |
|--------|------|------|
| `num_replicas` | 1 | 副本数（或 autoscaling） |
| `max_ongoing_requests` | ~5 | 单副本并发上限 |
| `max_queued_requests` | -1 | handle 队列上限 |
| `graceful_shutdown_timeout_s` / `_wait_loop_s` | 20 / 2 | 优雅下线超时/等待 |
| `health_check_period_s` / `_timeout_s` | 10 / 30 | 健康检查 |
| `request_timeout_s`（HTTP） | None | 端到端超时 |
| autoscaling: `min/max_replicas`, `target_ongoing_requests`, `look_back_period_s`, `metrics_interval_s` | — | 伸缩策略 |
| `CONTROL_LOOP_INTERVAL_S` | 0.1 | reconcile 周期 |
| `MAX_CACHED_HANDLES` | 100 | long-poll client 缓存上限 |
| `RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S` | 1.0 | 多路复用模型匹配超时 |

---

## 本篇三条主线

1. **一个 Controller + 软状态分发**：Controller 是唯一有状态中心，通过版本化 long poll 把路由表/副本表/配置推给 proxy 与 handle；它从不在请求热路径上，挂了从 GCS KV 恢复。
2. **请求链路 = 路由 + power-of-two + 一次 actor 调用**：HTTP→proxy→handle→副本调度(挑 2 选短)→replica actor 方法→流式回传；两层背压(handle 队列 / 副本并发)。
3. **建在 Core 之上**：Controller/Proxy/Replica 都是 detached actor（控制/数据面解耦），调用复用 Core 的 direct actor call，状态分发复用 pub/sub 思想，checkpoint 用 GCS KV。

→ 下一篇：[第 8 篇 · Ray Data 流式执行链路](./08-data-streaming-execution.md)
