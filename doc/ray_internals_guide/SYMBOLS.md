# 关键符号定位表（自动生成）

> 由 `tools/locate_symbols.sh` 对当前源码 grep 生成。**行号会随版本漂移**——以函数名为准，用此表快速跳转。
> 行号过期时，在仓库根目录重跑：`bash doc/ray_internals_guide/tools/locate_symbols.sh > doc/ray_internals_guide/SYMBOLS.md`

| 符号 | 文件 | 行 |
|------|------|----|
| CoreWorker::SubmitTask | `src/ray/core_worker/core_worker.cc` | 2511 |
| TaskManager::AddPendingTask | `src/ray/core_worker/task_manager.cc` | 213 |
| TaskManager::CompletePendingTask | `src/ray/core_worker/task_manager.cc` | 785 |
| TaskManager::FailOrRetryPendingTask | `src/ray/core_worker/task_manager.cc` | 1111 |
| NormalTaskSubmitter::SubmitTask | `src/ray/core_worker/transport/normal_task_submitter.cc` | 29 |
| NormalTaskSubmitter::RequestNewWorkerIfNeeded | `src/ray/core_worker/transport/normal_task_submitter.cc` | 86 |
| NormalTaskSubmitter::OnWorkerIdle | `src/ray/core_worker/transport/normal_task_submitter.cc` | 143 |
| LocalDependencyResolver::ResolveDependencies | `src/ray/core_worker/transport/dependency_resolver.cc` | 73 |
| LeasePolicy::GetBestNodeForTask | `src/ray/core_worker/lease_policy.cc` | 22 |
| ReferenceCounter (class) | `src/ray/core_worker/reference_count.h` | 43 |
| ReferenceCounter::AddOwnedObject | `src/ray/core_worker/reference_count.cc` | 189 |
| ReferenceCounter::WaitForRefRemoved | `src/ray/core_worker/reference_count.cc` | 1049 |
| ObjectRecoveryManager (class) | `src/ray/core_worker/object_recovery_manager.h` | 43 |
| ActorTaskSubmitter::SubmitTask | `src/ray/core_worker/transport/actor_task_submitter.cc` | 164 |
| NodeManager::HandleRequestWorkerLease | `src/ray/raylet/node_manager.cc` | 2003 |
| ClusterTaskManager::QueueAndScheduleTask | `src/ray/raylet/scheduling/cluster_task_manager.cc` | 47 |
| LocalTaskManager::DispatchScheduledTasksToWorkers | `src/ray/raylet/local_task_manager.cc` | 101 |
| DependencyManager::RequestTaskDependencies | `src/ray/raylet/dependency_manager.cc` | 175 |
| WorkerPool::PopWorker | `src/ray/raylet/worker_pool.cc` | 222 |
| WorkerPool::PrestartWorkers | `src/ray/raylet/worker_pool.cc` | 204 |
| LocalObjectManager::PinObjectsAndWaitForFree | `src/ray/raylet/local_object_manager.cc` | 31 |
| GcsServer (class) | `src/ray/gcs/gcs_server/gcs_server.h` | 90 |
| GcsActorManager::RegisterActor | `src/ray/gcs/gcs_server/gcs_actor_manager.cc` | 396 |
| GcsActorScheduler (class) | `src/ray/gcs/gcs_server/gcs_actor_scheduler.h` | 53 |
| GcsHealthCheckManager (class) | `src/ray/gcs/gcs_server/gcs_health_check_manager.h` | 45 |
| PullManager (class) | `src/ray/object_manager/pull_manager.h` | 57 |
| PushManager (class) | `src/ray/object_manager/push_manager.h` | 32 |
| OwnershipBasedObjectDirectory | `src/ray/object_manager/ownership_based_object_directory.h` | 39 |
| EvictionPolicy (class) | `src/ray/object_manager/plasma/eviction_policy.h` | 159 |
| actor.py ActorClass._remote | `python/ray/actor.py` | 324 |
| Serve ServeController | `python/ray/serve/_private/controller.py` | 84 |
| Serve LongPollHost | `python/ray/serve/_private/long_poll.py` | 204 |
| Data StreamingExecutor | `python/ray/data/_internal/execution/streaming_executor.py` | 48 |
| Data RefBundle | `python/ray/data/_internal/execution/interfaces/ref_bundle.py` | 13 |

_生成时间: 见 git 提交; 重跑见脚本顶部用法。_
