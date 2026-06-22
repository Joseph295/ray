#!/usr/bin/env bash
# 重新生成本指南引用的关键符号定位表（抵御行号漂移）。
# 用法: 在仓库根目录运行  bash doc/ray_internals_guide/tools/locate_symbols.sh > doc/ray_internals_guide/SYMBOLS.md
# 原理: 对真实源码 grep 函数/类定义，取当前 file:line。命中即说明文档引用的符号真实存在。
set -u
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT" || exit 1

emit() { # $1=label $2=ERE-pattern $3=path
  local hit file line
  hit=$(grep -rnE "$2" $3 2>/dev/null | grep -v -E "_test\.|/test/" | head -1)
  if [ -n "$hit" ]; then
    file=$(echo "$hit" | cut -d: -f1); line=$(echo "$hit" | cut -d: -f2)
    printf "| %s | \`%s\` | %s |\n" "$1" "$file" "$line"
  else
    printf "| %s | **未找到(需复核!)** | - |\n" "$1"
  fi
}

cat <<'HDR'
# 关键符号定位表（自动生成）

> 由 `tools/locate_symbols.sh` 对当前源码 grep 生成。**行号会随版本漂移**——以函数名为准，用此表快速跳转。
> 行号过期时，在仓库根目录重跑：`bash doc/ray_internals_guide/tools/locate_symbols.sh > doc/ray_internals_guide/SYMBOLS.md`

| 符号 | 文件 | 行 |
|------|------|----|
HDR

# ── Core Worker（第 1/5 篇）──
emit "CoreWorker::SubmitTask"            "CoreWorker::SubmitTask\("            "src/ray/core_worker/core_worker.cc"
emit "TaskManager::AddPendingTask"       "TaskManager::AddPendingTask\("       "src/ray/core_worker/task_manager.cc"
emit "TaskManager::CompletePendingTask"  "TaskManager::CompletePendingTask\("  "src/ray/core_worker/task_manager.cc"
emit "TaskManager::FailOrRetryPendingTask" "FailOrRetryPendingTask\("          "src/ray/core_worker/task_manager.cc"
emit "NormalTaskSubmitter::SubmitTask"   "NormalTaskSubmitter::SubmitTask\("   "src/ray/core_worker/transport/normal_task_submitter.cc"
emit "NormalTaskSubmitter::RequestNewWorkerIfNeeded" "RequestNewWorkerIfNeeded\(" "src/ray/core_worker/transport/normal_task_submitter.cc"
emit "NormalTaskSubmitter::OnWorkerIdle" "NormalTaskSubmitter::OnWorkerIdle\(" "src/ray/core_worker/transport/normal_task_submitter.cc"
emit "LocalDependencyResolver::ResolveDependencies" "ResolveDependencies\("    "src/ray/core_worker/transport/dependency_resolver.cc"
emit "LeasePolicy::GetBestNodeForTask"   "GetBestNodeForTask\("                "src/ray/core_worker/lease_policy.cc"
emit "ReferenceCounter (class)"          "class ReferenceCounter"              "src/ray/core_worker/reference_count.h"
emit "ReferenceCounter::AddOwnedObject"  "ReferenceCounter::AddOwnedObject"    "src/ray/core_worker/reference_count.cc"
emit "ReferenceCounter::WaitForRefRemoved" "WaitForRefRemoved"                 "src/ray/core_worker/reference_count.cc"
emit "ObjectRecoveryManager (class)"     "class ObjectRecoveryManager"         "src/ray/core_worker/object_recovery_manager.h"
emit "ActorTaskSubmitter::SubmitTask"    "ActorTaskSubmitter::SubmitTask"      "src/ray/core_worker/transport/actor_task_submitter.cc"

# ── Raylet（第 1/4 篇）──
emit "NodeManager::HandleRequestWorkerLease" "NodeManager::HandleRequestWorkerLease" "src/ray/raylet/node_manager.cc"
emit "ClusterTaskManager::QueueAndScheduleTask" "QueueAndScheduleTask\("       "src/ray/raylet/scheduling/cluster_task_manager.cc"
emit "LocalTaskManager::DispatchScheduledTasksToWorkers" "DispatchScheduledTasksToWorkers" "src/ray/raylet/local_task_manager.cc"
emit "DependencyManager::RequestTaskDependencies" "RequestTaskDependencies\("  "src/ray/raylet/dependency_manager.cc"
emit "WorkerPool::PopWorker"             "WorkerPool::PopWorker"               "src/ray/raylet/worker_pool.cc"
emit "WorkerPool::PrestartWorkers"       "PrestartWorkers"                     "src/ray/raylet/worker_pool.cc"
emit "LocalObjectManager::PinObjectsAndWaitForFree" "PinObjectsAndWaitForFree" "src/ray/raylet/local_object_manager.cc"

# ── GCS（第 3 篇）──
emit "GcsServer (class)"                 "class GcsServer"                     "src/ray/gcs/gcs_server/gcs_server.h"
emit "GcsActorManager::RegisterActor"    "RegisterActor\("                     "src/ray/gcs/gcs_server/gcs_actor_manager.cc"
emit "GcsActorScheduler (class)"         "class GcsActorScheduler"             "src/ray/gcs/gcs_server/gcs_actor_scheduler.h"
emit "GcsHealthCheckManager (class)"     "class GcsHealthCheckManager"         "src/ray/gcs/gcs_server/gcs_health_check_manager.h"

# ── 对象store（第 6 篇）──
emit "PullManager (class)"               "class PullManager"                   "src/ray/object_manager/pull_manager.h"
emit "PushManager (class)"               "class PushManager"                   "src/ray/object_manager/push_manager.h"
emit "OwnershipBasedObjectDirectory"     "class OwnershipBasedObjectDirectory" "src/ray/object_manager/ownership_based_object_directory.h"
emit "EvictionPolicy (class)"            "class EvictionPolicy"                "src/ray/object_manager/plasma/eviction_policy.h"

# ── Python / Serve / Data（第 2/7/8 篇）──
emit "actor.py ActorClass._remote"       "def _remote\("                       "python/ray/actor.py"
emit "Serve ServeController"             "class ServeController"               "python/ray/serve/_private/controller.py"
emit "Serve LongPollHost"               "class LongPollHost"                  "python/ray/serve/_private/long_poll.py"
emit "Data StreamingExecutor"           "class StreamingExecutor"             "python/ray/data/_internal/execution/streaming_executor.py"
emit "Data RefBundle"                   "class RefBundle"                     "python/ray/data/_internal/execution/interfaces/ref_bundle.py"

echo ""
echo "_生成时间: 见 git 提交; 重跑见脚本顶部用法。_"
