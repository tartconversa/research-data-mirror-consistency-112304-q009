"""跨区域科研数据镜像的一致性与可用目录能力包。

- ``records``：合成资料记录与加载（不附加结论）；
- ``mirror``：镜像回报折叠、就绪评估与接续计划；
- ``catalog``：跨区域数据产品目录与按范围可用性查询；
- ``snapshots``：读取快照与撤回/重发布影响；
- ``service``：一体化门面与生命周期事件。
"""
from .catalog import (
    AvailabilityAnswer,
    Catalog,
    CatalogEntry,
    Permission,
    local_permission,
)
from .mirror import (
    Action,
    ContinuationAction,
    MirrorReplica,
    Readiness,
    ReplicaEvaluation,
    evaluate_replica,
    fold_replicas,
    plan_continuation,
)
from .records import Materials, load_materials
from .rules import Scope
from .service import DataPlatformService
from .snapshots import SnapshotLease, SnapshotPin, SnapshotRegistry

__all__ = [
    "Action",
    "AvailabilityAnswer",
    "Catalog",
    "CatalogEntry",
    "ContinuationAction",
    "DataPlatformService",
    "Materials",
    "MirrorReplica",
    "Permission",
    "Readiness",
    "ReplicaEvaluation",
    "Scope",
    "SnapshotLease",
    "SnapshotPin",
    "SnapshotRegistry",
    "evaluate_replica",
    "fold_replicas",
    "load_materials",
    "local_permission",
    "plan_continuation",
]
