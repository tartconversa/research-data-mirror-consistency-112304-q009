"""测试共享辅助：构造合成场景记录与服务实例。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from research_data_mirror_consistency.records import (  # noqa: E402
    ConsumerRequestRow,
    DatasetRow,
    Materials,
    ReplicaObjectRow,
    ReplicaTaskRow,
    SourcePartitionRow,
)
from research_data_mirror_consistency.service import DataPlatformService  # noqa: E402

DATA_DIR = ROOT / "data"


def ds(
    dataset_id: str,
    *,
    owner: str = "LAB-A",
    sensitivity: str = "open",
    version: str = "v1",
    scope: str = "consortium",
    state: str = "published",
) -> DatasetRow:
    return DatasetRow(
        dataset_id=dataset_id,
        owner_lab=owner,
        sensitivity=sensitivity,
        release_version=version,
        share_scope=scope,
        release_state=state,
    )


def src(
    dataset_id: str,
    partition: str,
    *,
    version: str = "v1",
    checksum: str = "SHA-X",
    updated: str = "2026-10-01",
    withdrawal: str = "active",
) -> SourcePartitionRow:
    return SourcePartitionRow(
        dataset_id=dataset_id,
        partition_key=partition,
        source_version=version,
        checksum=checksum,
        updated_at=updated,
        withdrawal_state=withdrawal,
    )


def task(
    task_id: str,
    dataset_id: str,
    region: str,
    partition: str,
    *,
    version: str = "v1",
    attempt: int = 1,
    state: str = "copied",
    completed: str = "2026-10-02T00:00:00Z",
) -> ReplicaTaskRow:
    return ReplicaTaskRow(
        task_id=task_id,
        dataset_id=dataset_id,
        region=region,
        partition_key=partition,
        target_version=version,
        attempt=attempt,
        task_state=state,
        completed_at=completed,
    )


def obj(
    task_id: str,
    *,
    key: str | None = None,
    checksum: str = "SHA-X",
    meta: str = "META-1",
    pol: str = "POL-1",
    verified: str = "verified",
) -> ReplicaObjectRow:
    return ReplicaObjectRow(
        task_id=task_id,
        object_key=key or f"obj/{task_id}",
        checksum=checksum,
        metadata_version=meta,
        access_policy_version=pol,
        verified_state=verified,
    )


def req(
    request_id: str,
    dataset_id: str,
    *,
    lab: str = "LAB-Z",
    region: str = "R1",
    scope: str = "whole-release",
    at: str = "2026-10-05",
    decision: str = "allow",
    snapshot: str = "SNAP-T",
) -> ConsumerRequestRow:
    return ConsumerRequestRow(
        request_id=request_id,
        consumer_lab=lab,
        dataset_id=dataset_id,
        region=region,
        requested_scope=scope,
        requested_at=at,
        decision=decision,
        snapshot_ref=snapshot,
    )


def service_with(
    *,
    datasets=(),
    sources=(),
    tasks=(),
    objects=(),
    requests=(),
) -> DataPlatformService:
    return DataPlatformService(
        Materials(
            datasets=tuple(datasets),
            source_partitions=tuple(sources),
            replica_tasks=tuple(tasks),
            replica_objects=tuple(objects),
            consumer_requests=tuple(requests),
        )
    )


def mirror_service() -> DataPlatformService:
    """标准合成世界：覆盖就绪、滞后、权限、撤回与未发布各分支。"""
    datasets = [
        ds("DS-A", owner="LAB-A", sensitivity="open", version="v2", scope="consortium"),
        ds("DS-B", owner="LAB-B", sensitivity="controlled", version="v1", scope="project"),
        ds("DS-C", owner="LAB-C", sensitivity="restricted", version="v3", scope="consortium"),
        ds("DS-D", owner="LAB-A", sensitivity="open", version="v1", scope="none"),
        ds("DS-W", owner="LAB-A", sensitivity="open", version="v1", scope="consortium", state="withdrawn"),
        ds("DS-U", owner="LAB-A", sensitivity="open", version="v1", scope="consortium", state="review"),
    ]
    sources = [
        src("DS-A", "P1", version="v2"),
        src("DS-A", "P2", version="v2"),
        src("DS-B", "P1", version="v1"),
        src("DS-C", "P1", version="v3"),
        src("DS-D", "P1", version="v1"),
        src("DS-W", "P1", version="v1"),
        src("DS-U", "P1", version="v1"),
    ]
    tasks = [
        # DS-A/R1：两个分区均干净复制 → 就绪
        task("T-A1", "DS-A", "R1", "P1", version="v2", completed="2026-10-02T08:00:00Z"),
        task("T-A2", "DS-A", "R1", "P2", version="v2", completed="2026-10-02T09:00:00Z"),
        # DS-A/R2：策略版本落后于区域现行（R2 现行 POL-2）
        task("T-A3", "DS-A", "R2", "P1", version="v2", completed="2026-10-02T10:00:00Z"),
        # DS-B/R1：最近一次复制失败
        task("T-B1", "DS-B", "R1", "P1", version="v1", state="failed", attempt=2),
        # DS-C/R3：干净复制（restricted 数据，权限矩阵生效）
        task("T-C1", "DS-C", "R3", "P1", version="v3", completed="2026-10-02T11:00:00Z"),
        # DS-W/R1：对象干净但数据集已撤回
        task("T-W1", "DS-W", "R1", "P1", version="v1", completed="2026-10-02T12:00:00Z"),
        # DS-U/R1：对象干净但数据集未发布
        task("T-U1", "DS-U", "R1", "P1", version="v1", completed="2026-10-02T13:00:00Z"),
    ]
    objects = [
        obj("T-A1", pol="POL-1"),
        obj("T-A2", pol="POL-1"),
        obj("T-A3", pol="POL-1"),  # R2 现行为 POL-2 → 策略滞后
        obj("T-B1", verified="checksum-mismatch"),
        obj("T-C1", pol="POL-3"),
        obj("T-W1", pol="POL-1"),
        obj("T-U1", pol="POL-1"),
    ]
    return service_with(datasets=datasets, sources=sources, tasks=tasks, objects=objects)
