"""合成资料记录与加载。

按 ``docs/资料契约.md`` 原样承载 CSV 行，不在此层附加任何分析结论；
缺失值、争议状态、部分成功与待复核均保留原始含义。
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetRow:
    dataset_id: str
    owner_lab: str
    sensitivity: str
    release_version: str
    share_scope: str
    release_state: str


@dataclass(frozen=True)
class SourcePartitionRow:
    dataset_id: str
    partition_key: str
    source_version: str
    checksum: str
    updated_at: str
    withdrawal_state: str


@dataclass(frozen=True)
class ReplicaTaskRow:
    task_id: str
    dataset_id: str
    region: str
    partition_key: str
    target_version: str
    attempt: int
    task_state: str
    completed_at: str


@dataclass(frozen=True)
class ReplicaObjectRow:
    task_id: str
    object_key: str
    checksum: str
    metadata_version: str
    access_policy_version: str
    verified_state: str


@dataclass(frozen=True)
class ConsumerRequestRow:
    request_id: str
    consumer_lab: str
    dataset_id: str
    region: str
    requested_scope: str
    requested_at: str
    decision: str
    snapshot_ref: str


@dataclass(frozen=True)
class Materials:
    """一批原始合成资料。"""

    datasets: tuple[DatasetRow, ...]
    source_partitions: tuple[SourcePartitionRow, ...]
    replica_tasks: tuple[ReplicaTaskRow, ...]
    replica_objects: tuple[ReplicaObjectRow, ...]
    consumer_requests: tuple[ConsumerRequestRow, ...]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def load_materials(data_dir: str | Path) -> Materials:
    """从目录加载五份合成资料文件。"""
    root = Path(data_dir)
    datasets = tuple(DatasetRow(**row) for row in _read_csv(root / "datasets.csv"))
    sources = tuple(
        SourcePartitionRow(**row) for row in _read_csv(root / "source_partitions.csv")
    )
    tasks = tuple(
        ReplicaTaskRow(
            task_id=row["task_id"],
            dataset_id=row["dataset_id"],
            region=row["region"],
            partition_key=row["partition_key"],
            target_version=row["target_version"],
            attempt=int(row["attempt"]),
            task_state=row["task_state"],
            completed_at=row["completed_at"],
        )
        for row in _read_csv(root / "replica_tasks.csv")
    )
    objects = tuple(
        ReplicaObjectRow(**row) for row in _read_csv(root / "replica_objects.csv")
    )
    requests = tuple(
        ConsumerRequestRow(**row) for row in _read_csv(root / "consumer_requests.csv")
    )
    return Materials(
        datasets=datasets,
        source_partitions=sources,
        replica_tasks=tasks,
        replica_objects=objects,
        consumer_requests=requests,
    )
