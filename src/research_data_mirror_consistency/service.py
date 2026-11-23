"""数据平台门面：目录、可用性、接续计划与生命周期事件。

角色分工（合成设定）：

- 数据所有者：批准共享范围、执行撤回与重新发布；
- 复制平台主管：管理分区复制与完整性，核对矛盾回报、执行接续计划；
- 区域数据管理员：确认区域现行访问策略（见 ``rules.REGION_CURRENT_POLICY``）；
- 消费者：据目录选择可用镜像并凭快照读取。
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from .catalog import AvailabilityAnswer, Catalog, CatalogEntry
from .mirror import (
    ContinuationAction,
    MirrorReplica,
    ReplicaEvaluation,
    evaluate_replica,
    fold_replicas,
    plan_continuation,
)
from .records import (
    ConsumerRequestRow,
    DatasetRow,
    Materials,
    ReplicaObjectRow,
    ReplicaTaskRow,
    SourcePartitionRow,
    load_materials,
)
from .rules import parse_instant
from .snapshots import SnapshotLease, SnapshotPin, SnapshotRegistry


class DataPlatformService:
    """跨区域数据产品目录、镜像接续与按范围可用性查询的一体化门面。"""

    def __init__(self, materials: Materials) -> None:
        self._datasets: dict[str, DatasetRow] = {d.dataset_id: d for d in materials.datasets}
        self._source_rows: list[SourcePartitionRow] = list(materials.source_partitions)
        self._tasks: list[ReplicaTaskRow] = list(materials.replica_tasks)
        self._objects: list[ReplicaObjectRow] = list(materials.replica_objects)
        self._requests: list[ConsumerRequestRow] = list(materials.consumer_requests)
        self.registry = SnapshotRegistry()
        self._reconciled: dict[tuple[str, str, str], tuple[str, str, str]] = {}
        self._import_historical_leases()
        self._rebuild()

    @classmethod
    def from_data_dir(cls, data_dir: str | Path) -> "DataPlatformService":
        return cls(load_materials(data_dir))

    # ------------------------------------------------------------------ 构建

    def _fold_sources(self) -> dict[tuple[str, str], SourcePartitionRow]:
        """同一分区的多条源端记录按 updated_at 折叠，最新为准。"""
        latest: dict[tuple[str, str], SourcePartitionRow] = {}
        for row in self._source_rows:
            key = (row.dataset_id, row.partition_key)
            if key not in latest or parse_instant(row.updated_at) > parse_instant(
                latest[key].updated_at
            ):
                latest[key] = row
        return latest

    def _rebuild(self) -> None:
        self._sources = self._fold_sources()
        replicas = fold_replicas(self._tasks, self._objects)
        for replica in replicas:
            key = (replica.dataset_id, replica.region, replica.partition_key)
            if key in self._reconciled:
                note, by, at = self._reconciled[key]
                replica.reconciled = True
                replica.reconcile_note = note
                replica.reconciled_by = by
                replica.reconciled_at = at
        self._replicas: list[MirrorReplica] = replicas
        self._evaluations: list[ReplicaEvaluation] = []
        for replica in replicas:
            dataset = self._datasets.get(replica.dataset_id)
            source = self._sources.get((replica.dataset_id, replica.partition_key))
            self._evaluations.append(
                evaluate_replica(
                    replica,
                    release_state=dataset.release_state if dataset else "review",
                    source_version=source.source_version if source else None,
                    source_withdrawal=source.withdrawal_state if source else None,
                )
            )
        self._catalog = Catalog(self._datasets, self._sources, self._evaluations)

    def _import_historical_leases(self) -> None:
        """把历史消费请求登记为已开始的读取快照（版本信息以请求记录为准）。"""
        by_ref: dict[str, list[ConsumerRequestRow]] = {}
        for request in self._requests:
            by_ref.setdefault(request.snapshot_ref, []).append(request)
        for ref, rows in by_ref.items():
            first = min(rows, key=lambda r: parse_instant(r.requested_at))
            labs = tuple(dict.fromkeys(r.consumer_lab for r in rows))
            lease = SnapshotLease(
                ref=ref,
                pin=SnapshotPin(
                    dataset_id=first.dataset_id,
                    region=first.region,
                    versions=(),
                    policy_version="",
                    consistent_at="",
                ),
                consumer_labs=labs,
                scope=first.requested_scope,
                opened_at=first.requested_at,
            )
            self.registry.register(lease)

    # ------------------------------------------------------------ 查询能力

    def catalog_entries(self) -> list[CatalogEntry]:
        return self._catalog.entries()

    def catalog_entry(self, dataset_id: str, region: str) -> CatalogEntry | None:
        return self._catalog.entry(dataset_id, region)

    def availability(
        self,
        *,
        dataset_id: str,
        region: str,
        scope: str,
        consumer_lab: str,
        partition_key: str | None = None,
    ) -> AvailabilityAnswer:
        return self._catalog.availability(
            dataset_id=dataset_id,
            region=region,
            scope=scope,
            consumer_lab=consumer_lab,
            partition_key=partition_key,
        )

    def continuation_plan(self) -> list[ContinuationAction]:
        return plan_continuation(self._evaluations)

    def evaluations(self) -> list[ReplicaEvaluation]:
        return list(self._evaluations)

    # ------------------------------------------------------------ 快照读取

    def open_snapshot(
        self,
        answer: AvailabilityAnswer,
        *,
        consumer_lab: str,
        opened_at: str,
        ref: str | None = None,
    ) -> SnapshotLease:
        """凭可用性答复开启读取快照；答复不可用时拒绝。"""
        if answer.snapshot is None or answer.decision == "deny":
            raise ValueError("可用性答复为 deny，不能开启读取快照")
        return self.registry.open(
            answer.snapshot,
            consumer_lab=consumer_lab,
            scope=answer.scope,
            opened_at=opened_at,
            ref=ref,
        )

    def close_snapshot(self, ref: str) -> SnapshotLease:
        return self.registry.close(ref)

    def snapshot(self, ref: str) -> SnapshotLease | None:
        return self.registry.get(ref)

    def snapshots(self, *, dataset_id: str | None = None) -> list[SnapshotLease]:
        return self.registry.leases(dataset_id=dataset_id)

    # ------------------------------------------------------------ 生命周期事件

    def withdraw_dataset(
        self, dataset_id: str, *, at: str, by: str = "数据所有者"
    ) -> dict:
        """数据所有者撤回数据集：镜像下线、快照附加影响说明。"""
        dataset = self._datasets[dataset_id]
        self._datasets[dataset_id] = replace(dataset, release_state="withdrawn")
        self._source_rows = [
            replace(row, withdrawal_state="withdrawn", updated_at=at)
            if row.dataset_id == dataset_id
            else row
            for row in self._source_rows
        ]
        affected = self.registry.on_dataset_withdrawn(dataset_id, at)
        self._rebuild()
        return {
            "event": "withdraw-dataset",
            "dataset_id": dataset_id,
            "by": by,
            "at": at,
            "affected_snapshots": [lease.ref for lease in affected],
            "continuation": [
                a for a in self.continuation_plan() if a.dataset_id == dataset_id
            ],
        }

    def withdraw_partition(
        self, dataset_id: str, partition_key: str, *, at: str, by: str = "数据所有者"
    ) -> dict:
        """数据所有者撤回单个分区。"""
        self._source_rows = [
            replace(row, withdrawal_state="withdrawn", updated_at=at)
            if row.dataset_id == dataset_id and row.partition_key == partition_key
            else row
            for row in self._source_rows
        ]
        affected = self.registry.on_partition_withdrawn(dataset_id, partition_key, at)
        self._rebuild()
        return {
            "event": "withdraw-partition",
            "dataset_id": dataset_id,
            "partition_key": partition_key,
            "by": by,
            "at": at,
            "affected_snapshots": [lease.ref for lease in affected],
            "continuation": [
                a for a in self.continuation_plan() if a.dataset_id == dataset_id
            ],
        }

    def republish(
        self, dataset_id: str, new_version: str, *, at: str, by: str = "数据所有者"
    ) -> dict:
        """数据所有者重新发布：旧镜像立即过期，不得继续显示为最新可用。"""
        dataset = self._datasets[dataset_id]
        self._datasets[dataset_id] = replace(
            dataset, release_state="published", release_version=new_version
        )
        self._source_rows = [
            replace(
                row,
                source_version=new_version,
                withdrawal_state="active",
                updated_at=at,
            )
            if row.dataset_id == dataset_id
            else row
            for row in self._source_rows
        ]
        affected = self.registry.on_republished(dataset_id, new_version, at)
        self._rebuild()
        return {
            "event": "republish",
            "dataset_id": dataset_id,
            "new_version": new_version,
            "by": by,
            "at": at,
            "affected_snapshots": [lease.ref for lease in affected],
            "continuation": [
                a for a in self.continuation_plan() if a.dataset_id == dataset_id
            ],
        }

    # ------------------------------------------------------------ 复制平台主管

    def reconcile_reports(
        self,
        dataset_id: str,
        region: str,
        partition_key: str,
        *,
        by: str,
        note: str,
        at: str,
    ) -> None:
        """主管核对矛盾回报：以最新回报为准，解除回报冲突标记。"""
        key = (dataset_id, region, partition_key)
        self._reconciled[key] = (note, by, at)
        self._rebuild()

    def apply_task_report(
        self,
        *,
        dataset_id: str,
        region: str,
        partition_key: str,
        task_id: str,
        attempt: int,
        task_state: str,
        completed_at: str,
        object_key: str,
        checksum: str,
        metadata_version: str,
        access_policy_version: str,
        verified_state: str,
    ) -> ReplicaEvaluation:
        """登记一条新的任务回报（接续动作的执行结果），返回最新评估。"""
        self._tasks.append(
            ReplicaTaskRow(
                task_id=task_id,
                dataset_id=dataset_id,
                region=region,
                partition_key=partition_key,
                target_version=self._sources[(dataset_id, partition_key)].source_version,
                attempt=attempt,
                task_state=task_state,
                completed_at=completed_at,
            )
        )
        self._objects.append(
            ReplicaObjectRow(
                task_id=task_id,
                object_key=object_key,
                checksum=checksum,
                metadata_version=metadata_version,
                access_policy_version=access_policy_version,
                verified_state=verified_state,
            )
        )
        self._rebuild()
        for ev in self._evaluations:
            r = ev.replica
            if (
                r.dataset_id == dataset_id
                and r.region == region
                and r.partition_key == partition_key
            ):
                return ev
        raise KeyError((dataset_id, region, partition_key))
