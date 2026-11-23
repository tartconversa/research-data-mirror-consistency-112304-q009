"""读取快照与撤回影响。

已开始的读取凭快照身份（snapshot_ref）固定其版本、分区与访问策略；
源端撤回或重新发布后，快照身份保留并附加中文影响说明，但不得再被
解释为“最新可用”。
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SnapshotPin:
    """可用性答复随附的快照锚点：开启读取时据此固定身份。"""

    dataset_id: str
    region: str
    versions: tuple[tuple[str, str], ...]  # (分区, 副本版本)，按分区排序
    policy_version: str
    consistent_at: str

    def versions_dict(self) -> dict[str, str]:
        return dict(self.versions)


@dataclass
class SnapshotLease:
    """一次已开始读取的快照租约。"""

    ref: str
    pin: SnapshotPin
    consumer_labs: tuple[str, ...]
    scope: str
    opened_at: str
    status: str = "active"  # active / closed
    current: bool = True  # 是否仍为“最新可用”快照
    notices: list[str] = field(default_factory=list)

    @property
    def dataset_id(self) -> str:
        return self.pin.dataset_id

    @property
    def partitions(self) -> tuple[str, ...]:
        return tuple(partition for partition, _ in self.pin.versions)


class SnapshotRegistry:
    """快照租约登记处：负责开启、关闭与撤回/重发布影响传播。"""

    def __init__(self) -> None:
        self._leases: dict[str, SnapshotLease] = {}
        self._counter = itertools.count(1)

    def open(
        self,
        pin: SnapshotPin,
        *,
        consumer_lab: str,
        scope: str,
        opened_at: str,
        ref: str | None = None,
    ) -> SnapshotLease:
        """按快照锚点开启租约；未指定 ref 时自动分配。"""
        lease_ref = ref or f"SNAP-NEW-{next(self._counter):04d}"
        if lease_ref in self._leases:
            raise ValueError(f"快照引用 {lease_ref} 已存在")
        lease = SnapshotLease(
            ref=lease_ref,
            pin=pin,
            consumer_labs=(consumer_lab,),
            scope=scope,
            opened_at=opened_at,
        )
        self._leases[lease_ref] = lease
        return lease

    def register(self, lease: SnapshotLease) -> SnapshotLease:
        """登记外部（如历史请求记录）构造的租约。"""
        if lease.ref in self._leases:
            existing = self._leases[lease.ref]
            labs = tuple(
                dict.fromkeys(existing.consumer_labs + lease.consumer_labs)
            )
            existing.consumer_labs = labs
            return existing
        self._leases[lease.ref] = lease
        return lease

    def get(self, ref: str) -> SnapshotLease | None:
        return self._leases.get(ref)

    def close(self, ref: str) -> SnapshotLease:
        lease = self._leases[ref]
        lease.status = "closed"
        return lease

    def leases(self, *, dataset_id: str | None = None) -> list[SnapshotLease]:
        result = list(self._leases.values())
        if dataset_id is not None:
            result = [l for l in result if l.dataset_id == dataset_id]
        return result

    def _notify(self, lease: SnapshotLease, notice: str) -> None:
        if lease.status != "active":
            return
        lease.current = False
        lease.notices.append(notice)

    def on_dataset_withdrawn(self, dataset_id: str, at: str) -> list[SnapshotLease]:
        """数据集撤回：受影响租约保留快照身份，附中文影响说明。"""
        affected = []
        for lease in self.leases(dataset_id=dataset_id):
            if lease.status != "active":
                continue
            if lease.pin.versions:
                identity = "、".join(
                    f"{partition}@{version}" for partition, version in lease.pin.versions
                )
            else:
                identity = "历史快照，版本以请求记录为准"
            self._notify(
                lease,
                f"数据集 {dataset_id} 已于 {at} 被数据所有者撤回。快照 {lease.ref} "
                f"（{identity}）身份保留，已开始的读取可凭该快照完成；但撤回后不得"
                "据此快照开启新的读取、开展新的分析或再分发，目录中该快照不再视为最新可用。",
            )
            affected.append(lease)
        return affected

    def on_partition_withdrawn(
        self, dataset_id: str, partition_key: str, at: str
    ) -> list[SnapshotLease]:
        """分区撤回：仅影响覆盖该分区的租约。"""
        affected = []
        for lease in self.leases(dataset_id=dataset_id):
            if lease.status != "active" or partition_key not in lease.partitions:
                continue
            self._notify(
                lease,
                f"分区 {partition_key} 已于 {at} 撤回。快照 {lease.ref} 身份保留，"
                "已开始的读取可完成；但该分区内容撤回后不得用于新的分析或再分发。",
            )
            affected.append(lease)
        return affected

    def on_republished(
        self, dataset_id: str, new_version: str, at: str
    ) -> list[SnapshotLease]:
        """重新发布：旧版本快照保留身份，但不再是最新。"""
        affected = []
        for lease in self.leases(dataset_id=dataset_id):
            if lease.status != "active":
                continue
            self._notify(
                lease,
                f"数据集 {dataset_id} 已于 {at} 重新发布为 {new_version}。快照 "
                f"{lease.ref} 仍固定于其原版本，读取结果保持可复现，但不再代表最新"
                "可用数据；需要最新结果时请按新版本重新申请。",
            )
            affected.append(lease)
        return affected
