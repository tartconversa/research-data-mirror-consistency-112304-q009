"""跨区域数据产品目录与按范围可用性查询。

目录条目面向消费者展示：副本版本、最后一致时间、覆盖分区与本地访问许可；
可用性按请求范围（whole-release / partition-only / analysis）推导，
任何复制延迟、回报冲突或权限不一致都不会被解释为数据已就绪。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass

from .mirror import ReplicaEvaluation, Readiness
from .records import DatasetRow, SourcePartitionRow
from .rules import Scope, parse_instant, region_current_policy


class Permission(str, enum.Enum):
    """本地访问许可（由区域数据管理员政策确认）。"""

    ALLOW = "allow"  # 允许
    NEEDS_APPROVAL = "needs-approval"  # 需区域数据管理员或所有者审批
    DENY = "deny"  # 拒绝


def local_permission(
    *, share_scope: str, sensitivity: str, consumer_lab: str, owner_lab: str
) -> tuple[Permission, str]:
    """按共享范围 × 敏感级别 × 消费者身份计算本地访问许可。

    矩阵（合成政策，由区域数据管理员维护）：

    - ``none``：一律拒绝（所有者未批准共享）；
    - ``project``：所有者实验室按敏感级别放行，其他实验室需所有者项目授权；
    - ``consortium``：联盟成员放行，但 controlled 需区域确认、restricted 仅所有者。
    """
    is_owner = consumer_lab == owner_lab
    if share_scope == "none":
        return Permission.DENY, "数据所有者未批准任何共享范围"
    if share_scope == "project" and not is_owner:
        if sensitivity == "restricted":
            return Permission.DENY, "restricted 数据仅所有者实验室可在项目范围内访问"
        return Permission.NEEDS_APPROVAL, "项目范围共享：需数据所有者对该实验室的项目授权"
    if sensitivity == "restricted":
        if is_owner:
            return Permission.NEEDS_APPROVAL, "restricted 数据：所有者实验室亦需区域数据管理员审批"
        return Permission.DENY, "restricted 数据不对联盟成员开放"
    if sensitivity == "controlled":
        return Permission.NEEDS_APPROVAL, "controlled 数据：需区域数据管理员确认本地访问政策"
    return Permission.ALLOW, "open 数据：本地访问政策允许"


@dataclass(frozen=True)
class PartitionAvailability:
    """单个分区在区域内的可用性视图。"""

    partition_key: str
    ready: bool
    readiness: Readiness
    replica_version: str
    source_version: str
    consistent_at: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CatalogEntry:
    """目录条目：一个数据集在一个区域内的消费者视图。"""

    dataset_id: str
    region: str
    owner_lab: str
    sensitivity: str
    share_scope: str
    release_state: str
    release_version: str
    status: str  # available / partial / building / inconsistent / stale / withdrawn / unpublished
    replica_versions: tuple[tuple[str, str], ...]  # (分区, 副本版本)
    covered_partitions: tuple[str, ...]  # 已就绪分区
    pending_partitions: tuple[tuple[str, tuple[str, ...]], ...]  # (分区, 原因)
    last_consistent_at: str  # 最后一致时间：全部就绪分区一致时间的最小值
    access_owner: Permission
    access_other: Permission
    reasons: tuple[str, ...]

    def versions_dict(self) -> dict[str, str]:
        return dict(self.replica_versions)


@dataclass(frozen=True)
class AvailabilityAnswer:
    """按范围可用性查询的答复。"""

    decision: str  # allow / partial / deny
    dataset_id: str
    region: str
    scope: str
    consumer_lab: str
    permission: Permission
    permission_reason: str
    covered_partitions: tuple[str, ...]
    missing_partitions: tuple[tuple[str, tuple[str, ...]], ...]
    replica_versions: tuple[tuple[str, str], ...]
    consistent_at: str
    reasons: tuple[str, ...]
    snapshot: object | None  # SnapshotPin；仅在 allow/partial 时提供

    def versions_dict(self) -> dict[str, str]:
        return dict(self.replica_versions)


def _entry_status(
    dataset: DatasetRow,
    partitions: tuple[PartitionAvailability, ...],
    active_source_partitions: tuple[str, ...],
    source_states: set[str],
) -> str:
    if dataset.release_state == "withdrawn":
        return "withdrawn"
    if dataset.release_state != "published":
        return "unpublished"
    if not active_source_partitions:
        # 全部源端分区失效：区分“已撤回”与“已被新修订取代”
        return "stale" if "superseded" in source_states else "withdrawn"
    ready_count = sum(1 for p in partitions if p.ready)
    if ready_count == len(active_source_partitions) and ready_count > 0:
        return "available"
    if ready_count > 0:
        return "partial"
    order = (
        (Readiness.REPORT_CONFLICT, Readiness.INTEGRITY_FAILED,
         Readiness.METADATA_MISSING, Readiness.POLICY_MISSING, Readiness.POLICY_STALE),
        (Readiness.SOURCE_STALE, Readiness.VERSION_LAG, Readiness.SOURCE_WITHDRAWN),
    )
    for group, status in zip(order, ("inconsistent", "stale")):
        if any(p.readiness in group for p in partitions):
            return status
    return "building"


class Catalog:
    """跨区域数据产品目录：由数据集、源端分区与镜像评估构建。"""

    def __init__(
        self,
        datasets: dict[str, DatasetRow],
        source_partitions: dict[tuple[str, str], SourcePartitionRow],
        evaluations: list[ReplicaEvaluation],
    ) -> None:
        self._datasets = datasets
        self._sources = source_partitions
        by_entry: dict[tuple[str, str], list[ReplicaEvaluation]] = {}
        for ev in evaluations:
            by_entry.setdefault((ev.replica.dataset_id, ev.replica.region), []).append(ev)
        self._entries: dict[tuple[str, str], CatalogEntry] = {}
        for (dataset_id, region), evs in sorted(by_entry.items()):
            dataset = datasets.get(dataset_id)
            if dataset is None:
                continue
            self._entries[(dataset_id, region)] = self._build_entry(
                dataset, region, evs
            )

    def _build_entry(
        self, dataset: DatasetRow, region: str, evs: list[ReplicaEvaluation]
    ) -> CatalogEntry:
        active_sources = tuple(
            sorted(
                key[1]
                for key, row in self._sources.items()
                if key[0] == dataset.dataset_id and row.withdrawal_state == "active"
            )
        )
        source_states = {
            row.withdrawal_state
            for key, row in self._sources.items()
            if key[0] == dataset.dataset_id
        }
        parts: list[PartitionAvailability] = []
        for ev in sorted(evs, key=lambda e: e.replica.partition_key):
            parts.append(
                PartitionAvailability(
                    partition_key=ev.replica.partition_key,
                    ready=ev.ready,
                    readiness=ev.readiness,
                    replica_version=ev.target_version,
                    source_version=ev.source_version,
                    consistent_at=ev.consistent_at,
                    reasons=ev.reasons,
                )
            )
        covered = tuple(p.partition_key for p in parts if p.ready)
        pending = tuple((p.partition_key, p.reasons) for p in parts if not p.ready)
        ready_times = [p.consistent_at for p in parts if p.ready and p.consistent_at]
        last_consistent = (
            min(ready_times, key=parse_instant) if ready_times else ""
        )
        access_owner, _ = local_permission(
            share_scope=dataset.share_scope,
            sensitivity=dataset.sensitivity,
            consumer_lab=dataset.owner_lab,
            owner_lab=dataset.owner_lab,
        )
        access_other, _ = local_permission(
            share_scope=dataset.share_scope,
            sensitivity=dataset.sensitivity,
            consumer_lab="<其他实验室>",
            owner_lab=dataset.owner_lab,
        )
        reasons: list[str] = []
        for part in parts:
            reasons.extend(part.reasons)
        status = _entry_status(dataset, tuple(parts), active_sources, source_states)
        return CatalogEntry(
            dataset_id=dataset.dataset_id,
            region=region,
            owner_lab=dataset.owner_lab,
            sensitivity=dataset.sensitivity,
            share_scope=dataset.share_scope,
            release_state=dataset.release_state,
            release_version=dataset.release_version,
            status=status,
            replica_versions=tuple(
                (p.partition_key, p.replica_version) for p in parts
            ),
            covered_partitions=covered,
            pending_partitions=pending,
            last_consistent_at=last_consistent,
            access_owner=access_owner,
            access_other=access_other,
            reasons=tuple(reasons),
        )

    def entries(self) -> list[CatalogEntry]:
        return list(self._entries.values())

    def entry(self, dataset_id: str, region: str) -> CatalogEntry | None:
        return self._entries.get((dataset_id, region))

    def availability(
        self,
        *,
        dataset_id: str,
        region: str,
        scope: str,
        consumer_lab: str,
        partition_key: str | None = None,
    ) -> AvailabilityAnswer:
        """按范围查询可用性：whole-release / partition-only / analysis。"""
        from .snapshots import SnapshotPin  # 避免环：catalog 不依赖快照注册

        dataset = self._datasets.get(dataset_id)
        reasons: list[str] = []
        if dataset is None:
            return AvailabilityAnswer(
                decision="deny",
                dataset_id=dataset_id,
                region=region,
                scope=scope,
                consumer_lab=consumer_lab,
                permission=Permission.DENY,
                permission_reason="数据集不存在",
                covered_partitions=(),
                missing_partitions=(),
                replica_versions=(),
                consistent_at="",
                reasons=(f"数据集 {dataset_id} 不存在",),
                snapshot=None,
            )
        permission, permission_reason = local_permission(
            share_scope=dataset.share_scope,
            sensitivity=dataset.sensitivity,
            consumer_lab=consumer_lab,
            owner_lab=dataset.owner_lab,
        )
        if dataset.release_state == "withdrawn":
            reasons.append("数据集已撤回，任何范围均不可用")
        elif dataset.release_state != "published":
            reasons.append(f"数据集发布状态为 {dataset.release_state}，尚未发布")

        active_sources = {
            key[1]: row
            for key, row in self._sources.items()
            if key[0] == dataset_id and row.withdrawal_state == "active"
        }
        if scope == Scope.PARTITION_ONLY.value:
            if partition_key is None:
                reasons.append("partition-only 范围必须指定分区")
                needed: tuple[str, ...] = ()
            elif partition_key not in {
                key[1] for key in self._sources if key[0] == dataset_id
            }:
                reasons.append(f"分区 {partition_key} 不存在于数据集 {dataset_id}")
                needed = ()
            else:
                needed = (partition_key,)
        elif scope in (Scope.WHOLE_RELEASE.value, Scope.ANALYSIS.value):
            needed = tuple(sorted(active_sources))
            if not needed:
                reasons.append("数据集当前没有有效的源端分区")
        else:
            reasons.append(f"未知请求范围 {scope}")
            needed = ()

        entry = self._entries.get((dataset_id, region))
        if entry is None:
            reasons.append(f"区域 {region} 没有该数据集的本地镜像")
        part_status: dict[str, PartitionAvailability] = {}
        if entry is not None:
            # 重新按分区评估视图（entry 已聚合，此处按分区取回）
            for pk, version in entry.replica_versions:
                ready = pk in entry.covered_partitions
                part_reasons = dict(entry.pending_partitions).get(pk, ())
                part_status[pk] = PartitionAvailability(
                    partition_key=pk,
                    ready=ready,
                    readiness=Readiness.READY if ready else Readiness.INCOMPLETE,
                    replica_version=version,
                    source_version="",
                    consistent_at=entry.last_consistent_at if ready else "",
                    reasons=part_reasons,
                )

        covered: list[str] = []
        missing: list[tuple[str, tuple[str, ...]]] = []
        for pk in needed:
            status = part_status.get(pk)
            if status is not None and status.ready:
                covered.append(pk)
            elif status is not None:
                missing.append((pk, status.reasons or ("副本未就绪",)))
            else:
                missing.append((pk, (f"区域 {region} 尚无该分区镜像",)))

        if needed and len(covered) == len(needed):
            level = "full"
        elif covered:
            level = "partial"
        else:
            level = "none"

        if permission is Permission.DENY or level == "none":
            decision = "deny"
        elif permission is Permission.NEEDS_APPROVAL or level == "partial":
            decision = "partial"
        else:
            decision = "allow"

        if level == "partial":
            reasons.append("仅部分分区就绪，按范围只能提供部分可用")
        for pk, pk_reasons in missing:
            reasons.append(f"分区 {pk} 不可用：{'；'.join(pk_reasons)}")
        if scope == Scope.ANALYSIS.value and decision != "deny":
            reasons.append("analysis 范围：快照将固定访问策略版本以便审计")

        snapshot = None
        if decision in ("allow", "partial") and covered:
            versions = tuple(
                sorted(
                    (pk, part_status[pk].replica_version) for pk in covered
                )
            )
            snapshot = SnapshotPin(
                dataset_id=dataset_id,
                region=region,
                versions=versions,
                policy_version=region_current_policy(region) or "",
                consistent_at=entry.last_consistent_at if entry else "",
            )
        return AvailabilityAnswer(
            decision=decision,
            dataset_id=dataset_id,
            region=region,
            scope=scope,
            consumer_lab=consumer_lab,
            permission=permission,
            permission_reason=permission_reason,
            covered_partitions=tuple(covered),
            missing_partitions=tuple(missing),
            replica_versions=tuple(
                sorted((pk, part_status[pk].replica_version) for pk in covered)
            ),
            consistent_at=entry.last_consistent_at if entry else "",
            reasons=tuple(reasons),
            snapshot=snapshot,
        )
