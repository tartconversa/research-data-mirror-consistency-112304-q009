"""镜像副本折叠、就绪评估与接续计划。

核心原则：目录中的可用性必须反映真实的数据版本、完整性和访问范围，
不得把复制延迟、回报冲突或权限不一致解释为数据已就绪。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field

from .records import ReplicaObjectRow, ReplicaTaskRow
from .rules import (
    MISSING_METADATA,
    MISSING_POLICY,
    TaskState,
    VerifiedState,
    parse_instant,
    region_current_policy,
    version_key,
)


class Readiness(str, enum.Enum):
    """单个逻辑副本（数据集 × 区域 × 分区）的就绪判定。"""

    READY = "ready"  # 就绪：可复制、可校验、元数据与权限均为现行
    RELEASE_WITHDRAWN = "release-withdrawn"  # 数据集已被所有者撤回
    RELEASE_UNPUBLISHED = "release-unpublished"  # 数据集尚未发布（审核中）
    SOURCE_WITHDRAWN = "source-withdrawn"  # 源端分区已撤回
    SOURCE_STALE = "source-stale"  # 源端分区已被新修订取代
    ORPHAN_PARTITION = "orphan-partition"  # 镜像对应的源端分区不存在
    REPORT_CONFLICT = "report-conflict"  # 同一副本的多次回报互相矛盾
    INCOMPLETE = "incomplete"  # 复制未完成（排队或失败）
    INTEGRITY_FAILED = "integrity-failed"  # 校验未通过
    METADATA_MISSING = "metadata-missing"  # 只复制了对象，元数据未随附
    POLICY_MISSING = "policy-missing"  # 访问策略未随附
    POLICY_STALE = "policy-stale"  # 访问策略落后于区域现行版本
    VERSION_LAG = "version-lag"  # 副本版本落后于源端当前修订（复制延迟）


#: 就绪判定的优先级：排在前面的状态作为主状态输出。
_READINESS_ORDER = (
    Readiness.RELEASE_WITHDRAWN,
    Readiness.RELEASE_UNPUBLISHED,
    Readiness.SOURCE_WITHDRAWN,
    Readiness.SOURCE_STALE,
    Readiness.ORPHAN_PARTITION,
    Readiness.REPORT_CONFLICT,
    Readiness.INCOMPLETE,
    Readiness.INTEGRITY_FAILED,
    Readiness.METADATA_MISSING,
    Readiness.POLICY_MISSING,
    Readiness.POLICY_STALE,
    Readiness.VERSION_LAG,
    Readiness.READY,
)


@dataclass(frozen=True)
class MirrorReport:
    """一条任务回报与其对象回报的组合视图。"""

    task: ReplicaTaskRow
    object: ReplicaObjectRow | None


@dataclass
class MirrorReplica:
    """同一逻辑副本（数据集 × 区域 × 分区）的全部回报折叠结果。

    同一任务尝试可能被重复上报：以最新回报为准，但回报间在状态、校验、
    元数据或策略版本上的任何分歧都会被记录；未经复制平台主管核对前，
    该副本不得视为就绪。
    """

    dataset_id: str
    region: str
    partition_key: str
    reports: tuple[MirrorReport, ...] = ()
    reconciled: bool = False
    reconcile_note: str = ""
    reconciled_by: str = ""
    reconciled_at: str = ""

    @property
    def latest(self) -> MirrorReport | None:
        return self.reports[-1] if self.reports else None

    @property
    def last_report_at(self) -> str:
        latest = self.latest
        return latest.task.completed_at if latest else ""

    @property
    def max_attempt(self) -> int:
        return max((r.task.attempt for r in self.reports), default=0)

    def disagreements(self) -> dict[str, set[str]]:
        """各回报间不一致的字段及其出现过的取值。

        只比较与最新回报同一目标版本的回报：源端修订后开启的新一轮
        复制携带新的元数据/策略版本属正常演进，不构成回报冲突。
        """
        latest = self.latest
        if latest is None:
            return {}
        target = latest.task.target_version
        comparable = [r for r in self.reports if r.task.target_version == target]
        fields: dict[str, set[str]] = {
            "task_state": set(),
            "verified_state": set(),
            "metadata_version": set(),
            "access_policy_version": set(),
        }
        for report in comparable:
            fields["task_state"].add(report.task.task_state)
            if report.object is None:
                fields["verified_state"].add("<无对象回报>")
                fields["metadata_version"].add("<无对象回报>")
                fields["access_policy_version"].add("<无对象回报>")
            else:
                fields["verified_state"].add(report.object.verified_state)
                fields["metadata_version"].add(report.object.metadata_version)
                fields["access_policy_version"].add(
                    report.object.access_policy_version
                )
        return {name: values for name, values in fields.items() if len(values) > 1}

    @property
    def has_conflict(self) -> bool:
        return bool(self.disagreements())


def fold_replicas(
    tasks: list[ReplicaTaskRow] | tuple[ReplicaTaskRow, ...],
    objects: list[ReplicaObjectRow] | tuple[ReplicaObjectRow, ...],
) -> list[MirrorReplica]:
    """把任务与对象回报折叠为逻辑副本，按回报时间升序排列。"""
    object_by_task = {obj.task_id: obj for obj in objects}
    groups: dict[tuple[str, str, str], list[MirrorReport]] = {}
    for task in tasks:
        key = (task.dataset_id, task.region, task.partition_key)
        groups.setdefault(key, []).append(
            MirrorReport(task=task, object=object_by_task.get(task.task_id))
        )
    replicas: list[MirrorReplica] = []
    for (dataset_id, region, partition_key), reports in sorted(groups.items()):
        ordered = sorted(
            reports,
            key=lambda r: (parse_instant(r.task.completed_at), r.task.task_id),
        )
        replicas.append(
            MirrorReplica(
                dataset_id=dataset_id,
                region=region,
                partition_key=partition_key,
                reports=tuple(ordered),
            )
        )
    return replicas


@dataclass(frozen=True)
class ReplicaEvaluation:
    """单个逻辑副本的评估结果。"""

    replica: MirrorReplica
    readiness: Readiness
    ready: bool
    reasons: tuple[str, ...]
    source_version: str
    target_version: str
    policy_version: str
    consistent_at: str


def evaluate_replica(
    replica: MirrorReplica,
    *,
    release_state: str,
    source_version: str | None,
    source_withdrawal: str | None,
) -> ReplicaEvaluation:
    """评估逻辑副本是否可解释为“数据已就绪”。

    参数为副本所属数据集的发布状态、源端分区的当前版本与撤回状态；
    源端分区不存在时 ``source_version`` 与 ``source_withdrawal`` 传 ``None``。
    """
    reasons: list[str] = []
    found: set[Readiness] = set()
    latest = replica.latest
    target_version = latest.task.target_version if latest else ""
    policy_version = (
        latest.object.access_policy_version
        if latest and latest.object is not None
        else ""
    )

    if release_state == "withdrawn":
        found.add(Readiness.RELEASE_WITHDRAWN)
        reasons.append("数据集已被数据所有者撤回，任何镜像都不得继续显示为可用")
    elif release_state != "published":
        found.add(Readiness.RELEASE_UNPUBLISHED)
        reasons.append(f"数据集发布状态为 {release_state}，尚未发布，不进入可用目录")

    if source_version is None:
        found.add(Readiness.ORPHAN_PARTITION)
        reasons.append("镜像对应的源端分区不存在（可能已随撤回移除），副本身份无法确认")
    else:
        if source_withdrawal == "withdrawn":
            found.add(Readiness.SOURCE_WITHDRAWN)
            reasons.append("源端分区已撤回，镜像必须同步下线，不得继续显示为最新可用")
        elif source_withdrawal == "superseded":
            found.add(Readiness.SOURCE_STALE)
            reasons.append("源端分区已被新修订取代，现有镜像已过期，需按当前修订重新镜像")

    if latest is None:
        found.add(Readiness.INCOMPLETE)
        reasons.append("该逻辑副本尚无任何任务回报")
    else:
        if replica.has_conflict and not replica.reconciled:
            found.add(Readiness.REPORT_CONFLICT)
            parts = "、".join(
                f"{name}（{'/'.join(sorted(values))}）"
                for name, values in sorted(replica.disagreements().items())
            )
            same_round = sum(
                1
                for r in replica.reports
                if r.task.target_version == latest.task.target_version
            )
            reasons.append(
                f"同一副本同一目标版本的 {same_round} 条回报在 {parts} 上不一致，"
                "需复制平台主管核对后才能进入目录"
            )

        state = latest.task.task_state
        if state != TaskState.COPIED.value:
            found.add(Readiness.INCOMPLETE)
            if state == TaskState.QUEUED.value:
                reasons.append("复制任务仍在排队，尚未开始传输")
            elif state == TaskState.FAILED.value:
                reasons.append("最近一次复制尝试失败，等待重试")
            else:
                reasons.append(f"复制任务状态为 {state}，未完成")

        obj = latest.object
        if obj is None:
            found.add(Readiness.INTEGRITY_FAILED)
            reasons.append("缺少对象级回报，无法确认校验与元数据状态")
        else:
            if obj.verified_state == VerifiedState.CHECKSUM_MISMATCH.value:
                found.add(Readiness.INTEGRITY_FAILED)
                reasons.append("对象校验和与源端不一致，完整性未通过")
            if (
                obj.verified_state == VerifiedState.MISSING_META.value
                or obj.metadata_version == MISSING_METADATA
            ):
                found.add(Readiness.METADATA_MISSING)
                reasons.append("只复制了对象而元数据未随附，副本不具备完整身份")
            if obj.access_policy_version == MISSING_POLICY:
                found.add(Readiness.POLICY_MISSING)
                reasons.append("访问策略未随对象复制，访问范围无法确认")
            else:
                current = region_current_policy(replica.region)
                if current is not None and obj.access_policy_version != current:
                    found.add(Readiness.POLICY_STALE)
                    reasons.append(
                        f"副本访问策略版本 {obj.access_policy_version} 落后于区域 "
                        f"{replica.region} 现行版本 {current}，权限不一致不得解释为就绪"
                    )

        if (
            source_version is not None
            and version_key(target_version) != version_key(source_version)
        ):
            found.add(Readiness.VERSION_LAG)
            reasons.append(
                f"副本版本 {target_version} 落后于源端当前修订 {source_version}，"
                "属于复制延迟而非数据已就绪"
            )

    readiness = next((r for r in _READINESS_ORDER if r in found), Readiness.READY)
    if readiness is Readiness.READY:
        reasons.append("副本完整、已校验，元数据与访问策略均为区域现行版本")
    return ReplicaEvaluation(
        replica=replica,
        readiness=readiness,
        ready=readiness is Readiness.READY,
        reasons=tuple(reasons),
        source_version=source_version or "",
        target_version=target_version,
        policy_version=policy_version,
        consistent_at=replica.last_report_at if readiness is Readiness.READY else "",
    )


class Action(str, enum.Enum):
    """镜像接续动作。"""

    PROPAGATE_WITHDRAWAL = "propagate-withdrawal"  # 撤回传播：下线并墓碑化区域副本
    RESOLVE_CONFLICT = "resolve-conflict"  # 主管核对矛盾回报
    REPAIR_INTEGRITY = "repair-integrity"  # 重新传输并校验对象
    SYNC_METADATA = "sync-metadata"  # 补同步元数据
    SYNC_POLICY = "sync-policy"  # 按区域现行策略重同步访问策略
    REFRESH_SOURCE = "refresh-source"  # 源端已修订，按当前版本重新镜像
    RETRY_COPY = "retry-copy"  # 失败后重试
    RESUME_COPY = "resume-copy"  # 继续排队中的复制


#: 接续动作优先级：撤回传播最优先（安全），重试与排队靠后。
_ACTION_PRIORITY = {
    Action.PROPAGATE_WITHDRAWAL: 0,
    Action.RESOLVE_CONFLICT: 1,
    Action.REPAIR_INTEGRITY: 2,
    Action.SYNC_METADATA: 3,
    Action.SYNC_POLICY: 4,
    Action.REFRESH_SOURCE: 5,
    Action.RETRY_COPY: 6,
    Action.RESUME_COPY: 7,
}


@dataclass(frozen=True)
class ContinuationAction:
    """一条接续动作：由复制平台主管执行或派单。"""

    action: Action
    dataset_id: str
    region: str
    partition_key: str
    priority: int
    next_attempt: int | None
    reason: str


def plan_continuation(evaluations: list[ReplicaEvaluation]) -> list[ContinuationAction]:
    """根据评估结果生成镜像接续计划（按优先级与标识排序）。"""
    actions: list[ContinuationAction] = []

    def add(
        ev: ReplicaEvaluation,
        action: Action,
        reason: str,
        *,
        bump_attempt: bool = False,
    ) -> None:
        replica = ev.replica
        actions.append(
            ContinuationAction(
                action=action,
                dataset_id=replica.dataset_id,
                region=replica.region,
                partition_key=replica.partition_key,
                priority=_ACTION_PRIORITY[action],
                next_attempt=replica.max_attempt + 1 if bump_attempt else None,
                reason=reason,
            )
        )

    for ev in evaluations:
        if ev.ready:
            continue
        replica = ev.replica
        latest = replica.latest
        state = latest.task.task_state if latest else ""
        readiness = ev.readiness

        if readiness in (Readiness.RELEASE_WITHDRAWN, Readiness.SOURCE_WITHDRAWN):
            add(ev, Action.PROPAGATE_WITHDRAWAL, "源端已撤回：下线区域副本并记录墓碑，停止一切重试")
            continue
        if readiness is Readiness.ORPHAN_PARTITION:
            add(ev, Action.PROPAGATE_WITHDRAWAL, "源端分区已不存在：核对撤回记录后移除孤儿副本")
            continue
        if readiness is Readiness.RELEASE_UNPUBLISHED:
            continue  # 等待数据所有者发布，平台无可执行动作
        if readiness is Readiness.REPORT_CONFLICT:
            add(ev, Action.RESOLVE_CONFLICT, "同一副本回报互相矛盾：由复制平台主管核对后以最新事实为准")
            continue
        if readiness is Readiness.INTEGRITY_FAILED:
            add(ev, Action.REPAIR_INTEGRITY, "校验未通过：重新传输对象并复核校验和", bump_attempt=True)
            continue
        if readiness is Readiness.METADATA_MISSING:
            add(ev, Action.SYNC_METADATA, "补同步元数据与撤回状态，使副本具备完整身份")
            continue
        if readiness is Readiness.POLICY_MISSING:
            add(ev, Action.SYNC_POLICY, "补同步访问策略元数据")
            continue
        if readiness is Readiness.POLICY_STALE:
            add(ev, Action.SYNC_POLICY, "按区域现行访问策略版本重同步副本策略")
            continue
        if readiness in (Readiness.SOURCE_STALE, Readiness.VERSION_LAG):
            add(
                ev,
                Action.REFRESH_SOURCE,
                f"源端当前修订为 {ev.source_version}，副本停留在 {ev.target_version}：按当前修订重新镜像",
                bump_attempt=True,
            )
            continue
        if readiness is Readiness.INCOMPLETE:
            if state == TaskState.FAILED.value:
                add(ev, Action.RETRY_COPY, "最近一次复制失败：按指数退避重试", bump_attempt=True)
            else:
                add(ev, Action.RESUME_COPY, "复制任务排队中：继续执行")
            continue
    actions.sort(
        key=lambda a: (a.priority, a.dataset_id, a.region, a.partition_key)
    )
    return actions
