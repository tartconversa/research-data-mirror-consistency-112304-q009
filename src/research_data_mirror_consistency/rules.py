"""共享枚举、区域策略常量与版本/时间工具。

所有取值与 ``data/`` 下合成资料保持一致；本模块只做定义，不做业务判断。
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone


class TaskState(str, enum.Enum):
    """镜像任务回报状态（replica_tasks.task_state）。"""

    QUEUED = "queued"
    COPIED = "copied"
    FAILED = "failed"


class VerifiedState(str, enum.Enum):
    """副本对象校验状态（replica_objects.verified_state）。"""

    VERIFIED = "verified"
    MISSING_META = "missing-meta"
    CHECKSUM_MISMATCH = "checksum-mismatch"


class WithdrawalState(str, enum.Enum):
    """源端分区撤回状态（source_partitions.withdrawal_state）。"""

    ACTIVE = "active"
    SUPERSEDED = "superseded"
    WITHDRAWN = "withdrawn"


class ReleaseState(str, enum.Enum):
    """数据集发布状态（datasets.release_state）。"""

    PUBLISHED = "published"
    REVIEW = "review"
    WITHDRAWN = "withdrawn"


class ShareScope(str, enum.Enum):
    """数据所有者批准的共享范围（datasets.share_scope）。"""

    NONE = "none"
    PROJECT = "project"
    CONSORTIUM = "consortium"


class Scope(str, enum.Enum):
    """消费者请求范围（consumer_requests.requested_scope）。"""

    WHOLE_RELEASE = "whole-release"
    PARTITION_ONLY = "partition-only"
    ANALYSIS = "analysis"


#: 元数据版本哨兵值：表示对象已复制但元数据未随附。
MISSING_METADATA = "META-0"

#: 访问策略版本哨兵值：表示对象已复制但访问策略未随附。
MISSING_POLICY = "POL-0"

#: 区域现行访问策略版本（合成设定，由区域数据管理员确认并维护）。
#: 副本对象随附的 access_policy_version 必须与所在区域的现行版本一致，
#: 否则视为权限元数据滞后，不得解释为数据已就绪。
REGION_CURRENT_POLICY: dict[str, str] = {
    "R1": "POL-1",
    "R2": "POL-2",
    "R3": "POL-3",
}


def region_current_policy(region: str) -> str | None:
    """返回区域现行访问策略版本；未知区域返回 ``None``。"""
    return REGION_CURRENT_POLICY.get(region)


def version_key(version: str) -> tuple[int, object]:
    """把 ``v1``..``v6`` 这类版本号转成可比较的键；非数字版本按字符串处理。"""
    text = str(version)
    if len(text) > 1 and text[0] in "vV" and text[1:].isdigit():
        return (0, int(text[1:]))
    return (1, text)


def parse_instant(value: str) -> datetime:
    """解析 ISO 日期或日期时间（支持 ``Z`` 后缀），统一为 UTC 感知时间。"""
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    instant = datetime.fromisoformat(text)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant
