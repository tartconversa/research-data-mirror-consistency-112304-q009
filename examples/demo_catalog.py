"""目录、可用性与生命周期的可运行示例（合成数据）。

运行：python3 examples/demo_catalog.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from research_data_mirror_consistency import DataPlatformService  # noqa: E402

service = DataPlatformService.from_data_dir(ROOT / "data")

print("== 跨区域数据产品目录（前 6 条）==")
for entry in service.catalog_entries()[:6]:
    print(
        f"{entry.dataset_id}@{entry.region} 状态={entry.status} "
        f"发布={entry.release_state} 覆盖分区={list(entry.covered_partitions)} "
        f"最后一致时间={entry.last_consistent_at or '—'}"
    )
    for partition, reasons in entry.pending_partitions:
        print(f"    待处理 {partition}: {reasons[0]}")

print("\n== 按范围可用性查询 =")
entry = service.catalog_entries()[0]
answer = service.availability(
    dataset_id=entry.dataset_id,
    region=entry.region,
    scope="whole-release",
    consumer_lab="LAB-00",
)
print(f"{answer.dataset_id}@{answer.region} 决定={answer.decision} 许可={answer.permission.value}")
for reason in answer.reasons:
    print(f"    - {reason}")

print("\n== 镜像接续计划（前 8 条，按优先级）==")
for action in service.continuation_plan()[:8]:
    print(
        f"[P{action.priority}] {action.action.value} "
        f"{action.dataset_id}@{action.region}/{action.partition_key}: {action.reason}"
    )

print("\n== 撤回事件对快照的影响 =")
lease = service.snapshot("SNAP-001")
print(f"撤回前: {lease.ref} 数据集={lease.dataset_id} current={lease.current}")
impact = service.withdraw_dataset(lease.dataset_id, at="2026-11-24T00:00:00Z")
print(f"受影响快照: {impact['affected_snapshots']}")
for notice in lease.notices:
    print(f"    通知: {notice}")
