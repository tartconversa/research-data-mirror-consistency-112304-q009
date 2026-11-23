"""跨区域目录与按范围可用性查询的合成测试。"""
import unittest

from helpers import DATA_DIR, mirror_service

from research_data_mirror_consistency import DataPlatformService, Permission


class CsvCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = DataPlatformService.from_data_dir(DATA_DIR)
        cls.entries = cls.service.catalog_entries()

    def test_entry_count_and_reasons(self):
        self.assertEqual(len(self.entries), 42)
        for entry in self.entries:
            self.assertTrue(entry.reasons, entry.dataset_id)

    def test_no_entry_is_available_or_partial(self):
        """合成资料中权限回报普遍矛盾：目录不得出现任何可用条目。"""
        self.assertFalse(
            [e for e in self.entries if e.status in ("available", "partial")]
        )

    def test_withdrawn_datasets_show_withdrawn(self):
        for entry in self.entries:
            if entry.release_state == "withdrawn":
                self.assertEqual(entry.status, "withdrawn")
                self.assertEqual(entry.covered_partitions, ())

    def test_unpublished_datasets_show_unpublished(self):
        for entry in self.entries:
            if entry.release_state == "review":
                self.assertEqual(entry.status, "unpublished")

    def test_covered_partitions_are_active_source_partitions(self):
        sources = self.service._sources
        for entry in self.entries:
            active = {
                key[1]
                for key, row in sources.items()
                if key[0] == entry.dataset_id and row.withdrawal_state == "active"
            }
            self.assertTrue(set(entry.covered_partitions) <= active)

    def test_consistent_at_only_when_covered(self):
        for entry in self.entries:
            if entry.covered_partitions:
                self.assertTrue(entry.last_consistent_at)
            else:
                self.assertEqual(entry.last_consistent_at, "")

    def test_availability_denies_withdrawn_dataset(self):
        withdrawn = next(e for e in self.entries if e.release_state == "withdrawn")
        answer = self.service.availability(
            dataset_id=withdrawn.dataset_id,
            region=withdrawn.region,
            scope="whole-release",
            consumer_lab="LAB-00",
        )
        self.assertEqual(answer.decision, "deny")
        self.assertIsNone(answer.snapshot)
        self.assertTrue(any("撤回" in r for r in answer.reasons))

    def test_availability_denies_unknown_region_and_dataset(self):
        entry = self.entries[0]
        answer = self.service.availability(
            dataset_id=entry.dataset_id,
            region="R9",
            scope="whole-release",
            consumer_lab="LAB-00",
        )
        self.assertEqual(answer.decision, "deny")
        self.assertTrue(any("没有该数据集的本地镜像" in r for r in answer.reasons))
        missing = self.service.availability(
            dataset_id="DS-999",
            region="R1",
            scope="whole-release",
            consumer_lab="LAB-00",
        )
        self.assertEqual(missing.decision, "deny")


class SyntheticCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = mirror_service()

    def test_available_entry_fields(self):
        entry = self.service.catalog_entry("DS-A", "R1")
        self.assertEqual(entry.status, "available")
        self.assertEqual(set(entry.covered_partitions), {"P1", "P2"})
        self.assertEqual(entry.versions_dict(), {"P1": "v2", "P2": "v2"})
        # 最后一致时间取全部就绪分区一致时间的最小值
        self.assertEqual(entry.last_consistent_at, "2026-10-02T08:00:00Z")

    def test_stale_policy_entry_is_inconsistent(self):
        entry = self.service.catalog_entry("DS-A", "R2")
        self.assertEqual(entry.status, "inconsistent")
        self.assertEqual(entry.covered_partitions, ())
        self.assertTrue(any("POL-2" in r for r in entry.reasons))

    def test_failed_copy_entry_is_building(self):
        entry = self.service.catalog_entry("DS-B", "R1")
        self.assertEqual(entry.status, "building")

    def test_withdrawn_and_unpublished_entries(self):
        self.assertEqual(self.service.catalog_entry("DS-W", "R1").status, "withdrawn")
        self.assertEqual(self.service.catalog_entry("DS-U", "R1").status, "unpublished")

    def test_whole_release_allow_with_snapshot(self):
        answer = self.service.availability(
            dataset_id="DS-A",
            region="R1",
            scope="whole-release",
            consumer_lab="LAB-Z",
        )
        self.assertEqual(answer.decision, "allow")
        self.assertEqual(set(answer.covered_partitions), {"P1", "P2"})
        self.assertIsNotNone(answer.snapshot)
        self.assertEqual(answer.snapshot.versions_dict(), {"P1": "v2", "P2": "v2"})
        self.assertEqual(answer.snapshot.policy_version, "POL-1")

    def test_partition_only_scope(self):
        answer = self.service.availability(
            dataset_id="DS-A",
            region="R1",
            scope="partition-only",
            consumer_lab="LAB-Z",
            partition_key="P2",
        )
        self.assertEqual(answer.decision, "allow")
        self.assertEqual(answer.covered_partitions, ("P2",))
        missing = self.service.availability(
            dataset_id="DS-A",
            region="R1",
            scope="partition-only",
            consumer_lab="LAB-Z",
            partition_key="P9",
        )
        self.assertEqual(missing.decision, "deny")
        self.assertTrue(any("不存在" in r for r in missing.reasons))

    def test_analysis_scope_requires_full_coverage(self):
        answer = self.service.availability(
            dataset_id="DS-A",
            region="R2",
            scope="analysis",
            consumer_lab="LAB-Z",
        )
        self.assertEqual(answer.decision, "deny")
        self.assertEqual(answer.covered_partitions, ())

    def test_permission_matrix(self):
        # project + 非所有者 + controlled → 需审批（partial 权限）
        answer = self.service.availability(
            dataset_id="DS-B", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertIs(answer.permission, Permission.NEEDS_APPROVAL)
        self.assertEqual(answer.decision, "deny")  # 复制失败 → 无就绪分区
        # restricted + 非所有者 → 拒绝
        answer = self.service.availability(
            dataset_id="DS-C", region="R3", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertIs(answer.permission, Permission.DENY)
        self.assertEqual(answer.decision, "deny")
        # restricted + 所有者 → 需区域审批，副本就绪 → partial
        answer = self.service.availability(
            dataset_id="DS-C", region="R3", scope="whole-release", consumer_lab="LAB-C"
        )
        self.assertIs(answer.permission, Permission.NEEDS_APPROVAL)
        self.assertEqual(answer.decision, "partial")
        self.assertIsNotNone(answer.snapshot)
        # none 范围 → 一律拒绝
        answer = self.service.availability(
            dataset_id="DS-D", region="R1", scope="whole-release", consumer_lab="LAB-A"
        )
        self.assertIs(answer.permission, Permission.DENY)

    def test_partial_coverage_gives_partial_decision(self):
        # DS-A/R2 的 P1 策略滞后、P2 无镜像 → 不就绪；构造部分覆盖场景
        from helpers import ds, obj, service_with, src, task

        service = service_with(
            datasets=[ds("DS-P", version="v1")],
            sources=[src("DS-P", "P1"), src("DS-P", "P2")],
            tasks=[task("T1", "DS-P", "R1", "P1")],
            objects=[obj("T1", pol="POL-1")],
        )
        answer = service.availability(
            dataset_id="DS-P",
            region="R1",
            scope="whole-release",
            consumer_lab="LAB-Z",
        )
        self.assertEqual(answer.decision, "partial")
        self.assertEqual(answer.covered_partitions, ("P1",))
        self.assertEqual([pk for pk, _ in answer.missing_partitions], ["P2"])


if __name__ == "__main__":
    unittest.main()
