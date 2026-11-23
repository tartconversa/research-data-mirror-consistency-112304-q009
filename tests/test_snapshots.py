"""读取快照与撤回/重发布影响的合成测试。"""
import unittest

from helpers import DATA_DIR, mirror_service

from research_data_mirror_consistency import DataPlatformService


def open_first_snapshot(service, dataset_id="DS-A", region="R1", lab="LAB-Z"):
    answer = service.availability(
        dataset_id=dataset_id,
        region=region,
        scope="whole-release",
        consumer_lab=lab,
    )
    assert answer.decision in ("allow", "partial"), answer
    return service.open_snapshot(answer, consumer_lab=lab, opened_at="2026-10-06T00:00:00Z")


class SnapshotPinTests(unittest.TestCase):
    def setUp(self):
        self.service = mirror_service()

    def test_lease_pins_version_identity(self):
        lease = open_first_snapshot(self.service)
        self.assertEqual(lease.pin.versions_dict(), {"P1": "v2", "P2": "v2"})
        self.assertEqual(lease.pin.policy_version, "POL-1")
        self.assertTrue(lease.current)
        self.assertEqual(lease.status, "active")

    def test_deny_answer_cannot_open_snapshot(self):
        answer = self.service.availability(
            dataset_id="DS-W", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "deny")
        with self.assertRaises(ValueError):
            self.service.open_snapshot(answer, consumer_lab="LAB-Z", opened_at="2026-10-06")

    def test_withdrawal_keeps_identity_and_explains_impact(self):
        lease = open_first_snapshot(self.service)
        impact = self.service.withdraw_dataset("DS-A", at="2026-10-07T00:00:00Z")
        self.assertIn(lease.ref, impact["affected_snapshots"])
        # 快照身份保留
        self.assertEqual(lease.pin.versions_dict(), {"P1": "v2", "P2": "v2"})
        self.assertFalse(lease.current)
        self.assertTrue(any("撤回" in n and "不得" in n for n in lease.notices))
        # 撤回后目录不再提供该数据集
        answer = self.service.availability(
            dataset_id="DS-A", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "deny")
        self.assertIsNone(answer.snapshot)

    def test_partition_withdrawal_only_affects_covering_leases(self):
        lease = open_first_snapshot(self.service)
        other = self.service  # DS-C 在 R3 的就绪副本（所有者开启）
        answer = other.availability(
            dataset_id="DS-C", region="R3", scope="whole-release", consumer_lab="LAB-C"
        )
        lease_c = other.open_snapshot(answer, consumer_lab="LAB-C", opened_at="2026-10-06")
        impact = self.service.withdraw_partition("DS-A", "P1", at="2026-10-07")
        self.assertIn(lease.ref, impact["affected_snapshots"])
        self.assertNotIn(lease_c.ref, impact["affected_snapshots"])
        self.assertTrue(lease_c.current)

    def test_republish_marks_old_snapshot_not_latest(self):
        lease = open_first_snapshot(self.service)
        impact = self.service.republish("DS-A", "v3", at="2026-10-08T00:00:00Z")
        self.assertIn(lease.ref, impact["affected_snapshots"])
        self.assertFalse(lease.current)
        self.assertTrue(any("重新发布" in n for n in lease.notices))
        # 旧镜像不得继续显示为最新可用
        answer = self.service.availability(
            dataset_id="DS-A", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "deny")

    def test_closed_lease_is_not_notified(self):
        lease = open_first_snapshot(self.service)
        self.service.close_snapshot(lease.ref)
        impact = self.service.withdraw_dataset("DS-A", at="2026-10-07")
        self.assertNotIn(lease.ref, impact["affected_snapshots"])
        self.assertEqual(lease.notices, [])


class HistoricalLeaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = DataPlatformService.from_data_dir(DATA_DIR)

    def test_imports_thirty_historical_snapshots(self):
        leases = self.service.snapshots()
        self.assertEqual(len(leases), 30)
        for lease in leases:
            self.assertEqual(lease.status, "active")
            self.assertTrue(lease.consumer_labs)

    def test_shared_snapshot_merges_consumers(self):
        lease = self.service.snapshot("SNAP-001")
        self.assertIsNotNone(lease)
        self.assertEqual(len(lease.consumer_labs), 2)

    def test_withdrawal_reaches_historical_lease(self):
        lease = self.service.snapshot("SNAP-001")
        dataset_id = lease.dataset_id
        impact = self.service.withdraw_dataset(dataset_id, at="2026-11-01")
        self.assertIn("SNAP-001", impact["affected_snapshots"])
        self.assertTrue(any("撤回" in n for n in lease.notices))


if __name__ == "__main__":
    unittest.main()
