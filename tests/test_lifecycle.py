"""端到端生命周期与 CSV 接续计划的合成测试。"""
import unittest

from helpers import DATA_DIR, ds, mirror_service, obj, service_with, src, task

from research_data_mirror_consistency import (
    Action,
    DataPlatformService,
    Readiness,
)


class CsvContinuationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = DataPlatformService.from_data_dir(DATA_DIR)
        cls.plan = cls.service.continuation_plan()

    def test_plan_is_sorted_and_nonempty(self):
        self.assertTrue(self.plan)
        keys = [(a.priority, a.dataset_id, a.region, a.partition_key) for a in self.plan]
        self.assertEqual(keys, sorted(keys))

    def test_withdrawn_datasets_get_propagate_withdrawal(self):
        withdrawn_ids = {
            d.dataset_id
            for d in self.service._datasets.values()
            if d.release_state == "withdrawn"
        }
        propagate = {
            a.dataset_id
            for a in self.plan
            if a.action is Action.PROPAGATE_WITHDRAWAL
        }
        self.assertEqual(propagate, withdrawn_ids)

    def test_actions_match_readiness(self):
        """每种非就绪状态都映射到正确的接续动作。"""
        expected = {
            Readiness.RELEASE_WITHDRAWN: Action.PROPAGATE_WITHDRAWAL,
            Readiness.SOURCE_WITHDRAWN: Action.PROPAGATE_WITHDRAWAL,
            Readiness.ORPHAN_PARTITION: Action.PROPAGATE_WITHDRAWAL,
            Readiness.REPORT_CONFLICT: Action.RESOLVE_CONFLICT,
            Readiness.INTEGRITY_FAILED: Action.REPAIR_INTEGRITY,
            Readiness.METADATA_MISSING: Action.SYNC_METADATA,
            Readiness.POLICY_MISSING: Action.SYNC_POLICY,
            Readiness.POLICY_STALE: Action.SYNC_POLICY,
            Readiness.SOURCE_STALE: Action.REFRESH_SOURCE,
            Readiness.VERSION_LAG: Action.REFRESH_SOURCE,
        }
        by_key = {}
        for a in self.plan:
            by_key.setdefault((a.dataset_id, a.region, a.partition_key), set()).add(a.action)
        for ev in self.service.evaluations():
            key = (ev.replica.dataset_id, ev.replica.region, ev.replica.partition_key)
            if ev.readiness in expected:
                self.assertIn(expected[ev.readiness], by_key.get(key, set()), ev)
            elif ev.readiness is Readiness.RELEASE_UNPUBLISHED:
                self.assertNotIn(key, by_key)

    def test_review_datasets_have_no_action(self):
        review_ids = {
            d.dataset_id
            for d in self.service._datasets.values()
            if d.release_state == "review"
        }
        self.assertFalse({a.dataset_id for a in self.plan} & review_ids)

    def test_reconcile_then_replan(self):
        """主管核对矛盾回报后，该副本的原因不再包含回报冲突。"""
        marker = "需复制平台主管核对"
        ev = next(
            e for e in self.service.evaluations() if any(marker in r for r in e.reasons)
        )
        key = (ev.replica.dataset_id, ev.replica.region, ev.replica.partition_key)
        self.service.reconcile_reports(
            *key,
            by="复制平台主管",
            note="核对上报链路，以最新回报为准",
            at="2026-11-02T00:00:00Z",
        )
        after = next(
            e
            for e in self.service.evaluations()
            if (e.replica.dataset_id, e.replica.region, e.replica.partition_key) == key
        )
        self.assertFalse(any(marker in r for r in after.reasons), after.reasons)


class LifecycleTests(unittest.TestCase):
    def test_full_lifecycle(self):
        service = mirror_service()
        # 1. 就绪并可开启快照
        answer = service.availability(
            dataset_id="DS-A", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "allow")
        lease = service.open_snapshot(
            answer, consumer_lab="LAB-Z", opened_at="2026-10-06T00:00:00Z"
        )

        # 2. 数据所有者撤回 → 目录不可用、快照获影响说明、接续计划含撤回传播
        impact = service.withdraw_dataset("DS-A", at="2026-10-07T00:00:00Z")
        self.assertIn(lease.ref, impact["affected_snapshots"])
        self.assertEqual(
            service.catalog_entry("DS-A", "R1").status, "withdrawn"
        )
        self.assertEqual(
            service.availability(
                dataset_id="DS-A",
                region="R1",
                scope="whole-release",
                consumer_lab="LAB-Z",
            ).decision,
            "deny",
        )
        self.assertIn(
            Action.PROPAGATE_WITHDRAWAL,
            {a.action for a in impact["continuation"]},
        )

        # 3. 重新发布 v3 → 旧镜像立即过期，不得显示为最新可用
        impact = service.republish("DS-A", "v3", at="2026-10-08T00:00:00Z")
        self.assertIn(
            Action.REFRESH_SOURCE, {a.action for a in impact["continuation"]}
        )
        entry = service.catalog_entry("DS-A", "R1")
        self.assertEqual(entry.status, "stale")
        self.assertEqual(entry.covered_partitions, ())
        answer = service.availability(
            dataset_id="DS-A", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "deny")
        # 旧快照仍固定 v2，身份不变
        self.assertEqual(lease.pin.versions_dict(), {"P1": "v2", "P2": "v2"})
        self.assertFalse(lease.current)

        # 4. 执行接续：按 v3 重新镜像两个分区并随附现行策略
        for partition, task_id in (("P1", "T-A1-R"), ("P2", "T-A2-R")):
            ev = service.apply_task_report(
                dataset_id="DS-A",
                region="R1",
                partition_key=partition,
                task_id=task_id,
                attempt=2,
                task_state="copied",
                completed_at="2026-10-09T00:00:00Z",
                object_key=f"obj/{task_id}",
                checksum="SHA-NEW",
                metadata_version="META-9",
                access_policy_version="POL-1",
                verified_state="verified",
            )
        self.assertIs(ev.readiness, Readiness.READY)
        entry = service.catalog_entry("DS-A", "R1")
        self.assertEqual(entry.status, "available")
        self.assertEqual(entry.versions_dict(), {"P1": "v3", "P2": "v3"})
        answer = service.availability(
            dataset_id="DS-A", region="R1", scope="whole-release", consumer_lab="LAB-Z"
        )
        self.assertEqual(answer.decision, "allow")
        self.assertEqual(answer.snapshot.versions_dict(), {"P1": "v3", "P2": "v3"})
        # 旧快照保持 v2 身份与撤回/重发布说明
        self.assertEqual(lease.pin.versions_dict(), {"P1": "v2", "P2": "v2"})
        self.assertGreaterEqual(len(lease.notices), 2)

    def test_reconcile_flow_unblocks_ready(self):
        """矛盾回报经主管核对、且最新回报干净时，副本进入就绪。"""
        service = service_with(
            datasets=[ds("DS-K", version="v1")],
            sources=[src("DS-K", "P1", version="v1")],
            tasks=[
                task("T1", "DS-K", "R1", "P1", completed="2026-10-02T00:00:00Z"),
                task("T2", "DS-K", "R1", "P1", completed="2026-10-03T00:00:00Z"),
            ],
            objects=[obj("T1", pol="POL-0"), obj("T2", pol="POL-1")],
        )
        ev = next(iter(service.evaluations()))
        self.assertIs(ev.readiness, Readiness.REPORT_CONFLICT)
        plan = service.continuation_plan()
        self.assertEqual([a.action for a in plan], [Action.RESOLVE_CONFLICT])
        service.reconcile_reports(
            "DS-K", "R1", "P1", by="复制平台主管", note="最新回报为准", at="2026-10-04"
        )
        ev = next(iter(service.evaluations()))
        self.assertIs(ev.readiness, Readiness.READY)
        self.assertEqual(service.catalog_entry("DS-K", "R1").status, "available")
        self.assertEqual(service.continuation_plan(), [])

    def test_partition_withdrawal_shrinks_coverage(self):
        service = mirror_service()
        service.withdraw_partition("DS-A", "P2", at="2026-10-07T00:00:00Z")
        entry = service.catalog_entry("DS-A", "R1")
        # P2 不再是有效源端分区，覆盖范围按有效分区计算
        self.assertEqual(entry.status, "available")
        self.assertEqual(entry.covered_partitions, ("P1",))
        answer = service.availability(
            dataset_id="DS-A",
            region="R1",
            scope="partition-only",
            consumer_lab="LAB-Z",
            partition_key="P2",
        )
        self.assertEqual(answer.decision, "deny")


if __name__ == "__main__":
    unittest.main()
