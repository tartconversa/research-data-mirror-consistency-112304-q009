"""镜像回报折叠与就绪评估的合成测试。"""
import unittest

from helpers import DATA_DIR, mirror_service, obj, task

from research_data_mirror_consistency import (
    Action,
    Readiness,
    evaluate_replica,
    fold_replicas,
    load_materials,
    plan_continuation,
)


class FoldCsvTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.materials = load_materials(DATA_DIR)
        cls.replicas = fold_replicas(
            cls.materials.replica_tasks, cls.materials.replica_objects
        )

    def test_reports_fold_into_logical_replicas(self):
        """120 条任务回报折叠为 42 个逻辑副本，每个副本保留多条回报。"""
        self.assertEqual(len(self.materials.replica_tasks), 120)
        self.assertEqual(len(self.replicas), 42)
        self.assertTrue(all(len(r.reports) >= 2 for r in self.replicas))

    def test_reports_ordered_by_completion_time(self):
        for replica in self.replicas:
            times = [r.task.completed_at for r in replica.reports]
            self.assertEqual(times, sorted(times))

    def test_every_csv_replica_has_policy_disagreement(self):
        """合成资料中每个逻辑副本的策略版本回报均互相矛盾。"""
        for replica in self.replicas:
            disagreements = replica.disagreements()
            self.assertIn("access_policy_version", disagreements)


class EvaluateCsvTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from research_data_mirror_consistency import DataPlatformService

        cls.service = DataPlatformService.from_data_dir(DATA_DIR)
        cls.evaluations = cls.service.evaluations()

    def test_no_csv_replica_is_ready(self):
        """权限回报不一致或撤回状态下，任何副本都不得被解释为就绪。"""
        self.assertTrue(self.evaluations)
        self.assertFalse(any(ev.ready for ev in self.evaluations))

    def test_withdrawn_datasets_are_not_ready(self):
        for ev in self.evaluations:
            dataset = self.service._datasets[ev.replica.dataset_id]
            if dataset.release_state == "withdrawn":
                self.assertIs(ev.readiness, Readiness.RELEASE_WITHDRAWN)

    def test_published_with_superseded_source_is_stale(self):
        """已发布但源端已被新修订取代：判为过期，且回报矛盾仍写入原因。"""
        checked = 0
        for ev in self.evaluations:
            dataset = self.service._datasets[ev.replica.dataset_id]
            if dataset.release_state == "published":
                self.assertIs(ev.readiness, Readiness.SOURCE_STALE)
                self.assertTrue(any("不一致" in r for r in ev.reasons), ev.reasons)
                checked += 1
        self.assertEqual(checked, 14)

    def test_unpublished_review_datasets_not_ready(self):
        for ev in self.evaluations:
            dataset = self.service._datasets[ev.replica.dataset_id]
            if dataset.release_state == "review":
                self.assertIs(ev.readiness, Readiness.RELEASE_UNPUBLISHED)

    def test_reasons_are_chinese_and_nonempty(self):
        for ev in self.evaluations:
            self.assertTrue(ev.reasons)
            self.assertTrue(all(isinstance(r, str) and r for r in ev.reasons))


class EvaluateSyntheticTests(unittest.TestCase):
    def evaluate(self, *, tasks, objects, release_state="published", source_version="v1", withdrawal="active"):
        replicas = fold_replicas(tasks, objects)
        self.assertEqual(len(replicas), 1)
        return evaluate_replica(
            replicas[0],
            release_state=release_state,
            source_version=source_version,
            source_withdrawal=withdrawal,
        )

    def test_clean_replica_is_ready(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1", pol="POL-1")],
        )
        self.assertIs(ev.readiness, Readiness.READY)
        self.assertTrue(ev.ready)
        self.assertEqual(ev.consistent_at, "2026-10-02T00:00:00Z")

    def test_missing_metadata_is_not_ready(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1", meta="META-0")],
        )
        self.assertIs(ev.readiness, Readiness.METADATA_MISSING)

    def test_missing_policy_is_not_ready(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1", pol="POL-0")],
        )
        self.assertIs(ev.readiness, Readiness.POLICY_MISSING)

    def test_stale_policy_is_not_ready(self):
        """区域 R2 现行为 POL-2，副本随附 POL-1 属权限不一致。"""
        ev = self.evaluate(
            tasks=[task("T1", "D", "R2", "P1")],
            objects=[obj("T1", pol="POL-1")],
        )
        self.assertIs(ev.readiness, Readiness.POLICY_STALE)
        self.assertIn("POL-2", ev.reasons[0])

    def test_checksum_mismatch_is_not_ready(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1", verified="checksum-mismatch")],
        )
        self.assertIs(ev.readiness, Readiness.INTEGRITY_FAILED)

    def test_queued_and_failed_are_incomplete(self):
        for state in ("queued", "failed"):
            ev = self.evaluate(
                tasks=[task("T1", "D", "R1", "P1", state=state)],
                objects=[obj("T1")],
            )
            self.assertIs(ev.readiness, Readiness.INCOMPLETE)

    def test_version_lag_is_not_ready(self):
        """副本 v1 落后于源端 v2：复制延迟不得解释为就绪。"""
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1", version="v1")],
            objects=[obj("T1")],
            source_version="v2",
        )
        self.assertIs(ev.readiness, Readiness.VERSION_LAG)

    def test_superseded_source_is_stale(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1")],
            withdrawal="superseded",
        )
        self.assertIs(ev.readiness, Readiness.SOURCE_STALE)

    def test_withdrawn_release_dominates(self):
        ev = self.evaluate(
            tasks=[task("T1", "D", "R1", "P1")],
            objects=[obj("T1")],
            release_state="withdrawn",
        )
        self.assertIs(ev.readiness, Readiness.RELEASE_WITHDRAWN)

    def test_conflicting_reports_block_until_reconciled(self):
        tasks = [
            task("T1", "D", "R1", "P1", completed="2026-10-02T00:00:00Z"),
            task("T2", "D", "R1", "P1", completed="2026-10-03T00:00:00Z"),
        ]
        objects = [obj("T1", pol="POL-1"), obj("T2", pol="POL-2")]
        replicas = fold_replicas(tasks, objects)
        ev = evaluate_replica(
            replicas[0],
            release_state="published",
            source_version="v1",
            source_withdrawal="active",
        )
        self.assertIs(ev.readiness, Readiness.REPORT_CONFLICT)
        # 主管核对后以最新回报为准重新评估
        replicas[0].reconciled = True
        ev2 = evaluate_replica(
            replicas[0],
            release_state="published",
            source_version="v1",
            source_withdrawal="active",
        )
        # 最新回报 POL-2 与区域 R1 现行 POL-1 不符 → 策略滞后而非冲突
        self.assertIs(ev2.readiness, Readiness.POLICY_STALE)


class ContinuationMappingTests(unittest.TestCase):
    def test_failed_task_gets_retry_with_incremented_attempt(self):
        service = mirror_service()
        plan = service.continuation_plan()
        retry = [a for a in plan if a.action is Action.RETRY_COPY]
        self.assertEqual(len(retry), 1)
        self.assertEqual(retry[0].dataset_id, "DS-B")
        self.assertEqual(retry[0].next_attempt, 3)  # 原 attempt=2

    def test_stale_policy_gets_sync_policy(self):
        service = mirror_service()
        plan = service.continuation_plan()
        sync = [a for a in plan if a.action is Action.SYNC_POLICY]
        self.assertEqual(len(sync), 1)
        self.assertEqual((sync[0].dataset_id, sync[0].region), ("DS-A", "R2"))

    def test_withdrawn_gets_propagate_withdrawal_first(self):
        service = mirror_service()
        plan = service.continuation_plan()
        propagate = [a for a in plan if a.action is Action.PROPAGATE_WITHDRAWAL]
        self.assertEqual([a.dataset_id for a in propagate], ["DS-W"])
        self.assertEqual(plan[0].action, Action.PROPAGATE_WITHDRAWAL)

    def test_unpublished_and_ready_have_no_action(self):
        service = mirror_service()
        plan = service.continuation_plan()
        self.assertFalse(any(a.dataset_id == "DS-U" for a in plan))
        self.assertFalse(
            any(a.dataset_id == "DS-A" and a.region == "R1" for a in plan)
        )

    def test_plan_sorted_by_priority(self):
        service = mirror_service()
        plan = service.continuation_plan()
        priorities = [a.priority for a in plan]
        self.assertEqual(priorities, sorted(priorities))


if __name__ == "__main__":
    unittest.main()
