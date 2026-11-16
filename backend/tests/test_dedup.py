"""去重归并链路测试：增量归并、幂等写入、优先级仲裁、并发互斥与契约同步。

运行方式：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import copy
import threading
import unittest

from app.services.dedup import (
    DATASET_TABLE,
    GROUP_TABLE,
    MODULE,
    REGISTRY_TABLE,
    SOURCE_TABLE,
    DedupService,
)
from app.store import Store


class DedupMergeTest(unittest.TestCase):
    """每个用例都拿到一份全新的内存仓库，互不影响。"""

    def setUp(self) -> None:
        self.store = Store()
        self.service = DedupService(self.store)

    # --------------------------------------------------------------
    # 工具方法
    # --------------------------------------------------------------
    def dataset(self, key: str) -> dict | None:
        for row in self.store.rows(DATASET_TABLE):
            if row["去重键"] == key:
                return row
        return None

    def group(self, key: str) -> dict | None:
        for row in self.store.rows(GROUP_TABLE):
            if row["去重键"] == key:
                return row
        return None

    def registry(self) -> dict:
        return self.store.rows(REGISTRY_TABLE)[0]

    def execute(self, entry_id: int, action: str = "执行归并"):
        return self.service.run_action(entry_id, action)

    # --------------------------------------------------------------
    # 主场景：customer_id 去重、上一批保留首条、迟到记录增量归并
    # --------------------------------------------------------------
    def test_incremental_merge_only_touches_affected_groups(self):
        entry, message = self.execute(2)
        self.assertEqual(entry["status"], "已归并", message)
        # 顺着去重数据流只梳理出受影响重复组：C002/C003/C004/C007
        self.assertEqual(entry["影响重复组"], 4)
        self.assertEqual(entry["归并结果"], "保留4组/归并3行/新增1组/异常1行")

        # C002：同版本保留首条，旧批次保留记录不动，迟到行被归并
        row = self.dataset("C002")
        self.assertEqual(row["保留记录"], "B2026-09#3 李四")
        self.assertEqual(row["被归并行号"], ["B2026-11#1"])
        self.assertEqual(row["归并批次"], "B2026-11")
        self.assertEqual(self.group("C002")["重算次数"], 1)

        # C003：被归并行号合并旧批次与迟到批次两行
        row = self.dataset("C003")
        self.assertEqual(row["保留记录"], "B2026-09#4 王五")
        self.assertEqual(row["被归并行号"], ["B2026-09#5", "B2026-11#2"])

        # C004：同一去重键两个规则版本，契约版本 v2 优先仲裁，
        # 数据集只保留一条记录，版本历史可追溯
        row = self.dataset("C004")
        self.assertEqual(row["保留记录"], "B2026-11#3 赵六")
        self.assertEqual(row["规则版本"], "v2")
        self.assertEqual(row["被归并行号"], ["B2026-09#6"])
        self.assertEqual(self.group("C004")["版本历史"], ["v1", "v2"])
        rows = [r for r in self.store.rows(DATASET_TABLE) if r["去重键"] == "C004"]
        self.assertEqual(len(rows), 1, "同一去重键只允许保留一条记录")

        # C007：迟到的新键单独成组
        row = self.dataset("C007")
        self.assertIsNotNone(row)
        self.assertEqual(row["保留记录"], "B2026-11#4 孙八")
        self.assertEqual(row["规则版本"], "v2")

        # 旧批次其他记录不重算：归并批次与重算次数都保持原样
        for key in ("C001", "C005"):
            self.assertEqual(self.dataset(key)["归并批次"], "B2026-09")
            self.assertEqual(self.group(key)["重算次数"], 0)

        # 缺少去重键的迟到记录进入异常清单，有明确结果
        snapshot = entry["结果快照"]
        self.assertEqual(snapshot["异常记录"], ["B2026-11#5 缺少去重键 customer_id"])
        self.assertEqual(snapshot["复核口径"], "以清洗后的数据集为准")

        # 清洗后的数据集同步给去重字段登记
        registry = self.registry()
        self.assertEqual(registry["当前重复组"], 6)
        self.assertEqual(registry["保留记录数"], 6)
        self.assertEqual(registry["快照版本"], 2)
        self.assertEqual(registry["契约状态"], "已同步")

    # --------------------------------------------------------------
    # 幂等写入：重试与重复执行不重复落库
    # --------------------------------------------------------------
    def test_idempotent_replay_does_not_rewrite(self):
        self.execute(2)
        dataset_before = copy.deepcopy(self.store.rows(DATASET_TABLE))
        group_before = copy.deepcopy(self.store.rows(GROUP_TABLE))

        entry, message = self.execute(2, "重试归并")
        self.assertEqual(entry["status"], "已归并")
        self.assertIn("幂等命中", message)
        self.assertEqual(self.store.rows(DATASET_TABLE), dataset_before)
        self.assertEqual(self.store.rows(GROUP_TABLE), group_before)

        # 同幂等键的另一批次复用已落库结果，不再写数据集
        peer, missing = self.service.create_entry({
            "批次编号": "B2026-11R",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
            "幂等键": "B2026-11-customer_id-v2",
        })
        self.assertEqual(missing, [])
        entry, message = self.execute(peer["id"])
        self.assertEqual(entry["status"], "已归并")
        self.assertIn("幂等命中", message)
        self.assertEqual(self.store.rows(DATASET_TABLE), dataset_before)
        self.assertEqual(self.registry()["快照版本"], 2, "幂等命中不再推进快照版本")

    # --------------------------------------------------------------
    # 空批次：有明确结果，数据集保持不变
    # --------------------------------------------------------------
    def test_empty_batch_has_explicit_result(self):
        entry, message = self.execute(3)
        self.assertEqual(entry["status"], "空批次")
        self.assertIn("空批次", message)
        self.assertEqual(entry["影响重复组"], 0)
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 5)
        self.assertEqual(self.registry()["快照版本"], 1, "空批次不推进快照版本")

    # --------------------------------------------------------------
    # 中断与失败重试：不写部分结果，不留空数据集
    # --------------------------------------------------------------
    def test_interrupt_then_retry_converges(self):
        run, missing = self.service.create_entry({
            "批次编号": "B2026-11",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
            "故障注入": "中断",
        })
        self.assertEqual(missing, [])
        entry, message = self.execute(run["id"])
        self.assertEqual(entry["status"], "已中断")
        self.assertIn("可重试归并", message)
        # 中断不写任何结果，数据集保持上一致状态
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 5)
        self.assertEqual(self.dataset("C004")["保留记录"], "B2026-09#6 赵六")
        self.assertEqual(self.registry()["快照版本"], 1)

        # 重试归并：关掉故障注入后收敛到同一结果
        run["故障注入"] = "无"
        entry, message = self.execute(run["id"], "重试归并")
        self.assertEqual(entry["status"], "已归并", message)
        self.assertEqual(self.dataset("C004")["保留记录"], "B2026-11#3 赵六")
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 6)

    def test_failure_injection_leaves_dataset_untouched(self):
        run, _ = self.service.create_entry({
            "批次编号": "B2026-11",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
            "故障注入": "失败",
        })
        entry, message = self.execute(run["id"])
        self.assertEqual(entry["status"], "归并失败")
        self.assertIn("可重试归并", message)
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 5)
        self.assertGreater(len(self.store.rows(DATASET_TABLE)), 0, "不能留下空数据集")

    # --------------------------------------------------------------
    # 边界一致：去重字段、规则版本、迟到边界必须与登记契约一致
    # --------------------------------------------------------------
    def test_boundary_mismatch_is_rejected(self):
        # 规则版本与契约不符
        run, _ = self.service.create_entry({
            "批次编号": "B2026-14",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v9",
        })
        entry, message = self.execute(run["id"])
        self.assertEqual(entry["status"], "归并失败")
        self.assertIn("边界不一致", message)
        self.assertIn("同一重复组不允许拆进两个批次", message)
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 5)

        # 迟到边界与契约不符
        run, _ = self.service.create_entry({
            "批次编号": "B2026-15",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
            "迟到边界": "B2026-08",
        })
        entry, message = self.execute(run["id"])
        self.assertEqual(entry["status"], "归并失败")
        self.assertIn("迟到边界", message)

        # 去重字段未登记契约
        run, _ = self.service.create_entry({
            "批次编号": "B2026-16",
            "关联数据集": "客户主档",
            "去重字段": "phone_no",
            "规则版本": "v1",
        })
        entry, message = self.execute(run["id"])
        self.assertEqual(entry["status"], "归并失败")
        self.assertIn("未在去重字段登记", message)

    def test_seeded_conflict_run_stays_rejected_on_retry(self):
        # 种子里的 B2026-13（规则版本 v9）重试后仍被边界校验拦下
        entry, message = self.execute(4, "重试归并")
        self.assertEqual(entry["status"], "归并失败")
        self.assertIn("边界不一致", message)
        self.assertEqual(len(self.store.rows(DATASET_TABLE)), 5)

    # --------------------------------------------------------------
    # 并发争用：同一记录只允许一个结果落库
    # --------------------------------------------------------------
    def test_contended_key_allows_only_one_result(self):
        # 人为持有 C002 的键锁，模拟并发争用
        lock = self.service._lock_for("C002")
        lock.acquire()
        try:
            entry, message = self.execute(2)
        finally:
            lock.release()
        self.assertEqual(entry["status"], "已归并")
        self.assertIn("C002", entry["结果快照"]["并发冲突"])
        self.assertIn("并发冲突", message)
        # 争用键未落库，其他重复组正常归并
        self.assertEqual(self.dataset("C002")["归并批次"], "B2026-09")
        self.assertEqual(self.dataset("C002")["被归并行号"], [])
        self.assertEqual(self.dataset("C004")["归并批次"], "B2026-11")

    def test_concurrent_runs_only_one_result_persists(self):
        # 再造一个与 B2026-11 有重叠去重键的迟到批次
        self.store.rows(SOURCE_TABLE).append({
            "id": 13, "批次号": "B2026-11B", "行号": 1,
            "customer_id": "C002", "姓名": "李四", "规则版本": "v2", "到达时间": "2026-11-13",
        })
        run_a = self.store.find(MODULE, 2)
        run_b, _ = self.service.create_entry({
            "批次编号": "B2026-11B",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
        })

        # 用屏障强制两个批次都基于同一快照水位完成计划、再同时进入提交，
        # 复现“并发争用同一记录”的交错时序。
        barrier = threading.Barrier(2)
        service = self.service
        original_build_plan = service._build_plan

        def synced_build_plan(entry, records, contract):
            plan, anomalies = original_build_plan(entry, records, contract)
            barrier.wait(timeout=5)
            return plan, anomalies

        service._build_plan = synced_build_plan
        try:
            threads = [
                threading.Thread(target=service.execute_merge, args=(run_a,)),
                threading.Thread(target=service.execute_merge, args=(run_b,)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            service._build_plan = original_build_plan

        statuses = {run_a["status"], run_b["status"]}
        self.assertEqual(statuses, {"已归并", "归并失败"})
        loser = run_a if run_a["status"] == "归并失败" else run_b
        self.assertIn("并发冲突", loser["归并结果"])
        # 无论谁赢，每个去重键只有一条记录，且与登记口径一致
        keys = [row["去重键"] for row in self.store.rows(DATASET_TABLE)]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(self.registry()["当前重复组"], len(set(keys)))
        self.assertEqual(self.registry()["快照版本"], 2, "只有一个批次的结果落库")

    # --------------------------------------------------------------
    # 复核口径：以清洗后的数据集为准
    # --------------------------------------------------------------
    def test_review_uses_cleansed_dataset(self):
        self.execute(2)
        review = self.service.review_group("C004")
        self.assertEqual(review["保留记录"], "B2026-11#3 赵六")
        self.assertEqual(review["被归并行号"], ["B2026-09#6"])
        self.assertEqual(review["规则版本"], "v2")
        self.assertEqual(review["版本历史"], ["v1", "v2"])
        self.assertEqual(review["重算次数"], 1)
        self.assertEqual(review["复核口径"], "以清洗后的数据集为准")
        self.assertIsNone(self.service.review_group("C999"))

    # --------------------------------------------------------------
    # 查询口径：只读取归并后的当前重复组
    # --------------------------------------------------------------
    def test_query_reads_only_current_groups(self):
        self.execute(2)
        rows = self.service.dataset_rows("customer_id")
        self.assertEqual(len(rows), 6)
        keys = {row["去重键"] for row in rows}
        self.assertEqual(keys, {"C001", "C002", "C003", "C004", "C005", "C007"})

    # --------------------------------------------------------------
    # 清洗规则契约核对：其他业务模块要与主结果一致
    # --------------------------------------------------------------
    def test_contract_check_flags_drift(self):
        results = {item["规则编号"]: item["核对结果"] for item in self.service.contract_check()}
        self.assertEqual(results["CLEA-0003"], "一致")
        self.assertIn("版本漂移", results["CLEA-0002"])
        self.assertIn("字段未登记", results["CLEA-0001"])

    # --------------------------------------------------------------
    # 登记与动作守卫
    # --------------------------------------------------------------
    def test_create_requires_fields(self):
        entry, missing = self.service.create_entry({"批次编号": "B1"})
        self.assertIsNone(entry)
        self.assertEqual(missing, ["关联数据集", "去重字段", "规则版本"])

    def test_create_prefills_affected_count_via_fetch_pipeline(self):
        entry, missing = self.service.create_entry({
            "批次编号": "B2026-11",
            "关联数据集": "客户主档",
            "去重字段": "customer_id",
            "规则版本": "v2",
        })
        self.assertEqual(missing, [])
        self.assertEqual(entry["迟到记录数"], 5, "按既有取数链路预判受影响记录数")

    def test_unknown_action_rejected(self):
        entry, message = self.service.run_action(2, "删除批次")
        self.assertIsNone(entry)
        self.assertIn("不属于", message)

    def test_disabled_run_cannot_merge(self):
        entry, _ = self.execute(3, "停用批次")
        self.assertEqual(entry["status"], "已停用")
        entry, message = self.execute(3)
        self.assertIsNone(entry)
        self.assertIn("已停用", message)


if __name__ == "__main__":
    unittest.main()
