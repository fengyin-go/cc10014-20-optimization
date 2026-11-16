"""去重归并业务规则：增量归并、幂等写入、优先级仲裁与契约同步都收在这里。

数据流（顺着去重数据流梳理受影响重复组）：

    源记录（含迟到） → 按既有取数链路取批次 → 受影响重复组判定 → 优先级仲裁
    → 并发互斥落库（幂等） → 清洗后的数据集 → 同步去重字段登记（查询只读当前重复组）

关键口径：
- 增量归并：只重算迟到记录命中的重复组，旧批次其他记录不重算。
- 幂等写入：同一幂等键只落库一次，重试/重复执行直接复用结果快照。
- 优先级仲裁：契约规则版本优先，同版本保留首条（批次号、行号升序）。
- 边界一致：去重字段、规则版本、迟到边界必须与去重字段登记一致，
  否则同一重复组会被拆进两个批次，直接拒绝归并。
- 并发争用：同一去重键只允许一个结果落库，争用失败的键记入并发冲突。
- 复核口径：以清洗后的数据集为准（保留记录、被归并行号、规则版本可复核）。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any

from app.store import Store
from app.store import store as default_store

MODULE = "dedup"
SOURCE_TABLE = "dedup_source"      # 源记录（含迟到记录）
DATASET_TABLE = "dedup_dataset"    # 清洗后的数据集
GROUP_TABLE = "dedup_group"        # 重复组台账
REGISTRY_TABLE = "dedup_registry"  # 去重字段登记

REQUIRED_FIELDS = ["批次编号", "关联数据集", "去重字段", "规则版本"]
STATUSES = ["待归并", "归并中", "已归并", "空批次", "归并失败", "已中断", "已停用"]
ACTION_RULES = {"执行归并": None, "重试归并": None, "停用批次": "已停用"}
NEGATIVE_ACTIONS = ["停用批次"]
REVIEW_NOTE = "以清洗后的数据集为准"
PENDING_STATUSES = {"待归并", "归并失败", "已中断"}
ABNORMAL_STATUSES = {"归并失败", "已中断", "已停用"}


class DedupService:
    """去重归并服务：路由层只调用这里，状态流转不放在路由里。"""

    def __init__(self, store: Store = default_store) -> None:
        self._store = store
        self._commit_lock = threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        self._key_locks_guard = threading.Lock()

    # ------------------------------------------------------------------
    # 列表、明细、登记（与其他业务模块同构）
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("批次编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return self._store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = self._store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: str(values.get(field)).strip() for field in REQUIRED_FIELDS})
        entry["迟到边界"] = str(values.get("迟到边界") or "").strip()
        entry["幂等键"] = str(values.get("幂等键") or "").strip() or (
            f"{entry['批次编号']}-{entry['去重字段']}-{entry['规则版本']}"
        )
        entry["故障注入"] = str(values.get("故障注入") or "无").strip() or "无"
        entry["status"] = "待归并"
        entry["pending"] = True
        entry["abnormal"] = False
        # 按既有取数链路预判受影响记录数，登记时即可见。
        entry["迟到记录数"] = len(self.fetch_source_records(entry["批次编号"]))
        entry["影响重复组"] = 0
        entry["归并结果"] = "—"
        entry["复核状态"] = "未复核"
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self._store.find(MODULE, entry_id)
        if entry is None:
            return None, f"归并批次 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于去重归并可执行范围"
        if action == "停用批次":
            entry["status"] = "已停用"
            entry["pending"] = False
            entry["abnormal"] = True
            return entry, "归并批次已停用"
        if entry.get("status") == "已停用":
            return None, "归并批次已停用，不能再执行归并"
        return self.execute_merge(entry)

    # ------------------------------------------------------------------
    # 既有取数链路：与数据查询模块同一套过滤口径
    # ------------------------------------------------------------------
    def fetch_source_records(self, batch_no: str) -> list[dict[str, Any]]:
        """按批次号过滤源记录、按行号排序，判断受影响记录的取数链路。"""
        rows = [row for row in self._store.rows(SOURCE_TABLE) if str(row.get("批次号")) == str(batch_no)]
        return sorted(rows, key=lambda row: int(row.get("行号", 0)))

    # ------------------------------------------------------------------
    # 归并引擎
    # ------------------------------------------------------------------
    def execute_merge(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """执行一次增量归并：幂等命中 → 边界校验 → 取数 → 仲裁 → 互斥落库 → 契约同步。"""
        # 1. 幂等写入：本批次或同幂等键批次已落库，直接复用结果，不重复写数据集。
        if entry.get("status") == "已归并":
            return entry, "幂等命中：该批次已归并，结果以清洗后的数据集为准，不重复落库"
        peer = self._find_completed_peer(str(entry.get("幂等键") or ""), exclude_id=int(entry.get("id", 0)))
        if peer is not None:
            if peer.get("结果快照"):
                entry["结果快照"] = copy.deepcopy(peer["结果快照"])
            entry["影响重复组"] = peer.get("影响重复组", 0)
            entry["归并结果"] = f"幂等命中：复用批次 {peer.get('批次编号')} 的归并结果"
            self._finish(entry, "已归并", 复核状态="可复核")
            return entry, "幂等命中：同幂等键批次已落库，本批次复用结果不重复写入"

        # 2. 边界一致：去重字段、规则版本、迟到边界必须与登记契约一致。
        contract = self._contract_for(entry["去重字段"])
        if contract is None:
            return self._fail(entry, f"边界不一致：去重字段 {entry['去重字段']} 未在去重字段登记中注册契约")
        problems: list[str] = []
        if str(entry.get("规则版本")) != str(contract.get("当前规则版本")):
            problems.append(f"规则版本 {entry['规则版本']} 与契约 {contract.get('当前规则版本')} 不符")
        boundary = str(entry.get("迟到边界") or "").strip()
        if boundary and boundary != str(contract.get("迟到边界")):
            problems.append(f"迟到边界 {boundary} 与契约 {contract.get('迟到边界')} 不符")
        if problems:
            return self._fail(entry, "边界不一致：" + "；".join(problems) + "，同一重复组不允许拆进两个批次")

        # 3. 按既有取数链路取迟到记录；空批次给出明确结果，数据集保持不变。
        records = self.fetch_source_records(entry["批次编号"])
        entry["迟到记录数"] = len(records)
        if not records:
            entry["结果快照"] = {
                "幂等键": entry.get("幂等键"),
                "影响重复组": [],
                "保留行号": {},
                "被归并行号": {},
                "新增重复组": [],
                "异常记录": [],
                "并发冲突": [],
                "复核口径": REVIEW_NOTE,
            }
            entry["影响重复组"] = 0
            entry["归并结果"] = "空批次：无迟到记录，清洗后的数据集保持不变"
            self._finish(entry, "空批次", 复核状态="无需复核")
            return entry, "空批次：无迟到记录，清洗后的数据集保持不变"

        # 4. 中断与失败注入：不写任何结果，数据集保持上一致状态，可重试归并。
        injection = str(entry.get("故障注入") or "无")
        if injection == "中断":
            entry["归并结果"] = "归并中断：未写入任何结果，可重试归并"
            self._finish(entry, "已中断", 复核状态="待重试")
            return entry, "归并中断：计算阶段被中断，清洗后的数据集保持上一致状态，可重试归并"
        if injection == "失败":
            return self._fail(entry, "归并失败：故障注入触发，未写入任何结果，可重试归并")

        # 5. 计算归并计划：只覆盖受影响重复组，旧批次其他记录不重算。
        entry["status"] = "归并中"
        plan_watermark = int(contract.get("快照版本", 0))
        plan, anomalies = self._build_plan(entry, records, contract)
        if not plan:
            return self._fail(entry, "归并失败：批次内无有效记录（全部缺少去重键），清洗后的数据集保持不变")

        # 6. 并发互斥落库：同一去重键只允许一个结果落库，争用失败的键记入并发冲突。
        with self._commit_lock:
            if int(contract.get("快照版本", 0)) != plan_watermark:
                # 乐观并发控制：归并期间已有其他批次落库，本批次结果不允许再写。
                entry["归并结果"] = "并发冲突：归并期间契约快照已变化，本批次未落库，可重试归并"
                self._finish(entry, "归并失败", 复核状态="待重试")
                return entry, "并发冲突：归并期间已有其他批次落库，同一记录只允许一个结果落库，本批次未写入，可重试归并"
            acquired = self._acquire_key_locks(plan.keys())
            try:
                written = [key for key in plan if key in acquired]
                conflicts = [key for key in plan if key not in acquired]
                for key in written:
                    self._apply_group(entry, plan[key])
                if not self._store.rows(DATASET_TABLE):
                    # 兜底：归并只增不改其他组，正常不会触发；触发则说明写入异常。
                    raise RuntimeError("归并后清洗后的数据集为空，已中止落库")
                # 7. 契约同步：清洗后的数据集同步给去重字段登记，查询只读当前重复组。
                self._sync_registry(entry["去重字段"])
            finally:
                self._release_key_locks(acquired)

        # 8. 写结果快照：保留记录、被归并行号、规则版本都可从清洗后的数据集复核。
        merged_rows = sum(len(plan[key]["新增归并"]) for key in written)
        snapshot = {
            "幂等键": entry.get("幂等键"),
            "影响重复组": sorted(written),
            "保留行号": {key: plan[key]["保留引用"] for key in written},
            "被归并行号": {key: plan[key]["被归并行号"] for key in written if plan[key]["被归并行号"]},
            "新增重复组": sorted(key for key in written if plan[key]["是否新组"]),
            "异常记录": anomalies,
            "并发冲突": sorted(conflicts),
            "复核口径": REVIEW_NOTE,
        }
        entry["结果快照"] = snapshot
        entry["影响重复组"] = len(written)
        result_parts = [f"保留{len(written)}组", f"归并{merged_rows}行"]
        new_groups = [key for key in written if plan[key]["是否新组"]]
        if new_groups:
            result_parts.append(f"新增{len(new_groups)}组")
        if anomalies:
            result_parts.append(f"异常{len(anomalies)}行")
        if conflicts:
            result_parts.append(f"并发冲突{len(conflicts)}键")
        entry["归并结果"] = "/".join(result_parts)
        self._finish(entry, "已归并", 复核状态="可复核")
        message = f"归并完成：影响 {len(written)} 个重复组，{REVIEW_NOTE}"
        if conflicts:
            message += f"；并发冲突未落库：{'、'.join(sorted(conflicts))}"
        return entry, message

    # ------------------------------------------------------------------
    # 复核、台账、登记与契约核对
    # ------------------------------------------------------------------
    def dataset_rows(self, field: str | None = None) -> list[dict[str, Any]]:
        """清洗后的数据集：查询侧只读取归并后的当前重复组。"""
        rows = self._store.rows(DATASET_TABLE)
        if field:
            rows = [row for row in rows if str(row.get("去重字段")) == str(field)]
        return rows

    def group_rows(self) -> list[dict[str, Any]]:
        return self._store.rows(GROUP_TABLE)

    def registry_rows(self) -> list[dict[str, Any]]:
        return self._store.rows(REGISTRY_TABLE)

    def review_group(self, key: str) -> dict[str, Any] | None:
        """复核口径：以清洗后的数据集为准，返回保留记录、被归并行号与规则版本。"""
        row = self._dataset_row(key)
        if row is None:
            return None
        group = self._group_row(key) or {}
        return {
            "去重键": key,
            "保留记录": row.get("保留记录"),
            "保留批次": row.get("保留批次"),
            "保留行号": row.get("保留行号"),
            "被归并行号": list(row.get("被归并行号", [])),
            "规则版本": row.get("规则版本"),
            "版本历史": list(group.get("版本历史", [])),
            "重算次数": group.get("重算次数", 0),
            "归并批次": row.get("归并批次"),
            "复核口径": REVIEW_NOTE,
        }

    def contract_check(self) -> list[dict[str, Any]]:
        """清洗规则契约核对：其他业务模块的去重字段、规则版本要与主结果一致。"""
        registry = {str(row.get("去重字段")): row for row in self._store.rows(REGISTRY_TABLE)}
        results: list[dict[str, Any]] = []
        for rule in self._store.rows("cleanse"):
            field = str(rule.get("去重字段") or "").strip()
            version = str(rule.get("规则版本") or "").strip()
            contract = registry.get(field)
            if contract is None:
                verdict = f"字段未登记：{field} 不在去重字段登记中"
            elif version != str(contract.get("当前规则版本")):
                verdict = f"版本漂移：规则版本 {version}，契约版本 {contract.get('当前规则版本')}"
            else:
                verdict = "一致"
            results.append({
                "规则编号": rule.get("规则编号"),
                "去重字段": field,
                "规则版本": version,
                "核对结果": verdict,
            })
        return results

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------
    def _build_plan(
        self,
        entry: dict[str, Any],
        records: list[dict[str, Any]],
        contract: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        """只针对迟到记录命中的重复组生成归并计划，旧批次其他记录不重算。"""
        field = str(entry["去重字段"])
        contract_version = str(contract.get("当前规则版本"))
        late_by_key: dict[str, list[dict[str, Any]]] = {}
        anomalies: list[str] = []
        for record in records:
            key = str(record.get(field) or "").strip()
            if not key:
                anomalies.append(f"{record.get('批次号')}#{record.get('行号')} 缺少去重键 {field}")
                continue
            late_by_key.setdefault(key, []).append(record)

        plan: dict[str, dict[str, Any]] = {}
        for key, late_records in late_by_key.items():
            current = self._dataset_row(key)
            candidates: list[dict[str, Any]] = []
            if current is not None:
                candidates.append({
                    "引用": f"{current['保留批次']}#{current['保留行号']}",
                    "批次号": current["保留批次"],
                    "行号": int(current["保留行号"]),
                    "规则版本": str(current.get("规则版本")),
                    "记录摘要": current.get("保留记录"),
                })
            for record in late_records:
                candidates.append({
                    "引用": f"{record['批次号']}#{record['行号']}",
                    "批次号": record["批次号"],
                    "行号": int(record["行号"]),
                    "规则版本": str(record.get("规则版本")),
                    "记录摘要": f"{record['批次号']}#{record['行号']} {record.get('姓名', '')}".strip(),
                })
            # 优先级仲裁：契约规则版本优先；同版本保留首条（批次号、行号升序）。
            ordered = sorted(
                candidates,
                key=lambda item: (
                    0 if item["规则版本"] == contract_version else 1,
                    str(item["批次号"]),
                    int(item["行号"]),
                ),
            )
            winner = ordered[0]
            losers = ordered[1:]
            old_merged = list(current.get("被归并行号", [])) if current is not None else []
            merged = sorted(set(old_merged) | {item["引用"] for item in losers})
            group = self._group_row(key) or {}
            versions = sorted(
                set(group.get("版本历史", [])) | {item["规则版本"] for item in candidates}
            )
            plan[key] = {
                "去重键": key,
                "保留引用": winner["引用"],
                "保留记录": winner["记录摘要"],
                "保留批次": winner["批次号"],
                "保留行号": winner["行号"],
                "规则版本": winner["规则版本"],
                "被归并行号": merged,
                "新增归并": sorted({item["引用"] for item in losers} - set(old_merged)),
                "版本历史": versions,
                "是否新组": current is None,
            }
        return plan, anomalies

    def _apply_group(self, entry: dict[str, Any], item: dict[str, Any]) -> None:
        """把单个重复组的仲裁结果写进清洗后的数据集与台账（幂等 upsert）。"""
        key = item["去重键"]
        dataset_row = self._dataset_row(key)
        if dataset_row is None:
            dataset_row = {
                "id": self._next_id(DATASET_TABLE),
                "去重键": key,
                "去重字段": entry["去重字段"],
            }
            self._store.rows(DATASET_TABLE).append(dataset_row)
        dataset_row["保留记录"] = item["保留记录"]
        dataset_row["保留批次"] = item["保留批次"]
        dataset_row["保留行号"] = item["保留行号"]
        dataset_row["被归并行号"] = item["被归并行号"]
        dataset_row["规则版本"] = item["规则版本"]
        dataset_row["归并批次"] = entry["批次编号"]

        group = self._group_row(key)
        if group is None:
            group = {"id": self._next_id(GROUP_TABLE), "去重键": key, "重算次数": 0, "影响来源批次": []}
            self._store.rows(GROUP_TABLE).append(group)
        group["版本历史"] = item["版本历史"]
        group["重算次数"] = int(group.get("重算次数", 0)) + 1
        group["最近归并批次"] = entry["批次编号"]
        group["影响来源批次"] = sorted(set(group.get("影响来源批次", [])) | {entry["批次编号"]})
        group["状态"] = "当前"

    def _sync_registry(self, field: str) -> None:
        """契约同步：清洗后的数据集快照写回去重字段登记，查询只读当前重复组。"""
        contract = self._contract_for(field)
        if contract is None:
            return
        rows = self.dataset_rows(field)
        contract["当前重复组"] = len(rows)
        contract["保留记录数"] = len(rows)
        contract["快照版本"] = int(contract.get("快照版本", 0)) + 1
        contract["同步时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        contract["契约状态"] = "已同步"

    def _find_completed_peer(self, idem_key: str, *, exclude_id: int) -> dict[str, Any] | None:
        if not idem_key:
            return None
        for row in self._store.rows(MODULE):
            if int(row.get("id", 0)) == exclude_id:
                continue
            if str(row.get("幂等键")) == idem_key and row.get("status") == "已归并":
                return row
        return None

    def _contract_for(self, field: str) -> dict[str, Any] | None:
        for row in self._store.rows(REGISTRY_TABLE):
            if str(row.get("去重字段")) == str(field):
                return row
        return None

    def _dataset_row(self, key: str) -> dict[str, Any] | None:
        for row in self._store.rows(DATASET_TABLE):
            if str(row.get("去重键")) == str(key):
                return row
        return None

    def _group_row(self, key: str) -> dict[str, Any] | None:
        for row in self._store.rows(GROUP_TABLE):
            if str(row.get("去重键")) == str(key):
                return row
        return None

    def _next_id(self, table: str) -> int:
        return max((int(row.get("id", 0)) for row in self._store.rows(table)), default=0) + 1

    def _lock_for(self, key: str) -> threading.Lock:
        with self._key_locks_guard:
            return self._key_locks.setdefault(str(key), threading.Lock())

    def _acquire_key_locks(self, keys: Any) -> set[str]:
        """非阻塞抢锁：争用中的去重键不等待、不落库，只允许一个结果写成功。"""
        acquired: set[str] = set()
        for key in sorted(str(key) for key in keys):
            if self._lock_for(key).acquire(blocking=False):
                acquired.add(key)
        return acquired

    def _release_key_locks(self, keys: Any) -> None:
        for key in keys:
            self._lock_for(key).release()

    def _finish(self, entry: dict[str, Any], status: str, *, 复核状态: str) -> None:
        entry["status"] = status
        entry["pending"] = status in PENDING_STATUSES
        entry["abnormal"] = status in ABNORMAL_STATUSES
        entry["复核状态"] = 复核状态

    def _fail(self, entry: dict[str, Any], message: str) -> tuple[dict[str, Any], str]:
        entry["归并结果"] = message
        self._finish(entry, "归并失败", 复核状态="待重试")
        return entry, message
