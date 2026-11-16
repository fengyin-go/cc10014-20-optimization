"""去重归并接口：迟到重复记录的增量归并、幂等写入、优先级仲裁与复核口径。

注意路由顺序：固定路径（dataset、groups、registry 等）必须声明在
`/{entry_id}` 之前，否则会被路径参数吞掉。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.dedup import DedupService

router = APIRouter(prefix="/api/dedup", tags=["去重归并"])

service = DedupService()

STATUSES = ["待归并", "归并中", "已归并", "空批次", "归并失败", "已中断", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按批次编号检索"),
    status: str | None = Query(default=None, description="待归并、已归并、空批次、归并失败、已中断、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按批次编号与状态过滤归并批次列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/dataset/current")
def current_dataset(field: str | None = Query(default=None, description="按去重字段过滤")) -> dict[str, Any]:
    """清洗后的数据集：每个去重键只保留一条当前记录，是复核与查询的唯一口径。"""
    rows = service.dataset_rows(field)
    return {"module": "dedup", "total": len(rows), "items": rows}


@router.get("/groups")
def group_ledger() -> dict[str, Any]:
    """重复组台账：版本历史、重算次数与影响来源批次，只随受影响重复组更新。"""
    rows = service.group_rows()
    return {"module": "dedup", "total": len(rows), "items": rows}


@router.get("/registry")
def registry() -> dict[str, Any]:
    """去重字段登记：清洗后的数据集同步到这里，查询只读取归并后的当前重复组。"""
    rows = service.registry_rows()
    return {"module": "dedup", "total": len(rows), "items": rows}


@router.get("/source")
def source_records(batch_no: str = Query(..., description="按批次号取源记录")) -> dict[str, Any]:
    """按既有取数链路读取批次源记录（含迟到记录），用于判断受影响记录。"""
    rows = service.fetch_source_records(batch_no)
    return {"module": "dedup", "total": len(rows), "items": rows}


@router.get("/review")
def review_group(key: str = Query(..., description="要复核的去重键")) -> dict[str, Any]:
    """复核口径：以清洗后的数据集为准，返回保留记录、被归并行号与规则版本。"""
    result = service.review_group(key)
    if result is None:
        raise HTTPException(status_code=404, detail=f"去重键 {key} 不在清洗后的数据集中")
    return result


@router.get("/contract/check")
def contract_check() -> dict[str, Any]:
    """清洗规则契约核对：其他业务模块的去重字段、规则版本要与主结果一致。"""
    results = service.contract_check()
    drift = sum(1 for item in results if item["核对结果"] != "一致")
    return {"module": "dedup", "total": len(results), "drift": drift, "items": results}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出去重归并清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dedup", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条归并批次明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"归并批次 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一个归并批次，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="归并批次已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对归并批次执行归并、重试归并、停用批次；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
