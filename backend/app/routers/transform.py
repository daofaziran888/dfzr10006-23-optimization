"""数据加工转换接口：维护加工规则，覆盖启用规则、暂停规则、关闭规则等动作。

批量导入与转换取证：
- POST /api/transform/import            批量导入（指纹缓存复用，只解析新增或变更项）
- POST /api/transform/dry-run           试运行（输出格式、被裁剪字段、转换日志）
- GET  /api/transform/batches           导入批次列表
- GET  /api/transform/parse-cache       解析缓存（指纹 → 解析版本）
- GET  /api/transform/results           加工结果（书签、导入版本、下载路径）
- POST /api/transform/results/{书签}/verify         核对结果并发布已验证输出字段快照
- GET  /api/transform/results/{书签}/download       下载（仅已核对的输出内容）
- POST /api/transform/results/{书签}/sync-subscribe 同步到数据订阅推送
- POST /api/transform/rollback/{规则编号}           回滚到上一版加工结果
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.transform import TransformService
from app.services.transform_import import transform_import_service

router = APIRouter(prefix="/api/transform", tags=["数据加工转换"])

service = TransformService()
import_service = transform_import_service

LIST_FIELDS = ["规则编号", "输入格式", "输出格式", "映射表达式", "字段过滤", "编码设置", "转换日志", "规则状态"]
STATUSES = ["草稿", "生效中", "已暂停", "已关闭"]


class ImportPayload(BaseModel):
    """批量导入请求：rules 为加工规则数组，auto_dry_run 控制导入后是否自动试运行。"""

    rules: list[dict[str, Any]] = Field(default_factory=list)
    auto_dry_run: bool = True


class DryRunPayload(BaseModel):
    """试运行请求：rule_nos 为空时对全部已导入规则试运行。"""

    rule_nos: list[str] = Field(default_factory=list)


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、生效中、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按规则编号与状态过滤数据加工转换列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据加工转换清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "transform", "total": total, "items": items}


@router.post("/import")
def import_entries(payload: ImportPayload) -> dict[str, Any]:
    """批量导入加工规则：已解析过的规则直接复用缓存，只解析新增或变更项。"""
    result = import_service.import_batch(payload.rules, auto_dry_run=payload.auto_dry_run)
    if not result.get("ok") and result.get("明细") is None:
        raise HTTPException(status_code=400, detail=result.get("message", "导入失败"))
    return result


@router.post("/dry-run")
def dry_run_entries(payload: DryRunPayload) -> dict[str, Any]:
    """试运行：输出输出格式、被裁剪字段和转换日志；失败标明规则编号和缺失字段。"""
    return import_service.dry_run(payload.rule_nos or None)


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    """导入批次列表：每个批次即一个导入版本，供核对与回滚追溯。"""
    batches = import_service.list_batches()
    return {"items": batches, "total": len(batches)}


@router.get("/parse-cache")
def list_parse_cache() -> dict[str, Any]:
    """解析缓存：首次解析后的指纹与解析版本，相同规则再次导入只标注未变化。"""
    items = import_service.list_parse_cache()
    return {"items": items, "total": len(items)}


@router.get("/results")
def list_results(rule_no: str | None = Query(default=None, description="按规则编号过滤")) -> dict[str, Any]:
    """加工结果列表：书签、导入版本、解析版本；下载路径只引用已核对的输出内容。"""
    items = import_service.list_results(rule_no)
    return {"items": items, "total": len(items)}


@router.post("/results/{bookmark}/verify", response_model=ActionResult)
def verify_result(bookmark: str) -> ActionResult:
    """核对加工结果：核对后发布已验证的输出字段快照，供订阅推送引用。"""
    view, message = import_service.verify_result(bookmark)
    if view is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=view)


@router.get("/results/{bookmark}/download")
def download_result(bookmark: str) -> dict[str, Any]:
    """下载加工结果：只允许已核对的输出内容，未核对返回 409。"""
    payload, message = import_service.download_result(bookmark)
    if payload is None:
        raise HTTPException(status_code=409, detail=message)
    return payload


@router.post("/results/{bookmark}/sync-subscribe", response_model=ActionResult)
def sync_subscribe(bookmark: str) -> ActionResult:
    """把已核对的加工结果同步到数据订阅推送；推送格式只引用已验证的输出字段快照。"""
    task, message = import_service.sync_subscribe(bookmark)
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


@router.post("/rollback/{rule_no}", response_model=ActionResult)
def rollback_result(rule_no: str) -> ActionResult:
    """回滚到上一版加工结果，并在服务版本回退模块留痕。"""
    view, message = import_service.rollback(rule_no)
    if view is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=view)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条加工规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"加工规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条加工规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="加工规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条加工规则执行启用规则、暂停规则、关闭规则；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
