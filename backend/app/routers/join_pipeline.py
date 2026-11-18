"""序号7（流关联）关联结果版本路由：供流关联工作台与检查点模块消费。

接口约定：
  - 写接口全部幂等：同窗口并发重试只保留一版
  - 生命周期单向流转，已确认窗口不可回退
  - 检查点与聚合落库快照随关联结果数据在同一事务内可见
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.join_stream import (
    ARCHIVE_CONDITION,
    LIFECYCLE_ORDER,
    MERGE_LAST_ARRIVAL,
    MERGE_WINDOW_CUTOFF,
    RESTORE_CONDITION,
    JoinStreamService,
)

router = APIRouter(prefix="/api/join-stream", tags=["序号7-流关联版本"])

service = JoinStreamService()
checkpoint_service = CheckpointMgrService()


class ProducePayload(BaseModel):
    join_name: str
    left_events: list[dict[str, Any]] = Field(default_factory=list)
    right_events: list[dict[str, Any]] = Field(default_factory=list)
    merge_mode: str = MERGE_WINDOW_CUTOFF
    window_start: float | None = None
    window_end: float | None = None
    checkpoint_config: str | None = None
    fail_sink: bool = False
    fail_checkpoint: bool = False


class ResumePayload(BaseModel):
    checkpoint_config: str | None = None
    fail_sink: bool = False
    fail_checkpoint: bool = False


class CheckpointSwitchPayload(BaseModel):
    checkpoint_id: str | None = None


class CheckpointRefreshPayload(BaseModel):
    checkpoint_config: str | None = None


@router.get("/lifecycle")
def lifecycle_meta() -> dict[str, Any]:
    """返回序号7单向状态链路、合并模式与归档/恢复条件。"""
    return {
        "lifecycle": LIFECYCLE_ORDER,
        "merge_modes": [MERGE_WINDOW_CUTOFF, MERGE_LAST_ARRIVAL],
        "archive_condition": ARCHIVE_CONDITION,
        "restore_condition": RESTORE_CONDITION,
    }


@router.post("/versions")
def produce_version(payload: ProducePayload) -> dict[str, Any]:
    """读取双流事件，生成关联结果版本（含聚合快照与检查点的原子事务）。"""
    result = service.produce_join_version(
        payload.join_name,
        payload.left_events,
        payload.right_events,
        merge_mode=payload.merge_mode,
        window_start=payload.window_start,
        window_end=payload.window_end,
        checkpoint_config=payload.checkpoint_config,
        fail_sink=payload.fail_sink,
        fail_checkpoint=payload.fail_checkpoint,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=409 if result.get("conflict") else 400,
                            detail=result)
    return result


@router.get("/versions")
def list_versions(
    join_name: str | None = None,
    window_end: float | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """工作台窗口筛选：查看每个版本对应的检查点和聚合落库状态。"""
    items = service.list_workbench_versions(
        join_name=join_name, window_end=window_end, status=status,
    )
    return {"items": items, "total": len(items)}


@router.get("/versions/{version_id}")
def version_detail(version_id: str) -> dict[str, Any]:
    """状态详情：按窗口版本核对关联结果数据和聚合落库快照。"""
    detail = service.version_detail(version_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"关联结果版本「{version_id}」不存在")
    return detail


@router.post("/versions/{version_id}/resume")
def resume_version(version_id: str, payload: ResumePayload | None = None) -> dict[str, Any]:
    """关联中断后恢复：只补未完成键，已确认窗口不回退。"""
    payload = payload or ResumePayload()
    result = service.resume_version(
        version_id,
        checkpoint_config=payload.checkpoint_config,
        fail_sink=payload.fail_sink,
        fail_checkpoint=payload.fail_checkpoint,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/versions/{version_id}/confirm")
def confirm_version(version_id: str) -> dict[str, Any]:
    """待检查点 → 已确认：窗口封存。"""
    result = service.confirm_window(version_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/versions/{version_id}/archive")
def archive_version(version_id: str) -> dict[str, Any]:
    """已确认 → 已归档：归档条件写入关联结果数据。"""
    result = service.archive_version(version_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/versions/{version_id}/restore-read")
def restore_archived(version_id: str) -> dict[str, Any]:
    """按恢复条件只读恢复归档版本，不重新计算、不回退状态。"""
    result = service.restore_archived_version(version_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/versions/{version_id}/checkpoint/switch")
def switch_checkpoint(
    version_id: str, payload: CheckpointSwitchPayload | None = None,
) -> dict[str, Any]:
    """在版本详情窗口切换活动检查点引用。"""
    checkpoint_id = (payload.checkpoint_id if payload else None) or ""
    result = service.switch_checkpoint_ref(version_id, checkpoint_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.post("/versions/{version_id}/checkpoint/refresh")
def refresh_checkpoint(
    version_id: str, payload: CheckpointRefreshPayload | None = None,
) -> dict[str, Any]:
    """为版本补一版检查点，并发刷新只保留一个活动版本。"""
    result = service.refresh_checkpoint_ref(
        version_id, payload.checkpoint_config if payload else None,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result)
    return result


@router.get("/versions/{version_id}/conflict")
def conflict_priority(
    version_id: str,
    window_start: float,
    window_end: float,
) -> dict[str, Any]:
    """判断冲突优先级并核对关联结果数据事务影响。"""
    version = service.version_detail(version_id)
    if version is None:
        raise HTTPException(status_code=404, detail=f"关联结果版本「{version_id}」不存在")
    return service.inspect_conflict(
        version["join_name"], window_start, window_end,
    )


@router.get("/checkpoints/locate/{checkpoint_id}")
def locate_by_checkpoint(checkpoint_id: str) -> dict[str, Any]:
    """回放定位：检查点引用 → 完整关联结果版本。"""
    result = service.locate_version_by_checkpoint(checkpoint_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result)
    return result


@router.get("/checkpoints")
def list_join_checkpoints(version_id: str | None = None) -> dict[str, Any]:
    """检查点模块另一个入口：联动工作台查看流关联检查点。"""
    items = checkpoint_service.list_join_checkpoints(version_id)
    return {"items": items, "total": len(items)}


@router.get("/statistics")
def join_statistics() -> dict[str, Any]:
    """流关联模块统计：配置维度 + 版本生命周期分布。"""
    return {
        "joins": service.list_join_statistics(),
        "snapshot": service.domain_snapshot(),
    }
