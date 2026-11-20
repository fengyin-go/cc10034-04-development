"""分区同步工作流接口：计划生成、权限核对、计划执行与续做查询。

与通用资源路由分开承载，路径固定在 /api/partition-workflow 下，
避免被 /api/{module} 动态路由抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/partition-workflow", tags=["分区同步工作流"])


class OwnerPayload(BaseModel):
    database: str
    table: str
    partition: str
    owner_team: str
    scope: str = "读写"


class RevokePayload(BaseModel):
    database: str
    table: str
    partition: str
    reason: str = "维护团队交接"


class RecheckPayload(BaseModel):
    database: str
    table: str
    partitions: list[str] = Field(default_factory=list)


class DdlPayload(BaseModel):
    database: str
    table: str
    ddl_statement: str


class PlanPayload(BaseModel):
    config_id: int | None = None
    source_database: str
    source_table: str
    target_database: str | None = None
    target_table: str | None = None
    handover_group: list[str] = Field(default_factory=list)
    owner_overrides: dict[str, str] = Field(default_factory=dict)
    plan_name: str = ""
    # 允许直接带入 DDL 追踪产出的分区动作；缺省时从 DDL 日志自动汇总
    partition_changes: list[dict[str, Any]] | None = None


class ApplyPayload(BaseModel):
    plan_id: str
    target_database: str | None = None
    target_table: str | None = None


def _partition_service():
    return service_for("partition_sync")


def _ddl_service():
    return service_for("ddl_tracker")


@router.post("/ddl/record")
def record_ddl(payload: DdlPayload) -> dict[str, Any]:
    """登记并分析一条带语句编号的分区 DDL。"""
    ddl = _ddl_service().record_ddl(payload.database, payload.table, payload.ddl_statement)
    return {
        "ok": True,
        "record_id": ddl.record_id,
        "stmt_no": ddl.stmt_no,
        "partition_version": ddl.partition_version,
        "partition_changes": ddl.partition_changes,
    }


@router.get("/ddl/pending")
def pending_ddl(database: str, table: str,
                target_database: str | None = None,
                target_table: str | None = None) -> dict[str, Any]:
    """DDL追踪按目标侧已应用分区版本生成的剩余待同步变更。"""
    return _ddl_service().pending_partition_changes(
        database, table, target_database, target_table)


@router.post("/owners")
def register_owner(payload: OwnerPayload) -> dict[str, Any]:
    """登记分区归属维护团队与写入范围。"""
    grant = _partition_service().register_partition_owner(
        payload.database, payload.table, payload.partition,
        payload.owner_team, payload.scope)
    return {"ok": True, "grant": grant}


@router.post("/owners/revoke")
def revoke_owner(payload: RevokePayload) -> dict[str, Any]:
    """交接收回目标分区写权限（降级为只读）。"""
    result = _partition_service().revoke_partition_write(
        payload.database, payload.table, payload.partition, payload.reason)
    return {"ok": True, "grant": result}


@router.post("/permissions/recheck")
def recheck_permissions(payload: RecheckPayload) -> dict[str, Any]:
    """执行前重新核对目标分区写权限。"""
    return _partition_service().recheck_write_permission(
        payload.database, payload.table, payload.partitions)


@router.post("/plans")
def build_plan(payload: PlanPayload) -> dict[str, Any]:
    """按维护团队核对范围、按分区归属切分，生成分区变更计划数据集。"""
    service = _partition_service()
    config: dict[str, Any] = {"配置名称": f"plan-{payload.source_table}"}
    if payload.config_id is not None:
        entry = service.get_entry(payload.config_id)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail=f"同步配置 {payload.config_id} 不存在")
        config = entry

    changes = payload.partition_changes
    if not changes:
        # 缺省从 DDL 追踪日志汇总该表全部分区动作
        changes = []
        for ddl in service.detect_partition_changes(
                payload.source_table, payload.target_table or payload.source_table)["ddls"]:
            changes.extend(ddl["changes"])

    plan = service.build_partition_change_plan(
        config,
        payload.source_database,
        payload.source_table,
        changes,
        target_database=payload.target_database,
        target_table=payload.target_table,
        handover_group=payload.handover_group,
        owner_overrides=payload.owner_overrides,
        plan_name=payload.plan_name,
    )
    return {"ok": True, "plan": plan}


@router.get("/plans")
def list_plans() -> dict[str, Any]:
    plans = _partition_service().list_plans()
    return {"ok": True, "items": plans, "total": len(plans)}


@router.get("/plans/{plan_id}")
def get_plan(plan_id: str) -> dict[str, Any]:
    plan = _partition_service().get_plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"分区变更计划 {plan_id} 不存在")
    return {"ok": True, "plan": plan}


@router.post("/plans/apply")
def apply_plan(payload: ApplyPayload) -> dict[str, Any]:
    """执行计划：重新核对写权限、替换组整体判定、幂等应用并产出续做成员。"""
    result = _partition_service().apply_partition_plan(
        payload.plan_id,
        target_database=payload.target_database,
        target_table=payload.target_table)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.get("/plans/{plan_id}/continuation")
def get_continuation(plan_id: str) -> dict[str, Any]:
    """核对续做成员：未应用计划项与 DDL 剩余待同步语句。"""
    service = _partition_service()
    if service.get_plan(plan_id) is None:
        raise HTTPException(status_code=404, detail=f"分区变更计划 {plan_id} 不存在")
    return {"ok": True, "continuation": service.build_continuation(plan_id)}
