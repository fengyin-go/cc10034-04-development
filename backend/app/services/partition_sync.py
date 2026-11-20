"""分区同步领域服务：管理分区表的DDL同步策略。

上游：ddl_tracker 检测分区变更（带语句编号 stmt_no 与分区版本 version）
消费：schema_sync 消费分区映射进行列信息同步

分区变更计划数据集（核心新增能力）：
1. 按维护团队核对数据范围：只接受归属团队仍持有写权限的分区变更
2. 按分区归属切分变更记录：交接组（删除并重建的关联分区）/ 独立分区（单独新增）
3. 生成分区变更计划数据集：每个计划项携带目标分区、动作、语句编号、分区版本、归属团队

执行约束：
- 执行时重新核对目标分区写权限，写权限未生效时操作只读（plan 保持待应用）
- 同一分区的删除与重建视为一次完整替换（replacement 组整体判定）
- 重复提交幂等：已落库的分区版本不会被再次应用
- DDL追踪按目标侧实际应用的分区版本生成剩余待同步变更，供下一轮续做
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class PartitionSyncService(BaseModuleService):
    MODULE = "partition_sync"
    ENTITY = "同步配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "源分区策略", "目标分区策略", "分区映射", "新增分区行为", "过期清理", "分区字段"]
    REQUIRED_FIELDS = ["配置名称", "源分区策略", "目标分区策略"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已启用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def __init__(self) -> None:
        # 分区变更计划数据集：plan_id -> 计划（含计划项与执行状态）
        self.partition_plans: dict[str, dict[str, Any]] = {}
        self._plan_seq = 0

    # ------------------------------------------------------------------
    # 维护团队与写权限
    # ------------------------------------------------------------------
    def register_partition_owner(
        self, database: str, table: str, partition: str, owner_team: str,
        scope: str = "读写",
    ) -> dict[str, Any]:
        """登记分区归属维护团队及初始写入范围。"""
        grant = pipeline_bus.grant_partition_write(database, table, partition, owner_team, scope)
        pipeline_bus.write_audit(
            "分区归属登记", grant["key"],
            f"分区 {partition} 归属维护团队 {owner_team}，写入范围 {scope}",
            "partition_sync", "已登记"
        )
        return grant

    def revoke_partition_write(
        self, database: str, table: str, partition: str, reason: str = "维护团队交接",
    ) -> dict[str, Any]:
        """交接收回：撤掉目标分区写权限，保留只读。"""
        grant = pipeline_bus.revoke_partition_write(database, table, partition, reason)
        if grant is None:
            return {"key": pipeline_bus.partition_key(database, table, partition),
                    "writable": False, "message": "分区权限未登记，按只读处理"}
        pipeline_bus.write_audit(
            "分区写权限收回", grant["key"],
            f"分区 {partition} 写权限已收回（{reason}），当前范围 {grant['scope']}",
            "partition_sync", "只读"
        )
        return grant

    def recheck_write_permission(self, database: str, table: str,
                                 partitions: list[str]) -> dict[str, Any]:
        """执行前重新核对一组目标分区的写入权限。"""
        checks = {
            name: pipeline_bus.check_partition_write(database, table, name)
            for name in partitions
        }
        return {
            "database": database,
            "table": table,
            "checks": checks,
            "writable_partitions": [name for name, c in checks.items() if c["writable"]],
            "readonly_partitions": [name for name, c in checks.items() if not c["writable"]],
            "checked_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # 步骤一/二/三：按维护团队核对范围 → 按归属切分 → 生成计划数据集
    # ------------------------------------------------------------------
    def build_partition_change_plan(
        self,
        config: dict[str, Any],
        source_database: str,
        source_table: str,
        partition_changes: list[dict[str, Any]],
        *,
        target_database: str | None = None,
        target_table: str | None = None,
        handover_group: list[str] | None = None,
        owner_overrides: dict[str, str] | None = None,
        plan_name: str = "",
    ) -> dict[str, Any]:
        """生成分区变更计划数据集。

        参数：
            partition_changes: ddl_tracker 产出的分区动作（含 stmt_no/version）
            handover_group: 交接组分区名列表，同分区的 drop+add 归入完整替换组
            owner_overrides: 分区名 -> 维护团队 的归属覆盖（默认从权限注册表读取）
        """
        self._plan_seq += 1
        plan_id = f"pp_{datetime.now().strftime('%Y%m%d')}_{self._plan_seq:04d}"
        target_database = target_database or source_database
        target_table = target_table or source_table
        handover_group = handover_group or []
        owner_overrides = owner_overrides or {}
        partition_field = config.get("分区字段", "dt")

        # 步骤一：按维护团队核对数据范围（权限 + 归属）
        scoped_changes, scope_skipped = self._scope_changes_by_owner(
            target_database, target_table, partition_changes, owner_overrides)

        # 步骤二：按分区归属切分（交接替换组 / 独立分区）
        groups = self._split_by_ownership(scoped_changes, handover_group)

        # 步骤三：组装计划数据集
        plan_items: list[dict[str, Any]] = []
        for group in groups:
            for change in group["changes"]:
                part_name = change.get("name", "")
                plan_items.append({
                    "item_id": f"{plan_id}_i{len(plan_items) + 1:02d}",
                    "plan_id": plan_id,
                    "target_partition": part_name,
                    "target_table": f"{target_database}.{target_table}",
                    "action": change.get("action"),
                    "stmt_no": change.get("stmt_no"),
                    "partition_version": change.get("version"),
                    "ddl_id": change.get("ddl_id"),
                    "owner_team": change.get("owner_team", ""),
                    "group_type": group["group_type"],  # replacement / standalone
                    "group_id": group["group_id"],
                    "write_scope": change.get("write_scope", "未授权"),
                    "apply_status": "待应用",
                    "applied_version": None,
                })

        plan = {
            "plan_id": plan_id,
            "plan_name": plan_name or f"{config.get('配置名称', '分区同步')}-{plan_id}",
            "config_name": config.get("配置名称"),
            "source_table": f"{source_database}.{source_table}",
            "target_table": f"{target_database}.{target_table}",
            "partition_field": partition_field,
            "generated_at": datetime.now().isoformat(),
            "status": "已登记",
            "round": 1,
            "groups": groups,
            "items": plan_items,
            "scope_skipped": scope_skipped,
            "execution_history": [],
        }
        self.partition_plans[plan_id] = plan

        pipeline_bus.write_audit(
            "分区变更计划", plan_id,
            (f"生成计划数据集：{len(plan_items)} 个计划项，"
             f"替换组 {sum(1 for g in groups if g['group_type'] == 'replacement')} 个，"
             f"独立分区 {sum(1 for g in groups if g['group_type'] == 'standalone')} 个，"
             f"范围外跳过 {len(scope_skipped)} 项"),
            "partition_sync", "已登记"
        )
        return self._plan_view(plan)

    def _scope_changes_by_owner(
        self, database: str, table: str,
        changes: list[dict[str, Any]],
        owner_overrides: dict[str, str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """步骤一：按维护团队核对数据范围，标注归属与写入范围。"""
        scoped: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for change in changes:
            part_name = change.get("name", "")
            grant = pipeline_bus.check_partition_write(database, table, part_name)
            owner = owner_overrides.get(part_name) or grant.get("owner_team", "")
            enriched = dict(change)
            enriched["owner_team"] = owner
            enriched["write_scope"] = grant.get("scope", "未授权")
            enriched["writable"] = grant.get("writable", False)
            # 范围核对只做标注不剔除：收回写权限的分区仍进计划，执行时按只读拦截
            scoped.append(enriched)
            if not owner:
                skipped.append({
                    "partition": part_name,
                    "stmt_no": change.get("stmt_no"),
                    "reason": "未核对到维护团队归属，保留计划项待权限确认",
                })
        return scoped, skipped

    def _split_by_ownership(
        self, changes: list[dict[str, Any]], handover_group: list[str],
    ) -> list[dict[str, Any]]:
        """步骤二：按分区归属切分——交接组（删除+重建完整替换）与独立分区。"""
        groups: list[dict[str, Any]] = []
        grouped_keys: set[str] = set()

        # 交接组：同一分区的 drop_partition + add_partition 视为一次完整替换
        for part_name in handover_group:
            pair = [c for c in changes if c.get("name") == part_name]
            if not pair:
                continue
            actions = {c.get("action") for c in pair}
            group_type = "replacement" if {"drop_partition", "add_partition"} <= actions else "handover"
            groups.append({
                "group_id": f"grp_replace_{part_name}",
                "group_type": group_type,
                "partition": part_name,
                "owner_team": pair[0].get("owner_team", ""),
                "changes": sorted(pair, key=lambda c: (c.get("stmt_no", 0), c.get("version", 0))),
                "replacement": group_type == "replacement",
            })
            grouped_keys.add(part_name)

        # 独立分区：未进入交接组的变更，按分区各自成组
        for change in changes:
            part_name = change.get("name", "")
            if part_name in grouped_keys:
                continue
            groups.append({
                "group_id": f"grp_standalone_{part_name or change.get('stmt_no')}",
                "group_type": "standalone",
                "partition": part_name,
                "owner_team": change.get("owner_team", ""),
                "changes": [change],
                "replacement": False,
            })
            grouped_keys.add(part_name)

        groups.sort(key=lambda g: min(c.get("stmt_no", 0) for c in g["changes"]))
        return groups

    # ------------------------------------------------------------------
    # 执行：重新核对权限 → 替换组整体判定 → 幂等应用
    # ------------------------------------------------------------------
    def apply_partition_plan(
        self, plan_id: str, *, target_database: str | None = None,
        target_table: str | None = None,
    ) -> dict[str, Any]:
        """执行分区变更计划。

        - 执行时重新核对每个目标分区的写权限
        - 替换组内任一分区写权限未生效，整组只读跳过（保持待应用，留给下一轮）
        - 已落库的分区版本直接跳过，重复提交不会再次应用
        """
        plan = self.partition_plans.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"分区变更计划 {plan_id} 不存在", "plan_id": plan_id}

        target = plan["target_table"].split(".")
        t_db = target_database or target[0]
        t_tbl = target_table or target[1]
        partitions = sorted({item["target_partition"] for item in plan["items"] if item["target_partition"]})
        permission = self.recheck_write_permission(t_db, t_tbl, partitions)

        applied_items: list[dict[str, Any]] = []
        blocked_items: list[dict[str, Any]] = []
        skipped_idempotent: list[dict[str, Any]] = []

        for group in plan["groups"]:
            group_items = [it for it in plan["items"] if it["group_id"] == group["group_id"]]
            group_partitions = {it["target_partition"] for it in group_items}
            # 替换组：删除与重建视为一次完整替换，权限按整组核对
            blocked = {
                p for p in group_partitions
                if p in permission["readonly_partitions"]
            }
            for item in group_items:
                part = item["target_partition"]
                applied_version = pipeline_bus.applied_partition_version(
                    t_db, t_tbl, part)

                # 幂等：计划项版本已落库，重复提交不再应用
                if applied_version >= item["partition_version"]:
                    item["apply_status"] = "已应用"
                    item["applied_version"] = applied_version
                    skipped_idempotent.append({
                        "item_id": item["item_id"], "partition": part,
                        "applied_version": applied_version,
                        "plan_version": item["partition_version"],
                    })
                    continue

                if blocked or part in permission["readonly_partitions"]:
                    item["apply_status"] = "只读拦截"
                    item["block_reason"] = permission["checks"].get(part, {}).get(
                        "reason", "写权限未生效")
                    blocked_items.append({
                        "item_id": item["item_id"], "partition": part,
                        "action": item["action"], "stmt_no": item["stmt_no"],
                        "group_type": item["group_type"],
                        "reason": item["block_reason"],
                    })
                    continue

                # 实际应用：登记目标分区版本
                new_version = pipeline_bus.mark_partition_version_applied(
                    t_db, t_tbl, part, item["partition_version"])
                item["apply_status"] = "已应用"
                item["applied_version"] = new_version
                applied_items.append({
                    "item_id": item["item_id"], "partition": part,
                    "action": item["action"], "stmt_no": item["stmt_no"],
                    "version": new_version, "group_type": item["group_type"],
                })

        all_done = all(it["apply_status"] == "已应用" for it in plan["items"])
        plan["status"] = "已完成" if all_done else "部分应用"
        record = {
            "round": plan["round"],
            "executed_at": datetime.now().isoformat(),
            "applied": applied_items,
            "blocked": blocked_items,
            "skipped_idempotent": skipped_idempotent,
            "permission": {
                "writable": permission["writable_partitions"],
                "readonly": permission["readonly_partitions"],
            },
        }
        plan["execution_history"].append(record)
        plan["round"] += 1

        pipeline_bus.write_audit(
            "分区计划执行", plan_id,
            (f"第{record['round']}轮：应用 {len(applied_items)} 项，"
             f"只读拦截 {len(blocked_items)} 项，幂等跳过 {len(skipped_idempotent)} 项"),
            "partition_sync", plan["status"]
        )

        return {
            "ok": True,
            "plan_id": plan_id,
            "status": plan["status"],
            "round": record["round"],
            "applied_partitions": self._applied_target_partitions(plan),
            "next_import_scope": self._next_import_scope(plan, t_db, t_tbl),
            "continuation": self.build_continuation(plan_id),
            "execution": record,
        }

    def _applied_target_partitions(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """本轮实际落库的目标分区及版本。"""
        return [
            {
                "partition": item["target_partition"],
                "applied_version": item["applied_version"],
                "owner_team": item["owner_team"],
                "group_type": item["group_type"],
            }
            for item in plan["items"] if item["apply_status"] == "已应用"
        ]

    def _next_import_scope(self, plan: dict[str, Any], database: str,
                           table: str) -> list[dict[str, Any]]:
        """下一次导入可用范围：当前持有写权限且已落库的分区。"""
        scope: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in plan["items"]:
            part = item["target_partition"]
            if part in seen:
                continue
            seen.add(part)
            grant = pipeline_bus.check_partition_write(database, table, part)
            applied = pipeline_bus.applied_partition_version(database, table, part)
            if grant["writable"] and applied > 0:
                scope.append({
                    "partition": part,
                    "write_scope": grant["scope"],
                    "owner_team": grant["owner_team"],
                    "applied_version": applied,
                    "available_for_import": True,
                })
        return scope

    def build_continuation(self, plan_id: str) -> dict[str, Any]:
        """续做成员：未应用的计划项 + DDL追踪按已应用版本给出的剩余待同步变更。"""
        plan = self.partition_plans.get(plan_id)
        if plan is None:
            return {"plan_id": plan_id, "pending_items": [], "ddl_pending": []}
        pending_items = [
            {
                "item_id": it["item_id"],
                "target_partition": it["target_partition"],
                "action": it["action"],
                "stmt_no": it["stmt_no"],
                "partition_version": it["partition_version"],
                "group_type": it["group_type"],
                "apply_status": it["apply_status"],
                "block_reason": it.get("block_reason", ""),
            }
            for it in plan["items"] if it["apply_status"] != "已应用"
        ]
        source = plan["source_table"].split(".")
        target = plan["target_table"].split(".")
        ddl_pending = self.pending_sync_changes(
            source[0], source[1], target[0], target[1])
        return {
            "plan_id": plan_id,
            "next_round": plan["round"],
            "pending_items": pending_items,
            "pending_stmt_nos": sorted({it["stmt_no"] for it in pending_items}),
            "ddl_pending": ddl_pending,
        }

    def pending_sync_changes(self, source_database: str, source_table: str,
                             target_database: str, target_table: str) -> dict[str, Any]:
        """读取 DDL 追踪按目标侧已应用分区版本生成的剩余待同步变更。"""
        # 延迟导入避免模块循环依赖
        from app.services import service_for
        ddl_service = service_for("ddl_tracker")
        return ddl_service.pending_partition_changes(
            source_database, source_table, target_database, target_table)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        plan = self.partition_plans.get(plan_id)
        return self._plan_view(plan) if plan else None

    def list_plans(self) -> list[dict[str, Any]]:
        return [self._plan_view(plan) for plan in self.partition_plans.values()]

    def _plan_view(self, plan: dict[str, Any]) -> dict[str, Any]:
        """计划视图：数据集条目 + 分组 + 执行历史，便于核对。"""
        return {
            "plan_id": plan["plan_id"],
            "plan_name": plan["plan_name"],
            "config_name": plan["config_name"],
            "source_table": plan["source_table"],
            "target_table": plan["target_table"],
            "partition_field": plan["partition_field"],
            "status": plan["status"],
            "round": plan["round"],
            "generated_at": plan["generated_at"],
            "scope_skipped": plan["scope_skipped"],
            "groups": [
                {k: v for k, v in g.items() if k != "changes"} for g in plan["groups"]
            ],
            "items": [dict(it) for it in plan["items"]],
            "execution_history": plan["execution_history"],
        }

    # ------------------------------------------------------------------
    # 原有能力保留
    # ------------------------------------------------------------------
    def sync_partition_ddl(self, config: dict[str, Any], ddl_partition_changes: list[dict[str, Any]]) -> dict[str, Any]:
        """根据DDL中检测到的分区变更生成目标DDL。

        从 ddl_tracker 产出的 partition_changes 读取变更列表，
        结合分区映射和增量清理策略生成同步DDL。
        """
        source_policy = config.get("源分区策略", "RANGE")
        target_policy = config.get("目标分区策略", "RANGE")
        partition_mapping = config.get("分区映射", "直接映射")
        add_behavior = config.get("新增分区行为", "自动创建")
        expire_cleanup = config.get("过期清理", "自动") == "自动"
        partition_field = config.get("分区字段", "dt")

        sync_actions: list[dict[str, Any]] = []
        for change in ddl_partition_changes:
            action = change.get("action", "")
            if action == "add_partition":
                sync_actions.append({
                    "action": "add",
                    "source_ddl": "ADD PARTITION",
                    "stmt_no": change.get("stmt_no"),
                    "partition_version": change.get("version"),
                    "target_ddl": self._build_target_add_partition(
                        target_policy, partition_field, add_behavior),
                    "status": "synced",
                })
            elif action == "drop_partition":
                cleanup_action = "drop" if expire_cleanup else "archive"
                sync_actions.append({
                    "action": "drop",
                    "partition_name": change.get("name", ""),
                    "stmt_no": change.get("stmt_no"),
                    "partition_version": change.get("version"),
                    "target_action": cleanup_action,
                    "status": "synced",
                })

        pipeline_bus.write_audit(
            "分区同步", partition_field,
            f"同步 {len(sync_actions)} 个分区变更操作",
            "partition_sync", "已同步"
        )
        return {
            "config": config.get("配置名称"),
            "partition_field": partition_field,
            "source_policy": source_policy,
            "target_policy": target_policy,
            "sync_actions": sync_actions,
            "action_count": len(sync_actions),
            "synced_at": datetime.now().isoformat(),
        }

    def _build_target_add_partition(self, target_policy: str, field: str, behavior: str) -> str:
        if behavior == "自动创建":
            return f"ALTER TABLE ... ADD PARTITION ({field})"
        return f"-- 待手动创建分区 ({field})，策略：{target_policy}"

    def detect_partition_changes(self, source_table: str, target_table: str) -> dict[str, Any]:
        """检测分区表的变化差异。"""
        recent_ddls = pipeline_bus.get_recent_ddls(limit=100)
        partition_ddls = []
        for ddl in recent_ddls:
            if ddl.table_name == source_table and ddl.partition_changes:
                partition_ddls.append({
                    "ddl_id": ddl.record_id,
                    "stmt_no": ddl.stmt_no,
                    "changes": ddl.partition_changes,
                    "executed_at": ddl.executed_at.isoformat(),
                })
        return {
            "source_table": source_table,
            "target_table": target_table,
            "partition_ddl_count": len(partition_ddls),
            "ddls": partition_ddls,
        }

    def estimate_partition_growth(self, field: str, policy: str) -> dict[str, Any]:
        """估算分区增长趋势。"""
        partitions_per_month = 30 if policy in ("RANGE", "LIST") else 1
        retention_days = 90 if policy == "RANGE" else 30
        return {
            "partition_field": field,
            "policy": policy,
            "partitions_per_month_estimate": partitions_per_month,
            "retention_days": retention_days,
            "monthly_storage_factor": partitions_per_month * 10,  # ~10GB/分区
            "recommended_backup_window": "7d",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用配置":
            field = entry.get("分区字段", "dt")
            pipeline_bus.write_audit(
                "分区策略启用", field,
                f"分区同步配置 {entry.get('配置名称')} 已启用",
                "partition_sync", "已启用"
            )
        return entry, msg
