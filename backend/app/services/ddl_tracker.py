"""DDL追踪领域服务：追踪和解析DDL变更对Schema的影响。

上游：从管道总线消费 DdlRecord 进行自动分析
下游：schema_sync(列级变更)、partition_sync(分区变更) 消费分析结果

分区追踪增强：
- 每条 DDL 按「库.表」分配单调递增的语句编号 stmt_no
- 每个分区动作（add_partition/drop_partition）携带语句编号与分区版本
- 按目标侧实际应用的分区版本计算剩余待同步变更，供下一轮分区同步读取
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import DdlRecord, pipeline_bus


class DdlTrackerService(BaseModuleService):
    MODULE = "ddl_tracker"
    ENTITY = "DDL记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"]
    REQUIRED_FIELDS = ["记录ID", "变更数据库", "变更表名"]
    STATUS_ORDER = ["已记录", "已分析", "已通知", "已处理"]
    ACTION_RULES = {"分析变更": "已记录", "发送通知": "已分析", "完成处理": "已分析"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 按「库.表」维护的语句编号序列
        self._stmt_seq: dict[str, int] = {}
        # 按「库.表.分区」维护的分区版本序列（每出现一次分区动作 +1）
        self._partition_version: dict[str, int] = {}

    def _next_stmt_no(self, database: str, table: str) -> int:
        key = f"{database}.{table}"
        self._stmt_seq[key] = self._stmt_seq.get(key, 0) + 1
        return self._stmt_seq[key]

    def _next_partition_version(self, database: str, table: str, partition: str) -> int:
        key = pipeline_bus.partition_key(database, table, partition)
        self._partition_version[key] = self._partition_version.get(key, 0) + 1
        return self._partition_version[key]

    def analyze_ddl_impact(self, ddl: DdlRecord) -> dict[str, Any]:
        """分析DDL变更对目标表的影响范围。

        解析 DDL 语句中的列变化、分区变化、索引变化，
        产出影响报告供 schema_sync 和 partition_sync 消费。
        分区变化会标注语句编号(stmt_no)与分区版本(version)。
        """
        ddl_text = ddl.ddl_statement.upper()
        column_changes: list[dict[str, Any]] = []
        partition_changes: list[dict[str, Any]] = []

        # 检测 ADD COLUMN
        add_cols = re.findall(r'ADD\s+COLUMN\s+(\w+)\s+(\w+)', ddl_text, re.IGNORECASE)
        for col_name, col_type in add_cols:
            column_changes.append({"action": "add", "column": col_name, "type": col_type})
            ddl.column_changes.append({"action": "add", "column": col_name, "type": col_type})

        # 检测 DROP COLUMN
        drop_cols = re.findall(r'DROP\s+(?:COLUMN\s+)?(\w+)', ddl_text, re.IGNORECASE)
        for col_name in drop_cols:
            column_changes.append({"action": "drop", "column": col_name})
            ddl.column_changes.append({"action": "drop", "column": col_name})

        # 检测 MODIFY COLUMN / CHANGE COLUMN
        modify_cols = re.findall(r'(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?(\w+)\s+(\w+)\s+(\w+)',
                                 ddl_text, re.IGNORECASE)
        for old_name, new_name, col_type in modify_cols:
            column_changes.append({"action": "modify", "column": old_name, "new_type": col_type})
            ddl.column_changes.append({"action": "modify", "column": old_name, "new_type": col_type})

        # 检测 ADD PARTITION（兼容 ADD PARTITION (PARTITION pxx ...) 写法）
        add_parts = re.findall(
            r'ADD\s+PARTITION\s*(?:\(\s*PARTITION\s+(\w+)|PARTITION\s+(\w+))',
            ddl.ddl_statement, re.IGNORECASE,
        )
        add_names = [name for pair in add_parts for name in pair if name]
        if add_parts and not add_names:
            add_names = [""]  # 解析不出分区名时保留动作，名称留空由计划层补齐
        for part_name in add_names:
            change = self._build_partition_change(ddl, "add_partition", part_name)
            partition_changes.append(change)
            ddl.partition_changes.append(change)

        # 检测 DROP PARTITION
        drop_parts = re.findall(r'DROP\s+PARTITION\s+(\w+)', ddl.ddl_statement, re.IGNORECASE)
        for part_name in drop_parts:
            change = self._build_partition_change(ddl, "drop_partition", part_name)
            partition_changes.append(change)
            ddl.partition_changes.append(change)

        affected_cols = len(column_changes)
        rollback = self.generate_rollback(ddl.ddl_statement)

        impact = {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "stmt_no": ddl.stmt_no,
            "column_changes": column_changes,
            "partition_changes": partition_changes,
            "affected_columns": affected_cols,
            "rollback_statement": rollback,
            "has_schema_impact": len(column_changes) > 0,
            "has_partition_impact": len(partition_changes) > 0,
            "analyzed_at": datetime.now().isoformat(),
        }

        # 对 schema_sync 有影响的DDL，写入管道供冲突检测消费
        if impact["has_schema_impact"]:
            for cc in column_changes:
                pipeline_bus.schema_registry.setdefault(ddl.table_name, []).append({
                    "column": cc.get("column"),
                    "action": cc.get("action"),
                    "type": cc.get("type", ""),
                    "timestamp": datetime.now().isoformat(),
                })

        pipeline_bus.add_ddl(ddl)
        pipeline_bus.write_audit(
            "DDL分析", f"{ddl.database}.{ddl.table_name}",
            f"分析DDL影响：语句{ddl.stmt_no}，列变更{affected_cols}，分区变更{len(partition_changes)}",
            "ddl_tracker", "已分析"
        )
        return impact

    def _build_partition_change(self, ddl: DdlRecord, action: str,
                                partition_name: str) -> dict[str, Any]:
        """组装带语句编号和分区版本的分区动作。"""
        version = self._next_partition_version(ddl.database, ddl.table_name, partition_name)
        ddl.partition_version = max(ddl.partition_version, version)
        return {
            "action": action,
            "name": partition_name,
            "stmt_no": ddl.stmt_no,
            "version": version,
            "ddl_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
        }

    def pending_partition_changes(self, database: str, table: str,
                                  target_database: str | None = None,
                                  target_table: str | None = None) -> dict[str, Any]:
        """按目标侧实际应用的分区版本，计算剩余待同步的分区变更。

        遍历该表全部已分析 DDL 的分区动作，凡是分区版本高于目标侧已落库版本的，
        都属于剩余待同步变更，供下一轮分区同步直接读取。
        """
        target_database = target_database or database
        target_table = target_table or table
        pending: list[dict[str, Any]] = []
        applied_cursor: dict[str, int] = {}
        for ddl in pipeline_bus.get_recent_ddls(limit=10_000):
            if ddl.database != database or ddl.table_name != table:
                continue
            for change in ddl.partition_changes:
                part_name = change.get("name", "")
                applied = pipeline_bus.applied_partition_version(
                    target_database, target_table, part_name)
                applied_cursor[part_name] = applied
                if change.get("version", 0) > applied:
                    pending.append(dict(change))
        # 语句编号升序，保证下一轮按原始 DDL 顺序续做
        pending.sort(key=lambda item: (item.get("stmt_no", 0), item.get("version", 0)))
        return {
            "database": database,
            "table": table,
            "target_database": target_database,
            "target_table": target_table,
            "applied_versions": applied_cursor,
            "pending_changes": pending,
            "pending_count": len(pending),
            "next_stmt_nos": sorted({item["stmt_no"] for item in pending}),
            "generated_at": datetime.now().isoformat(),
        }

    def track_temporal_ddl(self, table_name: str, from_dt: str, to_dt: str) -> list[dict[str, Any]]:
        """查询指定表在时间范围内的DDL变更序列。"""
        results: list[dict[str, Any]] = []
        recent = pipeline_bus.get_recent_ddls()
        for ddl in recent:
            if ddl.table_name == table_name and from_dt <= ddl.executed_at.isoformat() <= to_dt:
                results.append({
                    "record_id": ddl.record_id,
                    "stmt_no": ddl.stmt_no,
                    "ddl": ddl.ddl_statement[:200],
                    "executed_at": ddl.executed_at.isoformat(),
                    "affected_columns": ddl.affected_columns,
                })
        return results

    def generate_rollback(self, ddl_stmt: str) -> str:
        """根据DDL语句生成回滚语句。"""
        ddl_upper = ddl_stmt.upper()
        if "ADD COLUMN" in ddl_upper:
            match = re.search(r'ADD\s+COLUMN\s+(\w+)', ddl_stmt, re.IGNORECASE)
            if match:
                return f"ALTER TABLE ... DROP COLUMN {match.group(1)};"
        if "DROP COLUMN" in ddl_upper:
            return "-- 回滚需从备份恢复列定义"
        if "MODIFY" in ddl_upper:
            return "-- 回滚需记录修改前的列类型"
        if "ADD PARTITION" in ddl_upper:
            return "ALTER TABLE ... DROP PARTITION ...;"
        return "-- 自动回滚生成中"

    def record_ddl(self, database: str, table_name: str, ddl_stmt: str) -> DdlRecord:
        """记录并分析一条DDL，自动分配语句编号。"""
        stmt_no = self._next_stmt_no(database, table_name)
        ddl = DdlRecord(
            record_id=f"ddl_{uuid.uuid4().hex[:8]}",
            database=database,
            table_name=table_name,
            ddl_statement=ddl_stmt,
            executed_at=datetime.now(),
            affected_columns=0,
            rollback_statement=self.generate_rollback(ddl_stmt),
            stmt_no=stmt_no,
        )
        self.analyze_ddl_impact(ddl)
        return ddl

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "发送通知":
            pipeline_bus.write_audit(
                "DDL通知", f"{entry.get('变更数据库', '')}.{entry.get('变更表名', '')}",
                f"DDL记录 {entry_id} 变更通知已发送",
                "ddl_tracker", "已通知"
            )
        return entry, msg
