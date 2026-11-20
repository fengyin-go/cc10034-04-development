"""HTTP 端到端冒烟：通过真实 FastAPI 路由验证 sales.orders 交接场景。"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:8000/api/partition-workflow"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def show(title: str, body: dict) -> None:
    print(f"\n===== {title} =====")
    print(json.dumps(body, ensure_ascii=False, indent=2))


# 1. 三条 DDL（语句 1/2/3）
for stmt in [
    "ALTER TABLE sales.orders DROP PARTITION p_old",
    "ALTER TABLE sales.orders ADD PARTITION (PARTITION p_old VALUES LESS THAN (202612))",
    "ALTER TABLE sales.orders ADD PARTITION (PARTITION p_next VALUES LESS THAN (202701))",
]:
    call("POST", "/ddl/record", {"database": "sales", "table": "orders", "ddl_statement": stmt})

# 2. 归属登记
call("POST", "/owners", {"database": "sales", "table": "orders",
                         "partition": "p_old", "owner_team": "订单维护组-交接方"})
call("POST", "/owners", {"database": "sales", "table": "orders",
                         "partition": "p_next", "owner_team": "订单维护组-承接方"})

# 3. 生成计划
plan = call("POST", "/plans", {
    "source_database": "sales", "source_table": "orders",
    "handover_group": ["p_old"], "plan_name": "orders-交接轮次"})["plan"]
pid = plan["plan_id"]
show("计划数据集", {
    "plan_id": pid, "status": plan["status"],
    "groups": [(g["group_id"], g["group_type"]) for g in plan["groups"]],
    "items": [(i["target_partition"], i["action"], i["stmt_no"],
               i["partition_version"], i["group_type"]) for i in plan["items"]],
})

# 4. 撤掉 p_old 写权限
revoked = call("POST", "/owners/revoke", {
    "database": "sales", "table": "orders",
    "partition": "p_old", "reason": "交接收回写入范围"})
show("p_old 权限收回", revoked["grant"])

# 5. 执行前重新核对
recheck = call("POST", "/permissions/recheck", {
    "database": "sales", "table": "orders", "partitions": ["p_old", "p_next"]})
show("执行前权限核对", {
    "writable": recheck["writable_partitions"],
    "readonly": recheck["readonly_partitions"]})

# 6. 第一次执行
exec1 = call("POST", "/plans/apply", {"plan_id": pid})
show("第一次执行", {
    "status": exec1["status"], "round": exec1["round"],
    "applied": [(a["partition"], a["action"], a["version"]) for a in exec1["execution"]["applied"]],
    "blocked": [(b["partition"], b["action"]) for b in exec1["execution"]["blocked"]],
    "目标分区": exec1["applied_partitions"],
    "下一次导入可用范围": exec1["next_import_scope"],
})

# 7. DDL 剩余待同步
pending = call("GET", "/ddl/pending?database=sales&table=orders")
show("DDL追踪剩余待同步变更", {
    "pending_count": pending["pending_count"],
    "next_stmt_nos": pending["next_stmt_nos"],
    "applied_versions": pending["applied_versions"],
    "partitions": [(c["name"], c["action"], c["stmt_no"], c["version"])
                   for c in pending["pending_changes"]],
})

# 8. 再次提交原计划（幂等）
exec2 = call("POST", "/plans/apply", {"plan_id": pid})
show("再次提交原计划（幂等核对）", {
    "status": exec2["status"], "round": exec2["round"],
    "applied": exec2["execution"]["applied"],
    "skipped_idempotent": exec2["execution"]["skipped_idempotent"],
    "blocked": [(b["partition"], b["action"]) for b in exec2["execution"]["blocked"]],
})

# 9. 续做成员
cont = call("GET", f"/plans/{pid}/continuation")
show("续做成员", {
    "next_round": cont["continuation"]["next_round"],
    "pending_stmt_nos": cont["continuation"]["pending_stmt_nos"],
    "pending_items": [(i["target_partition"], i["action"], i["apply_status"])
                      for i in cont["continuation"]["pending_items"]],
})

# 10. 权限恢复后第三轮续做
call("POST", "/owners", {"database": "sales", "table": "orders",
                         "partition": "p_old", "owner_team": "订单维护组-承接方"})
exec3 = call("POST", "/plans/apply", {"plan_id": pid})
show("权限恢复后续做", {
    "status": exec3["status"],
    "applied": [(a["partition"], a["action"], a["version"]) for a in exec3["execution"]["applied"]],
    "下一次导入可用范围": [s["partition"] for s in exec3["next_import_scope"]],
})
final = call("GET", "/ddl/pending?database=sales&table=orders")
show("完成后剩余变更", {"pending_count": final["pending_count"]})

print("\nHTTP 冒烟流程全部跑通")
