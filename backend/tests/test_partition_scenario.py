"""端到端场景验证：sales.orders 分区交接与幂等续做。

场景：
- 同一轮 DDL：p_old 删除并重建（交接组，完整替换）+ p_next 单独新增
- 计划登记后先撤掉 p_old 写权限，p_next 仍有权限且下游导入等待
- 执行：p_old 只读拦截，p_next 实际落库
- 再次提交原计划：已落库的 p_next 幂等跳过，p_old 仍待续做
- 核对：目标分区、下一次导入可用范围、续做成员（剩余待同步语句）
"""
from __future__ import annotations

import sys

from app.pipeline import pipeline_bus
from app.services import service_for

ddl = service_for("ddl_tracker")
ps = service_for("partition_sync")

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -- {detail}" if detail else ""))
    if not condition:
        failures.append(name)


# ---------------------------------------------------------------- 1. DDL 追踪
# 语句1：DROP PARTITION p_old（交接组删除）
r1 = ddl.record_ddl("sales", "orders",
                    "ALTER TABLE sales.orders DROP PARTITION p_old")
# 语句2：ADD PARTITION p_old（交接组同分区重建，与语句1构成完整替换）
r2 = ddl.record_ddl("sales", "orders",
                    "ALTER TABLE sales.orders ADD PARTITION (PARTITION p_old VALUES LESS THAN (202612))")
# 语句3：ADD PARTITION p_next（独立新增，供下一次导入）
r3 = ddl.record_ddl("sales", "orders",
                    "ALTER TABLE sales.orders ADD PARTITION (PARTITION p_next VALUES LESS THAN (202701))")

check("DDL语句编号递增", [r1.stmt_no, r2.stmt_no, r3.stmt_no] == [1, 2, 3],
      f"stmt_no={[r1.stmt_no, r2.stmt_no, r3.stmt_no]}")

all_changes: list[dict] = []
for record in (r1, r2, r3):
    all_changes.extend(record.partition_changes)

check("分区动作携带语句编号", all(c.get("stmt_no") for c in all_changes),
      f"changes={[(c['action'], c['name'], c['stmt_no'], c['version']) for c in all_changes]}")
versions = {(c["name"], c["action"]): c["version"] for c in all_changes}
check("p_old 删除与重建各有分区版本",
      versions.get(("p_old", "drop_partition")) and versions.get(("p_old", "add_partition")))

# ---------------------------------------------------------------- 2. 权限登记
ps.register_partition_owner("sales", "orders", "p_old", "订单维护组-交接方")
ps.register_partition_owner("sales", "orders", "p_next", "订单维护组-承接方")

grant_old = pipeline_bus.check_partition_write("sales", "orders", "p_old")
grant_next = pipeline_bus.check_partition_write("sales", "orders", "p_next")
check("初始 p_old/p_next 均有写权限",
      grant_old["writable"] and grant_next["writable"])

# ---------------------------------------------------------------- 3. 生成计划数据集
config = {"配置名称": "sales.orders分区同步", "分区字段": "dt",
          "目标分区策略": "RANGE", "新增分区行为": "自动创建", "过期清理": "自动"}
plan = ps.build_partition_change_plan(
    config, "sales", "orders", all_changes,
    handover_group=["p_old"],
    plan_name="orders-202611交接轮次",
)
pid = plan["plan_id"]

check("计划数据集已登记", plan["status"] == "已登记")
check("计划项数=3（drop p_old / add p_old / add p_next）",
      len(plan["items"]) == 3, f"items={len(plan['items'])}")
group_types = {g["group_type"] for g in plan["groups"]}
check("按归属切分为替换组+独立分区",
      group_types == {"replacement", "standalone"}, f"groups={group_types}")
replace_group = next(g for g in plan["groups"] if g["group_type"] == "replacement")
check("p_old 替换组含删除与重建两个动作",
      replace_group["partition"] == "p_old" and replace_group["replacement"] is True)
standalone = next(g for g in plan["groups"] if g["group_type"] == "standalone")
check("p_next 为独立分区组", standalone["partition"] == "p_next")
check("计划项均携带 stmt_no 与分区版本",
      all(it["stmt_no"] and it["partition_version"] for it in plan["items"]))

# ---------------------------------------------------------------- 4. 交接：撤掉 p_old 写权限
revoked = ps.revoke_partition_write("sales", "orders", "p_old",
                                    reason="交接收回写入范围")
check("p_old 写权限已收回且保留只读",
      revoked["writable"] is False and revoked["scope"] == "只读")
check("p_next 写权限不受影响",
      pipeline_bus.check_partition_write("sales", "orders", "p_next")["writable"] is True)

# 执行前重新核对
recheck = ps.recheck_write_permission("sales", "orders", ["p_old", "p_next"])
check("执行前权限核对：p_old只读 / p_next可写",
      recheck["readonly_partitions"] == ["p_old"]
      and recheck["writable_partitions"] == ["p_next"],
      f"recheck={recheck['readonly_partitions']} / {recheck['writable_partitions']}")

# ---------------------------------------------------------------- 5. 第一次执行
exec1 = ps.apply_partition_plan(pid)
check("第一轮状态为部分应用", exec1["status"] == "部分应用", exec1["status"])
applied = {(a["partition"], a["action"]) for a in exec1["execution"]["applied"]}
blocked = {(b["partition"], b["action"]) for b in exec1["execution"]["blocked"]}
check("本轮仅 p_next 实际落库",
      applied == {("p_next", "add_partition")}, f"applied={applied}")
check("p_old 删除与重建整体只读拦截（完整替换组）",
      blocked == {("p_old", "drop_partition"), ("p_old", "add_partition")},
      f"blocked={blocked}")
check("目标分区核对：仅 p_next 有已应用版本",
      [p["partition"] for p in exec1["applied_partitions"]] == ["p_next"],
      f"targets={exec1['applied_partitions']}")
check("下一次导入可用范围仅含 p_next",
      [s["partition"] for s in exec1["next_import_scope"]] == ["p_next"]
      and exec1["next_import_scope"][0]["available_for_import"] is True,
      f"scope={exec1['next_import_scope']}")
check("p_old 未落库任何版本",
      pipeline_bus.applied_partition_version("sales", "orders", "p_old") == 0)
check("p_next 已落库版本>0",
      pipeline_bus.applied_partition_version("sales", "orders", "p_next") > 0)

# ---------------------------------------------------------------- 6. DDL 追踪：剩余待同步变更
pending = ddl.pending_partition_changes("sales", "orders")
pending_parts = {(c["name"], c["action"]) for c in pending["pending_changes"]}
check("DDL追踪按已应用版本给出剩余变更= p_old 两条",
      pending_parts == {("p_old", "drop_partition"), ("p_old", "add_partition")},
      f"pending={pending_parts}")
check("剩余待同步语句编号为 1、2",
      pending["next_stmt_nos"] == [1, 2], f"stmt_nos={pending['next_stmt_nos']}")
check("p_next 语句3不在剩余集合",
      all(c["name"] != "p_next" for c in pending["pending_changes"]))

# ---------------------------------------------------------------- 7. 再次提交原计划（幂等）
exec2 = ps.apply_partition_plan(pid)
check("重复提交不再次应用已落库的 p_next",
      len(exec2["execution"]["applied"]) == 0,
      f"applied={exec2['execution']['applied']}")
check("p_next 被幂等跳过",
      {(s["partition"]) for s in exec2["execution"]["skipped_idempotent"]} == {"p_next"},
      f"skipped={exec2['execution']['skipped_idempotent']}")
check("p_old 仍被只读拦截，留给下一轮",
      {b["partition"] for b in exec2["execution"]["blocked"]} == {"p_old"})
check("计划仍为部分应用，轮次推进到3",
      exec2["status"] == "部分应用" and exec2["round"] == 2,
      f"status={exec2['status']} round={exec2['round']}")

# ---------------------------------------------------------------- 8. 续做成员核对
cont = ps.build_continuation(pid)
check("续做计划项仅 p_old 两条",
      {i["target_partition"] for i in cont["pending_items"]} == {"p_old"}
      and len(cont["pending_items"]) == 2)
check("续做语句编号 1、2", cont["pending_stmt_nos"] == [1, 2])
check("续做成员含DDL剩余待同步变更",
      cont["ddl_pending"]["pending_count"] == 2
      and cont["ddl_pending"]["next_stmt_nos"] == [1, 2])
check("下一轮轮次=3", cont["next_round"] == 3)

# ---------------------------------------------------------------- 9. 权限恢复后下一轮可续做
ps.register_partition_owner("sales", "orders", "p_old", "订单维护组-承接方")
exec3 = ps.apply_partition_plan(pid)
check("权限恢复后 p_old 完整替换得以应用",
      {(a["partition"], a["action"]) for a in exec3["execution"]["applied"]}
      == {("p_old", "drop_partition"), ("p_old", "add_partition")},
      f"applied={exec3['execution']['applied']}")
check("计划最终完成", exec3["status"] == "已完成", exec3["status"])
final_pending = ddl.pending_partition_changes("sales", "orders")
check("完成后无剩余待同步变更", final_pending["pending_count"] == 0,
      f"remaining={final_pending['pending_count']}")
check("下一次导入可用范围含 p_old 与 p_next",
      {s["partition"] for s in exec3["next_import_scope"]} == {"p_old", "p_next"})

print("\n审计轨迹（分区相关）：")
for entry in pipeline_bus.audit_log:
    if entry["操作来源"] in ("partition_sync",) or "分区" in entry["操作类型"]:
        print(f"  - {entry['操作类型']} | {entry['目标对象']} | {entry['结果状态']} | {entry['变更摘要']}")

print()
if failures:
    print(f"==== {len(failures)} 项失败: {failures}")
    sys.exit(1)
print("==== 全部场景断言通过 ====")
