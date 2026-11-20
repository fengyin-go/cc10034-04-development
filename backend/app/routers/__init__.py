"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import partition_workflow, resource

# 固定路径的工作流路由先注册，避免被资源路由的 /api/{module} 动态段抢先匹配
ROUTERS = [partition_workflow, resource]
