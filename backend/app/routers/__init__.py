"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import join_pipeline, resource

# 序号7专用路由带固定前缀，必须先于通用 /api/{module} 动态路由注册
ROUTERS = [join_pipeline, resource]
