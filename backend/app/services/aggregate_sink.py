"""聚合落库领域服务：将实时聚合结果按定时频率批量写入目标存储表。

增强能力: 聚合粒度校验、落库频率管理、分区键生成、覆盖/追加模式、与检查点对齐
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AggregateSinkService(BaseModuleService):
    MODULE = "aggregate_sink"
    ENTITY = "聚合任务"
    LIST_FIELDS = ["任务名称", "源指标集", "聚合粒度", "输出表名", "落库频率", "覆盖模式", "分区键"]
    REQUIRED_FIELDS = ["任务名称", "源指标集", "聚合粒度"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "写入失败"]
    ACTION_RULES = {"启动任务": "待启动", "停止任务": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_sink_task(self, task_name: str) -> dict[str, Any]:
        """校验聚合落库任务的配置完整性。"""
        task = None
        for r in store.rows(self.MODULE):
            if r.get("任务名称") == task_name:
                task = r
                break
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        checks = []
        granularity = task.get("聚合粒度", "")
        valid_granularities = {"minute", "hour", "day", "week", "month"}
        checks.append({
            "check": "granularity",
            "ok": granularity.lower() in valid_granularities,
            "detail": granularity,
        })

        try:
            frequency = int(task.get("落库频率", "60"))
            checks.append({
                "check": "frequency",
                "ok": frequency >= 10 and frequency <= 86400,
                "detail": f"{frequency}s",
            })
        except (ValueError, TypeError):
            checks.append({"check": "frequency", "ok": False, "detail": "格式错误"})

        partition_key = task.get("分区键", "")
        checks.append({
            "check": "partition_key",
            "ok": bool(partition_key and partition_key.strip()),
            "detail": partition_key,
        })

        mode = task.get("覆盖模式", "append")
        checks.append({
            "check": "mode",
            "ok": mode in ("append", "overwrite", "merge"),
            "detail": mode,
        })

        return {
            "ok": all(c["ok"] for c in checks),
            "task": task_name,
            "checks": checks,
        }

    def batch_flush_to_target(self, task_name: str, aggregated: list[dict[str, Any]]) -> dict[str, Any]:
        """将聚合结果批量写入目标存储表并同步检查点。"""
        task = None
        for r in store.rows(self.MODULE):
            if r.get("任务名称") == task_name:
                task = r
                break
        if task is None:
            return {"ok": False, "error": f"聚合任务「{task_name}」不存在"}

        partition_key = task.get("分区键", "dt")
        mode = task.get("覆盖模式", "append")

        written = 0
        for item in aggregated:
            partition_val = time.strftime("%Y%m%d%H")
            sink_record = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("sink_output")), default=0
                ) + 1,
                "配置名称": f"agg_{task_name}_{partition_val}",
                "输出类型": "aggregate_batch",
                "目标地址": f"table://{task.get('输出表名', task_name)}",
                "写入模式": mode,
                "分区策略": partition_key,
                "格式转换": "json",
                "status": "已生效",
                "sink": f"agg_{task_name}",
                "write_status": "success",
                "partition": partition_val,
                "data": item,
                "written_at": time.time(),
            }
            store.rows("sink_output").append(sink_record)
            written += 1

        # 同步检查点
        cp_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("checkpoint_mgr")), default=0
            ) + 1,
            "配置名称": f"checkpoint_{task_name}_{int(time.time())}",
            "间隔时间": task.get("落库频率", "60"),
            "超时时长": "120",
            "最大并发": "3",
            "对齐策略": "exactly_once",
            "保留数量": 5,
            "外部化路径": f"/data/checkpoints/{task_name}",
            "status": "已启用",
            "checkpoint_id": f"cp_agg_{task_name}_{int(time.time())}",
            "operator_states": {"task": task_name, "written": written},
            "snapshot_ts": time.time(),
        }
        store.rows("checkpoint_mgr").append(cp_entry)

        task["status"] = "运行中"
        task["pending"] = True

        return {
            "ok": True,
            "task": task_name,
            "written": written,
            "checkpoint_id": cp_entry["checkpoint_id"],
        }

    def compute_storage_metrics(self) -> dict[str, Any]:
        """计算存储指标：所有聚合任务的写入统计。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        metrics = {}
        total_records = 0
        for task in tasks:
            name = task.get("任务名称", "")
            count = sum(
                1 for s in store.rows("sink_output")
                if name in str(s.get("sink", ""))
            )
            total_records += count
            metrics[name] = count

        return {
            "ok": True,
            "task_count": len(tasks),
            "total_records": total_records,
            "per_task": metrics,
            "avg_per_task": total_records / max(len(tasks), 1),
        }

    def list_sink_status(self) -> list[dict[str, Any]]:
        """列出所有聚合落库任务的状态及关联检查点信息。"""
        tasks = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for t in tasks:
            name = t.get("任务名称", "")
            checkpoints = [
                c for c in store.rows("checkpoint_mgr")
                if name in str(c.get("配置名称", ""))
            ]
            result.append({
                "task": name,
                "granularity": t.get("聚合粒度", ""),
                "mode": t.get("覆盖模式", ""),
                "status": t.get("status", ""),
                "checkpoints": len(checkpoints),
            })
        return result

    def get_snapshot_by_version(self, version_id: str) -> dict[str, Any] | None:
        """序号7联动：按关联结果版本定位聚合落库快照与写入状态。"""
        for row in store.rows(self.MODULE):
            if row.get("join_version_id") == version_id:
                return {
                    "sink_record_id": row.get("id"),
                    "task_name": row.get("任务名称"),
                    "snapshot_status": row.get("snapshot_status", "未落地"),
                    "status": row.get("status"),
                    "window_start": row.get("window_start"),
                    "window_end": row.get("window_end"),
                    "stats": {
                        k.replace("stat_", ""): row.get(k)
                        for k in ("stat_total_keys", "stat_matched",
                                  "stat_unmatched", "stat_match_rate")
                    },
                }
        return None

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["completed"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["failed"] = sum(1 for r in rows if r.get("status") == "写入失败")
        return base