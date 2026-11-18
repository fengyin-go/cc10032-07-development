"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self, config_name: str, operator_states: dict[str, Any],
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。
        """
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        snapshot = {
            "id": cp_id,
            "配置名称": config_name,
            "间隔时间": str(interval),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "3"),
            "对齐策略": config.get("对齐策略", "exactly_once"),
            "保留数量": int(config.get("保留数量", 5)),
            "外部化路径": config.get("外部化路径", "/data/checkpoints"),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": copy.deepcopy(operator_states),
            "snapshot_ts": time.time(),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(snapshot)

        # 清理旧检查点（保留最近 N 个）
        self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "interval": interval,
        }

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        max_keep = int((config or {}).get("保留数量", 5))

        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        for s in snapshots[max_keep:]:
            s["status"] = "已停用"
            s["_pruned"] = True

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。"""
        snapshot = None
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == checkpoint_id:
                snapshot = r
                break
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}

        states = snapshot.get("operator_states", {})
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "validation": validation,
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0))

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = snapshots[0].get("operator_states", {})
        inconsistencies = []
        for i, s in enumerate(snapshots[1:], 1):
            current = s.get("operator_states", {})
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点。"""
        available = [
            r for r in store.rows(self.MODULE)
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        ]
        result = []
        for cp in available:
            states = cp.get("operator_states", {})
            result.append({
                "checkpoint_id": cp.get("checkpoint_id"),
                "config": cp.get("配置名称", ""),
                "operator_count": len(states),
                "operators": list(states.keys()),
                "snapshot_ts": cp.get("snapshot_ts", 0),
                "external_path": cp.get("外部化路径", ""),
            })
        return result

    # ---- 序号7：流关联结果版本的检查点消费 ----

    def consume_join_version(
        self,
        version: dict[str, Any],
        *,
        config_name: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """检查点模块读取同一份关联结果数据并保存窗口、键和聚合结果。

        幂等保证：
          - 非刷新模式下，版本已有活动检查点则直接返回原检查点（并发只保留一版）；
          - 检查点快照与关联结果数据在同一调用栈内一起落库，保证同时可见。
        """
        version_id = version["version_id"]
        existing = next(
            (r for r in store.rows(self.MODULE)
             if r.get("join_version_id") == version_id and r.get("is_active")),
            None,
        )
        if existing is not None and not refresh:
            return {
                "checkpoint_id": existing["checkpoint_id"],
                "snapshot_ts": existing.get("snapshot_ts"),
                "idempotent": True,
            }

        config = self._resolve_join_checkpoint_config(config_name)
        cp_id = hashlib.md5(
            f"join_cp|{version_id}|{time.time() if refresh else version['created_at']}".encode()
        ).hexdigest()[:14]

        # 刷新时把旧检查点置为非活动，保证一个版本对外只有一版活动检查点
        for row in store.rows(self.MODULE):
            if row.get("join_version_id") == version_id and row.get("is_active"):
                row["is_active"] = False
                row["status"] = "已停用"

        cp_row = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)
                 if isinstance(r.get("id"), int)),
                default=0,
            ) + 1,
            "配置名称": config.get("配置名称", f"join_checkpoint_{version['join_name']}"),
            "间隔时间": config.get("间隔时间", "30"),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "1"),
            "对齐策略": "exactly_once",
            "保留数量": config.get("保留数量", 5),
            "外部化路径": (
                f"{config.get('外部化路径', '/data/checkpoints')}/join/{version_id}"
            ),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "join_version_id": version_id,
            "join_name": version["join_name"],
            "window_start": version["window_start"],
            "window_end": version["window_end"],
            "join_keys": sorted(version["key_results"].keys()),
            "operator_states": {
                # 回放时凭此即可定位完整版本并核对聚合结果
                "join_version_id": version_id,
                "window": [version["window_start"], version["window_end"]],
                "stats": copy.deepcopy(version["stats"]),
                "key_results": copy.deepcopy(version["key_results"]),
                "snapshot": copy.deepcopy(version.get("snapshot")),
            },
            "snapshot_ts": time.time(),
            "is_active": True,
            "source": "join_stream",
        }
        store.rows(self.MODULE).append(cp_row)
        return {"checkpoint_id": cp_id, "snapshot_ts": cp_row["snapshot_ts"], "idempotent": False}

    def _resolve_join_checkpoint_config(self, config_name: str | None) -> dict[str, Any]:
        if config_name:
            for r in store.rows(self.MODULE):
                if r.get("配置名称") == config_name and not r.get("checkpoint_id"):
                    return r
        # 默认取一个已启用的检查点配置；没有则用内置默认值
        for r in store.rows(self.MODULE):
            if not r.get("checkpoint_id") and r.get("status") == "已启用":
                return r
        return {}

    def mark_active_checkpoint(self, version_id: str, checkpoint_id: str) -> None:
        """在版本详情窗口切换检查点引用：同版本只保留一个活动检查点。"""
        for row in store.rows(self.MODULE):
            if row.get("join_version_id") == version_id:
                is_target = row.get("checkpoint_id") == checkpoint_id
                row["is_active"] = is_target
                if is_target:
                    row["status"] = "已启用"

    def list_join_checkpoints(self, version_id: str | None = None) -> list[dict[str, Any]]:
        """另一个入口（检查点模块）：列出流关联版本产生的检查点并联动工作台。"""
        rows = [
            r for r in store.rows(self.MODULE)
            if r.get("source") == "join_stream"
            and (version_id is None or r.get("join_version_id") == version_id)
        ]
        return [
            {
                "checkpoint_id": r.get("checkpoint_id"),
                "join_version_id": r.get("join_version_id"),
                "join_name": r.get("join_name"),
                "window_start": r.get("window_start"),
                "window_end": r.get("window_end"),
                "key_count": len(r.get("join_keys", [])),
                "is_active": r.get("is_active", False),
                "status": r.get("status"),
                "snapshot_ts": r.get("snapshot_ts"),
                "external_path": r.get("外部化路径"),
            }
            for r in rows
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows
            if r.get("checkpoint_id") and r.get("status") == "已启用"
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        return base