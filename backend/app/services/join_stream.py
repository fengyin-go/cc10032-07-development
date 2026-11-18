"""流关联领域服务（序号7）：定义两条流之间的关联计算配置。

增强能力:
  - 关联类型校验、时间窗口比对、左/右流源校验、关联输出到聚合落库
  - 关联结果版本：按关联配置读取双流事件，计算确定性关联结果数据，
    并与聚合落库快照、检查点引用在同一事务内原子可见
  - 单向生命周期：计算中 → 待检查点 → 已确认 → 已归档，已确认窗口不可回退
  - 并发重试幂等：同一窗口版本只保留一版，重复键按合并模式去重
  - 可恢复：关联中断后只补未完成键，检查点可反查完整版本用于回放
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.checkpoint_mgr import CheckpointMgrService
from app.store import store

# 序号7 关联结果版本的单向生命周期
LIFECYCLE_COMPUTING = "计算中"
LIFECYCLE_PENDING_CP = "待检查点"
LIFECYCLE_CONFIRMED = "已确认"
LIFECYCLE_ARCHIVED = "已归档"
LIFECYCLE_ORDER = [
    LIFECYCLE_COMPUTING,
    LIFECYCLE_PENDING_CP,
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_ARCHIVED,
]
# 只允许沿链路正向流转，任何回退请求都会被拒绝
ALLOWED_TRANSITIONS: dict[str, str] = {
    LIFECYCLE_COMPUTING: LIFECYCLE_PENDING_CP,
    LIFECYCLE_PENDING_CP: LIFECYCLE_CONFIRMED,
    LIFECYCLE_CONFIRMED: LIFECYCLE_ARCHIVED,
}

# 左右事件重复键的两种合并模式
MERGE_WINDOW_CUTOFF = "window_cutoff"   # 以窗口结束时间统一截断，重复键保留最早到达
MERGE_LAST_ARRIVAL = "last_arrival"     # 按最后到达事件合并，重复键保留最新到达
MERGE_MODES = {MERGE_WINDOW_CUTOFF, MERGE_LAST_ARRIVAL}

# 归档与恢复条件，随关联结果数据一起持久化
ARCHIVE_CONDITION = (
    "窗口已确认且检查点快照在保留数量内，超过热存保留期后可归档"
)
RESTORE_CONDITION = (
    "归档版本仅允许凭检查点引用只读恢复，不重新计算、不回退已确认窗口"
)


class JoinStreamService(BaseModuleService):
    MODULE = "join_stream"
    ENTITY = "关联配置"
    LIST_FIELDS = ["配置名称", "左流源", "右流源", "关联字段", "关联类型", "窗口区间", "输出模式"]
    REQUIRED_FIELDS = ["配置名称", "左流源", "右流源"]
    STATUS_ORDER = ["草稿", "运行中", "已停止"]
    ACTION_RULES = {"启动关联": "草稿", "停止关联": "运行中"}
    NEGATIVE_ACTIONS = []

    # 版本序号跨实例共享，保证同一关联配置的版本号全局单调
    _version_seq: dict[str, int] = {}

    # ------------------------------------------------------------------
    # 基础校验（保留原有能力）
    # ------------------------------------------------------------------

    def validate_source_availability(
        self, left_source: str, right_source: str,
    ) -> dict[str, Any]:
        """校验左右流源是否可用（存在且未停止/异常断连）。"""
        left_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == left_source and r.get("status") not in ("已下线", "异常断连"):
                left_ok = True
                break

        right_ok = False
        for r in store.rows("stream_source"):
            if r.get("源端名称") == right_source and r.get("status") not in ("已下线", "异常断连"):
                right_ok = True
                break

        return {
            "ok": left_ok and right_ok,
            "left": {"source": left_source, "available": left_ok},
            "right": {"source": right_source, "available": right_ok},
        }

    # ------------------------------------------------------------------
    # 关联结果版本：确定性标识与事件归并
    # ------------------------------------------------------------------

    def _find_config(self, join_name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(self.MODULE) if r.get("配置名称") == join_name),
            None,
        )

    @staticmethod
    def _window_sec(join_cfg: dict[str, Any]) -> float:
        try:
            return float(join_cfg.get("窗口区间", "10"))
        except (ValueError, TypeError):
            return 10.0

    @staticmethod
    def _event_key(event: dict[str, Any], join_field: str) -> str:
        return str(event.get("payload", {}).get(join_field, event.get(join_field, "")))

    @staticmethod
    def _event_ts(event: dict[str, Any]) -> float:
        try:
            return float(event.get("event_time", 0))
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _fingerprint(left_events: list[dict], right_events: list[dict]) -> str:
        """对输入事件生成确定性指纹，用于审计与并发重试识别。"""
        ids = sorted(
            str(e.get("event_id", f"{side}:{i}"))
            for side, events in (("L", left_events), ("R", right_events))
            for i, e in enumerate(events)
        )
        return hashlib.md5("|".join(ids).encode()).hexdigest()[:16]

    def _version_id(
        self, join_name: str, window_start: float, window_end: float, merge_mode: str,
    ) -> str:
        """版本标识只由配置+窗口+合并模式决定：同窗口并发重试必然命中同一版本。"""
        raw = f"{join_name}|{window_start:.3f}|{window_end:.3f}|{merge_mode}"
        return "jv_" + hashlib.md5(raw.encode()).hexdigest()[:16]

    def _window_key(
        self, join_name: str, window_start: float, window_end: float,
    ) -> tuple[str, float, float]:
        return join_name, float(window_start), float(window_end)

    def _merge_events(
        self,
        buffered: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
        join_field: str,
        merge_mode: str,
        window_end: float,
    ) -> tuple[list[dict[str, Any]], int, int]:
        """把新到达事件并入窗口缓冲，返回 (去重后事件, 重复键覆盖数, 迟到截断数)。"""
        merged = list(buffered)
        duplicate_overwrites = 0
        late_dropped = 0
        for event in incoming:
            if self._event_ts(event) > window_end:
                # 窗口结束时间统一截断：晚于窗口结束的事件不进入本版本
                late_dropped += 1
                continue
            key = self._event_key(event, join_field)
            if not key:
                continue
            existed_idx = next(
                (i for i, old in enumerate(merged)
                 if self._event_key(old, join_field) == key),
                None,
            )
            if existed_idx is None:
                merged.append(copy.deepcopy(event))
                continue
            duplicate_overwrites += 1
            if merge_mode == MERGE_LAST_ARRIVAL:
                # 最后到达优先：事件时间更晚（并列时后到的覆盖）
                if self._event_ts(event) >= self._event_ts(merged[existed_idx]):
                    merged[existed_idx] = copy.deepcopy(event)
            else:
                # 窗口截断模式：保留最早到达，迟到重复键直接丢弃
                if self._event_ts(event) < self._event_ts(merged[existed_idx]):
                    merged[existed_idx] = copy.deepcopy(event)
        return merged, duplicate_overwrites, late_dropped

    # ------------------------------------------------------------------
    # 关联结果版本：主流程
    # ------------------------------------------------------------------

    def produce_join_version(
        self,
        join_name: str,
        left_events: list[dict],
        right_events: list[dict],
        *,
        merge_mode: str = MERGE_WINDOW_CUTOFF,
        window_start: float | None = None,
        window_end: float | None = None,
        checkpoint_config: str | None = None,
        fail_sink: bool = False,
        fail_checkpoint: bool = False,
    ) -> dict[str, Any]:
        """按关联配置读取双流事件并生成关联结果版本。

        并发重试幂等：同一 (配置, 窗口, 合并模式) 只保留一版；
        原子可见：关联结果数据、聚合落库快照、检查点引用在同一事务内生效，
        任一步失败则整体回滚并保留原状态。
        """
        join_cfg = self._find_config(join_name)
        if join_cfg is None:
            return {"ok": False, "error": f"关联配置「{join_name}」不存在"}
        if merge_mode not in MERGE_MODES:
            return {"ok": False, "error": f"未知合并模式：{merge_mode}"}

        join_field = join_cfg.get("关联字段", "user_id")
        window_sec = self._window_sec(join_cfg)
        all_ts = [self._event_ts(e) for e in left_events + right_events]
        if window_start is None:
            window_start = min(all_ts) if all_ts else 0.0
        if window_end is None:
            window_end = float(window_start) + window_sec
        window_start, window_end = float(window_start), float(window_end)

        fingerprint = self._fingerprint(left_events, right_events)
        version_id = self._version_id(join_name, window_start, window_end, merge_mode)
        lock = store.join_version_lock(version_id)
        with lock:
            window_key = self._window_key(join_name, window_start, window_end)
            routed = store.find_join_window(window_key)
            if routed is not None and routed != version_id:
                # 同窗口已存在另一合并模式版本：冲突优先级判定
                existing = store.get_join_version(routed)
                return self._conflict_response(existing, fingerprint)

            version = store.get_join_version(version_id)
            if version is None:
                version = self._new_version(
                    join_cfg, version_id, window_start, window_end,
                    merge_mode, fingerprint,
                )
                store.register_join_window(window_key, version_id)
            else:
                if version["lifecycle_status"] in (
                    LIFECYCLE_CONFIRMED, LIFECYCLE_ARCHIVED,
                ):
                    # 已确认窗口不能回退：幂等返回既有确定结果
                    return {
                        "ok": True,
                        "idempotent": True,
                        "reason": "窗口已确认，重复请求不重新计算",
                        "version": self._version_view(version),
                    }
                if version.get("input_fingerprint") == fingerprint:
                    # 完全相同的并发重试：只保留一版，不产生任何副作用
                    return {
                        "ok": True,
                        "idempotent": True,
                        "reason": "相同输入的并发重试命中既有版本",
                        "version": self._version_view(version),
                    }
                version["history"].append(self._history_item(
                    version["lifecycle_status"], version["lifecycle_status"],
                    "新事件到达，并入窗口缓冲后继续未完成键计算",
                ))

            # 归并左右事件（重复键去重、迟到截断）
            left_buf, dup_l, late_l = self._merge_events(
                version["left_buffer"], left_events, join_field,
                merge_mode, window_end,
            )
            right_buf, dup_r, late_r = self._merge_events(
                version["right_buffer"], right_events, join_field,
                merge_mode, window_end,
            )
            version["left_buffer"] = left_buf
            version["right_buffer"] = right_buf
            version["duplicate_overwrites"] += dup_l + dup_r
            version["late_dropped"] += late_l + late_r
            version["input_fingerprint"] = fingerprint
            version["updated_at"] = time.time()

            # 中断恢复语义：只计算尚未完成的键
            self._compute_pending_keys(version, join_cfg)

            # 原子事务：聚合落库快照 + 检查点消费同时生效
            txn = self._materialize_transaction(
                version, checkpoint_config=checkpoint_config,
                fail_sink=fail_sink, fail_checkpoint=fail_checkpoint,
            )
            store.save_join_version(version)
            if not txn["ok"]:
                return txn
            return {
                "ok": True,
                "idempotent": False,
                "version": self._version_view(version),
                "transaction": txn,
            }

    def _new_version(
        self,
        join_cfg: dict[str, Any],
        version_id: str,
        window_start: float,
        window_end: float,
        merge_mode: str,
        fingerprint: str,
    ) -> dict[str, Any]:
        join_name = join_cfg.get("配置名称", "")
        seq = self._version_seq.get(join_name, 0) + 1
        self._version_seq[join_name] = seq
        now = time.time()
        return {
            "version_id": version_id,
            "version_seq": seq,
            "join_name": join_name,
            "join_config_id": join_cfg.get("id"),
            "join_type": join_cfg.get("关联类型", "inner"),
            "join_field": join_cfg.get("关联字段", "user_id"),
            "window_start": window_start,
            "window_end": window_end,
            "merge_mode": merge_mode,
            "input_fingerprint": fingerprint,
            "lifecycle_status": LIFECYCLE_COMPUTING,
            "left_buffer": [],
            "right_buffer": [],
            "late_dropped": 0,
            "duplicate_overwrites": 0,
            "key_results": {},
            "confirmed_keys": [],
            "stats": {"total_keys": 0, "matched": 0, "unmatched": 0, "match_rate": 0.0},
            "snapshot": None,
            "sink_record_id": None,
            "checkpoint_id": None,
            "checkpoint_history": [],
            "active_checkpoint_id": None,
            "archive_condition": ARCHIVE_CONDITION,
            "restore_condition": RESTORE_CONDITION,
            "archived_at": None,
            "confirmed_at": None,
            "last_error": None,
            "history": [self._history_item(None, LIFECYCLE_COMPUTING, "创建关联结果版本")],
            "created_at": now,
            "updated_at": now,
        }

    def _compute_pending_keys(
        self, version: dict[str, Any], join_cfg: dict[str, Any],
    ) -> None:
        """只对未完成键执行关联计算，已确认键保持原结果不动。"""
        join_field = version["join_field"]
        join_type = version["join_type"]
        left_index = {
            self._event_key(e, join_field): e for e in version["left_buffer"]
            if self._event_key(e, join_field)
        }
        right_index = {
            self._event_key(e, join_field): e for e in version["right_buffer"]
            if self._event_key(e, join_field)
        }
        all_keys = sorted(set(left_index) | set(right_index))
        for key in all_keys:
            if key in version["confirmed_keys"]:
                continue  # 中断恢复：已完成键不重算
            lr = left_index.get(key)
            rr = right_index.get(key)
            if join_type == "inner" and (lr is None or rr is None):
                continue
            version["key_results"][key] = {
                "key": key,
                "matched": lr is not None and rr is not None,
                "left_ts": self._event_ts(lr) if lr else 0.0,
                "right_ts": self._event_ts(rr) if rr else 0.0,
                "left_event_id": (lr or {}).get("event_id"),
                "right_event_id": (rr or {}).get("event_id"),
                "result_status": "已计算",
            }
        matched = sum(1 for r in version["key_results"].values() if r["matched"])
        total = len(version["key_results"])
        version["stats"] = {
            "total_keys": total,
            "matched": matched,
            "unmatched": total - matched,
            "match_rate": matched / total if total else 0.0,
        }
        version["updated_at"] = time.time()

    def _materialize_transaction(
        self,
        version: dict[str, Any],
        *,
        checkpoint_config: str | None,
        fail_sink: bool,
        fail_checkpoint: bool,
    ) -> dict[str, Any]:
        """同一事务内写聚合落库快照并让检查点消费关联结果数据。

        - 同一版本只保留一份聚合落库记录：再物化时原地更新快照而非追加；
        - 检查点以刷新方式轮换，旧检查点保留在版本历史中；
        - 任一步失败：补偿新建/已改记录、恢复旧检查点引用，状态保持回滚前原样。
        """
        previous_status = version["lifecycle_status"]
        prev_snapshot = copy.deepcopy(version.get("snapshot"))
        prev_sink_id = version.get("sink_record_id")
        prev_cp_id = version.get("checkpoint_id")
        created_sink_id: int | None = None
        updated_sink_backup: dict[str, Any] | None = None
        try:
            if fail_sink:
                raise RuntimeError("模拟聚合落库写入失败")
            snapshot = self._build_snapshot(version)
            sink_rows = store.rows("aggregate_sink")
            existing_sink = next(
                (r for r in sink_rows
                 if r.get("join_version_id") == version["version_id"]),
                None,
            )
            if existing_sink is not None:
                # 再物化：同一版本只保留一份落库快照，原地更新
                updated_sink_backup = copy.deepcopy(existing_sink)
                existing_sink.update({
                    "snapshot_status": "已落库",
                    "window_start": version["window_start"],
                    "window_end": version["window_end"],
                    **{f"stat_{k}": v for k, v in version["stats"].items()},
                })
                created_sink_id = existing_sink["id"]
            else:
                sink_row = {
                    "id": max(
                        (int(r.get("id", 0)) for r in sink_rows),
                        default=0,
                    ) + 1,
                    "任务名称": f"join_{version['join_name']}_{version['version_id']}",
                    "源指标集": f"join://{version['join_name']}/{version['version_id']}",
                    "聚合粒度": version["join_type"],
                    "输出表名": f"table_{version['join_name']}",
                    "落库频率": "60",
                    "覆盖模式": "append",
                    "分区键": version["join_field"],
                    "status": "运行中",
                    "pending": True,
                    "abnormal": False,
                    "join_version_id": version["version_id"],
                    "snapshot_status": "已落库",
                    "window_start": version["window_start"],
                    "window_end": version["window_end"],
                    **{f"stat_{k}": v for k, v in version["stats"].items()},
                }
                sink_rows.append(sink_row)
                created_sink_id = sink_row["id"]
            version["snapshot"] = snapshot
            version["sink_record_id"] = created_sink_id

            if fail_checkpoint:
                raise RuntimeError("模拟检查点快照写入失败")
            # 已有检查点时走刷新：旧检查点停用进历史，新版本只保留一个活动引用
            cp_row = CheckpointMgrService().consume_join_version(
                version, config_name=checkpoint_config,
                refresh=prev_cp_id is not None,
            )
            version["checkpoint_id"] = cp_row["checkpoint_id"]
            version["active_checkpoint_id"] = cp_row["checkpoint_id"]
            if cp_row["checkpoint_id"] not in version["checkpoint_history"]:
                version["checkpoint_history"].append({
                    "checkpoint_id": cp_row["checkpoint_id"],
                    "snapshot_ts": cp_row["snapshot_ts"],
                    "note": "关联结果数据与检查点快照原子可见",
                })

            self._advance_or_record(
                version, LIFECYCLE_PENDING_CP, "聚合落库快照与检查点引用原子生效",
            )
            version["last_error"] = None
            return {
                "ok": True,
                "sink_record_id": created_sink_id,
                "checkpoint_id": cp_row["checkpoint_id"],
                "committed_status": version["lifecycle_status"],
            }
        except RuntimeError as exc:
            # 事务回滚：补偿落库记录
            if updated_sink_backup is not None:
                for idx, row in enumerate(store.rows("aggregate_sink")):
                    if row.get("id") == updated_sink_backup.get("id"):
                        store.rows("aggregate_sink")[idx] = updated_sink_backup
                        break
            elif created_sink_id is not None:
                store.rows("aggregate_sink")[:] = [
                    r for r in store.rows("aggregate_sink")
                    if r.get("id") != created_sink_id
                ]
            # 补偿检查点：删除新建检查点并恢复旧活动引用
            new_cp = version.get("checkpoint_id")
            if new_cp and new_cp != prev_cp_id:
                store.rows("checkpoint_mgr")[:] = [
                    r for r in store.rows("checkpoint_mgr")
                    if r.get("checkpoint_id") != new_cp
                ]
                if prev_cp_id is not None:
                    CheckpointMgrService().mark_active_checkpoint(
                        version["version_id"], prev_cp_id,
                    )
            version["snapshot"] = prev_snapshot
            version["sink_record_id"] = prev_sink_id
            version["checkpoint_id"] = prev_cp_id
            version["active_checkpoint_id"] = prev_cp_id
            version["lifecycle_status"] = previous_status
            version["last_error"] = str(exc)
            version["history"].append(self._history_item(
                previous_status, previous_status, f"事务回滚：{exc}，保留原状态",
            ))
            return {
                "ok": False,
                "error": str(exc),
                "rolled_back": True,
                "retained_status": previous_status,
                "version_id": version["version_id"],
            }

    def _build_snapshot(self, version: dict[str, Any]) -> dict[str, Any]:
        """生成聚合落库快照：窗口、键结果与聚合指标一起冻结。"""
        return {
            "snapshot_id": f"snap_{version['version_id']}",
            "join_version_id": version["version_id"],
            "join_name": version["join_name"],
            "window_start": version["window_start"],
            "window_end": version["window_end"],
            "merge_mode": version["merge_mode"],
            "stats": copy.deepcopy(version["stats"]),
            "keys": sorted(version["key_results"].keys()),
            "key_results": copy.deepcopy(version["key_results"]),
            "snapshot_ts": time.time(),
        }

    # ------------------------------------------------------------------
    # 生命周期流转（单向）
    # ------------------------------------------------------------------

    def _transition(
        self, version: dict[str, Any], target: str, reason: str,
    ) -> None:
        current = version["lifecycle_status"]
        expected = ALLOWED_TRANSITIONS.get(current)
        if expected != target:
            raise RuntimeError(
                f"关联结果版本不允许从「{current}」流转到「{target}」"
            )
        version["lifecycle_status"] = target
        version["history"].append(self._history_item(current, target, reason))

    def _advance_or_record(
        self, version: dict[str, Any], target: str, reason: str,
    ) -> None:
        """正向流转；若已处于目标状态（再物化）则仅记录历史，不回退也不报错。"""
        if version["lifecycle_status"] == target:
            version["history"].append(
                self._history_item(target, target, reason),
            )
            return
        self._transition(version, target, reason)

    @staticmethod
    def _history_item(frm: str | None, to: str, reason: str) -> dict[str, Any]:
        return {"ts": time.time(), "from": frm, "to": to, "reason": reason}

    def confirm_window(self, version_id: str) -> dict[str, Any]:
        """确认窗口：待检查点 → 已确认。确认后窗口封存，任何重算请求都不能回退。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            if version["lifecycle_status"] != LIFECYCLE_PENDING_CP:
                return {
                    "ok": False,
                    "error": f"版本当前为「{version['lifecycle_status']}」，仅「待检查点」可确认窗口",
                }
            if not version.get("checkpoint_id") or not version.get("snapshot"):
                return {"ok": False, "error": "检查点引用或聚合快照缺失，不能确认窗口"}
            version["confirmed_keys"] = sorted(version["key_results"].keys())
            version["confirmed_at"] = time.time()
            self._transition(version, LIFECYCLE_CONFIRMED, "窗口确认封存，结果不可回退")
            store.save_join_version(version)
            return {"ok": True, "version": self._version_view(version)}

    def archive_version(self, version_id: str) -> dict[str, Any]:
        """归档版本：已确认 → 已归档，归档条件写入关联结果数据。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            if version["lifecycle_status"] != LIFECYCLE_CONFIRMED:
                return {
                    "ok": False,
                    "error": f"版本当前为「{version['lifecycle_status']}」，仅「已确认」可归档",
                }
            version["archived_at"] = time.time()
            self._transition(
                version, LIFECYCLE_ARCHIVED,
                f"满足归档条件：{ARCHIVE_CONDITION}",
            )
            store.save_join_version(version)
            return {"ok": True, "version": self._version_view(version)}

    def restore_archived_version(self, version_id: str) -> dict[str, Any]:
        """按恢复条件只读恢复归档版本：不重新计算、不改变生命周期。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            if version["lifecycle_status"] != LIFECYCLE_ARCHIVED:
                return {
                    "ok": False,
                    "error": f"版本当前为「{version['lifecycle_status']}」，仅归档版本可恢复读取",
                }
            version["history"].append(self._history_item(
                LIFECYCLE_ARCHIVED, LIFECYCLE_ARCHIVED,
                f"按恢复条件只读恢复：{RESTORE_CONDITION}",
            ))
            store.save_join_version(version)
            return {
                "ok": True,
                "restored": True,
                "checkpoint_id": version.get("active_checkpoint_id"),
                "version": self._version_view(version),
            }

    def resume_version(
        self,
        version_id: str,
        *,
        checkpoint_config: str | None = None,
        fail_sink: bool = False,
        fail_checkpoint: bool = False,
    ) -> dict[str, Any]:
        """关联中断后的恢复入口：只补未完成键，已确认键不重算、窗口不回退。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            status = version["lifecycle_status"]
            if status in (LIFECYCLE_CONFIRMED, LIFECYCLE_ARCHIVED):
                return {
                    "ok": False,
                    "error": "已确认窗口不能回退，恢复只允许补未完成键",
                    "retained_status": status,
                }
            if status == LIFECYCLE_PENDING_CP:
                return {
                    "ok": True,
                    "idempotent": True,
                    "reason": "快照与检查点已原子生效，无需补算",
                    "version": self._version_view(version),
                }
            join_cfg = self._find_config(version["join_name"])
            version["history"].append(self._history_item(
                status, status, "中断恢复：仅补算未完成键",
            ))
            self._compute_pending_keys(version, join_cfg)
            txn = self._materialize_transaction(
                version, checkpoint_config=checkpoint_config,
                fail_sink=fail_sink, fail_checkpoint=fail_checkpoint,
            )
            store.save_join_version(version)
            if not txn["ok"]:
                return txn
            return {"ok": True, "resumed": True, "transaction": txn,
                    "version": self._version_view(version)}

    # ------------------------------------------------------------------
    # 冲突优先级与检查点联动
    # ------------------------------------------------------------------

    def _conflict_response(
        self, existing: dict[str, Any] | None, fingerprint: str,
    ) -> dict[str, Any]:
        """冲突优先级：已确认/已归档版本优先且不可回退，在途版本合并，相同重试幂等。"""
        if existing is None:
            return {"ok": False, "error": "窗口版本冲突且无法定位既有版本"}
        priority = {
            LIFECYCLE_ARCHIVED: 100,
            LIFECYCLE_CONFIRMED: 90,
            LIFECYCLE_PENDING_CP: 50,
            LIFECYCLE_COMPUTING: 10,
        }.get(existing["lifecycle_status"], 0)
        return {
            "ok": False,
            "conflict": True,
            "priority": priority,
            "existing_version_id": existing["version_id"],
            "existing_status": existing["lifecycle_status"],
            "same_fingerprint": existing.get("input_fingerprint") == fingerprint,
            "message": "同窗口已存在另一版本，按既有版本优先级处理，不新建版本",
        }

    def inspect_conflict(
        self, join_name: str, window_start: float, window_end: float,
    ) -> dict[str, Any]:
        """判断冲突优先级，并核对关联结果数据的事务影响。"""
        routed = store.find_join_window(
            self._window_key(join_name, window_start, window_end),
        )
        if routed is None:
            return {"ok": True, "conflict": False, "message": "窗口尚无版本，可直接生成"}
        version = store.get_join_version(routed)
        response = self._conflict_response(version, version.get("input_fingerprint", ""))
        response["ok"] = True
        response["snapshot_visible"] = bool(version.get("snapshot"))
        response["checkpoint_visible"] = bool(version.get("checkpoint_id"))
        response["transaction_effect"] = (
            "事务已提交，结果数据与检查点同时可见"
            if version.get("snapshot") and version.get("checkpoint_id")
            else "事务未提交或已回滚，仅保留计算中状态"
        )
        return response

    def locate_version_by_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """检查点回放入口：通过检查点引用定位完整关联结果版本。"""
        version = store.find_join_version(checkpoint_id)
        if version is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」未关联任何流关联版本"}
        return {
            "ok": True,
            "checkpoint_id": checkpoint_id,
            "version": self._version_view(version),
            "replay_ready": version["lifecycle_status"]
            in (LIFECYCLE_PENDING_CP, LIFECYCLE_CONFIRMED, LIFECYCLE_ARCHIVED),
        }

    def switch_checkpoint_ref(
        self, version_id: str, checkpoint_id: str,
    ) -> dict[str, Any]:
        """切换版本详情窗口中的活动检查点引用（在该版本自己的检查点历史内切换）。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            owned = {cp["checkpoint_id"] for cp in version["checkpoint_history"]}
            if checkpoint_id not in owned:
                return {"ok": False, "error": "检查点不属于该版本，不能切换引用"}
            previous = version["active_checkpoint_id"]
            version["active_checkpoint_id"] = checkpoint_id
            version["checkpoint_id"] = checkpoint_id
            CheckpointMgrService().mark_active_checkpoint(version_id, checkpoint_id)
            version["history"].append(self._history_item(
                version["lifecycle_status"], version["lifecycle_status"],
                f"检查点引用切换：{previous} → {checkpoint_id}",
            ))
            store.save_join_version(version)
            return {"ok": True, "active_checkpoint_id": checkpoint_id,
                    "version": self._version_view(version)}

    def refresh_checkpoint_ref(
        self, version_id: str, checkpoint_config: str | None = None,
    ) -> dict[str, Any]:
        """为在途/已确认版本补一版检查点（外部化路径轮换等场景），并发只保留一版。"""
        version = store.get_join_version(version_id)
        if version is None:
            return {"ok": False, "error": f"关联结果版本「{version_id}」不存在"}
        with store.join_version_lock(version_id):
            if version["lifecycle_status"] == LIFECYCLE_COMPUTING:
                return {"ok": False, "error": "版本尚未生成聚合快照，不能刷新检查点"}
            cp_row = CheckpointMgrService().consume_join_version(
                version, config_name=checkpoint_config, refresh=True,
            )
            if cp_row["checkpoint_id"] not in version["checkpoint_history"]:
                version["checkpoint_history"].append({
                    "checkpoint_id": cp_row["checkpoint_id"],
                    "snapshot_ts": cp_row["snapshot_ts"],
                    "note": "检查点刷新，关联结果数据版本不变",
                })
            version["active_checkpoint_id"] = cp_row["checkpoint_id"]
            version["checkpoint_id"] = cp_row["checkpoint_id"]
            store.save_join_version(version)
            return {"ok": True, "checkpoint_id": cp_row["checkpoint_id"],
                    "version": self._version_view(version)}

    # ------------------------------------------------------------------
    # 工作台查询与视图
    # ------------------------------------------------------------------

    def list_workbench_versions(
        self,
        *,
        join_name: str | None = None,
        window_end: float | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """工作台窗口筛选：每个版本附带检查点与聚合落库状态。"""
        versions = store.list_join_versions(
            join_name=join_name, window_end=window_end, status=status,
        )
        return [self._version_view(v) for v in versions]

    def version_detail(self, version_id: str) -> dict[str, Any] | None:
        """状态详情：按窗口版本核对关联结果数据与聚合落库快照。"""
        version = store.get_join_version(version_id)
        if version is None:
            return None
        view = self._version_view(version)
        sink_row = next(
            (r for r in store.rows("aggregate_sink")
             if r.get("join_version_id") == version_id),
            None,
        )
        view["sink_record"] = {
            "id": sink_row.get("id") if sink_row else None,
            "snapshot_status": (sink_row or {}).get("snapshot_status", "未落地"),
            "task_name": (sink_row or {}).get("任务名称"),
        }
        view["checkpoint_rows"] = [
            cp for cp in store.rows("checkpoint_mgr")
            if cp.get("join_version_id") == version_id
        ]
        return view

    def _version_view(self, version: dict[str, Any]) -> dict[str, Any]:
        """工作台/接口只读视图，接口在状态变更后读取同一版本数据。"""
        return {
            "version_id": version["version_id"],
            "version_seq": version["version_seq"],
            "join_name": version["join_name"],
            "join_type": version["join_type"],
            "join_field": version["join_field"],
            "window_start": version["window_start"],
            "window_end": version["window_end"],
            "merge_mode": version["merge_mode"],
            "lifecycle_status": version["lifecycle_status"],
            "stats": copy.deepcopy(version["stats"]),
            "late_dropped": version["late_dropped"],
            "duplicate_overwrites": version["duplicate_overwrites"],
            "key_count": len(version["key_results"]),
            "confirmed_keys": list(version["confirmed_keys"]),
            "snapshot_status": "已落库" if version.get("snapshot") else "未落地",
            "checkpoint_id": version.get("checkpoint_id"),
            "active_checkpoint_id": version.get("active_checkpoint_id"),
            "checkpoint_count": len(version.get("checkpoint_history", [])),
            "archive_condition": version["archive_condition"],
            "restore_condition": version["restore_condition"],
            "last_error": version.get("last_error"),
            "updated_at": version.get("updated_at"),
        }

    # ------------------------------------------------------------------
    # 兼容原有统计能力
    # ------------------------------------------------------------------

    def evaluate_join_window(
        self, join_name: str, left_events: list[dict], right_events: list[dict],
    ) -> dict[str, Any]:
        """在时间窗口内评估双流关联匹配度（保留原接口，内部走版本引擎）。"""
        join_cfg = self._find_config(join_name)
        if join_cfg is None:
            return {"ok": False, "error": f"关联配置「{join_name}」不存在"}
        result = self.produce_join_version(join_name, left_events, right_events)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "关联计算失败")}
        view = result["version"]
        return {
            "ok": True,
            "join_config": join_name,
            "type": view["join_type"],
            "total_keys": view["stats"]["total_keys"],
            "matched": view["stats"]["matched"],
            "match_rate": view["stats"]["match_rate"],
            "version_id": view["version_id"],
            "checkpoint_id": view["checkpoint_id"],
        }

    def list_join_statistics(self) -> list[dict[str, Any]]:
        """列出所有流关联的统计信息（含版本数与检查点数）。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                name = r.get("配置名称", "")
                agg_matches = sum(
                    1 for a in store.rows("aggregate_sink")
                    if name in str(a.get("源指标集", ""))
                )
                versions = store.list_join_versions(join_name=name)
                result.append({
                    "join": name,
                    "left_src": r.get("左流源", ""),
                    "right_src": r.get("右流源", ""),
                    "type": r.get("关联类型", ""),
                    "status": r.get("status", ""),
                    "aggregations": agg_matches,
                    "versions": len(versions),
                    "confirmed_versions": sum(
                        1 for v in versions
                        if v["lifecycle_status"] in (LIFECYCLE_CONFIRMED, LIFECYCLE_ARCHIVED)
                    ),
                })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        versions = store.list_join_versions()
        base["join_versions"] = len(versions)
        base["versions_by_lifecycle"] = {
            status: sum(1 for v in versions if v["lifecycle_status"] == status)
            for status in LIFECYCLE_ORDER
        }
        base["checkpoint_linked_versions"] = sum(
            1 for v in versions if v.get("checkpoint_id")
        )
        return base
