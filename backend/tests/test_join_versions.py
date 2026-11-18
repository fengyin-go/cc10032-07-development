"""序号7（流关联）关联结果版本测试。

覆盖：
  - 确定性版本与并发重试幂等（只保留一版）
  - 重复键合并（窗口截断 / 最后到达）与迟到事件截断
  - 关联结果数据、聚合落库快照、检查点引用原子可见
  - 事务失败回滚并保留原状态
  - 单向生命周期、已确认窗口不可回退、中断恢复只补未完成键
  - 检查点反查完整版本（回放定位）
  - 归档/恢复条件、检查点引用切换
  - 每个关联键只在一个窗口版本中出结果、冲突优先级
  - API 层端到端
"""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.join_stream import (
    LIFECYCLE_ARCHIVED,
    LIFECYCLE_COMPUTING,
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_PENDING_CP,
    MERGE_LAST_ARRIVAL,
    MERGE_WINDOW_CUTOFF,
    JoinStreamService,
)
from app.store import store

JOIN_NAME = "流关联样例1"
JOIN_FIELD = "user_id"


@pytest.fixture(autouse=True)
def clean_join_versions():
    """每个用例前清空序号7版本数据与联动写入，保证相互隔离。"""
    store._join_versions.clear()
    store._join_windows.clear()
    store._join_locks.clear()
    # 清理历史用例写入的联动记录
    store.rows("aggregate_sink")[:] = [
        r for r in store.rows("aggregate_sink") if not r.get("join_version_id")
    ]
    store.rows("checkpoint_mgr")[:] = [
        r for r in store.rows("checkpoint_mgr") if r.get("source") != "join_stream"
    ]
    yield


def _events(side: str, keys: list[str], base: float = 1000.0) -> list[dict]:
    return [
        {
            "event_id": f"{side}-{i}-{key}",
            "event_time": base + i,
            "payload": {"user_id": key, "amount": i + 1},
            "source": side,
        }
        for i, key in enumerate(keys)
    ]


def _patch_config(monkeypatch, join_field=JOIN_FIELD, window_sec="10", join_type="inner"):
    """种子数据字段为占位值，用例内把关联配置修正成可计算的值。"""
    for row in store.rows("join_stream"):
        if row.get("配置名称") == JOIN_NAME:
            monkeypatch.setitem(row, "关联字段", join_field)
            monkeypatch.setitem(row, "窗口区间", window_sec)
            monkeypatch.setitem(row, "关联类型", join_type)


@pytest.fixture
def service():
    return JoinStreamService()


# ---------------------------------------------------------------------------
# 1. 确定性版本 + 基础关联计算
# ---------------------------------------------------------------------------

class TestVersionDeterminism:

    def test_produces_deterministic_version_with_snapshot_and_checkpoint(
        self, service, monkeypatch,
    ):
        _patch_config(monkeypatch)
        r1 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1", "u2"]), _events("R", ["u1", "u3"]),
        )
        assert r1["ok"] and not r1["idempotent"]
        v = r1["version"]
        assert v["lifecycle_status"] == LIFECYCLE_PENDING_CP
        # inner 关联只保留双侧命中键：u2/u3 单侧出现不计入结果
        assert v["stats"]["total_keys"] == 1
        assert v["stats"]["matched"] == 1  # 只有 u1 双侧命中
        # 聚合快照与检查点必须同时可见
        assert v["snapshot_status"] == "已落库"
        assert v["checkpoint_id"]

    def test_same_window_concurrent_retry_keeps_single_version(
        self, service, monkeypatch,
    ):
        _patch_config(monkeypatch)
        left, right = _events("L", ["u1"]), _events("R", ["u1"])
        r1 = service.produce_join_version(JOIN_NAME, left, right)
        r2 = service.produce_join_version(JOIN_NAME, left, right)
        assert r1["version"]["version_id"] == r2["version"]["version_id"]
        assert r2["idempotent"] is True
        assert len(store.list_join_versions()) == 1
        # 只写一份聚合快照与检查点
        sinks = [r for r in store.rows("aggregate_sink") if r.get("join_version_id")]
        cps = [r for r in store.rows("checkpoint_mgr") if r.get("source") == "join_stream"]
        assert len(sinks) == 1 and len(cps) == 1

    def test_concurrent_threads_only_persist_one_version(self, service, monkeypatch):
        _patch_config(monkeypatch)
        left, right = _events("L", ["u1", "u2"]), _events("R", ["u1"])
        results: list[dict] = []

        def worker():
            results.append(service.produce_join_version(JOIN_NAME, left, right))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        version_ids = {r["version"]["version_id"] for r in results}
        assert len(version_ids) == 1
        assert len(store.list_join_versions()) == 1

    def test_each_join_key_belongs_to_single_window_version(self, service, monkeypatch):
        _patch_config(monkeypatch, join_type="left")
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1", "u2"]), _events("R", ["u1", "u2"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        # 同窗口再次提交必须路由到同一个版本
        routed = store.find_join_window((JOIN_NAME, 1000.0, 1010.0))
        assert routed == vid
        detail = service.version_detail(vid)
        assert set(detail["confirmed_keys"]) == set()  # 未确认前为空
        assert detail["snapshot_status"] == "已落库"
        assert detail["key_count"] == 2
        # 完整快照通过 store 中的版本数据核对
        assert set(store.get_join_version(vid)["snapshot"]["keys"]) == {"u1", "u2"}


# ---------------------------------------------------------------------------
# 2. 重复键合并与迟到截断
# ---------------------------------------------------------------------------

class TestDuplicateAndLateEvents:

    def test_window_cutoff_keeps_earliest_duplicate(self, service, monkeypatch):
        _patch_config(monkeypatch)
        left = [
            {"event_id": "l-old", "event_time": 1001.0,
             "payload": {"user_id": "u1", "amount": 1}, "source": "L"},
            {"event_id": "l-new", "event_time": 1005.0,
             "payload": {"user_id": "u1", "amount": 99}, "source": "L"},
        ]
        r = service.produce_join_version(
            JOIN_NAME, left, _events("R", ["u1"]),
            merge_mode=MERGE_WINDOW_CUTOFF,
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        version = store.get_join_version(vid)
        assert version["duplicate_overwrites"] == 1
        assert version["key_results"]["u1"]["left_event_id"] == "l-old"

    def test_last_arrival_keeps_latest_duplicate(self, service, monkeypatch):
        _patch_config(monkeypatch)
        left = [
            {"event_id": "l-old", "event_time": 1001.0,
             "payload": {"user_id": "u1", "amount": 1}, "source": "L"},
            {"event_id": "l-new", "event_time": 1005.0,
             "payload": {"user_id": "u1", "amount": 99}, "source": "L"},
        ]
        r = service.produce_join_version(
            JOIN_NAME, left, _events("R", ["u1"]),
            merge_mode=MERGE_LAST_ARRIVAL,
            window_start=1000.0, window_end=1010.0,
        )
        version = store.get_join_version(r["version"]["version_id"])
        assert version["key_results"]["u1"]["left_event_id"] == "l-new"

    def test_events_after_window_end_are_truncated(self, service, monkeypatch):
        _patch_config(monkeypatch)
        left = _events("L", ["u1", "u9"], base=1000.0)
        left.append({"event_id": "late", "event_time": 1099.0,
                     "payload": {"user_id": "u8"}, "source": "L"})
        r = service.produce_join_version(
            JOIN_NAME, left, _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        version = store.get_join_version(r["version"]["version_id"])
        assert version["late_dropped"] == 1
        assert "u8" not in version["key_results"]

    def test_arriving_late_event_merges_into_inflight_version(self, service, monkeypatch):
        _patch_config(monkeypatch, join_type="left")
        # 首次事务失败，版本保持「计算中」在途状态
        r1 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), [],
            window_start=1000.0, window_end=1010.0, fail_sink=True,
        )
        assert r1["ok"] is False
        vid = r1["version_id"]
        # 右流晚到：同一版本继续合并，不新建版本
        r2 = service.produce_join_version(
            JOIN_NAME, [], _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        assert r2["ok"]
        assert r2["version"]["version_id"] == vid
        version = store.get_join_version(vid)
        assert version["stats"]["matched"] == 1


# ---------------------------------------------------------------------------
# 3. 原子事务与回滚
# ---------------------------------------------------------------------------

class TestAtomicTransaction:

    def test_sink_failure_rolls_back_and_keeps_status(self, service, monkeypatch):
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            fail_sink=True,
            window_start=1000.0, window_end=1010.0,
        )
        assert r["ok"] is False and r["rolled_back"] is True
        assert r["retained_status"] == LIFECYCLE_COMPUTING
        version = store.get_join_version(r["version_id"])
        assert version["lifecycle_status"] == LIFECYCLE_COMPUTING
        assert version["snapshot"] is None and version["checkpoint_id"] is None
        # 补偿后没有残留落库记录
        assert not [
            x for x in store.rows("aggregate_sink")
            if x.get("join_version_id") == version["version_id"]
        ]

    def test_checkpoint_failure_rolls_back_sink_too(self, service, monkeypatch):
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            fail_checkpoint=True,
            window_start=1000.0, window_end=1010.0,
        )
        assert r["ok"] is False
        version = store.get_join_version(r["version_id"])
        assert version["lifecycle_status"] == LIFECYCLE_COMPUTING
        assert version["snapshot"] is None
        assert not [
            x for x in store.rows("checkpoint_mgr")
            if x.get("join_version_id") == version["version_id"]
        ]

    def test_resume_after_failure_only_recomputes_pending_keys(self, service, monkeypatch):
        _patch_config(monkeypatch)
        failed = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1", "u2"]), _events("R", ["u1"]),
            fail_sink=True, window_start=1000.0, window_end=1010.0,
        )
        vid = failed["version_id"]
        resumed = service.resume_version(vid)
        assert resumed["ok"] and resumed["resumed"]
        version = store.get_join_version(vid)
        assert version["lifecycle_status"] == LIFECYCLE_PENDING_CP
        assert version["snapshot"] and version["checkpoint_id"]

    def test_rematerialize_updates_single_snapshot_and_checkpoint(
        self, service, monkeypatch,
    ):
        """再物化：同一版本只保留一份落库快照，检查点轮换但仅一个活动引用。"""
        _patch_config(monkeypatch, join_type="left")
        r1 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r1["version"]["version_id"]
        old_cp = r1["version"]["checkpoint_id"]
        r2 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u2"], base=1002.0), [],
            window_start=1000.0, window_end=1010.0,
        )
        assert r2["ok"] and r2["version"]["version_id"] == vid
        sinks = [
            x for x in store.rows("aggregate_sink")
            if x.get("join_version_id") == vid
        ]
        assert len(sinks) == 1  # 用户只看到一份关联结果
        cps = [
            x for x in store.rows("checkpoint_mgr")
            if x.get("join_version_id") == vid
        ]
        assert len(cps) == 2 and sum(1 for c in cps if c.get("is_active")) == 1
        assert r2["version"]["checkpoint_id"] != old_cp
        assert r2["version"]["lifecycle_status"] == LIFECYCLE_PENDING_CP

    def test_rollback_after_rematerialize_restores_old_checkpoint(
        self, service, monkeypatch,
    ):
        """再物化时检查点失败：旧快照与旧检查点引用全部恢复，状态保留。"""
        _patch_config(monkeypatch, join_type="left")
        r1 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid, old_cp = r1["version"]["version_id"], r1["version"]["checkpoint_id"]
        r2 = service.produce_join_version(
            JOIN_NAME, _events("L", ["u2"], base=1002.0), [],
            window_start=1000.0, window_end=1010.0, fail_checkpoint=True,
        )
        assert r2["ok"] is False and r2["rolled_back"] is True
        version = store.get_join_version(vid)
        assert version["checkpoint_id"] == old_cp
        assert version["snapshot"] is not None  # 旧快照恢复
        assert version["lifecycle_status"] == LIFECYCLE_PENDING_CP
        cps = [
            x for x in store.rows("checkpoint_mgr")
            if x.get("join_version_id") == vid
        ]
        assert len(cps) == 1 and cps[0]["is_active"] is True


# ---------------------------------------------------------------------------
# 4. 单向生命周期、确认不可回退、归档恢复
# ---------------------------------------------------------------------------

class TestLifecycle:

    def _confirmed(self, service, monkeypatch) -> str:
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        assert service.confirm_window(vid)["ok"]
        return vid

    def test_full_one_way_lifecycle_chain(self, service, monkeypatch):
        vid = self._confirmed(service, monkeypatch)
        version = store.get_join_version(vid)
        assert version["lifecycle_status"] == LIFECYCLE_CONFIRMED
        assert version["confirmed_keys"] == ["u1"]
        # 反向流转被拒绝
        bad = service.confirm_window(vid)
        assert bad["ok"] is False
        archived = service.archive_version(vid)
        assert archived["ok"]
        assert store.get_join_version(vid)["lifecycle_status"] == LIFECYCLE_ARCHIVED
        # 归档后不能再次归档
        assert service.archive_version(vid)["ok"] is False

    def test_confirmed_window_cannot_rollback(self, service, monkeypatch):
        vid = self._confirmed(service, monkeypatch)
        again = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        assert again["idempotent"] is True
        assert "已确认" in again["reason"]
        # 恢复接口也不能回退已确认窗口
        assert service.resume_version(vid)["ok"] is False
        assert store.get_join_version(vid)["lifecycle_status"] == LIFECYCLE_CONFIRMED

    def test_restore_archived_is_read_only(self, service, monkeypatch):
        vid = self._confirmed(service, monkeypatch)
        service.archive_version(vid)
        restored = service.restore_archived_version(vid)
        assert restored["ok"] and restored["restored"]
        version = store.get_join_version(vid)
        assert version["lifecycle_status"] == LIFECYCLE_ARCHIVED  # 状态不变
        assert restored["checkpoint_id"] == version["active_checkpoint_id"]

    def test_confirm_requires_checkpoint_and_snapshot(self, service, monkeypatch):
        _patch_config(monkeypatch)
        failed = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            fail_sink=True, window_start=1000.0, window_end=1010.0,
        )
        r = service.confirm_window(failed["version_id"])
        assert r["ok"] is False and "检查点" in r["error"]


# ---------------------------------------------------------------------------
# 5. 检查点联动：回放定位、引用切换、刷新
# ---------------------------------------------------------------------------

class TestCheckpointLinkage:

    def test_locate_full_version_by_checkpoint(self, service, monkeypatch):
        _patch_config(monkeypatch, join_type="left")
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1", "u2"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        cp_id = r["version"]["checkpoint_id"]
        located = service.locate_version_by_checkpoint(cp_id)
        assert located["ok"] and located["replay_ready"] is True
        assert located["version"]["version_id"] == r["version"]["version_id"]
        # 检查点行内保存了窗口、键与聚合结果，回放可直接核对
        cp_row = next(
            x for x in store.rows("checkpoint_mgr")
            if x.get("checkpoint_id") == cp_id
        )
        states = cp_row["operator_states"]
        assert states["window"] == [1000.0, 1010.0]
        assert set(states["key_results"].keys()) == {"u1", "u2"}
        assert states["stats"]["total_keys"] == 2

    def test_checkpoint_switch_within_version_history(self, service, monkeypatch):
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        old_cp = r["version"]["checkpoint_id"]
        refreshed = service.refresh_checkpoint_ref(vid)
        new_cp = refreshed["checkpoint_id"]
        assert new_cp != old_cp
        # 切换回旧检查点
        switched = service.switch_checkpoint_ref(vid, old_cp)
        assert switched["active_checkpoint_id"] == old_cp
        # 不允许引用其他版本的检查点
        other = service.produce_join_version(
            JOIN_NAME, _events("L", ["u9"]), [],
            window_start=2000.0, window_end=2010.0,
        )
        deny = service.switch_checkpoint_ref(vid, other["version"]["checkpoint_id"])
        assert deny["ok"] is False

    def test_checkpoint_refresh_keeps_single_active(self, service, monkeypatch):
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        service.refresh_checkpoint_ref(vid)
        active = [
            x for x in store.rows("checkpoint_mgr")
            if x.get("join_version_id") == vid and x.get("is_active")
        ]
        assert len(active) == 1


# ---------------------------------------------------------------------------
# 6. 冲突优先级
# ---------------------------------------------------------------------------

class TestConflictPriority:

    def test_conflict_priority_and_transaction_effect(self, service, monkeypatch):
        _patch_config(monkeypatch)
        r = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            merge_mode=MERGE_WINDOW_CUTOFF,
            window_start=1000.0, window_end=1010.0,
        )
        # 同窗口不同合并模式 → 冲突，不新建版本
        conflict = service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            merge_mode=MERGE_LAST_ARRIVAL,
            window_start=1000.0, window_end=1010.0,
        )
        assert conflict["ok"] is False and conflict["conflict"] is True
        inspect = service.inspect_conflict(JOIN_NAME, 1000.0, 1010.0)
        assert inspect["conflict"] is True
        assert inspect["snapshot_visible"] and inspect["checkpoint_visible"]
        assert "同时可见" in inspect["transaction_effect"]
        # 确认后优先级更高
        service.confirm_window(r["version"]["version_id"])
        inspect2 = service.inspect_conflict(JOIN_NAME, 1000.0, 1010.0)
        assert inspect2["priority"] == 90


# ---------------------------------------------------------------------------
# 7. 工作台视图与聚合快照联动
# ---------------------------------------------------------------------------

class TestWorkbenchViews:

    def test_workbench_filters_and_detail_snapshot(self, service, monkeypatch):
        _patch_config(monkeypatch)
        service.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        service.produce_join_version(
            JOIN_NAME, _events("L", ["u5"]), [],
            window_start=2000.0, window_end=2010.0,
        )
        items = service.list_workbench_versions(join_name=JOIN_NAME)
        assert len(items) == 2
        filtered = service.list_workbench_versions(window_end=1010.0)
        assert len(filtered) == 1 and filtered[0]["window_end"] == 1010.0
        detail = service.version_detail(items[0]["version_id"])
        assert detail["sink_record"]["snapshot_status"] == "已落库"
        assert len(detail["checkpoint_rows"]) >= 1
        assert detail["archive_condition"] and detail["restore_condition"]

    def test_aggregate_sink_lookup_by_version(self, monkeypatch):
        from app.services.aggregate_sink import AggregateSinkService
        _patch_config(monkeypatch)
        svc = JoinStreamService()
        r = svc.produce_join_version(
            JOIN_NAME, _events("L", ["u1"]), _events("R", ["u1"]),
            window_start=1000.0, window_end=1010.0,
        )
        vid = r["version"]["version_id"]
        snap = AggregateSinkService().get_snapshot_by_version(vid)
        assert snap is not None and snap["snapshot_status"] == "已落库"
        assert snap["stats"]["matched"] == 1


# ---------------------------------------------------------------------------
# 8. API 端到端
# ---------------------------------------------------------------------------

class TestJoinPipelineApi:

    def test_api_full_flow(self, monkeypatch):
        _patch_config(monkeypatch)
        client = TestClient(app)
        payload = {
            "join_name": JOIN_NAME,
            "left_events": _events("L", ["u1", "u2"]),
            "right_events": _events("R", ["u1"]),
            "window_start": 1000.0,
            "window_end": 1010.0,
        }
        resp = client.post("/api/join-stream/versions", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        vid = body["version"]["version_id"]
        assert body["version"]["checkpoint_id"]

        # 幂等重试
        again = client.post("/api/join-stream/versions", json=payload)
        assert again.json()["idempotent"] is True

        # 列表筛选
        listed = client.get("/api/join-stream/versions", params={"window_end": 1010.0})
        assert listed.json()["total"] == 1

        # 详情
        detail = client.get(f"/api/join-stream/versions/{vid}")
        assert detail.status_code == 200

        # 确认 → 归档 → 只读恢复
        assert client.post(f"/api/join-stream/versions/{vid}/confirm").json()["ok"]
        assert client.post(f"/api/join-stream/versions/{vid}/archive").json()["ok"]
        restored = client.post(f"/api/join-stream/versions/{vid}/restore-read")
        assert restored.json()["restored"] is True

        # 检查点回放定位
        cp_id = body["version"]["checkpoint_id"]
        located = client.get(f"/api/join-stream/checkpoints/locate/{cp_id}")
        assert located.status_code == 200
        assert located.json()["version"]["version_id"] == vid

    def test_api_transaction_rollback(self, monkeypatch):
        _patch_config(monkeypatch)
        client = TestClient(app)
        resp = client.post("/api/join-stream/versions", json={
            "join_name": JOIN_NAME,
            "left_events": _events("L", ["u1"]),
            "right_events": _events("R", ["u1"]),
            "window_start": 1000.0,
            "window_end": 1010.0,
            "fail_sink": True,
        })
        assert resp.status_code == 400
        assert resp.json()["detail"]["rolled_back"] is True

    def test_api_checkpoint_entry_lists_join_checkpoints(self, monkeypatch):
        _patch_config(monkeypatch)
        client = TestClient(app)
        client.post("/api/join-stream/versions", json={
            "join_name": JOIN_NAME,
            "left_events": _events("L", ["u1"]),
            "right_events": _events("R", ["u1"]),
            "window_start": 1000.0,
            "window_end": 1010.0,
        })
        resp = client.get("/api/join-stream/checkpoints")
        assert resp.status_code == 200
        assert resp.json()["total"] >= 1
        assert resp.json()["items"][0]["key_count"] == 1
