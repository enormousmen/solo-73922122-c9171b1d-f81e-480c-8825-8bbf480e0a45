"""清单切换（不停机更新）相关流程自测。"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime.now(timezone.utc) - timedelta(minutes=30)


@pytest.fixture()
def make_client(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def report(client, node, healthy=True, boot="boot-1", sampled_at=None):
    sampled_at = sampled_at or datetime.now(timezone.utc)
    return client.post(
        "/liveness",
        json={
            "reports": [
                {
                    "node_id": node,
                    "sampled_at": sampled_at.isoformat(),
                    "healthy": healthy,
                    "boot_id": boot,
                }
            ]
        },
    )


def seed_all_healthy(client, boot="boot-1", sampled_at=T0):
    for n in ALL_NODES:
        assert report(client, n, True, boot, sampled_at).status_code == 201


def switch(client, version, shards):
    return client.post(
        "/manifest/switch", json={"current_version": version, "shards": shards}
    )


def request_permit(client, request_id, nodes):
    return client.post("/permits", json={"request_id": request_id, "nodes": nodes})


# ---------- 查询版本号 ----------


def test_manifest_query_returns_version(client):
    body = client.get("/manifest").json()
    assert body["version"] == 1
    assert body["shards"]["shard-a"] == {
        "voters": ["n1", "n2", "n3"],
        "majority_needed": 2,
    }


def test_initial_version_from_mounted_file(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({**MANIFEST, "version": 7}))
    c = TestClient(
        create_app(db_path=str(tmp_path / "p.db"), manifest_path=str(manifest_path))
    )
    try:
        assert c.get("/manifest").json()["version"] == 7
        # 版本号必须与查询到的一致
        assert switch(c, 7, {"shard-a": ["n1", "n2", "n3"]}).status_code == 201
        assert c.get("/manifest").json()["version"] == 8
    finally:
        c.close()


# ---------- 成功切换 ----------


def test_switch_success_persists_and_takes_effect_immediately(client):
    seed_all_healthy(client)
    new_shards = {
        "shard-a": ["n1", "n2", "n3", "n4"],  # 成员变动：4 票需 3
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
        "shard-d": ["n1", "n2", "n5"],  # 新增分片
    }
    r = switch(client, 1, new_shards)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["version"] == 2
    assert body["shards"]["shard-a"]["majority_needed"] == 3
    assert body["shards"]["shard-d"]["voters"] == ["n1", "n2", "n5"]

    # 查询立刻反映新清单
    assert client.get("/manifest").json() == body

    # 新申请按新清单受理：n4 现在是 shard-a 的投票成员（旧清单中 n4 不属于
    # shard-a），单独重启 n4 后 shard-a 仍有 n2,n3 两票可用（4 票需 3，扣本批 1 票）
    assert request_permit(client, "req-1", ["n4"]).status_code == 201, (
        request_permit(client, "req-1", ["n4"]).text
    )
    # n6 不在新清单中
    r = request_permit(client, "req-2", ["n6"])
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["n6"]


def test_switch_excludes_occupied_and_unhealthy_but_can_still_pass(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-open", ["n1"]).status_code == 201
    report(client, "n3", healthy=False)  # n3 异常
    # 新分片 4 票需 3：n1 被未结束许可占用、n3 异常 -> 可用 n2,n4 = 2 < 3，拒绝
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n3", "n4"]})
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "manifest_switch_rejected"
    shards = {s["shard"]: s for s in detail["affected_shards"]}
    assert set(shards) == {"shard-d"}
    assert shards["shard-d"]["excluded"] == {
        "n1": "occupied_by_open_permit",
        "n3": "unhealthy",
    }
    assert shards["shard-d"]["available"] == 2

    # 换成 4 票需 3、且只含 n2 一个可用票的分片仍拒绝
    r = switch(
        client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n3", "n9"]}
    )
    # n9 无探活，n1 占用、n3 异常 -> 可用 1 < 3
    assert r.status_code == 409, r.text
    # 新分片全部为健康且未被占用节点时可通过
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n2", "n4", "n5"]})
    assert r.status_code == 201, r.text
    assert r.json()["shards"]["shard-d"]["majority_needed"] == 2


def test_switch_only_checks_new_or_changed_shards(client):
    seed_all_healthy(client)
    # shard-b、shard-c 各有两票异常，现有状态已无法过半，但本次不涉及
    report(client, "n3", healthy=False)
    report(client, "n4", healthy=False)
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n5"]})
    assert r.status_code == 201, r.text
    detail = r.json()
    assert detail["version"] == 2
    assert "shard-b" in detail["shards"]  # 仍保留在清单里

    # 而一旦改动 shard-b，就必须重新过半数：恰好扣到只剩 n2 -> 拒绝
    r = switch(client, 2, {**MANIFEST["shards"], "shard-b": ["n2", "n3", "n4", "n5"]})
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    shards = {s["shard"]: s for s in detail["affected_shards"]}
    assert set(shards) == {"shard-b"}
    assert shards["shard-b"]["required_available"] == 3
    assert shards["shard-b"]["available"] == 2
    # 查询仍是旧清单（版本 2）
    assert client.get("/manifest").json()["version"] == 2


# ---------- 失败不生效 ----------


def test_switch_version_mismatch_keeps_old_manifest(client):
    seed_all_healthy(client)
    r = switch(client, 2, {"shard-a": ["n1", "n2", "n3"]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail == {
        "error": "manifest_version_conflict",
        "current_version": 1,
        "submitted_version": 2,
    }
    body = client.get("/manifest").json()
    assert body["version"] == 1
    assert set(body["shards"]) == {"shard-a", "shard-b", "shard-c"}

    # 旧版本号重复提交（已被消耗的版本）同样冲突
    assert switch(client, 1, {"shard-a": ["n1", "n2", "n3"]}).status_code == 201
    r = switch(client, 1, {"shard-a": ["n1", "n2", "n3"]})
    assert r.status_code == 409
    assert r.json()["detail"]["current_version"] == 2


def test_switch_cannot_drop_node_occupied_by_open_permit(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-open", ["n1"]).status_code == 201
    r = switch(client, 1, {"shard-a": ["n2", "n3"], "shard-b": ["n2", "n3", "n4"]})
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "manifest_switch_rejected"
    assert detail["missing_occupied_nodes"] == [
        {"node": "n1", "permits": ["req-open"]}
    ]
    # 旧清单继续生效：n1 仍是已知节点，仍按 3 票清单判定
    body = client.get("/manifest").json()
    assert body["version"] == 1
    assert body["shards"]["shard-a"]["voters"] == ["n1", "n2", "n3"]
    assert report(client, "n1").status_code == 201


def test_switch_rejects_bad_payload_without_effect(client):
    seed_all_healthy(client)
    r = switch(client, 1, {})
    assert r.status_code == 422  # pydantic：shards 不能为空
    r = client.post(
        "/manifest/switch",
        json={"current_version": 1, "shards": {"shard-x": []}},
    )
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "invalid_manifest"
    assert client.get("/manifest").json()["version"] == 1

    # 版本号非正整数
    r = client.post(
        "/manifest/switch",
        json={"current_version": 0, "shards": {"shard-a": ["n1"]}},
    )
    assert r.status_code == 422


def test_identical_manifest_is_accepted_without_quorum_recheck(client):
    # 全部节点异常：若重新校验全部分片，任何切换都会被拒
    for n in ALL_NODES:
        report(client, n, healthy=False)
    # 提交与当前完全一致的清单（节点顺序也不同，规范化后视为无变动）仍可提交
    r = switch(client, 1, {
        "shard-c": ["n5", "n3", "n4", "n3"],
        "shard-b": ["n4", "n2", "n3"],
        "shard-a": ["n3", "n1", "n2"],
    })
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 2
    assert r.json()["shards"]["shard-a"]["voters"] == ["n1", "n2", "n3"]


def test_retry_same_version_after_rejection(client):
    seed_all_healthy(client)
    # 先让一次切换失败（新增分片票数不足）
    assert request_permit(client, "req-open", ["n1"]).status_code == 201
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2"]})
    # shard-d 3 票? 否，2 票需 2；n1 占用 -> 可用 1 < 2，拒绝
    assert r.status_code == 409, r.text
    # 失败不消耗版本：同一版本号整改后可重试
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n2", "n5"]})
    assert r.status_code == 201, r.text
    assert r.json()["version"] == 2


def test_persist_failure_keeps_old_manifest(client, monkeypatch):
    import sqlite3

    from app.store import Store

    seed_all_healthy(client)

    def boom(self, manifest, applied_at):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(Store, "insert_manifest", boom)
    r = switch(client, 1, {"shard-a": ["n1", "n2", "n3", "n4"]})
    assert r.status_code == 500
    assert r.json()["detail"]["error"] == "manifest_persist_failed"
    body = client.get("/manifest").json()
    assert body["version"] == 1
    assert body["shards"]["shard-a"]["voters"] == ["n1", "n2", "n3"]
    # 旧清单下照常受理
    assert request_permit(client, "req-1", ["n1"]).status_code == 201


# ---------- 与探活、许可、结束的关系 ----------


def test_liveness_and_permits_follow_effective_manifest(client):
    seed_all_healthy(client)
    # n6 尚未在任何清单中，探活被拒
    assert report(client, "n6").status_code == 422
    # 通过新增分片把 n6 引入清单（已有的 n1/n2/n5 提供 3 张已知健康票中的 2 张）
    r = switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n6"]})
    assert r.status_code == 201, r.text
    # 新清单生效后，n6 的探活被接受
    assert report(client, "n6").status_code == 201
    assert request_permit(client, "req-6", ["n6"]).status_code == 201
    # 不在新清单中的 n9 仍被拒
    assert report(client, "n9").status_code == 422


def test_idempotent_replay_after_node_removed_from_manifest(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    # 结束许可
    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later)
    assert client.post("/permits/req-1/finish").status_code == 200
    # 切换清单，去掉 n1（无未结束许可占用，且改动分片仍过半）
    r = switch(client, 1, {"shard-a": ["n2", "n3"], "shard-b": ["n2", "n3", "n4"]})
    assert r.status_code == 201, r.text

    # 原许可同号同批重发仍返回原记录
    replay = request_permit(client, "req-1", ["n1"])
    assert replay.status_code == 200, replay.text
    assert replay.json()["request_id"] == "req-1"
    assert replay.json()["status"] == "finished"
    # 但 n1 已不是清单内节点，新申请被拒
    r = request_permit(client, "req-new", ["n1"])
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["n1"]


def test_state_and_committed_manifest_survive_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1)
    r = switch(c1, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n5"]})
    assert r.status_code == 201, r.text
    assert request_permit(c1, "req-1", ["n1"]).status_code == 201
    c1.close()

    c2 = make_client()
    m = c2.get("/manifest").json()
    assert m["version"] == 2 and "shard-d" in m["shards"]
    assert c2.get("/permits/req-1").json()["status"] == "open"
    assert c2.get("/liveness/n1").json()["healthy"] is True


def test_mounted_manifest_only_used_for_initialization(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = str(tmp_path / "permits.db")

    c1 = TestClient(create_app(db_path=db_path, manifest_path=str(manifest_path)))
    try:
        seed_all_healthy(c1)
        r = switch(c1, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2"]})
        assert r.status_code == 201, r.text
    finally:
        c1.close()

    # 重启前改写挂载文件：已提交清单优先，挂载文件不覆盖
    manifest_path.write_text(json.dumps({"shards": {"shard-z": ["z1"]}}))
    c2 = TestClient(create_app(db_path=db_path, manifest_path=str(manifest_path)))
    try:
        body = c2.get("/manifest").json()
        assert body["version"] == 2
        assert "shard-d" in body["shards"]
        assert "shard-z" not in body["shards"]
    finally:
        c2.close()


# ---------- 并发 ----------


def test_concurrent_switches_same_version_only_one_wins(client):
    seed_all_healthy(client)

    def submit(i):
        return switch(client, 1, {**MANIFEST["shards"], f"shard-x{i}": ["n1", "n2"]})

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(submit, range(6)))
    codes = [r.status_code for r in results]
    assert codes.count(201) == 1, codes
    assert codes.count(409) == 5
    body = client.get("/manifest").json()
    assert body["version"] == 2
    assert len([n for n in body["shards"] if n.startswith("shard-x")]) == 1


def test_concurrent_switch_and_liveness_are_consistent(client):
    seed_all_healthy(client)
    codes = []

    def do_liveness(i):
        r = report(client, "n6", healthy=True, boot=f"b{i}",
                   sampled_at=T0 + timedelta(seconds=i))
        codes.append(r.status_code)
        return r.status_code

    def do_switch():
        return switch(client, 1, {**MANIFEST["shards"], "shard-d": ["n1", "n2", "n6"]})

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(do_liveness, i) for i in range(10)]
        futures.append(pool.submit(do_switch))
        [f.result() for f in futures]

    # n6 的探活只可能“切换前被拒（422）”或“切换后接受（201）”，不会 5xx
    assert set(codes) <= {201, 422}, codes
    final = client.get("/manifest").json()
    assert "shard-d" in final["shards"]
    # 若有探活被接受，则生效清单中一定能查到该节点
    if 201 in codes:
        assert "n6" in client.get("/liveness").json()["nodes"]


def test_concurrent_switch_and_permits_use_consistent_state(client):
    seed_all_healthy(client)
    accepted = []

    # 先切一次，让 n4 进入 shard-a 的 4 票形态（n1..n4 均健康，>=3 可用）
    assert switch(
        client, 1, {**MANIFEST["shards"], "shard-a": ["n1", "n2", "n3", "n4"]}
    ).status_code == 201

    def do_permit(i):
        # 交替申请 n1、n2 的一部分：无论按旧（4 票）还是新（3 票）清单，
        # 判定都必须基于完整一致的状态，不能出现“半新半旧”的放行。
        r = request_permit(client, f"p-{i}", [["n1"], ["n2"], ["n4"]][i % 3])
        accepted.append((r.status_code, r.json()))
        return r.status_code

    def do_switch():
        return switch(
            client, 2, {**MANIFEST["shards"], "shard-a": ["n1", "n2", "n3"]}
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(do_permit, i) for i in range(6)]
        futures.append(pool.submit(do_switch))
        [f.result() for f in futures]

    # 每个申请要么批准、要么按完整状态拒绝，不会 5xx/异常
    assert all(code in (200, 201, 409) for code, _ in accepted), accepted
    final = client.get("/manifest").json()
    assert final["version"] == 3
    opens = client.get("/permits", params={"status": "open"}).json()["permits"]
    final_nodes = {
        n for shard in final["shards"].values() for n in shard["voters"]
    }
    for p in opens:
        # 所有未结束许可的节点仍在最终清单中
        assert set(p["nodes"]) <= final_nodes
