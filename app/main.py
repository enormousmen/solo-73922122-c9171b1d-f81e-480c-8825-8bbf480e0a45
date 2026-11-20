"""FastAPI 应用：分批重启许可 API（支持不停机切换分片清单）。"""
from __future__ import annotations

import os
import sqlite3

from fastapi import FastAPI, HTTPException, Query, Response

from .logic import (
    evaluate_finish,
    evaluate_manifest_switch,
    evaluate_permit_request,
)
from .manifest import (
    Manifest,
    ManifestError,
    load_manifest,
    normalize_shards,
)
from .models import PermitRecord, now_utc, to_iso
from .schemas import LivenessBatchIn, ManifestSwitchIn, PermitRequestIn
from .store import Store


def create_app(db_path: str, manifest_path: str) -> FastAPI:
    store = Store(db_path)
    # 挂载的清单文件只用于首次初始化（数据库里还没有已提交清单时）。
    # 之后一律沿用数据库里最近一次已提交的清单，重启不丢失。
    with store.transaction():
        committed = store.latest_manifest()
        if committed is None:
            committed = load_manifest(manifest_path)
            store.insert_manifest(committed, now_utc())

    app = FastAPI(title="restart-permit-api", version="1.1.0")
    app.state.store = store
    app.state.manifest = committed

    def current_manifest() -> Manifest:
        return app.state.manifest

    def manifest_view(m: Manifest) -> dict:
        return {
            "version": m.version,
            "shards": {
                name: {
                    "voters": list(voters),
                    "majority_needed": m.majority_needed(name),
                }
                for name, voters in sorted(m.shards.items())
            },
        }

    def permit_view(p: PermitRecord) -> dict:
        return {
            "request_id": p.request_id,
            "nodes": list(p.nodes),
            "status": p.status,
            "approved_at": to_iso(p.approved_at),
            "finished_at": to_iso(p.finished_at) if p.finished_at else None,
            "boot_ids_at_approval": p.boot_ids_at_approval,
        }

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/manifest")
    def get_manifest():
        return manifest_view(current_manifest())

    @app.post("/manifest/switch", status_code=201)
    def switch_manifest(body: ManifestSwitchIn):
        """不停机切换清单：携带当前版本号 + 完整新清单。

        校验与落库在同一写事务中完成；任何失败（版本不符、条件不满足、
        保存失败）都回滚，内存与数据库继续使用旧清单。成功后版本号 +1。
        """
        try:
            new_shards = normalize_shards(body.shards)
        except ManifestError as e:
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_manifest", "message": str(e)},
            ) from e

        try:
            with store.transaction() as tx:
                current = current_manifest()
                if body.current_version != current.version:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error": "manifest_version_conflict",
                            "current_version": current.version,
                            "submitted_version": body.current_version,
                        },
                    )
                new_manifest = Manifest(shards=new_shards, version=current.version + 1)
                ok, detail = evaluate_manifest_switch(
                    current, new_manifest, store.latest_reports(), store.open_permits()
                )
                if not ok:
                    raise HTTPException(status_code=409, detail=detail)

                store.insert_manifest(new_manifest, now_utc())
                # 只有事务提交成功后才切换内存中的生效清单；提交失败则不生效。
                tx.after_commit(
                    lambda: setattr(app.state, "manifest", new_manifest)
                )
                result = manifest_view(new_manifest)
        except sqlite3.Error as e:  # 写入或提交失败：事务已回滚，旧清单继续生效
            raise HTTPException(
                status_code=500,
                detail={"error": "manifest_persist_failed", "message": str(e)},
            ) from e
        return result

    # ---- 探活报告 ----

    @app.post("/liveness", status_code=201)
    def post_liveness(batch: LivenessBatchIn):
        # 与清单切换共用应用锁：受理判定与写入要么整体在旧清单下，
        # 要么整体在新清单下，不会出现“按旧清单放行、按新清单落库”。
        with store.guard():
            manifest = current_manifest()
            unknown = sorted({r.node_id for r in batch.reports} - manifest.nodes)
            if unknown:
                raise HTTPException(
                    status_code=422,
                    detail={"error": "unknown_nodes", "unknown_nodes": unknown},
                )
            store.add_reports(batch.reports)
        return {"accepted": len(batch.reports)}

    @app.get("/liveness")
    def get_all_liveness():
        manifest = current_manifest()
        latest = store.latest_reports()
        return {
            "nodes": {
                n: {
                    "sampled_at": to_iso(r.sampled_at),
                    "healthy": r.healthy,
                    "boot_id": r.boot_id,
                }
                for n, r in sorted(latest.items())
            },
            "nodes_without_reports": sorted(manifest.nodes - latest.keys()),
        }

    @app.get("/liveness/{node_id}")
    def get_node_liveness(node_id: str):
        rep = store.latest_reports().get(node_id)
        if rep is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_liveness_report", "node": node_id},
            )
        return {
            "node_id": node_id,
            "sampled_at": to_iso(rep.sampled_at),
            "healthy": rep.healthy,
            "boot_id": rep.boot_id,
        }

    # ---- 许可 ----

    @app.post("/permits", status_code=201)
    def request_permit(req: PermitRequestIn, response: Response):
        nodes = sorted(set(req.nodes))  # 规范化：去重排序
        with store.transaction():
            # 幂等重发优先于新申请校验：即使清单已变、节点已不在清单中，
            # 同号同批仍返回原许可记录。
            existing = store.get_permit(req.request_id)
            if existing is not None:
                if list(existing.nodes) == nodes:
                    response.status_code = 200
                    return permit_view(existing)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "request_id_conflict",
                        "message": "request_id 已存在且节点集合不同",
                        "existing_nodes": list(existing.nodes),
                    },
                )
            # 新申请按事务开始时实际生效的清单受理。
            manifest = current_manifest()
            unknown = [n for n in nodes if n not in manifest.nodes]
            if unknown:
                raise HTTPException(
                    status_code=422,
                    detail={"error": "unknown_nodes", "unknown_nodes": unknown},
                )
            latest = store.latest_reports()
            ok, detail = evaluate_permit_request(
                manifest, nodes, latest, store.open_permits()
            )
            if not ok:
                raise HTTPException(status_code=409, detail=detail)
            record = PermitRecord(
                request_id=req.request_id,
                nodes=tuple(nodes),
                status="open",
                approved_at=now_utc(),
                finished_at=None,
                boot_ids_at_approval={n: latest[n].boot_id for n in nodes},
            )
            store.insert_permit(record)
            return permit_view(record)

    @app.get("/permits")
    def list_permits(
        status: str | None = Query(default=None, pattern="^(open|finished)$"),
    ):
        return {"permits": [permit_view(p) for p in store.list_permits(status)]}

    @app.get("/permits/{request_id}")
    def get_permit(request_id: str):
        p = store.get_permit(request_id)
        if p is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "permit_not_found", "request_id": request_id},
            )
        return permit_view(p)

    @app.post("/permits/{request_id}/finish")
    def finish_permit(request_id: str):
        with store.transaction():
            p = store.get_permit(request_id)
            if p is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if p.status == "finished":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "permit_already_finished",
                        "request_id": request_id,
                    },
                )
            problems = evaluate_finish(p, store.latest_reports())
            if problems:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "finish_conditions_not_met", "nodes": problems},
                )
            store.mark_finished(request_id, now_utc())
            return permit_view(store.get_permit(request_id))

    return app


def create_app_from_env() -> FastAPI:
    return create_app(
        db_path=os.environ.get("DB_PATH", "./data/permits.db"),
        manifest_path=os.environ.get("MANIFEST_PATH", "./manifest.json"),
    )


app = create_app_from_env()
