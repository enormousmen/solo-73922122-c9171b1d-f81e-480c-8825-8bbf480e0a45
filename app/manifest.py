"""分片投票副本清单（manifest）的解析、校验与不可变模型。"""
from __future__ import annotations

import json
from dataclasses import dataclass

MANIFEST_VERSION_KEY = "version"


class ManifestError(ValueError):
    """清单结构不合法。"""


@dataclass(frozen=True)
class Manifest:
    # 分片名 -> 投票节点列表（有序、去重）
    shards: dict[str, tuple[str, ...]]
    # 清单版本号；每次成功切换递增。挂载文件未写 version 时从 1 开始。
    version: int = 1

    @property
    def nodes(self) -> frozenset[str]:
        """清单中出现过的全部节点。"""
        out: set[str] = set()
        for voters in self.shards.values():
            out.update(voters)
        return frozenset(out)

    def majority_needed(self, shard: str) -> int:
        """该分片过半所需的最少可用投票副本数。"""
        return len(self.shards[shard]) // 2 + 1


def normalize_shards(raw: object) -> dict[str, tuple[str, ...]]:
    """校验并规范化 ``{"shard": ["n1", ...]}`` 结构。

    分片名和节点名必须是非空字符串；节点去重后排序，使成员顺序不构成变更。
    返回新 dict，不与输入共享可变列表。
    """
    if not isinstance(raw, dict) or not raw:
        raise ManifestError("manifest 必须包含非空的 'shards' 对象")
    shards: dict[str, tuple[str, ...]] = {}
    for name, voters in raw.items():
        if not isinstance(name, str) or not name.strip():
            raise ManifestError("分片名必须是非空字符串")
        if not isinstance(voters, list) or not voters:
            raise ManifestError(f"分片 {name!r} 的投票副本列表必须是非空数组")
        norm: list[str] = []
        for voter in voters:
            if not isinstance(voter, str) or not voter.strip():
                raise ManifestError(f"分片 {name!r} 的投票副本必须是非空字符串")
            norm.append(voter)
        shards[name] = tuple(sorted(set(norm)))
    return shards


def parse_manifest(raw: object) -> Manifest:
    """解析完整清单对象（含可选的 version）。"""
    if not isinstance(raw, dict):
        raise ManifestError("manifest 必须是 JSON 对象")
    version = raw.get(MANIFEST_VERSION_KEY, 1)
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ManifestError("manifest 的 'version' 必须是正整数")
    return Manifest(shards=normalize_shards(raw.get("shards")), version=version)


def load_manifest(path: str) -> Manifest:
    """读取挂载的初始清单文件。只在数据库尚无已提交清单时调用。"""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return parse_manifest(raw)
