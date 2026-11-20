"""请求体的 Pydantic 模型。"""
from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class LivenessReportIn(BaseModel):
    node_id: str = Field(min_length=1, max_length=128)
    sampled_at: datetime = Field(description="采样时间，ISO8601")
    healthy: bool
    boot_id: str = Field(min_length=1, max_length=128, description="启动标识")

    @field_validator("sampled_at")
    @classmethod
    def _normalize_tz(cls, v: datetime) -> datetime:
        # 不带时区按 UTC 处理，统一归一到 UTC
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class LivenessBatchIn(BaseModel):
    reports: list[LivenessReportIn] = Field(min_length=1, max_length=5000)


class PermitRequestIn(BaseModel):
    request_id: str = Field(min_length=1, max_length=128, description="申请号")
    nodes: list[str] = Field(min_length=1, max_length=5000, description="拟重启节点")


class ManifestSwitchIn(BaseModel):
    """切换清单：携带当前版本号与完整的新清单。"""

    current_version: int = Field(ge=1, description="调用方看到的当前清单版本号")
    shards: dict[str, list[str]] = Field(
        description="完整的新清单：分片名 -> 投票副本节点列表"
    )

    @field_validator("shards")
    @classmethod
    def _reject_empty(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        if not v:
            raise ValueError("shards 不能为空")
        return v
