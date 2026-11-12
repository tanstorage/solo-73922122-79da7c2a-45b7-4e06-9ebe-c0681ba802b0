"""API 请求/响应模型（Pydantic v2）。"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------- 保全


class PreservationCreate(BaseModel):
    """按车号 + 时间闭区间提交保全；覆盖所有与该区间重叠的分区。"""

    vehicle_id: str = Field(examples=["T203"], description="车号")
    start: datetime = Field(examples=["2026-10-03T00:30:00Z"], description="区间起点（含）")
    end: datetime = Field(examples=["2026-10-03T00:35:00Z"], description="区间终点（含）")
    investigator: str = Field(default="unknown", examples=["inspector-01"], description="调查人员")
    reason: str = Field(default="", examples=["转向架故障调查：保全冲击段波形"])

    @model_validator(mode="after")
    def _check_range(self) -> "PreservationCreate":
        if self.end < self.start:
            raise ValueError("end 必须不早于 start（闭区间）")
        return self


class PreservationOut(BaseModel):
    preservation_id: str
    vehicle_id: str
    start: str
    end: str
    investigator: str
    reason: str
    status: Literal["active", "released"]
    created_at: str
    released_at: str | None
    partitions: list[str] = Field(description="本保全覆盖的分区")


class PreservationCreateResult(BaseModel):
    preservation: PreservationOut
    message: str


# ---------------------------------------------------------------- 分区 / 波形

PartitionStatus = Literal["active", "deleting", "delete_failed", "cleaned"]


class PartitionOut(BaseModel):
    partition_id: str
    vehicle_id: str
    start: str
    end: str
    expires_at: str
    expired: bool
    status: PartitionStatus
    file_exists: bool = Field(description="JSONL 文件当前是否在盘上（索引与文件一致性观察用）")
    active_preservations: int = Field(description="覆盖该分区的生效中保全数")
    delete_attempts: int
    delete_owner: str | None = None
    delete_lease_expires_at: str | None = None
    last_error: str | None = None
    cleaned_at: str | None = None


class Sample(BaseModel):
    ts: str
    vehicle_id: str
    ax: float
    ay: float
    az: float


class SamplesResponse(BaseModel):
    vehicle_id: str
    start: str
    end: str
    partitions: list[str] = Field(description="实际参与读取的尚存分区")
    matched: int = Field(description="区间内命中样本总数")
    returned: int = Field(description="本次返回条数")
    samples: list[Sample]
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------- 清理


class DeleteOutcome(BaseModel):
    partition_id: str
    result: Literal["cleaned", "failed"]
    detail: str


class CleanupRunResult(BaseModel):
    process: str
    reason: str
    started_at: str
    finished_at: str
    outcomes: list[DeleteOutcome]
    message: str


class InFlightDelete(BaseModel):
    partition_id: str
    owner: str | None
    lease_expires_at: str | None
    attempts: int


class DeleteFailure(BaseModel):
    partition_id: str
    attempts: int
    last_error: str | None


class CleanupStatus(BaseModel):
    process: str = Field(description="回答本次请求的进程")
    now: str
    counts: dict[str, int] = Field(description="按分区状态统计")
    pending_expired_unpreserved: int = Field(description="已到期且无保全、等待清理的分区数")
    in_flight: list[InFlightDelete] = Field(description="删除资格已发出、删除进行中的分区")
    failures: list[DeleteFailure] = Field(description="删除失败（绝未标记为已回收）的分区")
    active_preservations: int
    background_cleanup_enabled: bool
    cleanup_interval_seconds: float
    last_run: CleanupRunResult | None = Field(
        default=None, description="本进程最近一次清理 pass（另一进程可能另有其记录）"
    )
