from datetime import datetime, timezone
from enum import Enum
from functools import partial
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Job status enumeration."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobModel(BaseModel):
    """Background job model."""

    id: str = Field(default=..., description="Unique job identifier")
    status: JobStatus = Field(default=JobStatus.PENDING, description="Job status")
    queue_name: str = Field(default="default", description="Queue name for this job")

    # Job data
    handler: str = Field(
        default=..., description="Registered handlers name to process this job"
    )
    data: Dict[str, Any] = Field(
        default_factory=dict, description="Job data to be processed by handler"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Job metadata")

    # Timing
    created_at: datetime = Field(
        default_factory=partial(datetime.now, tz=timezone.utc),
        description="Job creation time",
    )
    updated_at: Optional[datetime] = Field(
        default=None, description="Job last update time"
    )
    scheduled_at: Optional[datetime] = Field(
        default=None, description="Scheduled execution time"
    )
    schedule_time: Optional[datetime] = Field(
        default=None,
        description="Cron schedule timestamp that triggered this job (if applicable)",
    )
    started_at: Optional[datetime] = Field(default=None, description="Job start time")
    completed_at: Optional[datetime] = Field(
        default=None, description="Job completion time"
    )

    # Retry configuration
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_count: int = Field(default=0, description="Current retry count")
    retry_delay: float = Field(default=60, description="Retry delay in seconds")

    # Error handling
    error_message: Optional[str] = Field(
        default=None, description="Error message if failed"
    )
    error_traceback: Optional[str] = Field(
        default=None, description="Error traceback if failed"
    )

    # Worker info
    worker_id: Optional[str] = Field(
        default=None, description="Worker that processed this job"
    )
    processing_time: Optional[float] = Field(
        default=None, description="Processing time in seconds"
    )


class JobCreateRequest(BaseModel):
    """Request model for creating a new job."""

    queue_name: str = "default"
    handler: str
    data: Dict[str, Any] = Field(
        default_factory=dict, description="Job data to be processed by handler"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)
    scheduled_at: Optional[datetime] = None
    schedule_time: Optional[datetime] = None
    max_retries: int = 3
    retry_delay: float = 60


class JobUpdateRequest(BaseModel):
    """Request model for updating a job."""

    status: Optional[JobStatus] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class JobResponse(BaseModel):
    """Response model for job operations."""

    success: bool
    message: str
    data: Optional[JobModel] = None
