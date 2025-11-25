import uuid
from datetime import UTC, datetime, timezone
from typing import Any, Dict, List, Optional, Type, Union

from aqworker.constants import get_job_status_key
from aqworker.handler import BaseHandler
from aqworker.job.models import Job, JobCreateRequest, JobStatus
from aqworker.job.queue import JobQueue
from aqworker.logger import logger


class JobService:
    """Background job service for managing jobs."""

    def __init__(
        self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0
    ):
        """Initialize job service using environment configuration or overrides."""
        self.redis_db = redis_db
        self.redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

        # Single queue instance for all operations
        self._queue: Optional[JobQueue] = None

    def _get_queue(self) -> JobQueue:
        """
        Get cached queue instance or create new one.

        Returns:
            JobQueue instance
        """
        if self._queue is None:
            self._queue = JobQueue(redis_url=self.redis_url)
        return self._queue

    async def _create_job(self, request: JobCreateRequest) -> Job:
        """
        Create a new background job.

        Args:
            request: Job creation request

        Returns:
            Created job
        """
        job_id = str(uuid.uuid4())

        job = Job(
            id=job_id,
            queue_name=request.queue_name,
            handler=request.handler,
            data=request.data,
            metadata=request.metadata,
            scheduled_at=request.scheduled_at,
            max_retries=request.max_retries,
            retry_delay=request.retry_delay,
            created_at=datetime.now(timezone.utc),
        )

        # Enqueue the job to specific queue
        queue = self._get_queue()
        if await queue.enqueue(job):
            logger.info(f"Created job {job_id} in queue {job.queue_name}")
            return job
        else:
            raise Exception(f"Failed to create job {job_id}")

    async def enqueue_job(
        self,
        queue_name: str,
        handler: Union[str, Type[BaseHandler]],
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        scheduled_at: Optional[datetime] = None,
        max_retries: int = 3,
        retry_delay: float = 60,
    ) -> Job:
        """
        Enqueue a job to be processed by a handler.

        Args:
            queue_name: Queue name to enqueue the job
            handler: Handler name (string) or handler class (Type[BaseHandler])
            data: Job data dictionary to be processed by handler
            metadata: Optional job metadata
            scheduled_at: Optional scheduled execution time
            max_retries: Maximum retry attempts
            retry_delay: Retry delay in seconds

        Returns:
            Created job
        """
        if isinstance(handler, str):
            handler_str = handler
        elif isinstance(handler, type) and issubclass(handler, BaseHandler):
            # Use handler's get_name method if available, otherwise class name
            handler_name = getattr(handler, "get_name", None)
            if callable(handler_name):
                handler_str = handler.get_name()
            else:
                handler_str = handler.__name__
        else:
            raise ValueError(
                f"handler must be a string or BaseHandler class, got {type(handler)}"
            )

        request = JobCreateRequest(
            queue_name=queue_name,
            handler=handler_str,
            data=data or {},
            metadata=metadata or {},
            scheduled_at=scheduled_at,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
        return await self._create_job(request)

    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get job by ID.

        Args:
            job_id: Job identifier

        Returns:
            Job or None if not found
        """
        try:
            queue = self._get_queue()  # Service queue for general operations
            return await queue.get_job_status(job_id)
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return None

    async def get_queue_stats(
        self, queue_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get queue statistics.

        Args:
            queue_names: List of queue names to include in stats.

        Returns:
            Queue statistics
        """
        queue = self._get_queue()  # Service queue for general operations
        return await queue.get_queue_stats(queue_names)

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a pending job.

        Args:
            job_id: Job identifier

        Returns:
            True if successful, False otherwise
        """
        job = await self.get_job(job_id)
        if not job:
            return False

        if job.status != JobStatus.PENDING:
            logger.warning(f"Cannot cancel job {job_id} with status {job.status}")
            return False

        # Update job status
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(timezone.utc)

        # Update in Redis
        queue = self._get_queue()  # Service queue for general operations
        await queue.redis_client.hset(
            get_job_status_key(job_id),
            mapping={
                "status": job.status.value,
                "completed_at": job.completed_at.isoformat(),
            },
        )

        logger.info(f"Cancelled job {job_id}")
        return True

    async def retry_job(self, job_id: str) -> bool:
        """
        Retry a failed job.

        Args:
            job_id: Job identifier

        Returns:
            True if successful, False otherwise
        """
        job = await self.get_job(job_id)
        if not job:
            return False

        if job.status != JobStatus.FAILED:
            logger.warning(f"Cannot retry job {job_id} with status {job.status}")
            return False

        if job.retry_count >= job.max_retries:
            logger.warning(f"Job {job_id} has exceeded max retries")
            return False

        # Reset job for retry
        job.status = JobStatus.PENDING
        job.retry_count += 1
        job.error_message = None
        job.error_traceback = None
        job.started_at = None
        job.completed_at = None

        # Re-enqueue to same queue
        queue = self._get_queue()
        if await queue.enqueue(job):
            logger.info(f"Retrying job {job_id} (attempt {job.retry_count})")
            return True
        else:
            return False

    async def cleanup_old_jobs(self, days: int = 7) -> int:
        """
        Clean up old completed/failed jobs.

        Args:
            days: Number of days to keep jobs

        Returns:
            Number of jobs cleaned up
        """
        try:
            cutoff_date = datetime.now(UTC).timestamp() - (days * 24 * 60 * 60)
            cleaned_count = 0

            # Clean up completed jobs
            queue = self._get_queue()  # Service queue for general operations
            completed_jobs = await queue.redis_client.lrange(
                queue.completed_queue, 0, -1
            )

            for job_data in completed_jobs:
                job = Job.model_validate_json(job_data)
                if job.completed_at and job.completed_at.timestamp() < cutoff_date:
                    await queue.redis_client.lrem(queue.completed_queue, 1, job_data)
                    await queue.redis_client.delete(get_job_status_key(job.id))
                    cleaned_count += 1

            # Clean up failed jobs
            failed_jobs = await queue.redis_client.lrange(queue.failed_queue, 0, -1)

            for job_data in failed_jobs:
                job = Job.model_validate_json(job_data)
                if job.completed_at and job.completed_at.timestamp() < cutoff_date:
                    await queue.redis_client.lrem(queue.failed_queue, 1, job_data)
                    await queue.redis_client.delete(get_job_status_key(job.id))
                    cleaned_count += 1

            logger.info(f"Cleaned up {cleaned_count} old jobs")
            return cleaned_count

        except Exception as e:
            logger.error(f"Failed to cleanup old jobs: {e}")
            return 0

    async def get_next_job(
        self, queue_names: List[str], timeout: int = 0, worker_id: Optional[str] = None
    ) -> Optional[Job]:
        """
        Get next job from specified queues.

        Args:
            queue_names: List of queue names to check
            timeout: Blocking timeout in seconds (0 = no blocking)
            worker_id: Worker identifier for job locking

        Returns:
            Job or None if no jobs available
        """
        try:
            # Use a generic queue for dequeue operations
            queue = self._get_queue()
            return await queue.dequeue(
                queue_names=queue_names, timeout=timeout, worker_id=worker_id
            )

        except Exception as e:
            logger.error(f"Failed to get next job: {e}")
            return None

    async def complete_job(
        self, job: Job, success: bool, error_message: Optional[str] = None
    ) -> bool:
        """
        Mark a job as completed or failed.

        Args:
            job: Job to complete
            success: Whether job completed successfully
            error_message: Error message if failed

        Returns:
            True if successful, False otherwise
        """
        try:
            # Use a generic queue for complete operations
            queue = self._get_queue()
            return await queue.complete_job(
                job=job, success=success, error_message=error_message
            )

        except Exception as e:
            logger.error(f"Failed to complete job {job.id}: {e}")
            return False

    async def cleanup_stale_processing_jobs(self, max_age_seconds: int = 300) -> int:
        """
        Clean up stale jobs from processing queue.

        Args:
            max_age_seconds: Maximum age for processing jobs (default 5 minutes)

        Returns:
            Number of stale jobs removed
        """
        try:
            queue = self._get_queue()
            return await queue.cleanup_stale_processing_jobs(max_age_seconds)
        except Exception as e:
            logger.error(f"Failed to cleanup stale processing jobs: {e}")
            return 0

    async def clear_processing_queue(self) -> bool:
        """
        Clear all jobs from processing queue.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            queue = self._get_queue()
            return await queue.clear_processing_queue()
        except Exception as e:
            logger.error(f"Failed to clear processing queue: {e}")
            return False

    async def release_job_lock(self, job_id: str, worker_id: str) -> bool:
        """
        Release a job lock.

        Args:
            job_id: Job identifier
            worker_id: Worker identifier that holds the lock

        Returns:
            True if successful, False otherwise
        """
        try:
            queue = self._get_queue()
            await queue.release_job_lock(job_id=job_id, worker_id=worker_id)
            return True
        except Exception as e:
            logger.error(f"Failed to release job lock for {job_id}: {e}")
            return False
