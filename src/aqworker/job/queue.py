from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as redis

from aqworker.constants import (
    COMPLETED_QUEUE,
    FAILED_QUEUE,
    JOB_QUEUE_PREFIX,
    PROCESSING_QUEUE,
    get_job_lock_key,
    get_job_status_key,
    get_queue_name,
)
from aqworker.job.models import JobModel, JobStatus, JobStatusInfo
from aqworker.logger import logger


class JobQueue:
    """Redis-based job queue implementation."""

    def __init__(self, redis_url: str):
        """
        Initialize job queue with Redis connection.

        Args:
            redis_url: Redis connection URL. Example: redis://localhost:6379/0
        """
        # Store URL for lazy connection
        self.redis_url = redis_url
        self._redis_client = redis.from_url(self.redis_url, decode_responses=True)
        self.queue_prefix = JOB_QUEUE_PREFIX

        self.processing_queue = PROCESSING_QUEUE
        self.completed_queue = COMPLETED_QUEUE
        self.failed_queue = FAILED_QUEUE

    @property
    def redis_client(self):
        return self._redis_client

    @redis_client.setter
    def redis_client(self, client):
        """Allow tests to inject a fake redis client."""
        self._redis_client = client

    @staticmethod
    def _get_queue_name(queue_name: str) -> str:
        """Get queue name for specific queue."""
        return get_queue_name(queue_name)

    async def enqueue(self, job: JobModel) -> bool:
        """
        Add job to queue.

        Args:
            job: Job to enqueue

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            queue_name = self._get_queue_name(job.queue_name)
            job_data = job.model_dump_json()

            # Add to queue (FIFO)
            await self.redis_client.lpush(queue_name, job_data)

            # Set job status
            await self.redis_client.hset(
                get_job_status_key(job.id),
                mapping={
                    "status": job.status.value,
                    "created_at": job.created_at.isoformat(),
                    "data": job_data,
                },
            )

            logger.info(f"Enqueued job {job.id} to queue {job.queue_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to enqueue job {job.id}: {e}")
            return False

    async def dequeue(
        self, queue_names: List[str], timeout: int = 0, worker_id: Optional[str] = None
    ) -> Optional[JobModel]:
        """
        Get next job from queue (FIFO) using atomic operations to prevent race conditions.

        Args:
            queue_names: List of queue names to check for jobs
            timeout: Blocking timeout in seconds (0 = no blocking)
            worker_id: Worker identifier for job locking

        Returns:
            Job or None if no jobs available
        """
        try:
            # Check queues in order for each queue name (FIFO)
            for queue_name in queue_names:
                queue_key = self._get_queue_name(queue_name)

                if timeout > 0:
                    # Use BRPOPLPUSH for atomic move from queue to processing
                    job_data = await self.redis_client.brpoplpush(
                        queue_key, self.processing_queue, timeout=timeout
                    )
                    if job_data is None:
                        continue
                else:
                    # Use RPOPLPUSH for atomic move (non-blocking)
                    job_data = await self.redis_client.rpoplpush(
                        queue_key, self.processing_queue
                    )
                    if job_data is None:
                        continue

                if job_data:
                    job = JobModel.model_validate_json(job_data)

                    # Try to acquire job lock to prevent duplicate processing
                    if worker_id and not await self._acquire_job_lock(
                        job.id, worker_id
                    ):
                        # If we can't acquire lock, put job back and continue
                        await self.redis_client.lpush(queue_key, job_data)
                        await self.redis_client.lrem(self.processing_queue, 1, job_data)
                        logger.warning(
                            f"Could not acquire lock for job {job.id}, skipping"
                        )
                        continue

                    # Update job status atomically
                    job.status = JobStatus.PROCESSING
                    job.started_at = datetime.now(timezone.utc)

                    # Update job status in Redis
                    await self.redis_client.hset(
                        get_job_status_key(job.id),
                        mapping={
                            "status": job.status.value,
                            "started_at": job.started_at.isoformat(),
                            "worker_id": worker_id or "unknown",
                            "data": job.model_dump_json(),  # Update data field to keep it in sync
                        },
                    )

                    # Update job in processing queue with new status
                    try:
                        updated_job_data = job.model_dump_json()
                        # Remove old job data and add updated job data
                        lrem_result = await self.redis_client.lrem(
                            self.processing_queue, 1, job_data
                        )
                        logger.debug(f"LREM result for job {job.id}: {lrem_result}")
                        if lrem_result > 0:
                            await self.redis_client.lpush(
                                self.processing_queue, updated_job_data
                            )
                            logger.debug(
                                f"Updated job {job.id} in processing queue with status {job.status}"
                            )
                        else:
                            # If old job data not found, just add the updated version
                            await self.redis_client.lpush(
                                self.processing_queue, updated_job_data
                            )
                            logger.debug(
                                f"Added updated job {job.id} to processing queue (old version not found)"
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to update job {job.id} in processing queue: {e}"
                        )

                    logger.info(
                        f"Atomically dequeued job {job.id} from queue {queue_name} (aq_worker: {worker_id})"
                    )
                    return job

            return None

        except Exception as e:
            logger.error(f"Failed to dequeue job: {e}")
            return None

    async def _acquire_job_lock(
        self, job_id: str, worker_id: str, lock_timeout: int = 300
    ) -> bool:
        """
        Acquire a lock for a job to prevent duplicate processing.

        Args:
            job_id: Job identifier
            worker_id: Worker identifier
            lock_timeout: Lock timeout in seconds (default 5 minutes)

        Returns:
            True if lock acquired, False otherwise
        """
        try:
            lock_key = get_job_lock_key(job_id)
            # Use SET with NX and EX for atomic lock acquisition
            result = await self.redis_client.set(
                lock_key,
                worker_id,
                nx=True,  # Only set if key doesn't exist
                ex=lock_timeout,  # Expire after timeout
            )
            return result is not None
        except Exception as e:
            logger.error(f"Failed to acquire job lock for {job_id}: {e}")
            return False

    async def release_job_lock(self, job_id: str, worker_id: str) -> bool:
        """
        Release a job lock.

        Args:
            job_id: Job identifier
            worker_id: Worker identifier

        Returns:
            True if lock released, False otherwise
        """
        try:
            lock_key = get_job_lock_key(job_id)
            # Use Lua script to ensure only the lock owner can release it
            lua_script = """
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
            """
            result = await self.redis_client.eval(lua_script, 1, lock_key, worker_id)
            return bool(result == 1)
        except Exception as e:
            logger.error(f"Failed to release job lock for {job_id}: {e}")
            return False

    async def _remove_from_processing_queue(self, job: JobModel) -> bool:
        """
        Remove job from processing queue by job ID.

        Args:
            job: Job to remove

        Returns:
            True if successful, False otherwise
        """
        try:
            return await self._remove_job_from_processing_queue_by_id(job.id)
        except Exception as e:
            logger.error(f"Failed to remove job {job.id} from processing queue: {e}")
            return False

    async def _remove_job_from_processing_queue_by_id(self, job_id: str) -> bool:
        """
        Remove job from processing queue by job ID.

        Args:
            job_id: Job ID to remove

        Returns:
            True if successful, False otherwise
        """
        try:
            processing_jobs = await self.redis_client.lrange(
                self.processing_queue, 0, -1
            )
            if not processing_jobs:
                logger.debug(f"No jobs in processing queue")
                return False

            removed_count = 0
            for processing_job in processing_jobs:
                try:
                    processing_job_obj = JobModel.model_validate_json(processing_job)
                    if processing_job_obj.id == job_id:
                        # Found job with matching ID, remove it
                        result = await self.redis_client.lrem(
                            self.processing_queue, 1, processing_job
                        )
                        if result > 0:
                            removed_count += result
                            logger.info(f"Removed job {job_id} from processing queue")
                except Exception as e:
                    # Skip malformed entries
                    logger.debug(f"Skipping malformed job in processing queue: {e}")
                    continue

            if removed_count > 0:
                logger.info(
                    f"Successfully removed {removed_count} occurrence(s) of job {job_id} from processing queue"
                )
                return True
            else:
                logger.debug(f"Job {job_id} not found in processing queue")
                return False
        except Exception as e:
            logger.error(f"Failed to remove job {job_id} from processing queue: {e}")
            return False

    async def complete_job(
        self, job: JobModel, success: bool = True, error_message: Optional[str] = None
    ) -> bool:
        """
        Mark job as completed or failed.

        Args:
            job: Job to complete
            success: Whether job completed successfully
            error_message: Error message if failed

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            job.completed_at = datetime.now(timezone.utc)

            if success:
                job.status = JobStatus.COMPLETED
                # Move to completed queue
                await self.redis_client.lpush(
                    self.completed_queue, job.model_dump_json()
                )
            else:
                job.status = JobStatus.FAILED
                job.error_message = error_message
                # Move to failed queue
                await self.redis_client.lpush(self.failed_queue, job.model_dump_json())

            # Update job status and data field
            await self.redis_client.hset(
                get_job_status_key(job.id),
                mapping={
                    "status": job.status.value,
                    "completed_at": job.completed_at.isoformat(),
                    "error_message": error_message or "",
                    "data": job.model_dump_json(),  # Update data field to keep it in sync
                },
            )

            # Remove from processing queue using job ID
            logger.debug(f"Attempting to remove job {job.id} from processing queue")
            job_removed = await self._remove_job_from_processing_queue_by_id(job.id)

            if not job_removed:
                logger.warning(f"Job {job.id} was not found in processing queue!")

            logger.info(f"Completed job {job.id} with status {job.status}")
            # Opportunistic cleanup: prune old completed/failed entries (>120s)
            try:
                if success:
                    await self._cleanup_old_entries(self.completed_queue, 120)
                else:
                    await self._cleanup_old_entries(self.failed_queue, 120)
            except Exception as _cleanup_err:
                # Non-critical
                logger.debug(f"Cleanup after completion skipped: {_cleanup_err}")
            return True

        except Exception as e:
            logger.error(f"Failed to complete job {job.id}: {e}")
            return False

    async def get_job(self, job_id: str) -> Optional[JobModel]:
        """
        Get job by ID.

        Args:
            job_id: Job identifier

        Returns:
            Job or None if not found
        """
        try:
            job_data = await self.redis_client.hget(get_job_status_key(job_id), "data")
            if job_data:
                return JobModel.model_validate_json(job_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get job for {job_id}: {e}")
            return None

    async def get_job_status(self, job_id: str) -> Optional[JobStatusInfo]:
        """
        Get job status information by ID (lightweight, without parsing full job data).

        Args:
            job_id: Job identifier

        Returns:
            JobStatusInfo or None if not found
        """
        try:
            # Get all hash fields except 'data' to avoid parsing JSON
            status_data = await self.redis_client.hgetall(get_job_status_key(job_id))
            if not status_data:
                return None

            # Parse status fields directly from hash (no JSON parsing needed)
            status_value = status_data.get("status")
            if not status_value:
                return None

            # Parse datetime fields
            created_at = None
            if "created_at" in status_data:
                try:
                    created_at = datetime.fromisoformat(status_data["created_at"])
                except (ValueError, TypeError):
                    pass

            started_at = None
            if "started_at" in status_data:
                try:
                    started_at = datetime.fromisoformat(status_data["started_at"])
                except (ValueError, TypeError):
                    pass

            completed_at = None
            if "completed_at" in status_data:
                try:
                    completed_at = datetime.fromisoformat(status_data["completed_at"])
                except (ValueError, TypeError):
                    pass

            return JobStatusInfo(
                status=JobStatus(status_value),
                created_at=created_at,
                started_at=started_at,
                completed_at=completed_at,
                error_message=status_data.get("error_message") or None,
                worker_id=status_data.get("worker_id") or None,
            )
        except Exception as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            return None

    async def get_queue_stats(
        self, queue_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive queue statistics.

        Args:
            queue_names: List of queue names to include in stats. If None, uses default queues.

        Returns:
            Dict with detailed queue statistics
        """
        try:
            # Prune old entries (>120s) before counting
            await self._cleanup_old_entries(self.completed_queue, 120)
            await self._cleanup_old_entries(self.failed_queue, 120)

            # Clean up stale processing jobs (>5 minutes)
            stale_removed = await self.cleanup_stale_processing_jobs(300)
            if stale_removed > 0:
                logger.info(
                    f"Cleaned up {stale_removed} stale processing jobs during stats collection"
                )

            stats: Dict[str, Any] = {}

            # Count jobs in all queues
            if queue_names:
                # Count jobs in specified queues
                total_pending = 0
                for queue_name in queue_names:
                    queue_key = self._get_queue_name(queue_name)
                    count = await self.redis_client.llen(queue_key)
                    total_pending += count
                stats["pending"] = total_pending
            else:
                # Default behavior - count all known queues
                # If no queue_names provided, we can't determine pending count
                # This should not happen in normal operation
                stats["pending"] = 0

            stats["processing"] = await self.redis_client.llen(self.processing_queue)
            stats["completed"] = await self.redis_client.llen(self.completed_queue)
            stats["failed"] = await self.redis_client.llen(self.failed_queue)

            # Calculate processing rate
            finished_jobs = stats["completed"] + stats["failed"]
            if finished_jobs > 0:
                stats["processing_rate"] = round(stats["completed"] / finished_jobs, 3)
            else:
                stats["processing_rate"] = 0.0

            # Calculate queue health
            total_jobs = stats["pending"] + stats["processing"] + finished_jobs
            if total_jobs > 0:
                stats["queue_health"] = (
                    "healthy" if stats["processing_rate"] > 0.8 else "unhealthy"
                )
            else:
                stats["queue_health"] = "empty"

            # Calculate processing ratio (processing vs completed)
            if stats["completed"] > 0:
                stats["processing_ratio"] = round(
                    stats["processing"] / stats["completed"], 3
                )
            else:
                stats["processing_ratio"] = (
                    float("inf") if stats["processing"] > 0 else 0.0
                )

            return stats

        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            return {}

    async def _cleanup_old_entries(self, queue_key: str, max_age_seconds: int) -> None:
        """Remove entries older than max_age_seconds from a list queue.

        This performs a simple O(n) pass over the list and removes elements
        whose `completed_at` is older than the threshold. Safe to run best-effort.
        """
        try:
            # Fetch current snapshot
            items = await self.redis_client.lrange(queue_key, 0, -1)
            if not items:
                return

            threshold = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)

            for raw in items:
                try:
                    job = JobModel.model_validate_json(raw)
                except Exception as e:
                    logger.warning(f"Ignore job because invalid info, {str(e)}")
                    continue

                # Use completed_at if present, else skip
                if job.completed_at and job.completed_at < threshold:
                    # Remove all occurrences of this serialized payload
                    await self.redis_client.lrem(queue_key, 0, raw)
        except Exception as e:
            # Best-effort cleanup; never raise
            logger.debug(f"Cleanup on {queue_key} failed: {e}")

    async def cleanup_stale_processing_jobs(self, max_age_seconds: int = 300) -> int:
        """
        Clean up stale jobs from processing queue that have been stuck for too long.

        Args:
            max_age_seconds: Maximum age for processing jobs (default 5 minutes)

        Returns:
            Number of stale jobs removed
        """
        try:
            processing_jobs = await self.redis_client.lrange(
                self.processing_queue, 0, -1
            )
            if not processing_jobs:
                return 0

            threshold = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
            removed_count = 0
            stale_job_ids = []

            # First pass: identify stale jobs
            for processing_job in processing_jobs:
                try:
                    job = JobModel.model_validate_json(processing_job)
                    # Check if job has been in processing for too long
                    if job.started_at and job.started_at < threshold:
                        stale_job_ids.append(job.id)
                        logger.warning(
                            f"Found stale processing job {job.id} (started at {job.started_at})"
                        )
                except Exception as e:
                    # Skip malformed entries
                    logger.debug(f"Skipping malformed job in processing queue: {e}")
                    continue

            # Second pass: remove stale jobs by ID
            for job_id in stale_job_ids:
                removed = await self._remove_job_from_processing_queue_by_id(job_id)
                if removed:
                    removed_count += 1

            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} stale processing jobs")

            return removed_count
        except Exception as e:
            logger.error(f"Failed to cleanup stale processing jobs: {e}")
            return 0

    async def clear_queue(self, queue_type: str = "all") -> bool:
        """
        Clear queue(s).

        Args:
            queue_type: Type of queue to clear (all, pending, processing, completed, failed)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if queue_type == "all":
                await self.redis_client.delete(
                    self.processing_queue, self.completed_queue, self.failed_queue
                )
            elif queue_type == "processing":
                await self.redis_client.delete(self.processing_queue)
            elif queue_type == "completed":
                await self.redis_client.delete(self.completed_queue)
            elif queue_type == "failed":
                await self.redis_client.delete(self.failed_queue)

            logger.info(f"Cleared {queue_type} queue(s)")
            return True

        except Exception as e:
            logger.error(f"Failed to clear {queue_type} queue: {e}")
            return False

    async def clear_processing_queue(self) -> bool:
        """
        Clear all jobs from processing queue.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = await self.redis_client.delete(self.processing_queue)
            logger.info(f"Cleared processing queue (deleted {result} keys)")
            return True
        except Exception as e:
            logger.error(f"Failed to clear processing queue: {e}")
            return False


# Job queue instances are created per aq_worker with specific queue_names
