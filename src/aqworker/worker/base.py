import asyncio
import inspect
import os
import signal
import socket
import time
from abc import ABC
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from aqworker.job.models import JobModel
from aqworker.job.service import JobService
from aqworker.logger import logger
from aqworker.worker.dispatcher import HandlerDispatcher


def default_worker_id() -> str:
    return f"aq_worker-{socket.gethostname()}-{os.getpid()}"


@dataclass
class WorkerConfig:
    """
    Convenience dataclass for aq_worker configuration.

    Can be assigned directly to `worker_config` in a `BaseWorker` subclass.
    """

    worker_id: str = field(default_factory=default_worker_id)
    max_concurrent_jobs: int = 5
    poll_interval: float = 0.3  # 300ms
    queue_names: List[str] = field(default_factory=list)
    job_timeout: int = 30

    def as_dict(self) -> Dict[str, Any]:
        """Return config as plain dict for serialization/inspection."""
        data = asdict(self)
        # ensure queue_names is a list copy
        data["queue_names"] = list(self.queue_names)
        return data


class BaseWorker(ABC):
    """Base class for all workers."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        handler_dispatcher: Optional[HandlerDispatcher] = None,
        job_service: Optional[JobService] = None,
    ):
        """
        Initialize base aq_worker.

        Args:
            config: Worker configuration dictionary (optional, will use worker_config if not provided)
            handler_dispatcher: HandlerDispatcher instance for executing handlers
            job_service: JobService instance for managing jobs
        """
        self.handler_dispatcher = handler_dispatcher
        self._job_service = job_service
        # Prepare class-level default config
        self._last_health_check = 0
        self._last_stats_log = 0
        class_config: Dict[str, Any] = {}
        if hasattr(self, "worker_config"):
            wc = getattr(self, "worker_config")
            if inspect.isclass(wc):
                try:
                    wc = wc()
                except TypeError as exc:
                    raise TypeError(
                        f"{self.__class__.__name__}.worker_config must be instantiable without arguments"
                    ) from exc
            if hasattr(wc, "model_dump") and callable(getattr(wc, "model_dump")):
                class_config = wc.model_dump()
            elif isinstance(wc, dict):
                class_config = wc
            else:
                class_config = dict(getattr(wc, "__dict__", {}))

        # Merge provided config over class defaults
        if config is None:
            config = class_config
        else:
            merged = class_config.copy()
            merged.update(config)
            config = merged

        self.config = config

        # Worker settings
        self.worker_id = config.get(
            "worker_id", f"{self.__class__.__name__.lower()}-{int(time.time())}"
        )
        self.poll_interval = config.get("poll_interval", 1)
        self.max_concurrent_jobs = config.get("max_concurrent_jobs", 5)
        self.job_timeout = config.get("job_timeout", 300)

        # Queue settings
        self.queue_names = config.get("queue_names", [])

        # Track running jobs
        self.running_jobs: Set[str] = set()
        # Track asyncio Tasks for concurrent job execution
        self._tasks: Set[asyncio.Task[Any]] = set()
        self.shutdown_requested = False

        # Setup signal handlers
        self._setup_signal_handlers()

    @property
    def job_service(self) -> JobService:
        """Get job service instance."""
        if self._job_service is None:
            raise ValueError(
                "JobService not provided! Pass job_service to aq_worker constructor."
            )
        return self._job_service

    def _setup_signal_handlers(self):
        """Setup signal handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(
            f"Worker {self.worker_id} received signal {signum}, initiating graceful shutdown..."
        )
        self.shutdown_requested = True

    def _can_process_job(self) -> bool:
        """Check if aq_worker can process more jobs."""
        return (
            len(self._tasks) < self.max_concurrent_jobs and not self.shutdown_requested
        )

    async def _process_job_async(self, job: JobModel):
        """Process a single job asynchronously."""
        job_id = job.id
        self.running_jobs.add(job_id)
        logger.debug(
            f"Added job {job_id} to running_jobs. Total running: {len(self.running_jobs)}"
        )

        try:
            # Skip expired cron jobs (too far in the past) without executing handler
            try:
                if job.metadata.get("cron_job") and job.schedule_time:
                    now = datetime.now(timezone.utc)
                    delay = (now - job.schedule_time).total_seconds()
                    # Reuse job_timeout as the maximum acceptable delay window
                    if delay > float(self.job_timeout):
                        logger.warning(
                            f"Skipping expired cron job {job_id}: "
                            f"schedule_time={job.schedule_time.isoformat()}, "
                            f"delay={delay:.2f}s > max_delay={self.job_timeout}s"
                        )
                        await self.job_service.complete_job(
                            job=job, success=True, error_message=None
                        )
                        return
            except (
                Exception
            ) as e:  # Best-effort guard; never fail job because of this check
                logger.warning(
                    f"Failed to evaluate expiration for cron job {job_id}: {e}"
                )

            logger.info(
                f"Worker {self.worker_id} processing job {job_id} from queue {job.queue_name}"
            )

            if not self.handler_dispatcher:
                raise ValueError(
                    "HandlerDispatcher not provided! Pass handler_dispatcher to aq_worker constructor."
                )

            # Process the job using aq_worker-specific logic
            success = await self.handler_dispatcher.execute(job)

            if success:
                # Mark job as completed via job service
                await self.job_service.complete_job(job=job, success=True)
                logger.info(f"Successfully completed job {job_id}")
            else:
                logger.error(f"Failed to process job {job_id}")
                await self._retry_or_fail(job, error_message="Job processing failed")

        except Exception as e:
            logger.exception(f"Error processing job {job_id}: {e}")

            await self._retry_or_fail(job, error_message=str(e))

        finally:
            # Remove from running jobs
            self.running_jobs.discard(job_id)
            logger.debug(
                f"Removed job {job_id} from running_jobs. Total running: {len(self.running_jobs)}"
            )

            # Release job lock
            try:
                await self.job_service.release_job_lock(
                    job_id=job_id,
                    worker_id=self.worker_id,
                )
            except Exception as e:
                logger.warning(f"Failed to release job lock for {job_id}: {e}")

            # Note: Job removal from processing queue is handled by complete_job/retry_job
            # No need to manually remove here to avoid duplicate removal

    async def _get_queue_stats(self) -> Dict[str, int]:
        """Get current queue statistics."""
        return await self.job_service.get_queue_stats(self.queue_names)

    async def _health_check(self):
        """Perform health check on aq_worker and Redis connection."""
        try:
            # Check Redis connection
            stats = await self._get_queue_stats()
            logger.debug(
                f"Worker {self.worker_id} health check: Redis OK, stats={stats}"
            )

            # Check if aq_worker is responsive
            if len(self.running_jobs) > self.max_concurrent_jobs:
                logger.warning(
                    f"Worker {self.worker_id} has more running jobs than max allowed: {len(self.running_jobs)} > {self.max_concurrent_jobs}"
                )
                logger.debug(f"Running jobs: {list(self.running_jobs)}")

        except Exception as e:
            logger.error(f"Worker {self.worker_id} health check failed: {e}")
            # Could implement restart logic here if needed

    async def _log_periodic_stats(self):
        """Log comprehensive queue statistics every minute."""
        try:
            stats = await self._get_queue_stats()

            # Format stats for better readability
            logger.info("=" * 60)
            logger.info(f"📊 QUEUE STATS - Worker: {self.worker_id}")
            logger.info("=" * 60)
            logger.info(f"📋 Job Status Summary:")
            logger.info(f"  • Pending:     {stats.get('pending', 0)}")
            logger.info(f"  • Processing:  {stats.get('processing', 0)}")
            logger.info(f"  • Completed:   {stats.get('completed', 0)}")
            logger.info(f"  • Failed:      {stats.get('failed', 0)}")
            logger.info("")

            # Show individual queue breakdown
            if len(self.queue_names) > 1:
                logger.info(f"📋 Queue Breakdown:")
                for queue_name in self.queue_names:
                    count = stats.get(queue_name, 0)
                    logger.info(f"  • {queue_name}: {count}")
                logger.info("")

            logger.info(f"📈 Performance Metrics:")
            logger.info(
                f"  • Processing rate:     {stats.get('processing_rate', 0):.1%}"
            )
            logger.info(
                f"  • Processing ratio:    {stats.get('processing_ratio', 0):.2f}"
            )
            logger.info(
                f"  • Queue health:        {stats.get('queue_health', 'unknown')}"
            )
            logger.info("")
            logger.info(f"🔧 Worker Status:")
            logger.info(f"  • Running jobs:        {len(self.running_jobs)}")
            logger.info(f"  • Max concurrent:      {self.max_concurrent_jobs}")
            logger.info(f"  • Poll interval:       {self.poll_interval}s")
            logger.info(f"  • Listening queues:    {', '.join(self.queue_names)}")

            logger.info("=" * 60)

            # Add warnings for critical states
            if stats.get("processing", 0) > 1000:
                logger.warning("🚨 CRITICAL: Processing queue is very large!")
            elif stats.get("processing", 0) > 500:
                logger.warning("⚠️ WARNING: Processing queue is large")

            if stats.get("processing_ratio", 0) > 10:
                logger.warning(
                    "🚨 CRITICAL: High processing ratio - workers may be overwhelmed"
                )
            elif stats.get("processing_ratio", 0) > 5:
                logger.warning(
                    "⚠️ WARNING: Medium processing ratio - consider scaling up"
                )

        except Exception as e:
            logger.error(f"Failed to log periodic stats: {e}")

    async def _retry_or_fail(self, job: JobModel, error_message: str):
        """Complete job as failed; retry if allowed, with delay."""
        try:
            # Mark as failed first so status/history is consistent
            await self.job_service.complete_job(
                job=job, success=False, error_message=error_message
            )

            # Check retry budget on current job snapshot
            if job.retry_count < job.max_retries:
                import asyncio

                delay = max(0.0, job.retry_delay)
                if delay > 0.0:
                    logger.info(
                        f"Retrying job {job.id} in {delay}s (attempt {job.retry_count + 1}/{job.max_retries})"
                    )
                    await asyncio.sleep(delay)
                # Enqueue retry (increments retry_count inside service)
                await self.job_service.retry_job(job.id)
            else:
                logger.error(
                    f"Job {job.id} exhausted retries ({job.retry_count}/{job.max_retries}), kept in failed queue"
                )
        except Exception as retry_exc:
            logger.exception(f"Retry handling failed for job {job.id}: {retry_exc}")

    async def run(self):
        """Main aq_worker loop."""
        logger.info(f"Starting {self.__class__.__name__} aq_worker {self.worker_id}")
        logger.info(
            f"Configuration: poll_interval={self.poll_interval}s, max_concurrent={self.max_concurrent_jobs}"
        )
        logger.info(f"Queue names: {self.queue_names}")

        # Print initial queue stats on startup
        try:
            await self._log_periodic_stats()
        except Exception as e:
            logger.warning(f"Failed to print initial queue stats: {e}")
        # Initialize timers so the next periodic logs occur after full interval
        now = int(time.time())
        self._last_stats_log = now
        self._last_health_check = now

        try:
            while not self.shutdown_requested:
                # Check if we can process more jobs
                if self._can_process_job():
                    # Try to get a job from queue via job service with worker_id for locking
                    job = await self.job_service.get_next_job(
                        queue_names=self.queue_names,
                        timeout=0,
                        worker_id=self.worker_id,
                    )  # Non-blocking

                    if job:
                        # Run job processing concurrently up to max_concurrent_jobs
                        task = asyncio.create_task(self._process_job_async(job))
                        self._tasks.add(task)

                        def _on_task_done(t: asyncio.Task[Any]) -> None:
                            # Remove finished task from tracking set
                            self._tasks.discard(t)

                        task.add_done_callback(_on_task_done)
                    else:
                        # No jobs available, wait a bit
                        await asyncio.sleep(self.poll_interval)
                else:
                    # At capacity, wait a bit
                    await asyncio.sleep(self.poll_interval)

                # Periodic queue stats every 30 seconds
                if hasattr(self, "_last_stats_log"):
                    if time.time() - self._last_stats_log > 30:  # 30 seconds
                        await self._log_periodic_stats()
                        self._last_stats_log = int(time.time())
                else:
                    self._last_stats_log = int(time.time())

                # Health check every 30 seconds
                if hasattr(self, "_last_health_check"):
                    if time.time() - self._last_health_check > 30:
                        await self._health_check()
                        self._last_health_check = int(time.time())
                else:
                    self._last_health_check = int(time.time())

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.exception(f"Worker {self.worker_id} error: {e}")
        finally:
            self._shutdown()

    def _shutdown(self):
        """Graceful shutdown."""
        logger.info(f"Shutting down aq_worker {self.worker_id}")

        # Wait for running jobs to complete
        if self.running_jobs:
            logger.info(
                f"Waiting for {len(self.running_jobs)} running jobs to complete..."
            )

            timeout = 30  # 30 seconds timeout
            start_time = time.time()

            while self.running_jobs and (time.time() - start_time) < timeout:
                time.sleep(1)

            if self.running_jobs:
                logger.warning(
                    f"Force shutdown with {len(self.running_jobs)} jobs still running"
                )
            else:
                logger.info("All jobs completed, aq_worker shutdown complete")
