"""
Cron job scheduler service.

This service periodically checks registered CronJob handlers and enqueues
jobs when their cron expressions match the current time.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from croniter import croniter

from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL, get_cron_last_run_key
from aqworker.handler.registry import HandlerRegistry
from aqworker.job.base import CronJob
from aqworker.job.service import JobService
from aqworker.logger import logger


class CronScheduler:
    """
    Scheduler service for CronJob handlers.

    This service runs in the background and periodically checks all registered
    CronJob handlers. When a cron expression matches the current time, it
    automatically enqueues a job for that handler.

    Supports both 5-field (minute-level) and 6-field (second-level) cron expressions.
    Default check interval is 0.1 seconds to support second-level precision.

    Note: Each CronJob handler must have queue_name defined (required).

    Usage:
        scheduler = CronScheduler(
            handler_registry=aq_worker.handler_registry,
            job_service=aq_worker.job_service,
            check_interval=0.1  # Check every 0.1 seconds (default)
        )
        await scheduler.start()
    """

    def __init__(
        self,
        handler_registry: HandlerRegistry,
        job_service: JobService,
        check_interval: float = DEFAULT_CRON_CHECK_INTERVAL,
    ):
        """
        Initialize cron scheduler.

        Args:
            handler_registry: Handler registry containing CronJob handlers
            job_service: Job service for enqueueing jobs
            check_interval: Interval in seconds to check for cron jobs (default: 0.1)
                           Supports float values for sub-second precision
        """
        self.handler_registry = handler_registry
        self.job_service = job_service
        self.check_interval = check_interval
        self._running = False
        self._task: Optional[asyncio.Task[None]] = None
        # Allow a small tolerance window so we don't miss schedules due to drift
        self._tolerance = max(self.check_interval * 2, 0.5)
        self._redis = job_service.get_redis_client()

        # Track last scheduled time that was enqueued for each cron job to avoid duplicates
        # Key: handler_name, Value: last scheduled time (prev_time from cron) that was enqueued
        self._last_scheduled_time: Dict[str, datetime] = {}

    async def start(self) -> None:
        """Start the cron scheduler in the background."""
        if self._running:
            logger.warning("CronScheduler is already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(f"CronScheduler started (check_interval={self.check_interval}s)")

    async def stop(self) -> None:
        """Stop the cron scheduler."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("CronScheduler stopped")

    async def _run(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._check_and_enqueue()
            except Exception as e:
                logger.exception(f"Error in CronScheduler: {e}")

            # Wait for next check interval
            await asyncio.sleep(self.check_interval)

    async def _check_and_enqueue(self) -> None:
        """Check all CronJob handlers and enqueue jobs if needed."""
        now = datetime.now(timezone.utc)

        # Get all registered handlers
        handlers = self.handler_registry.snapshot()

        for handler_name, handler_cls in handlers.items():
            # Only process CronJob handlers
            if not issubclass(handler_cls, CronJob):
                continue

            try:
                cron_expr = handler_cls.cron()
                if not cron_expr:
                    continue

                last_scheduled = self._last_scheduled_time.get(handler_name)
                if last_scheduled is None:
                    last_scheduled = await self._load_last_scheduled_from_store(
                        handler_name
                    )
                    if last_scheduled:
                        self._last_scheduled_time[handler_name] = last_scheduled

                # Check if this cron job should run now
                should_run, scheduled_time = self._should_run_cron_job(
                    handler_name, cron_expr, now, last_scheduled
                )

                if should_run and scheduled_time:
                    claimed, previous = await self._claim_scheduled_run(
                        handler_name, scheduled_time
                    )
                    if not claimed:
                        continue

                    try:
                        await self._enqueue_cron_job(
                            handler_name,
                            handler_cls,
                            scheduled_time,
                        )
                        self._last_scheduled_time[handler_name] = scheduled_time
                    except Exception:
                        await self._restore_scheduled_run(handler_name, previous)
                        raise

            except Exception as e:
                logger.error(
                    f"Error checking cron job {handler_name}: {e}",
                    exc_info=True,
                )

    def _should_run_cron_job(
        self,
        handler_name: str,
        cron_expr: str,
        now: datetime,
        last_scheduled: Optional[datetime],
    ) -> tuple[bool, Optional[datetime]]:
        """
        Check if a cron job should run at the given time.

        Supports both 5-field (minute-level) and 6-field (second-level) cron expressions.
        Format:
        - 5 fields: minute hour day month day_of_week (e.g., "0 0 * * *")
        - 6 fields: second minute hour day month day_of_week (e.g., "*/10 * * * * *")

        Args:
            handler_name: Name of the handler
            cron_expr: Cron expression (5 or 6 fields)
            now: Current time

        Returns:
            Tuple of (should_run, scheduled_time):
            - should_run: True if the job should run, False otherwise
            - scheduled_time: The scheduled time (prev_time) that should be enqueued, or None
        """
        try:
            # Create cron iterator (support both 5-field and 6-field expressions)
            fields = cron_expr.split()
            use_seconds = len(fields) >= 6
            cron = croniter(cron_expr, now, second_at_beginning=use_seconds)

            # Get the previous scheduled time (before now) - this is the time we should enqueue
            prev_time = cron.get_prev(datetime)

            # Check if we should run:
            # 1. If we haven't enqueued this scheduled time before
            # 2. We're within check_interval of the scheduled time (to handle timing precision)
            if last_scheduled is None:
                # First time running, check if we're within the check_interval of the scheduled time
                time_diff = abs((now - prev_time).total_seconds())
                if time_diff <= self._tolerance:
                    return True, prev_time
                return False, None
            else:
                # If the scheduled time is newer than what we've already run, enqueue it
                # even if we're catching up late (prev_time may be far behind 'now').
                if prev_time > last_scheduled:
                    return True, prev_time
                return False, None

        except Exception as e:
            logger.error(
                f"Error parsing cron expression '{cron_expr}' for {handler_name}: {e}"
            )
            return False, None

    async def _enqueue_cron_job(
        self,
        handler_name: str,
        handler_cls: type,
        scheduled_time: datetime,
    ) -> None:
        """
        Enqueue a cron job.

        Args:
            handler_name: Name of the handler
            handler_cls: Handler class
            scheduled_time: Time when the job was scheduled
        """
        try:
            # Pass handler class and queue_name=None to let enqueue_job use handler's queue_names
            job = await self.job_service.enqueue_job(
                handler=handler_cls,
                queue_name=None,  # Let enqueue_job use handler's queue_names or default
                metadata={
                    "cron_job": True,
                    "handler_class": handler_cls.__name__,
                },
                schedule_time=scheduled_time,
            )
            logger.info(
                f"Enqueued cron job '{handler_name}' (job_id={job.id}, "
                f"queue={job.queue_name}, schedule={scheduled_time.isoformat()})"
            )
        except Exception as e:
            logger.error(
                f"Failed to enqueue cron job '{handler_name}': {e}",
                exc_info=True,
            )

    async def _load_last_scheduled_from_store(
        self, handler_name: str
    ) -> Optional[datetime]:
        """Load persisted last scheduled time from Redis."""
        if not self._redis:
            return None
        try:
            value = await self._redis.get(get_cron_last_run_key(handler_name))
            if value:
                return datetime.fromisoformat(value)
        except Exception as exc:
            logger.warning(f"Failed to load last cron run for {handler_name}: {exc}")
        return None

    async def _claim_scheduled_run(
        self, handler_name: str, scheduled_time: datetime
    ) -> Tuple[bool, Optional[str]]:
        """
        Atomically register this scheduled run in Redis.

        Returns (claimed, previous_value) so callers can roll back on failure.
        """
        if not self._redis:
            return True, None

        script = """
        local key = KEYS[1]
        local new_time = ARGV[1]
        local current = redis.call('GET', key)
        if not current or new_time > current then
            redis.call('SET', key, new_time)
            if current then
                return {1, current}
            else
                return {1, ''}
            end
        end
        if current then
            return {0, current}
        end
        return {0, ''}
        """
        key = get_cron_last_run_key(handler_name)
        new_value = scheduled_time.isoformat()
        try:
            result = await self._redis.eval(script, 1, key, new_value)
        except Exception as exc:
            logger.error(f"Failed to claim cron schedule for {handler_name}: {exc}")
            return False, None

        if isinstance(result, list) and len(result) == 2:
            claimed = bool(int(result[0]))
            previous = result[1] or None
        else:
            claimed = bool(result)
            previous = None

        return claimed, previous

    async def _restore_scheduled_run(
        self, handler_name: str, previous_value: Optional[str]
    ) -> None:
        """Restore Redis state when enqueue fails after claiming."""
        if not self._redis:
            return
        key = get_cron_last_run_key(handler_name)
        try:
            if previous_value:
                await self._redis.set(key, previous_value)
                self._last_scheduled_time[handler_name] = datetime.fromisoformat(
                    previous_value
                )
            else:
                await self._redis.delete(key)
                self._last_scheduled_time.pop(handler_name, None)
        except Exception as exc:
            logger.error(
                f"Failed to restore cron schedule marker for {handler_name}: {exc}"
            )

    def _get_queue_name_for_handler(self, handler_cls: type) -> Optional[str]:
        """
        Get queue name for a cron job handler.

        Priority:
        1. Handler's queue_name() method (if returns non-None)
        2. Handler's queue_name attribute (required)

        Args:
            handler_cls: Handler class

        Returns:
            Queue name to use for this handler, or None to let enqueue_job validate

        Raises:
            ValueError: If handler does not have queue_name defined
        """
        handler_queue_name: Optional[str] = getattr(handler_cls, "queue_name", None)
        return handler_queue_name

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running
