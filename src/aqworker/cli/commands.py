"""
Reusable logic for CLI commands, extracted for easier testing.
"""

import asyncio
import signal
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from aqworker.core import AQWorker
from aqworker.job.scheduler import CronScheduler
from aqworker.job.service import JobService
from aqworker.logger import logger

if TYPE_CHECKING:
    from aqworker.worker.base import BaseWorker


class RegistryError(RuntimeError):
    """Raised when required registry data is missing."""


def list_worker_names(aq_worker: AQWorker) -> List[str]:
    """Return sorted aq_worker names."""
    return sorted(defn.name for defn in aq_worker.worker_registry.list_definitions())


def list_handler_descriptors(aq_worker: AQWorker) -> List[Tuple[str, str]]:
    """Return (name, dotted path) tuples for handlers."""
    handlers = aq_worker.handler_registry.snapshot()
    descriptors = []
    for name, handler_cls in handlers.items():
        descriptors.append((name, f"{handler_cls.__module__}.{handler_cls.__name__}"))
    return sorted(descriptors, key=lambda item: item[0])


def list_queue_names(aq_worker: AQWorker) -> List[str]:
    """Return sorted queue names inferred from workers."""
    definitions = aq_worker.worker_registry.list_definitions()
    queues = set[str]()
    for definition in definitions:
        queues.update(definition.queue_names)
    return sorted(queues)


def build_worker(aq_worker: AQWorker, worker_name: str) -> "BaseWorker":
    """Construct an aq_worker instance without starting it."""
    return aq_worker.create_worker(worker_name)


def run_worker(aq_worker: AQWorker, worker_name: str) -> None:
    """Run a aq_worker event loop."""
    worker = build_worker(aq_worker, worker_name)
    asyncio.run(worker.run())


async def get_queue_stats_async(
    queue_name: str, job_service: Optional[JobService] = None
) -> Dict[str, int]:
    """Async helper for queue stats (handy for unit tests)."""
    if job_service is None:
        raise ValueError("job_service is required for get_queue_stats_async")
    stats = await job_service.get_queue_stats([queue_name])
    return stats or {}


def get_queue_stats(
    queue_name: str, job_service: Optional[JobService] = None
) -> Dict[str, int]:
    """Sync wrapper for queue stats."""
    return asyncio.run(get_queue_stats_async(queue_name, job_service))


def run_beat(
    aq_worker: AQWorker,
    check_interval: Optional[float] = None,
) -> None:
    """
    Run the cron scheduler (beat service).

    Args:
        aq_worker: AQWorker instance
        check_interval: Interval in seconds to check for cron jobs (default: 0.1)
    """
    from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL

    if check_interval is None:
        check_interval = DEFAULT_CRON_CHECK_INTERVAL
    if not aq_worker.job_service:
        raise RegistryError(
            "JobService is required for beat. Make sure AQWorker has a job_service configured."
        )

    # Create and start cron scheduler
    scheduler = CronScheduler(
        handler_registry=aq_worker.handler_registry,
        job_service=aq_worker.job_service,
        check_interval=check_interval,
    )

    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down beat...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    async def run():
        try:
            await scheduler.start()
            logger.info(f"Beat scheduler started (check_interval={check_interval}s)")
            logger.info("Press Ctrl+C to stop")

            # Wait for shutdown signal
            await shutdown_event.wait()

        except KeyboardInterrupt:
            logger.info("Beat interrupted by user")
        finally:
            await scheduler.stop()
            logger.info("Beat scheduler stopped")

    asyncio.run(run())
