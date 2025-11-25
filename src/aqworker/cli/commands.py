"""
Reusable logic for CLI commands, extracted for easier testing.
"""

import asyncio
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from aqworker.core import AQWorker
from aqworker.job.service import JobService

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
    return aq_worker.create_worker(worker_name)  # type: ignore[no-any-return]


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
