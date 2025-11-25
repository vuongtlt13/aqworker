"""
Example workers for the simple FastAPI application.
"""

from aqworker import BaseWorker, WorkerConfig


class EmailWorker(BaseWorker):
    """Worker for processing email jobs."""

    worker_name = "email"
    worker_config = WorkerConfig(
        queue_names=["emails"],
        max_concurrent_jobs=3,
        poll_interval=0.5,
    )


class NotificationWorker(BaseWorker):
    """Worker for processing notification jobs."""

    worker_name = "notification"
    worker_config = WorkerConfig(
        queue_names=["notifications"],
        max_concurrent_jobs=5,
        poll_interval=0.3,
    )
