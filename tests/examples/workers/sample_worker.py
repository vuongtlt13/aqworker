from aqworker.worker.base import BaseWorker, WorkerConfig


class BackgroundWorker(BaseWorker):
    worker_name = "background"
    worker_config = WorkerConfig(
        worker_id="background-aq_worker",
        queue_names=["background", "bulk"],
        max_concurrent_jobs=1,
        poll_interval=0.1,
    )

    # Track whether the coroutine ran (used by tests)
    last_run = None

    async def run(self):
        type(self).last_run = {"worker_id": self.worker_id}

    async def handle(self, data):
        return True
