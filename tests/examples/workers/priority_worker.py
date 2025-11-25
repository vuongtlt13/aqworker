from aqworker.worker.base import BaseWorker, WorkerConfig


class PriorityWorker(BaseWorker):
    worker_name = "priority"
    worker_config = WorkerConfig(
        worker_id="priority-aq_worker",
        queue_names=["priority"],
        max_concurrent_jobs=1,
        poll_interval=0.1,
    )

    async def run(self):
        type(self).last_run = True
