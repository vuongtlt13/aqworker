import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from aqworker.job.models import JobModel
from aqworker.worker.base import BaseWorker, WorkerConfig


class StubDispatcher:
    def __init__(self, result=True):
        self.result = result
        self.jobs = []

    async def execute(self, job):
        self.jobs.append(job)
        return self.result


class StubJobService:
    def __init__(self):
        self.completed = []
        self.released = []
        self.retried = []
        self.retry_delays = []
        self.promote_calls = 0
        self.stats_calls = 0
        self.stats_value = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}

    async def complete_job(self, job, success, error_message=None):
        self.completed.append((job.id, success, error_message))

    async def release_job_lock(self, job_id, worker_id):
        self.released.append((job_id, worker_id))

    async def retry_job(self, job_id, delay=0):
        self.retried.append(job_id)
        self.retry_delays.append(delay)

    async def get_queue_stats(self, queue_names):
        self.stats_calls += 1
        return self.stats_value

    async def promote_due_jobs(self, queue_names):
        self.promote_calls += 1
        return 0

    async def cleanup_stale_processing_jobs(self, max_age_seconds=300):
        return 0


class ConfiguredWorker(BaseWorker):
    worker_config = WorkerConfig(
        queue_names=["alpha"], max_concurrent_jobs=3, poll_interval=0.2
    )


def make_job(**overrides):
    defaults = {
        "id": "job-1",
        "queue_name": "alpha",
        "handler": "email",
        "data": {"value": 1},
    }
    defaults.update(overrides)
    return JobModel(**defaults)


def test_worker_uses_class_config_and_overrides():
    dispatcher = StubDispatcher()
    service = StubJobService()

    worker = ConfiguredWorker(
        config={"max_concurrent_jobs": 10, "job_timeout": 42},
        handler_dispatcher=dispatcher,
        job_service=service,
    )

    assert worker.queue_names == ["alpha"]
    assert worker.max_concurrent_jobs == 10
    assert worker.job_timeout == 42


def test_job_service_property_requires_instance():
    worker = BaseWorker()
    with pytest.raises(ValueError):
        _ = worker.job_service  # noqa: B018


@pytest.mark.asyncio
async def test_process_job_successful_path_updates_running_set():
    dispatcher = StubDispatcher(result=True)
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    job = make_job()

    await worker._process_job_async(job)

    assert job.id not in worker.running_jobs
    assert service.completed == [(job.id, True, None)]
    assert service.released == [(job.id, worker.worker_id)]


@pytest.mark.asyncio
async def test_process_job_failure_triggers_retry_logic(monkeypatch):
    dispatcher = StubDispatcher(result=False)
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    job = make_job(max_retries=2, retry_delay=0)

    await worker._process_job_async(job)

    assert service.completed == [(job.id, False, "Job processing failed")]
    assert service.retried == [job.id]


@pytest.mark.asyncio
async def test_retry_or_fail_respects_retry_budget(monkeypatch):
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=StubDispatcher(), job_service=service)
    job = make_job(max_retries=1, retry_count=1)

    await worker._retry_or_fail(job, "boom")

    assert service.retried == []


@pytest.mark.asyncio
async def test_retry_or_fail_delegates_delay_without_sleeping(monkeypatch):
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=StubDispatcher(), job_service=service)
    job = make_job(max_retries=3, retry_count=0, retry_delay=60)

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await worker._retry_or_fail(job, "boom")

    assert service.retried == [job.id]
    assert service.retry_delays == [60]
    assert sleep_calls == []  # worker slot is not held during retry_delay


class SlowDispatcher:
    def __init__(self, seconds):
        self.seconds = seconds
        self.cancelled = False

    async def execute(self, job):
        try:
            await asyncio.sleep(self.seconds)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return True


@pytest.mark.asyncio
async def test_process_job_times_out_using_job_timeout():
    dispatcher = SlowDispatcher(5)
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.job_timeout = 30
    job = make_job(timeout=0.05, max_retries=1, retry_delay=0)

    started = time.monotonic()
    await worker._process_job_async(job)

    assert time.monotonic() - started < 1
    assert dispatcher.cancelled is True
    assert service.completed == [(job.id, False, "Job timed out after 0.05s")]
    assert service.retried == [job.id]
    assert service.released == [(job.id, worker.worker_id)]


@pytest.mark.asyncio
async def test_process_job_falls_back_to_worker_timeout():
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=SlowDispatcher(5), job_service=service)
    worker.job_timeout = 0.05
    job = make_job(max_retries=0)

    await worker._process_job_async(job)

    assert service.completed == [(job.id, False, "Job timed out after 0.05s")]
    assert service.retried == []


@pytest.mark.asyncio
async def test_zero_timeout_disables_limit():
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=SlowDispatcher(0.1), job_service=service)
    worker.job_timeout = 0.01
    job = make_job(timeout=0)

    await worker._process_job_async(job)

    assert service.completed == [(job.id, True, None)]


@pytest.mark.asyncio
async def test_shutdown_waits_for_running_jobs_to_finish():
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=SlowDispatcher(0.1), job_service=service)
    worker.shutdown_timeout = 2
    task = asyncio.create_task(worker._process_job_async(make_job(timeout=0)))
    worker._tasks.add(task)

    await worker._shutdown()

    assert task.done() and not task.cancelled()
    assert service.completed == [("job-1", True, None)]


@pytest.mark.asyncio
async def test_shutdown_cancels_jobs_after_timeout_and_records_retry():
    dispatcher = SlowDispatcher(10)
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.shutdown_timeout = 0.05
    job = make_job(timeout=0, max_retries=2, retry_delay=0)
    task = asyncio.create_task(worker._process_job_async(job))
    worker._tasks.add(task)
    await asyncio.sleep(0)  # let the job start

    started = time.monotonic()
    await worker._shutdown()

    assert time.monotonic() - started < 1
    assert dispatcher.cancelled is True
    assert service.completed == [(job.id, False, "Job cancelled by worker shutdown")]
    assert service.retried == [job.id]
    assert service.released == [(job.id, worker.worker_id)]


@pytest.mark.asyncio
async def test_signal_stops_run_and_waits_for_running_job():
    service = StubJobService()
    jobs = [make_job(timeout=0)]

    async def fake_get_next_job(**kwargs):
        return jobs.pop() if jobs else None

    service.get_next_job = fake_get_next_job  # type: ignore[assignment]
    dispatcher = SlowDispatcher(0.2)
    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.poll_interval = 0.01

    async def stop_soon():
        await asyncio.sleep(0.05)
        worker._signal_handler(15, None)

    await asyncio.gather(worker.run(), stop_soon())

    assert dispatcher.cancelled is False
    assert service.completed == [("job-1", True, None)]
    assert service.promote_calls >= 1


@pytest.mark.asyncio
async def test_health_check_and_log_stats(monkeypatch):
    service = StubJobService()
    service.stats_value = {
        "pending": 1,
        "processing": 5,
        "completed": 2,
        "failed": 1,
        "processing_rate": 0.5,
        "processing_ratio": 2.5,
        "queue_health": "unhealthy",
    }
    worker = BaseWorker(handler_dispatcher=StubDispatcher(), job_service=service)
    worker.running_jobs = {"job-a", "job-b", "job-c", "job-d", "job-e", "job-f"}
    worker.max_concurrent_jobs = 2

    await worker._health_check()
    assert service.stats_calls == 1

    # Exercise periodic stats logger
    await worker._log_periodic_stats()
    assert service.stats_calls == 2


@pytest.mark.asyncio
async def test_expired_cron_job_is_skipped_without_executing_handler():
    """Cron jobs with schedule_time too far in the past should be skipped."""
    dispatcher = StubDispatcher(result=True)
    service = StubJobService()

    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    # Make the allowed delay small so our test schedule_time is clearly expired
    worker.job_timeout = 30  # seconds

    job = make_job(id="cron-job-1")
    job.metadata["cron_job"] = True
    job.schedule_time = datetime.now(timezone.utc) - timedelta(seconds=120)

    # Track handler executions explicitly
    executed_jobs = []

    async def fake_execute(j):
        executed_jobs.append(j.id)
        return True

    dispatcher.execute = fake_execute  # type: ignore[assignment]

    await worker._process_job_async(job)

    # Handler should never be called for expired cron job
    assert executed_jobs == []
    # Job should be marked as completed by job_service without retry
    assert service.completed == [(job.id, True, None)]
    assert service.retried == []


@pytest.mark.asyncio
async def test_recent_cron_job_is_processed_normally():
    """Cron jobs within allowed delay window should still be executed."""
    dispatcher = StubDispatcher(result=True)
    service = StubJobService()

    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.job_timeout = 300  # 5 minutes

    job = make_job(id="cron-job-2")
    job.metadata["cron_job"] = True
    # schedule_time 10 seconds ago -> within job_timeout window
    job.schedule_time = datetime.now(timezone.utc) - timedelta(seconds=10)

    executed_jobs = []

    async def fake_execute(j):
        executed_jobs.append(j.id)
        return True

    dispatcher.execute = fake_execute  # type: ignore[assignment]

    await worker._process_job_async(job)

    # Handler should be called for non-expired cron job
    assert executed_jobs == ["cron-job-2"]
    # Job should be marked as completed via job_service
    assert service.completed == [(job.id, True, None)]


@pytest.mark.asyncio
async def test_non_cron_job_is_not_affected_by_expiration_logic():
    """Non-cron jobs should never be skipped based on schedule_time."""
    dispatcher = StubDispatcher(result=True)
    service = StubJobService()

    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.job_timeout = 30  # keep same as expiration test

    job = make_job(id="regular-job-1")
    # Intentionally set an old schedule_time but without cron_job metadata flag
    job.schedule_time = datetime.now(timezone.utc) - timedelta(seconds=120)

    executed_jobs = []

    async def fake_execute(j):
        executed_jobs.append(j.id)
        return True

    dispatcher.execute = fake_execute  # type: ignore[assignment]

    await worker._process_job_async(job)

    # Handler should still be called for non-cron jobs
    assert executed_jobs == ["regular-job-1"]
    # Job should be marked as completed normally
    assert service.completed == [(job.id, True, None)]


@pytest.mark.asyncio
async def test_run_processes_jobs_concurrently(monkeypatch):
    """Worker.run should start multiple jobs concurrently up to max_concurrent_jobs."""
    dispatcher = StubDispatcher(result=True)
    service = StubJobService()

    # Two distinct jobs for the same queue
    job1 = make_job(id="job-1")
    job2 = make_job(id="job-2")

    async def fake_get_next_job(
        queue_names, timeout=0, worker_id=None, default_job_timeout=None
    ):
        # First call -> job1, second call -> job2, then no more jobs
        call_count = getattr(fake_get_next_job, "call_count", 0)
        call_count += 1
        fake_get_next_job.call_count = call_count

        if call_count == 1:
            return job1
        if call_count == 2:
            return job2
        # After both jobs have been dispatched, no new jobs
        await asyncio.sleep(0)
        return None

    # Monkeypatch service.get_next_job used by BaseWorker.run
    service.get_next_job = fake_get_next_job  # type: ignore[assignment]

    # Events to coordinate execution and to detect true concurrency
    job1_started = asyncio.Event()
    job2_started = asyncio.Event()
    job1_may_finish = asyncio.Event()

    async def concurrent_execute(job):
        # Mark when each job starts
        if job.id == "job-1":
            job1_started.set()
            # Block job-1 until we explicitly allow it to finish
            await job1_may_finish.wait()
        elif job.id == "job-2":
            job2_started.set()
        return True

    dispatcher.execute = concurrent_execute  # type: ignore[assignment]

    worker = BaseWorker(handler_dispatcher=dispatcher, job_service=service)
    worker.max_concurrent_jobs = 2
    worker.poll_interval = 0.01

    async def orchestrate_shutdown():
        # Wait until both jobs have actually started executing
        await asyncio.wait_for(job1_started.wait(), timeout=0.5)
        await asyncio.wait_for(job2_started.wait(), timeout=0.5)

        # At this point, job2 has started while job1 is still blocked -> true concurrency
        worker.shutdown_requested = True
        # Allow job1 to complete so run() can exit cleanly
        job1_may_finish.set()

    run_task = asyncio.create_task(worker.run())
    orchestrator = asyncio.create_task(orchestrate_shutdown())

    await asyncio.gather(run_task, orchestrator)

    # Both jobs should have been processed
    assert {j[0] for j in service.completed} == {"job-1", "job-2"}
