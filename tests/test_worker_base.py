import asyncio
import time

import pytest

from aqworker.job.models import Job
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
        self.stats_calls = 0
        self.stats_value = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}

    async def complete_job(self, job, success, error_message=None):
        self.completed.append((job.id, success, error_message))

    async def release_job_lock(self, job_id, worker_id):
        self.released.append((job_id, worker_id))

    async def retry_job(self, job_id):
        self.retried.append(job_id)

    async def get_queue_stats(self, queue_names):
        self.stats_calls += 1
        return self.stats_value


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
    return Job(**defaults)


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
async def test_retry_or_fail_waits_before_retry(monkeypatch):
    service = StubJobService()
    worker = BaseWorker(handler_dispatcher=StubDispatcher(), job_service=service)
    job = make_job(max_retries=3, retry_count=0, retry_delay=0.01)

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await worker._retry_or_fail(job, "boom")

    assert service.retried == [job.id]
    assert sleep_calls == [0.01]


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


def test_shutdown_waits_for_running_jobs(monkeypatch):
    worker = BaseWorker(
        handler_dispatcher=StubDispatcher(), job_service=StubJobService()
    )
    worker.running_jobs = {"job-1"}

    def fake_sleep(seconds):
        worker.running_jobs.clear()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    worker._shutdown()
    assert worker.running_jobs == set()
