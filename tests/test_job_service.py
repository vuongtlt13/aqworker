from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from aqworker.constants import get_job_status_key
from aqworker.handler import BaseHandler
from aqworker.job.models import JobCreateRequest, JobModel, JobStatus, JobStatusInfo
from aqworker.job.service import JobService


class DummyRedis:
    def __init__(self):
        self.hash_sets = {}
        self.lists = defaultdict(list)
        self.deleted_keys = []

    async def hset(self, key, mapping):
        self.hash_sets[key] = mapping
        return 1

    async def lrange(self, key, start, end):
        return list(self.lists.get(key, []))

    async def lrem(self, key, count, value):
        items = self.lists.get(key, [])
        removed = 0
        result = []
        for item in items:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            result.append(item)
        self.lists[key] = result
        return removed

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.lists:
                removed += 1
                self.lists.pop(key, None)
            if key in self.hash_sets:
                removed += 1
                self.hash_sets.pop(key, None)
            self.deleted_keys.append(key)
        return removed


class DummyQueue:
    def __init__(self):
        self.redis_client = DummyRedis()
        self.completed_queue = "completed"
        self.failed_queue = "failed"
        self.jobs = {}
        self.enqueued_jobs = []
        self.stats_args = None
        self.dequeued_args = None
        self.next_job = None
        self.completed = None
        self.cleaned_age = None
        self.clear_called = False
        self.released = None

    async def enqueue(self, job):
        self.jobs[job.id] = job
        self.enqueued_jobs.append(job.id)
        return True

    async def get_job(self, job_id):
        return self.jobs.get(job_id)

    async def get_job_status(self, job_id):
        job = self.jobs.get(job_id)
        if not job:
            return None
        from aqworker.job.models import JobStatusInfo

        return JobStatusInfo(
            status=job.status,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_message=job.error_message,
            worker_id=job.worker_id,
        )

    async def get_queue_stats(self, queue_names):
        self.stats_args = queue_names
        return {"pending": len(self.jobs)}

    async def dequeue(self, queue_names, timeout, worker_id):
        self.dequeued_args = (queue_names, timeout, worker_id)
        return self.next_job

    async def complete_job(self, job, success, error_message=None):
        self.completed = (job.id, success, error_message)
        return True

    async def cleanup_stale_processing_jobs(self, max_age_seconds):
        self.cleaned_age = max_age_seconds
        return 3

    async def clear_processing_queue(self):
        self.clear_called = True
        return True

    async def release_job_lock(self, job_id, worker_id):
        self.released = (job_id, worker_id)
        return True


@pytest.fixture
def job_service():
    service = JobService()
    queue = DummyQueue()
    service._queue = queue
    return service, queue


@pytest.mark.asyncio
async def test_create_job_enqueues_request(job_service):
    service, queue = job_service
    request = JobCreateRequest(queue_name="tasks", handler="email")
    job = await service._create_job(request)
    assert queue.jobs[job.id] is job


class NamedHandler(BaseHandler):
    name = "named-handler"

    def handle(self, data):
        raise NotImplementedError


@pytest.mark.asyncio
async def test_enqueue_job_accepts_handler_class(job_service):
    service, queue = job_service
    job = await service.enqueue_job(NamedHandler, queue_name="tasks", data={"value": 1})
    assert job.handler == "named-handler"
    assert queue.enqueued_jobs[-1] == job.id


@pytest.mark.asyncio
async def test_enqueue_job_rejects_invalid_handler(job_service):
    service, _queue = job_service
    with pytest.raises(ValueError):
        await service.enqueue_job("tasks", 123)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_get_job_and_stats_use_queue(job_service):
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")

    fetched = await service.get_job(job.id)
    assert fetched.id == job.id

    stats = await service.get_queue_stats(["tasks"])
    assert stats["pending"] == 1
    assert queue.stats_args == ["tasks"]


@pytest.mark.asyncio
async def test_get_job_status_returns_lightweight_info(job_service):
    """Test that get_job_status returns status info without full job data."""
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")

    status_info = await service.get_job_status(job.id)
    assert status_info is not None
    assert isinstance(status_info, JobStatusInfo)
    assert status_info.status == JobStatus.PENDING
    assert status_info.created_at is not None
    assert status_info.completed_at is None
    assert status_info.error_message is None


@pytest.mark.asyncio
async def test_get_job_status_returns_none_for_missing_job(job_service):
    """Test get_job_status returns None for non-existent job."""
    service, queue = job_service

    status_info = await service.get_job_status("non-existent-job-id")
    assert status_info is None


@pytest.mark.asyncio
async def test_cancel_job_updates_pending_status(job_service):
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")
    assert await service.cancel_job(job.id) is True

    key = get_job_status_key(job.id)
    assert queue.redis_client.hash_sets[key]["status"] == JobStatus.CANCELLED.value
    # Verify data field is updated with new status
    data_job = JobModel.model_validate_json(queue.redis_client.hash_sets[key]["data"])
    assert data_job.status == JobStatus.CANCELLED
    assert data_job.completed_at is not None


@pytest.mark.asyncio
async def test_cancel_job_rejects_non_pending(job_service):
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")
    queue.jobs[job.id].status = JobStatus.FAILED
    assert await service.cancel_job(job.id) is False


@pytest.mark.asyncio
async def test_retry_job_enqueues_failed(job_service):
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")
    job.status = JobStatus.FAILED
    job.retry_count = 0
    job.max_retries = 2
    queue.jobs[job.id] = job

    assert await service.retry_job(job.id) is True
    assert queue.enqueued_jobs.count(job.id) == 2


@pytest.mark.asyncio
async def test_retry_job_respects_budget(job_service):
    service, queue = job_service
    job = await service.enqueue_job("tasks", "handler")
    job.status = JobStatus.FAILED
    job.retry_count = 3
    job.max_retries = 3
    queue.jobs[job.id] = job
    assert await service.retry_job(job.id) is False


@pytest.mark.asyncio
async def test_cleanup_old_jobs_removes_entries(job_service):
    service, queue = job_service
    old_time = datetime.now(timezone.utc) - timedelta(days=8)
    completed_job = JobModel(
        id="completed",
        handler="h",
        queue_name="tasks",
        completed_at=old_time,
    )
    failed_job = completed_job.model_copy(update={"id": "failed"})

    queue.redis_client.lists[queue.completed_queue].append(
        completed_job.model_dump_json()
    )
    queue.redis_client.lists[queue.failed_queue].append(failed_job.model_dump_json())
    cleaned = await service.cleanup_old_jobs(days=7)
    assert cleaned == 2
    assert get_job_status_key("completed") in queue.redis_client.deleted_keys


@pytest.mark.asyncio
async def test_get_next_job_and_complete(job_service):
    service, queue = job_service
    queue.next_job = JobModel(id="next", handler="h", queue_name="tasks")

    job = await service.get_next_job(["tasks"], timeout=1, worker_id="w1")
    assert job.id == "next"
    assert queue.dequeued_args == (["tasks"], 1, "w1")

    assert await service.complete_job(job, True) is True
    assert queue.completed == ("next", True, None)


@pytest.mark.asyncio
async def test_cleanup_and_release_helpers(job_service):
    service, queue = job_service
    assert await service.cleanup_stale_processing_jobs(100) == 3
    assert queue.cleaned_age == 100

    assert await service.clear_processing_queue() is True
    assert queue.clear_called is True

    assert await service.release_job_lock("job-1", "w1") is True
    assert queue.released == ("job-1", "w1")
