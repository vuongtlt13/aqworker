from datetime import datetime, timedelta, timezone

import pytest

from aqworker.constants import get_job_lock_key, get_job_status_key
from aqworker.job.models import JobModel, JobStatus
from aqworker.job.queue import JobQueue

pytestmark = pytest.mark.asyncio


def make_job(**overrides):
    defaults = {
        "id": "job-123",
        "queue_name": "emails",
        "handler": "email",
        "data": {"recipient": "user@example.com"},
    }
    defaults.update(overrides)
    return JobModel(**defaults)


async def _push_job(redis_client, key: str, job: JobModel):
    await redis_client.rpush(key, job.model_dump_json())


async def test_enqueue_pushes_payload_and_sets_status(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job()
    assert await queue.enqueue(job) is True

    queue_key = queue._get_queue_name(job.queue_name)
    assert await redis_client.lrange(queue_key, 0, -1) == [job.model_dump_json()]
    status = await redis_client.hgetall(get_job_status_key(job.id))
    assert status["status"] == JobStatus.PENDING.value
    assert status["data"] == job.model_dump_json()


async def test_dequeue_updates_status_and_moves_to_processing(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job()
    await _push_job(redis_client, queue._get_queue_name(job.queue_name), job)

    result = await queue.dequeue(["emails"], timeout=0, worker_id="worker-1")

    assert result.id == job.id
    assert result.status == JobStatus.PROCESSING
    processing_entries = await redis_client.lrange(queue.processing_queue, 0, -1)
    assert len(processing_entries) == 1
    persisted = JobModel.model_validate_json(processing_entries[0])
    assert persisted.status == JobStatus.PROCESSING
    status = await redis_client.hgetall(get_job_status_key(job.id))
    assert status["worker_id"] == "worker-1"


async def test_dequeue_requeues_when_lock_not_acquired(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job()
    queue_key = queue._get_queue_name(job.queue_name)
    await _push_job(redis_client, queue_key, job)
    await redis_client.set(get_job_lock_key(job.id), "other-worker")

    result = await queue.dequeue(["emails"], timeout=0, worker_id="worker-1")

    assert result is None
    assert await redis_client.llen(queue.processing_queue) == 0
    assert await redis_client.llen(queue_key) == 1


async def test_dequeue_skips_empty_queue_entries(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job(queue_name="notifications", id="job-456")
    await _push_job(redis_client, queue._get_queue_name("notifications"), job)

    result = await queue.dequeue(
        ["emails", "notifications"], timeout=0, worker_id="worker-1"
    )

    assert result.id == "job-456"
    assert await redis_client.llen(queue.processing_queue) == 1


async def test_acquire_job_lock_success(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    acquired = await queue._acquire_job_lock("job-1", "worker-1", lock_timeout=10)
    assert acquired is True
    assert await redis_client.get(get_job_lock_key("job-1")) == "worker-1"


async def test_acquire_job_lock_failure(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client
    await redis_client.set(get_job_lock_key("job-2"), "other")

    acquired = await queue._acquire_job_lock("job-2", "worker-9", lock_timeout=20)
    assert acquired is False


async def test_release_job_lock_uses_lua_script(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client
    await redis_client.set(get_job_lock_key("job-1"), "worker-1")

    async def fake_eval(script, num_keys, key, worker):
        if await redis_client.get(key) == worker:
            await redis_client.delete(key)
            return 1
        return 0

    queue.redis_client.eval = fake_eval  # type: ignore[assignment]

    assert await queue.release_job_lock("job-1", "worker-1") is True
    assert await redis_client.get(get_job_lock_key("job-1")) is None


async def test_complete_job_moves_payload(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job()
    await redis_client.lpush(queue.processing_queue, job.model_dump_json())

    assert await queue.complete_job(job, success=True) is True

    completed = await redis_client.lrange(queue.completed_queue, 0, -1)
    assert len(completed) == 1
    persisted = JobModel.model_validate_json(completed[0])
    assert persisted.status == JobStatus.COMPLETED
    assert await redis_client.llen(queue.processing_queue) == 0


async def test_complete_job_handles_failure(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    job = make_job()
    await redis_client.lpush(queue.processing_queue, job.model_dump_json())

    assert await queue.complete_job(job, success=False, error_message="boom") is True
    failed = await redis_client.lrange(queue.failed_queue, 0, -1)
    assert len(failed) == 1
    persisted = JobModel.model_validate_json(failed[0])
    assert persisted.status == JobStatus.FAILED
    status = await redis_client.hgetall(get_job_status_key(job.id))
    assert status["error_message"] == "boom"


async def test_get_job_status_returns_job(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client
    job = make_job()
    await redis_client.hset(
        get_job_status_key(job.id), mapping={"data": job.model_dump_json()}
    )

    result = await queue.get_job_status(job.id)
    assert result.id == job.id


async def test_cleanup_old_entries_removes_stale_entries(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    recent_job = make_job(id="recent")
    recent_job.completed_at = datetime.now(timezone.utc)
    old_job = make_job(id="old")
    old_job.completed_at = datetime.now(timezone.utc) - timedelta(seconds=1000)

    await redis_client.lpush(queue.completed_queue, recent_job.model_dump_json())
    await redis_client.lpush(queue.completed_queue, old_job.model_dump_json())

    await queue._cleanup_old_entries(queue.completed_queue, max_age_seconds=500)
    remaining = await redis_client.lrange(queue.completed_queue, 0, -1)
    assert old_job.model_dump_json() not in remaining
    assert recent_job.model_dump_json() in remaining


async def test_get_queue_stats_counts_queues(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    # Pending
    job1 = make_job(id="pending-1")
    job2 = make_job(id="pending-2")
    await redis_client.lpush(
        queue._get_queue_name("emails"), job1.model_dump_json(), job2.model_dump_json()
    )

    # Processing
    for idx in range(3):
        proc = make_job(id=f"proc-{idx}")
        proc.status = JobStatus.PROCESSING
        await redis_client.lpush(queue.processing_queue, proc.model_dump_json())

    # Completed
    for idx in range(4):
        done = make_job(id=f"done-{idx}")
        done.status = JobStatus.COMPLETED
        await redis_client.lpush(queue.completed_queue, done.model_dump_json())

    # Failed
    failed = make_job(id="failed-1")
    failed.status = JobStatus.FAILED
    await redis_client.lpush(queue.failed_queue, failed.model_dump_json())

    stats = await queue.get_queue_stats(["emails"])
    assert stats["pending"] == 2
    assert stats["processing"] == 3
    assert stats["completed"] == 4
    assert stats["failed"] == 1
    assert stats["processing_rate"] == 0.8
    assert stats["processing_ratio"] == pytest.approx(0.75)


async def test_cleanup_stale_processing_jobs_removes_expired(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    stale_job = make_job(id="old-job")
    stale_job.started_at = datetime.now(timezone.utc) - timedelta(seconds=601)
    await redis_client.lpush(queue.processing_queue, stale_job.model_dump_json())

    removed = await queue.cleanup_stale_processing_jobs(600)
    assert removed == 1
    assert await redis_client.llen(queue.processing_queue) == 0


async def test_clear_queue_variants(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    await redis_client.lpush(queue.processing_queue, "job")
    await redis_client.lpush(queue.completed_queue, "job")
    await redis_client.lpush(queue.failed_queue, "job")

    assert await queue.clear_queue("all") is True
    assert await redis_client.llen(queue.processing_queue) == 0
    assert await redis_client.llen(queue.completed_queue) == 0
    assert await redis_client.llen(queue.failed_queue) == 0

    await redis_client.lpush(queue.completed_queue, "job")
    assert await queue.clear_queue("completed") is True
    assert await redis_client.llen(queue.completed_queue) == 0


async def test_clear_processing_queue(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client

    await redis_client.lpush(queue.processing_queue, "job")
    assert await queue.clear_processing_queue() is True
    assert await redis_client.llen(queue.processing_queue) == 0


async def test_remove_from_processing_queue_success(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client
    job = make_job()
    await redis_client.lpush(queue.processing_queue, job.model_dump_json())

    assert await queue._remove_from_processing_queue(job) is True
    assert await redis_client.llen(queue.processing_queue) == 0


async def test_remove_from_processing_queue_by_id_handles_matches(redis_client):
    queue = JobQueue("redis://localhost:6379/0")
    queue.redis_client = redis_client
    target_job = make_job(id="target")
    other_job = make_job(id="other")
    await redis_client.lpush(
        queue.processing_queue,
        target_job.model_dump_json(),
        other_job.model_dump_json(),
    )

    assert await queue._remove_job_from_processing_queue_by_id("target") is True
    remaining = await redis_client.lrange(queue.processing_queue, 0, -1)
    assert target_job.model_dump_json() not in remaining
