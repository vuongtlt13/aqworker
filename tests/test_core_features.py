import logging

import pytest

from aqworker import AQWorker
from aqworker.worker.base import BaseWorker


class DemoWorker(BaseWorker):
    worker_config = {"queue_names": ["primary"], "poll_interval": 0.05}


def test_listen_replaces_job_service():
    worker = AQWorker()
    sentinel_service = object()
    worker.listen(sentinel_service)
    assert worker.job_service is sentinel_service


def test_create_worker_validates_names():
    worker = AQWorker()
    worker.register_worker(DemoWorker)

    instance = worker.create_worker("demo")
    assert isinstance(instance, DemoWorker)

    with pytest.raises(ValueError) as exc:
        worker.create_worker("unknown")
    assert "Unknown aq_worker type" in str(exc.value)


def test_get_available_workers_caches_registry():
    worker = AQWorker()
    worker.register_worker(DemoWorker)
    assert worker.get_available_workers() == ["demo"]


@pytest.mark.asyncio
async def test_handler_decorator_registers_sync_and_async_handlers():
    worker = AQWorker()

    @worker.handler("sync_task")
    def sync_handler(payload):
        return True

    @worker.handler("async_task")
    async def async_handler(payload):
        return True

    handlers = worker.handler_registry.snapshot()
    assert "sync_task" in handlers
    assert "async_task" in handlers

    sync_cls = handlers["sync_task"]
    async_cls = handlers["async_task"]
    sync_instance = sync_cls()
    async_instance = async_cls()

    assert await sync_instance.handle({"value": 1}) == True
    assert await async_instance.handle({"value": 2}) == True


def test_autodiscover_warns_when_package_missing():
    worker = AQWorker()
    before = worker.handler_registry.snapshot()
    worker.autodiscover_handlers(["does.not.exist"])
    after = worker.handler_registry.snapshot()
    assert before == after
