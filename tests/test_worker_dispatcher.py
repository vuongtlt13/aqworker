import pytest

from aqworker.handler import BaseHandler, HandlerRegistry
from aqworker.job.models import JobModel
from aqworker.worker.dispatcher import HandlerDispatcher


class AsyncFirstHandler(BaseHandler):
    name = "async-first"

    def handle(self, data):
        self.last_payload = data
        return True


class SyncHandler(BaseHandler):
    name = "sync"

    def handle(self, data):
        self.payload = data
        return data.get("ok", False)


class ErrorHandler(BaseHandler):
    name = "error"

    def handle(self, data):
        raise RuntimeError("boom")


@pytest.fixture
def dispatcher():
    registry = HandlerRegistry()
    registry.register(AsyncFirstHandler)
    registry.register(SyncHandler)
    registry.register(ErrorHandler)
    return HandlerDispatcher(registry)


@pytest.mark.asyncio
async def test_execute_returns_false_when_handler_missing(dispatcher):
    job = JobModel(id="1", handler="missing", data={})
    result = await dispatcher.execute(job)
    assert result is False


@pytest.mark.asyncio
async def test_execute_prefers_handle_async(dispatcher):
    job = JobModel(id="2", handler="async-first", data={"foo": "bar"})
    result = await dispatcher.execute(job)
    assert result is True


@pytest.mark.asyncio
async def test_execute_handles_sync_and_exceptions(dispatcher):
    sync_job = JobModel(id="3", handler="sync", data={"ok": True})
    assert await dispatcher.execute(sync_job) is True

    error_job = JobModel(id="4", handler="error", data={})
    assert await dispatcher.execute(error_job) is False
