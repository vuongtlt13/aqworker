import pytest

from aqworker import AQWorker
from aqworker.cli import commands as cli_cmds
from tests.examples.handlers import email, sms
from tests.examples.workers import priority_worker, sample_worker


@pytest.fixture
def aq_worker():
    worker = AQWorker()
    worker.register_worker(sample_worker.BackgroundWorker)
    worker.register_worker(priority_worker.PriorityWorker)
    worker.register_handler(email.EmailHandler)
    worker.register_handler(sms.SmsHandler)
    return worker


def test_list_worker_names_sorted(aq_worker):
    assert cli_cmds.list_worker_names(aq_worker) == ["background", "priority"]


def test_list_worker_names_empty():
    worker = AQWorker()
    assert cli_cmds.list_worker_names(worker) == []


def test_list_handler_descriptors(aq_worker):
    descriptors = cli_cmds.list_handler_descriptors(aq_worker)
    assert descriptors == [
        ("email", "tests.examples.handlers.email.EmailHandler"),
        ("sms", "tests.examples.handlers.sms.SmsHandler"),
    ]


def test_list_queue_names(aq_worker):
    queues = cli_cmds.list_queue_names(aq_worker)
    assert queues == ["background", "bulk", "priority"]


def test_list_queue_names_no_queues():
    worker = AQWorker()
    assert cli_cmds.list_queue_names(worker) == []


def test_build_worker_returns_instance(aq_worker):
    worker_instance = cli_cmds.build_worker(aq_worker, "background")
    assert worker_instance.__class__.__name__ == "BackgroundWorker"


def test_run_worker_invokes_coroutine(aq_worker):
    worker_cls = sample_worker.BackgroundWorker
    worker_cls.last_run = None
    cli_cmds.run_worker(aq_worker, "background")
    assert worker_cls.last_run is not None


def test_get_queue_stats_uses_job_service_instance(aq_worker):
    class FakeService:
        def __init__(self):
            self.queues = None

        async def get_queue_stats(self, queues):
            self.queues = queues
            return {"pending": len(queues)}

    fake = FakeService()
    stats = cli_cmds.get_queue_stats("background", fake)
    assert stats == {"pending": 1}
    assert fake.queues == ["background"]


@pytest.mark.asyncio
async def test_get_queue_stats_async_requires_service():
    with pytest.raises(ValueError):
        await cli_cmds.get_queue_stats_async("background")
