from aqworker import AQWorker
from aqworker.handler import HandlerRegistry
from aqworker.worker.registry import WorkerRegistry


def test_worker_registry_register_workers():
    from tests.examples.workers import priority_worker, sample_worker

    registry = WorkerRegistry()
    registry.register(sample_worker.BackgroundWorker)
    registry.register(priority_worker.PriorityWorker)

    assert registry.list_names() == ["background", "priority"]


def test_handler_registry_register_handlers():
    from tests.examples.handlers import email, sms

    registry = HandlerRegistry()
    registry.register(email.EmailJob)
    registry.register(sms.SmsHandler)

    assert sorted(registry.snapshot().keys()) == ["email", "sms"]


def test_include_packages_auto_registers_handlers():
    aq_worker = AQWorker(include_packages=["tests.examples.handlers"])

    assert sorted(aq_worker.handler_registry.snapshot().keys()) == ["email", "sms"]
