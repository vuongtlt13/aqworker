from dataclasses import dataclass, field

import pytest

from aqworker.handler import BaseHandler, HandlerRegistry
from aqworker.worker.base import BaseWorker, WorkerConfig
from aqworker.worker.registry import WorkerRegistry


@pytest.fixture
def handler_registry():
    return HandlerRegistry()


@pytest.fixture
def worker_registry():
    return WorkerRegistry()


def test_subclass_auto_registers_with_declared_name(handler_registry: HandlerRegistry):
    class EmailHandler(BaseHandler):
        name = "email_handler"

        def handle(self, data):
            return True

    handler_registry.register(EmailHandler)
    assert handler_registry.get("email_handler") is EmailHandler


def test_duplicate_handler_names_raise_value_error(handler_registry: HandlerRegistry):
    class FirstHandler(BaseHandler):
        name = "duplicate"

        def handle(self, data):
            return True

    class SecondHandler(BaseHandler):
        name = "duplicate"

        def handle(self, data):
            return True

    handler_registry.register(FirstHandler)
    with pytest.raises(ValueError):
        handler_registry.register(SecondHandler)


def test_default_handler_name_falls_back_to_class_name(
    handler_registry: HandlerRegistry,
):
    class InventoryHandler(BaseHandler):
        def handle(self, data):
            return True

    handler_registry.register(InventoryHandler)
    assert handler_registry.get("InventoryHandler") is InventoryHandler


def test_worker_name_defaults_to_snake_case_without_suffix(
    worker_registry: WorkerRegistry,
):
    class InventorySyncWorker(BaseWorker):
        worker_config = {"queue_names": ["inventory"]}

    worker_registry.register(InventorySyncWorker)
    definition = worker_registry.get_definition("inventory_sync")
    assert definition is not None
    assert definition.queue_names == ("inventory",)
    assert definition.worker_class is InventorySyncWorker


def test_queue_names_from_class_attribute_override_config(
    worker_registry: WorkerRegistry,
):
    class MultiQueueWorker(BaseWorker):
        queue_names = ["high", "low"]

    worker_registry.register(MultiQueueWorker)
    definition = worker_registry.get_definition("multi_queue")
    assert definition.queue_names == ("high", "low")


def test_worker_name_attribute_overrides_default(worker_registry: WorkerRegistry):
    class CustomNamedWorker(BaseWorker):
        worker_name = "reporting"
        worker_config = {"queue_names": ["reports"]}

    worker_registry.register(CustomNamedWorker)
    definition = worker_registry.get_definition("reporting")
    assert definition is not None
    assert definition.queue_names == ("reports",)


def test_worker_config_dataclass_instance_supported(worker_registry: WorkerRegistry):
    class DataclassWorker(BaseWorker):
        worker_config = WorkerConfig(
            queue_names=["background"], max_concurrent_jobs=10, job_timeout=30
        )

    worker_registry.register(DataclassWorker)
    definition = worker_registry.get_definition("dataclass")
    assert definition.queue_names == ("background",)


def test_worker_config_class_is_instantiated(worker_registry: WorkerRegistry):
    @dataclass
    class ReportsConfig:
        queue_names: list[str] = field(default_factory=lambda: ["reports"])
        poll_interval: float = 0.05

    class ReportsWorker(BaseWorker):
        worker_config = ReportsConfig

    worker_registry.register(ReportsWorker)
    definition = worker_registry.get_definition("reports")
    assert definition.queue_names == ("reports",)
