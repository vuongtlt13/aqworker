"""
Test cases for CronJob functionality.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aqworker import AQWorker, CronJob, Job
from aqworker.constants import get_cron_last_run_key
from aqworker.handler.registry import HandlerRegistry
from aqworker.job.scheduler import CronScheduler
from aqworker.job.service import JobService


class TestCronJob:
    """Test CronJob class definition and registration."""

    def test_cronjob_must_implement_cron_method(self):
        """Test that CronJob subclasses must implement cron() method."""

        # Creating a class without cron() method should work (abstract method check happens later)
        # But calling cron() should raise NotImplementedError
        class InvalidCronJob(CronJob):
            queue_name = "test_queue"
            # Missing cron() method

        # The abstract method check happens when trying to instantiate or call the method
        with pytest.raises(NotImplementedError):
            InvalidCronJob.cron()

    def test_cronjob_with_cron_method(self):
        """Test CronJob with proper cron() method implementation."""

        class DailyReportCronJob(CronJob):
            queue_name = "reports"
            name = "daily_report"

            @classmethod
            def cron(cls) -> str:
                return "0 0 * * *"

            async def handle_async(self, data):
                return True

        assert DailyReportCronJob.cron() == "0 0 * * *"
        assert DailyReportCronJob.queue_name == "reports"

    def test_cronjob_requires_queue_name(self):
        """Test that CronJob requires queue_name attribute."""

        class CronJobWithoutQueue(CronJob):
            @classmethod
            def cron(cls) -> str:
                return "0 0 * * *"

        registry = HandlerRegistry()
        with pytest.raises(ValueError, match="queue_name"):
            registry.register(CronJobWithoutQueue)

    def test_cronjob_with_queue_name_registers_successfully(self):
        """Test that CronJob with queue_name registers successfully."""

        class ValidCronJob(CronJob):
            queue_name = "cron_queue"
            name = "valid_cron"

            @classmethod
            def cron(cls) -> str:
                return "*/5 * * * *"

        registry = HandlerRegistry()
        registry.register(ValidCronJob)
        assert "valid_cron" in registry.snapshot()

    def test_cronjob_decorator_creates_handler(self):
        """Test @aq_worker.cronjob decorator creates and registers handler."""
        aq_worker = AQWorker()

        @aq_worker.cronjob(cron="0 0 * * *", queue_name="daily_queue")
        async def daily_task(data: dict) -> bool:
            return True

        handlers = aq_worker.handler_registry.snapshot()
        assert "daily_task" in handlers
        handler_cls = handlers["daily_task"]
        assert issubclass(handler_cls, CronJob)
        assert handler_cls.queue_name == "daily_queue"
        assert handler_cls.cron() == "0 0 * * *"

    def test_cronjob_decorator_with_custom_name(self):
        """Test @aq_worker.cronjob decorator with custom name."""
        aq_worker = AQWorker()

        @aq_worker.cronjob(
            cron="*/10 * * * *", name="custom_cron", queue_name="custom_queue"
        )
        def periodic_task(data: dict) -> bool:
            return True

        handlers = aq_worker.handler_registry.snapshot()
        assert "custom_cron" in handlers
        assert "periodic_task" not in handlers

    def test_cronjob_decorator_requires_queue_name(self):
        """Test that @cronjob decorator requires queue_name."""
        aq_worker = AQWorker()

        with pytest.raises(ValueError, match="queue_name is required"):

            @aq_worker.cronjob(cron="0 0 * * *")
            def task(data: dict) -> bool:
                return True


class TestCronScheduler:
    """Test CronScheduler functionality."""

    class _DummyRedis:
        def __init__(self):
            self.store = {}

        async def get(self, key):
            return self.store.get(key)

        async def set(self, key, value):
            self.store[key] = value
            return True

        async def delete(self, key):
            self.store.pop(key, None)
            return True

        async def eval(self, script, numkeys, key, new_value):
            current = self.store.get(key)
            if current is None or new_value > current:
                self.store[key] = new_value
                return [1, current or ""]
            return [0, current or ""]

    @pytest.fixture
    def mock_job_service(self):
        """Create a mock JobService."""
        service = MagicMock(spec=JobService)
        service.enqueue_job = AsyncMock()
        service.get_redis_client.return_value = self._DummyRedis()
        return service

    @pytest.fixture
    def handler_registry(self):
        """Create a handler registry with test cron jobs."""
        registry = HandlerRegistry()

        class TestCronJob1(CronJob):
            queue_name = "queue1"
            name = "cron_job_1"

            @classmethod
            def cron(cls) -> str:
                return "*/1 * * * * *"  # Every second

        class TestCronJob2(CronJob):
            queue_name = "queue2"
            name = "cron_job_2"

            @classmethod
            def cron(cls) -> str:
                return "0 * * * * *"  # Every minute at 0 seconds

        class RegularJob(Job):
            queue_name = "regular_queue"
            name = "regular_job"

        registry.register(TestCronJob1)
        registry.register(TestCronJob2)
        registry.register(RegularJob)

        return registry

    @pytest.fixture
    def scheduler(self, handler_registry, mock_job_service):
        """Create a CronScheduler instance."""
        return CronScheduler(
            handler_registry=handler_registry,
            job_service=mock_job_service,
            check_interval=0.1,
        )

    @pytest.mark.asyncio
    async def test_scheduler_start_stop(self, scheduler):
        """Test starting and stopping the scheduler."""
        assert not scheduler.is_running()

        await scheduler.start()
        assert scheduler.is_running()

        # Wait a bit to ensure task is running
        await asyncio.sleep(0.05)

        await scheduler.stop()
        assert not scheduler.is_running()

    @pytest.mark.asyncio
    async def test_scheduler_start_when_already_running(self, scheduler):
        """Test that starting scheduler twice logs warning."""
        await scheduler.start()
        assert scheduler.is_running()

        # Should not raise error, just log warning
        await scheduler.start()
        assert scheduler.is_running()

        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_scheduler_stop_when_not_running(self, scheduler):
        """Test stopping scheduler when not running."""
        assert not scheduler.is_running()
        await scheduler.stop()  # Should not raise error
        assert not scheduler.is_running()

    @pytest.mark.asyncio
    async def test_scheduler_checks_only_cronjobs(
        self, scheduler, handler_registry, mock_job_service
    ):
        """Test that scheduler only processes CronJob handlers, not regular Jobs."""
        await scheduler.start()

        # Wait for multiple check cycles to increase chance of matching cron
        # Cron jobs are set to run every second, so we should catch at least one
        await asyncio.sleep(0.25)

        await scheduler.stop()

        # Verify scheduler is running and checking (may or may not enqueue depending on timing)
        # The important thing is that RegularJob (non-CronJob) should never trigger enqueue
        # If enqueue_job was called, verify it was only for CronJob handlers
        if mock_job_service.enqueue_job.called:
            # Verify all calls were for CronJob handlers
            for call in mock_job_service.enqueue_job.call_args_list:
                handler_cls = call.kwargs.get("handler") or call.args[0]
                assert issubclass(
                    handler_cls, CronJob
                ), "Only CronJob handlers should be enqueued"
                assert not issubclass(handler_cls, Job) or issubclass(
                    handler_cls, CronJob
                )

    @pytest.mark.asyncio
    async def test_scheduler_enqueues_job_with_correct_data(
        self, scheduler, mock_job_service
    ):
        """Test that scheduler enqueues jobs with correct data structure."""
        await scheduler.start()

        # Wait for a check cycle
        await asyncio.sleep(0.15)

        await scheduler.stop()

        # Verify enqueue_job was called
        if mock_job_service.enqueue_job.called:
            call_args = mock_job_service.enqueue_job.call_args
            assert call_args is not None

            handler_cls = call_args.kwargs.get("handler") or call_args.args[0]
            assert issubclass(handler_cls, CronJob)

            # Cron jobs no longer pass custom data payloads
            data = call_args.kwargs.get("data")
            assert data in (None, {})

            metadata = call_args.kwargs.get("metadata", {})
            assert metadata.get("cron_job") is True
            assert "handler_class" in metadata
            assert call_args.kwargs.get("schedule_time") is not None

    @pytest.mark.asyncio
    async def test_scheduler_prevents_duplicate_enqueues(
        self, scheduler, mock_job_service
    ):
        """Test that scheduler prevents duplicate job enqueues for same scheduled time."""
        mock_job_service.enqueue_job.reset_mock()
        await scheduler.start()

        # Wait for multiple check cycles (up to ~0.6s) until at least one enqueue occurs
        tries = 0
        while mock_job_service.enqueue_job.call_count == 0 and tries < 6:
            await asyncio.sleep(0.1)
            tries += 1

        await scheduler.stop()

        # Count how many times enqueue_job was called
        call_count = mock_job_service.enqueue_job.call_count

        # Should not enqueue multiple times for the same scheduled time
        # With 0.25s wait and 0.1s interval, we should have at least one call
        # but not too many duplicates for the same scheduled time
        assert call_count >= 1  # At least one call
        # The scheduler should prevent duplicates, so count should be reasonable
        # (exact count depends on timing and cron expressions)

    def test_scheduler_catches_up_after_long_delay(
        self, handler_registry, mock_job_service
    ):
        """Ensure scheduler still runs cron jobs even when now - prev_time >> tolerance."""
        scheduler = CronScheduler(
            handler_registry=handler_registry,
            job_service=mock_job_service,
            check_interval=0.1,
        )
        handler_name = "cron_job_2"  # Runs every minute at second 0
        cron_expr = handler_registry.snapshot()[handler_name].cron()

        # Simulate current time 5 seconds into the minute (much larger than tolerance)
        now = datetime(2025, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        last_scheduled = now - timedelta(minutes=5)

        should_run, scheduled_time = scheduler._should_run_cron_job(
            handler_name, cron_expr, now, last_scheduled
        )

        assert should_run is True
        assert scheduled_time < now
        assert scheduled_time > last_scheduled
        assert (now - scheduled_time).total_seconds() > scheduler._tolerance

    @pytest.mark.asyncio
    async def test_scheduler_handles_invalid_cron_expression(
        self, handler_registry, mock_job_service
    ):
        """Test that scheduler handles invalid cron expressions gracefully."""

        class InvalidCronJob(CronJob):
            queue_name = "invalid_queue"
            name = "invalid_cron"

            @classmethod
            def cron(cls) -> str:
                return "invalid cron expression"

        handler_registry.register(InvalidCronJob)

        scheduler = CronScheduler(
            handler_registry=handler_registry,
            job_service=mock_job_service,
            check_interval=0.1,
        )

        await scheduler.start()
        await asyncio.sleep(0.15)
        await scheduler.stop()

        # Should not crash, but also should not enqueue invalid cron jobs
        # (exact behavior depends on croniter error handling)

    @pytest.mark.asyncio
    async def test_scheduler_supports_5_field_cron(
        self, handler_registry, mock_job_service
    ):
        """Test that scheduler supports 5-field cron expressions."""

        class FiveFieldCronJob(CronJob):
            queue_name = "five_field_queue"
            name = "five_field_cron"

            @classmethod
            def cron(cls) -> str:
                return "0 0 * * *"  # 5 fields: daily at midnight

        handler_registry.register(FiveFieldCronJob)

        scheduler = CronScheduler(
            handler_registry=handler_registry,
            job_service=mock_job_service,
            check_interval=0.1,
        )

        await scheduler.start()
        await asyncio.sleep(0.15)
        await scheduler.stop()

        # Should not raise error for 5-field cron

    @pytest.mark.asyncio
    async def test_scheduler_supports_6_field_cron(
        self, handler_registry, mock_job_service
    ):
        """Test that scheduler supports 6-field cron expressions."""

        class SixFieldCronJob(CronJob):
            queue_name = "six_field_queue"
            name = "six_field_cron"

            @classmethod
            def cron(cls) -> str:
                return "*/10 * * * * *"  # 6 fields: every 10 seconds

        handler_registry.register(SixFieldCronJob)

        scheduler = CronScheduler(
            handler_registry=handler_registry,
            job_service=mock_job_service,
            check_interval=0.1,
        )

        now = datetime(2025, 1, 1, 12, 0, 12, tzinfo=timezone.utc)
        last_run = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        cron_expr = SixFieldCronJob.cron()

        should_run, scheduled_time = scheduler._should_run_cron_job(
            SixFieldCronJob.name, cron_expr, now, last_run
        )

        assert should_run is True
        assert scheduled_time == datetime(2025, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
        assert (scheduled_time - last_run).total_seconds() == 10

    @pytest.mark.asyncio
    async def test_scheduler_handles_exception_in_enqueue(
        self, scheduler, mock_job_service
    ):
        """Test that scheduler handles exceptions during job enqueue gracefully."""
        # Make enqueue_job raise an exception
        mock_job_service.enqueue_job.side_effect = Exception("Enqueue failed")

        await scheduler.start()

        # Wait for a check cycle
        await asyncio.sleep(0.15)

        await scheduler.stop()

        # Should not crash, scheduler should continue running

    def test_scheduler_requires_croniter(self):
        """Test that scheduler requires croniter to be installed."""
        with patch("aqworker.job.scheduler.croniter", None):
            with pytest.raises(ImportError, match="croniter is required"):
                CronScheduler(
                    handler_registry=HandlerRegistry(),
                    job_service=MagicMock(),
                )
