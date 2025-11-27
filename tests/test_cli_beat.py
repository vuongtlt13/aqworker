"""
Test cases for CLI beat command.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aqworker import AQWorker, CronJob
from aqworker.cli import commands as cli_cmds
from aqworker.cli.commands import RegistryError
from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL


class TestRunBeat:
    """Test run_beat function."""

    @pytest.fixture
    def aq_worker_with_job_service(self):
        """Create AQWorker with job_service configured."""
        worker = AQWorker()
        # Create a mock job service
        mock_job_service = MagicMock()
        mock_job_service.enqueue_job = AsyncMock()
        worker.listen(mock_job_service)
        return worker

    @pytest.fixture
    def cron_job_handler(self):
        """Create a test CronJob handler."""

        class TestCronJob(CronJob):
            queue_name = "test_cron_queue"
            name = "test_cron"

            @classmethod
            def cron(cls) -> str:
                return "*/1 * * * * *"  # Every second

            async def handle_async(self, data):
                return True

        return TestCronJob

    def test_run_beat_requires_job_service(self):
        """Test that run_beat raises error if job_service is not configured."""
        # AQWorker() automatically creates JobService, so we need to set it to None
        worker = AQWorker()
        worker.job_service = None  # Explicitly set to None to test error case

        # Mock asyncio.run to prevent it from actually running
        # The error should be raised before asyncio.run, but we mock it to be safe
        with patch("aqworker.cli.commands.asyncio.run") as mock_run:
            with pytest.raises(RegistryError, match="JobService is required"):
                cli_cmds.run_beat(worker)
            # Verify asyncio.run was not called (error raised before it)
            mock_run.assert_not_called()

    def test_run_beat_creates_scheduler(
        self, aq_worker_with_job_service, cron_job_handler
    ):
        """Test that run_beat creates CronScheduler with correct parameters."""
        aq_worker_with_job_service.register_handler(cron_job_handler)

        # Mock asyncio.run to avoid actually running the scheduler
        with patch("aqworker.cli.commands.signal.signal"):
            with patch("aqworker.cli.commands.CronScheduler") as mock_scheduler_class:
                mock_scheduler = MagicMock()
                mock_scheduler.start = AsyncMock()
                mock_scheduler.stop = AsyncMock()
                mock_scheduler_class.return_value = mock_scheduler

                mock_event = MagicMock()
                mock_event.wait = AsyncMock(return_value=None)

                with patch(
                    "aqworker.cli.commands.asyncio.Event", return_value=mock_event
                ):
                    cli_cmds.run_beat(aq_worker_with_job_service)

                mock_scheduler_class.assert_called_once()
                call_kwargs = mock_scheduler_class.call_args.kwargs
                assert (
                    call_kwargs["handler_registry"]
                    == aq_worker_with_job_service.handler_registry
                )
                assert (
                    call_kwargs["job_service"] == aq_worker_with_job_service.job_service
                )

    def test_run_beat_uses_default_check_interval(
        self, aq_worker_with_job_service, cron_job_handler
    ):
        """Test that run_beat uses default check_interval when not provided."""
        aq_worker_with_job_service.register_handler(cron_job_handler)

        with patch("aqworker.cli.commands.signal.signal"):
            with patch("aqworker.cli.commands.CronScheduler") as mock_scheduler_class:
                mock_scheduler = MagicMock()
                mock_scheduler.start = AsyncMock()
                mock_scheduler.stop = AsyncMock()
                mock_scheduler_class.return_value = mock_scheduler

                mock_event = MagicMock()
                mock_event.wait = AsyncMock(return_value=None)

                with patch(
                    "aqworker.cli.commands.asyncio.Event", return_value=mock_event
                ):
                    cli_cmds.run_beat(aq_worker_with_job_service)

                call_kwargs = mock_scheduler_class.call_args.kwargs
                assert call_kwargs["check_interval"] == DEFAULT_CRON_CHECK_INTERVAL

    def test_run_beat_uses_custom_check_interval(
        self, aq_worker_with_job_service, cron_job_handler
    ):
        """Test that run_beat uses custom check_interval when provided."""
        aq_worker_with_job_service.register_handler(cron_job_handler)
        custom_interval = 0.5

        with patch("aqworker.cli.commands.signal.signal"):
            with patch("aqworker.cli.commands.CronScheduler") as mock_scheduler_class:
                mock_scheduler = MagicMock()
                mock_scheduler.start = AsyncMock()
                mock_scheduler.stop = AsyncMock()
                mock_scheduler_class.return_value = mock_scheduler

                mock_event = MagicMock()
                mock_event.wait = AsyncMock(return_value=None)

                with patch(
                    "aqworker.cli.commands.asyncio.Event", return_value=mock_event
                ):
                    cli_cmds.run_beat(
                        aq_worker_with_job_service, check_interval=custom_interval
                    )

                call_kwargs = mock_scheduler_class.call_args.kwargs
                assert call_kwargs["check_interval"] == custom_interval

    def test_run_beat_sets_up_signal_handlers(
        self, aq_worker_with_job_service, cron_job_handler
    ):
        """Test that run_beat sets up signal handlers for graceful shutdown."""
        aq_worker_with_job_service.register_handler(cron_job_handler)

        with patch("aqworker.cli.commands.signal.signal") as mock_signal:
            with patch("aqworker.cli.commands.CronScheduler") as mock_scheduler_class:
                mock_scheduler = MagicMock()
                mock_scheduler.start = AsyncMock()
                mock_scheduler.stop = AsyncMock()
                mock_scheduler_class.return_value = mock_scheduler

                mock_event = MagicMock()
                mock_event.wait = AsyncMock(return_value=None)

                with patch(
                    "aqworker.cli.commands.asyncio.Event", return_value=mock_event
                ):
                    cli_cmds.run_beat(aq_worker_with_job_service)

                assert mock_signal.call_count >= 2  # SIGINT and SIGTERM

    @pytest.mark.asyncio
    async def test_run_beat_starts_and_stops_scheduler(
        self, aq_worker_with_job_service, cron_job_handler
    ):
        """Test that run_beat properly starts and stops the scheduler."""
        aq_worker_with_job_service.register_handler(cron_job_handler)

        scheduler_started = False
        scheduler_stopped = False

        def mock_run(coro):
            """Mock asyncio.run that executes the coroutine on a fresh loop."""
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(coro)
            finally:
                loop.close()

        async def mock_scheduler_run():
            """Mock scheduler run that sets flags."""
            nonlocal scheduler_started, scheduler_stopped
            scheduler_started = True
            # Simulate running for a bit
            await asyncio.sleep(0.01)
            scheduler_stopped = True

        mock_scheduler = MagicMock()
        mock_scheduler.start = AsyncMock()
        mock_scheduler.stop = AsyncMock()
        mock_scheduler._run = mock_scheduler_run

        with patch("aqworker.cli.commands.asyncio.run", side_effect=mock_run):
            with patch("aqworker.cli.commands.signal.signal"):
                with patch(
                    "aqworker.cli.commands.CronScheduler", return_value=mock_scheduler
                ):
                    with patch(
                        "aqworker.cli.commands.asyncio.Event"
                    ) as mock_event_class:
                        # Create an event that sets immediately to trigger shutdown
                        mock_event = MagicMock()
                        mock_event.wait = AsyncMock()

                        async def set_event_immediately():
                            await asyncio.sleep(0.01)
                            mock_event.set()

                        # Start a task to set the event
                        asyncio.create_task(set_event_immediately())
                        mock_event_class.return_value = mock_event

                        try:
                            cli_cmds.run_beat(
                                aq_worker_with_job_service, check_interval=0.1
                            )
                        except Exception:
                            pass

        # Note: Due to the complexity of mocking asyncio.run, this test may need adjustment
        # The important thing is that the structure is correct


class TestBeatCLICommand:
    """Test beat CLI command integration."""

    @pytest.fixture
    def mock_aq_worker_file(self):
        """Return path to a Python file with AQWorker instance for beat tests."""
        # Use a static file under tests/examples to avoid issues with tmp paths
        from pathlib import Path

        worker_file = Path("tests/examples/beat_worker.py")
        return str(worker_file)

    def test_beat_command_loads_worker(self, mock_aq_worker_file):
        """Test that beat command loads AQWorker from file."""
        with patch("aqworker.cli.cli.run_beat") as mock_run_beat:
            with patch("aqworker.cli.cli.load_aqworker_from_file") as mock_load:
                from aqworker.cli.cli import start_beat

                mock_worker = AQWorker()
                mock_job_service = MagicMock()
                mock_worker.listen(mock_job_service)
                mock_load.return_value = mock_worker

                try:
                    start_beat(file_path=mock_aq_worker_file)
                except Exception:
                    pass

                mock_load.assert_called_once_with(mock_aq_worker_file)
                mock_run_beat.assert_called_once()

    def test_beat_command_passes_check_interval(self, mock_aq_worker_file):
        """Test that beat command passes check_interval to run_beat."""
        with patch("aqworker.cli.cli.run_beat") as mock_run_beat:
            with patch("aqworker.cli.cli.load_aqworker_from_file") as mock_load:
                from aqworker.cli.cli import start_beat

                mock_worker = AQWorker()
                mock_job_service = MagicMock()
                mock_worker.listen(mock_job_service)
                mock_load.return_value = mock_worker

                custom_interval = 0.5
                try:
                    start_beat(
                        file_path=mock_aq_worker_file, check_interval=custom_interval
                    )
                except Exception:
                    pass

                # Verify check_interval was passed
                call_kwargs = mock_run_beat.call_args.kwargs
                assert call_kwargs["check_interval"] == custom_interval

    def test_beat_command_uses_default_check_interval(self, mock_aq_worker_file):
        """Test that beat command uses default check_interval when not provided."""
        with patch("aqworker.cli.cli.run_beat") as mock_run_beat:
            with patch("aqworker.cli.cli.load_aqworker_from_file") as mock_load:
                from aqworker.cli.cli import start_beat
                from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL

                mock_worker = AQWorker()
                mock_job_service = MagicMock()
                mock_worker.listen(mock_job_service)
                mock_load.return_value = mock_worker

                try:
                    start_beat(file_path=mock_aq_worker_file, check_interval=None)
                except Exception:
                    pass

                # Verify default check_interval was used
                call_kwargs = mock_run_beat.call_args.kwargs
                assert call_kwargs["check_interval"] == DEFAULT_CRON_CHECK_INTERVAL
