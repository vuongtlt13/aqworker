"""
Command line entry points for aqworker.

Expose management commands via `python -m aqworker` or the installed
console script `aqworker`.
"""

from __future__ import annotations

from typing import Optional

import typer

from aqworker.cli.commands import (
    RegistryError,
    get_queue_stats,
    list_handler_descriptors,
    list_queue_names,
    list_worker_names,
    run_beat,
    run_worker,
)
from aqworker.cli.loader import get_aqworker_file, load_aqworker_from_file
from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL
from aqworker.core import AQWorker

app = typer.Typer(name="aqworker", help="AQWorker management CLI")


def _get_aqworker_instance(file_path: Optional[str] = None) -> AQWorker:
    """
    Get AQWorker instance from file.

    Args:
        file_path: Optional path to Python file containing AQWorker instance

    Returns:
        AQWorker instance

    Raises:
        typer.Exit: If AQWorker instance is not found
    """
    # Priority: file_path argument > env variable
    if file_path:
        try:
            return load_aqworker_from_file(file_path)
        except Exception as e:
            typer.echo(f"Error loading AQWorker from {file_path}: {e}", err=True)
            raise typer.Exit(code=1) from e

    env_file = get_aqworker_file()
    if env_file:
        try:
            return load_aqworker_from_file(env_file)
        except Exception as e:
            typer.echo(
                f"Error loading AQWorker from AQWORKER_FILE={env_file}: {e}", err=True
            )
            raise typer.Exit(code=1) from e

    # No AQWorker instance found
    typer.echo(
        "Error: AQWorker instance not found.\n"
        "Please provide a file path or set AQWORKER_FILE environment variable.\n"
        "Example: aqworker start email worker.py\n"
        "Or: export AQWORKER_FILE=worker.py && aqworker start email",
        err=True,
    )
    raise typer.Exit(code=1)


def _handle_registry_error(func):
    """Decorator-like helper to convert RegistryError to Typer exits."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RegistryError as exc:
            typer.echo(str(exc))
            raise typer.Exit(code=1) from exc

    return wrapper


@app.command("list:aq_worker")
def list_workers(
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    )
):
    """List available aq_worker names."""

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        for name in list_worker_names(aq_worker):
            typer.echo(name)

    _impl()


@app.command("list:handlers")
def list_handlers(
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    )
):
    """List registered handlers names."""

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        for name, dotted in list_handler_descriptors(aq_worker):
            typer.echo(f"{name} ({dotted})")

    _impl()


@app.command("list:queue")
def list_queues(
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    )
):
    """List queues declared by registered workers."""

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        for queue in list_queue_names(aq_worker):
            typer.echo(queue)

    _impl()


@app.command("start")
def start_worker(
    worker_name: str = typer.Argument(..., help="Worker name to start"),
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    ),
):
    """Start a aq_worker by name."""

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        run_worker(aq_worker, worker_name)

    try:
        _impl()
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        typer.echo("Worker interrupted, shutting down...")
    except Exception as exc:  # pragma: no cover
        typer.echo(f"Failed to run aq_worker: {exc}")
        raise typer.Exit(code=1) from exc


@app.command("stats")
def queue_stats(
    queue_name: str = typer.Argument(..., help="Queue name to get statistics for"),
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    ),
):
    """Print queue statistics for a given queue."""

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        job_service = aq_worker.job_service
        stats = get_queue_stats(queue_name, job_service)
        if not stats:
            typer.echo(f"No stats available for queue '{queue_name}'.")
            raise typer.Exit(code=1)

        typer.echo(f"Queue: {queue_name}")
        typer.echo(f"  Pending:    {stats.get('pending', 0)}")
        typer.echo(f"  Processing: {stats.get('processing', 0)}")
        typer.echo(f"  Completed:  {stats.get('completed', 0)}")
        typer.echo(f"  Failed:     {stats.get('failed', 0)}")

    _impl()


@app.command("beat")
def start_beat(
    file_path: Optional[str] = typer.Argument(
        None, help="Path to Python file containing AQWorker instance"
    ),
    check_interval: Optional[float] = typer.Option(
        None,
        "--check-interval",
        "-i",
        help="Interval in seconds to check for cron jobs (default: 0.1)",
    ),
):
    """
    Start the cron scheduler (beat service).

    This command runs a background service that periodically checks registered
    CronJob handlers and enqueues jobs when their cron expressions match.

    Note: Each CronJob must have queue_name defined (required).

    Example:
        aqworker beat worker.py
        aqworker beat worker.py --check-interval 0.5
    """

    @_handle_registry_error
    def _impl():
        aq_worker = _get_aqworker_instance(file_path)
        run_beat(
            aq_worker=aq_worker,
            check_interval=check_interval or DEFAULT_CRON_CHECK_INTERVAL,
        )

    try:
        _impl()
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        typer.echo("Beat interrupted, shutting down...")
    except Exception as exc:  # pragma: no cover
        typer.echo(f"Failed to run beat: {exc}")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
