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
    run_worker,
)
from aqworker.cli.loader import get_aqworker_file, load_aqworker_from_file
from aqworker.core import AQWorker

app = typer.Typer(name="aqworker", help="AQWorker management CLI")

# Create a default AQWorker instance for CLI
# Users should register workers/handlers before using CLI commands
_default_aq_worker = AQWorker()


def _get_aqworker_instance(file_path: Optional[str] = None) -> AQWorker:
    """
    Get AQWorker instance from file or use default.

    Args:
        file_path: Optional path to Python file containing AQWorker instance

    Returns:
        AQWorker instance
    """
    # Priority: file_path argument > env variable > default
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

    return _default_aq_worker


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


if __name__ == "__main__":
    app()
