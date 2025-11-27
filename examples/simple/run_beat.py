"""
Convenience script to run the CronScheduler (beat) using the CLI helper.

Usage:
    cd examples/simple
    python run_beat.py
"""

from pathlib import Path

from aqworker.cli.commands import run_beat
from aqworker.cli.loader import load_aqworker_from_file
from aqworker.constants import DEFAULT_CRON_CHECK_INTERVAL


def main() -> None:
    worker_file = Path(__file__).with_name("worker.py")
    aq_worker = load_aqworker_from_file(str(worker_file))
    run_beat(aq_worker=aq_worker, check_interval=DEFAULT_CRON_CHECK_INTERVAL)


if __name__ == "__main__":
    main()
