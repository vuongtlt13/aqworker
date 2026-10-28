"""
Constants for the aq_worker system.
"""

LOGGER_NAME = "aqworker"

# Queue prefixes
JOB_QUEUE_PREFIX = "aqw"
JOB_STATUS_PREFIX = "aqw:job:"
JOB_LOCK_PREFIX = "aqw:jl:"
CRON_LAST_RUN_PREFIX = "aqw:cron:last_run:"

# Special queue names
PROCESSING_QUEUE = f"{JOB_QUEUE_PREFIX}:processing"
COMPLETED_QUEUE = f"{JOB_QUEUE_PREFIX}:completed"
FAILED_QUEUE = f"{JOB_QUEUE_PREFIX}:failed"

# Cron scheduler defaults
DEFAULT_CRON_CHECK_INTERVAL = (
    0.1  # Default check interval in seconds for cron scheduler
)

# Worker defaults
DEFAULT_SHUTDOWN_TIMEOUT = 30  # Seconds to wait for running jobs on shutdown
DELAYED_PROMOTE_INTERVAL = 1.0  # Seconds between moves of due delayed jobs
STALE_JOB_MIN_AGE = 300  # Processing jobs older than this are considered stale
STALE_JOB_GRACE = 60  # Extra seconds on top of a job's own timeout before stale


# Queue name patterns
def get_queue_name(queue_name: str) -> str:
    """Get full queue name for a specific queue."""
    return f"{JOB_QUEUE_PREFIX}:{queue_name}"


def get_delayed_queue_name(queue_name: str) -> str:
    """Get sorted-set key holding delayed jobs (score = run-at timestamp) of a queue."""
    return f"{JOB_QUEUE_PREFIX}:delayed:{queue_name}"


def get_job_status_key(job_id: str) -> str:
    """Get job status key for a job ID."""
    return f"{JOB_STATUS_PREFIX}{job_id}"


def get_job_lock_key(job_id: str) -> str:
    """Get Redis key used for per-job locks."""
    return f"{JOB_LOCK_PREFIX}{job_id}"


def get_cron_last_run_key(handler_name: str) -> str:
    """Get Redis key used to store last scheduled run for a cron handler."""
    return f"{CRON_LAST_RUN_PREFIX}{handler_name}"
