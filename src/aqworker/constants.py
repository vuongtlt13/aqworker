"""
Constants for the aq_worker system.
"""

LOGGER_NAME = "aqworker"

# Queue prefixes
JOB_QUEUE_PREFIX = "aqw"
JOB_STATUS_PREFIX = "aqw:job:"
JOB_LOCK_PREFIX = "aqw:jl:"

# Special queue names
PROCESSING_QUEUE = f"{JOB_QUEUE_PREFIX}:processing"
COMPLETED_QUEUE = f"{JOB_QUEUE_PREFIX}:completed"
FAILED_QUEUE = f"{JOB_QUEUE_PREFIX}:failed"


# Queue name patterns
def get_queue_name(queue_name: str) -> str:
    """Get full queue name for a specific queue."""
    return f"{JOB_QUEUE_PREFIX}:{queue_name}"


def get_job_status_key(job_id: str) -> str:
    """Get job status key for a job ID."""
    return f"{JOB_STATUS_PREFIX}{job_id}"


def get_job_lock_key(job_id: str) -> str:
    """Get Redis key used for per-job locks."""
    return f"{JOB_LOCK_PREFIX}{job_id}"
