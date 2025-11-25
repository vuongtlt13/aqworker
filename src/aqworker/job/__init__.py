from .service import JobService

# Lazy initialization to avoid loading configuration at import time
_job_service_instance = None


def get_job_service() -> JobService:
    """Get the job service instance with lazy initialization."""
    global _job_service_instance
    if _job_service_instance is None:
        _job_service_instance = JobService()
    return _job_service_instance


# Create a lazy property that only initializes when accessed
class _LazyJobService:
    def __getattr__(self, name):
        return getattr(get_job_service(), name)


# Export a ready-to-use singleton for convenient imports
# This will be initialized only when accessed, not at import time
job_service = _LazyJobService()

__all__ = ["JobService", "job_service", "get_job_service"]
