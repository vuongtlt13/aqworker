"""
Static AQWorker application used by beat CLI tests.
"""

from aqworker import AQWorker
from aqworker.job.service import JobService

aq_worker = AQWorker()
job_service = JobService()
aq_worker.listen(job_service)

__all__ = ["aq_worker"]
