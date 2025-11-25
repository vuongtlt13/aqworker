"""
Job service instance for the simple job processing example.
"""

from aqworker.job.service import JobService

# Initialize job service with default Redis connection
job_service = JobService()
