"""
JobService configuration for the simple FastAPI application.
"""

from aqworker.job.service import JobService

# Create JobService instance with Redis configuration
# You can customize Redis connection here
job_service = JobService(redis_host="localhost", redis_port=6379, redis_db=0)
