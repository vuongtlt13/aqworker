"""
AQWorker initialization and configuration for the simple FastAPI application.
"""

from job_service import job_service
from workers import EmailWorker, NotificationWorker

from aqworker import AQWorker

# Initialize AQWorker
aq_worker = AQWorker(include_packages=["handlers"])

# Register workers
aq_worker.register_worker(EmailWorker)
aq_worker.register_worker(NotificationWorker)

# Listen to job service
aq_worker.listen(job_service)
