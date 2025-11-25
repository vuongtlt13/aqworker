"""
AQWorker initialization and configuration for the simple job processing example.
"""

from aq_worker.job_service import job_service
from aq_worker.workers import EmailWorker, NotificationWorker

from aqworker import AQWorker

# Initialize AQWorker
aq_worker = AQWorker(include_packages=["aq_worker"])

# Register workers
aq_worker.register_worker(EmailWorker)
aq_worker.register_worker(NotificationWorker)

# Listen to job service
aq_worker.listen(job_service)
