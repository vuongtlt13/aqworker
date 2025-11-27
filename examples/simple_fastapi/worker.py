"""
AQWorker initialization and configuration for the simple FastAPI application.
"""

from job_service import job_service
from workers import CronWorker, EmailWorker, NotificationWorker

from aqworker import AQWorker, CronScheduler

# Initialize AQWorker
aq_worker = AQWorker(include_packages=["handlers"])

# Register workers
aq_worker.register_worker(EmailWorker)
aq_worker.register_worker(NotificationWorker)
aq_worker.register_worker(CronWorker)

# Listen to job service
aq_worker.listen(job_service)

# # Initialize CronScheduler
# # This will automatically enqueue cron jobs based on their cron expressions
# # Note: Start the scheduler in FastAPI lifespan events (see main.py)
# # Uses default check_interval (0.1s) to support second-level cron expressions
# # Each CronJob must have queue_name defined (required)
# cron_scheduler = CronScheduler(
#     handler_registry=aq_worker.handler_registry,
#     job_service=aq_worker.job_service,
#     # check_interval defaults to 0.1 seconds
# )
