"""
Test AQWorker application used by CLI loader tests.
"""

from aqworker import AQWorker
from tests.examples.handlers import email as email_handler_module
from tests.examples.handlers import sms as sms_handler_module
from tests.examples.workers import priority_worker, sample_worker

aq_worker = AQWorker()
aq_worker.register_worker(sample_worker.BackgroundWorker)
aq_worker.register_worker(priority_worker.PriorityWorker)
aq_worker.register_handler(email_handler_module.EmailHandler)
aq_worker.register_handler(sms_handler_module.SmsHandler)

__all__ = ["aq_worker"]
