from aqworker import Job


class SmsHandler(Job):
    name = "sms"
    queue_name = "sms_queue"

    def handle(self, data):
        return True
