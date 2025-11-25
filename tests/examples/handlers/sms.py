from aqworker.handler import BaseHandler


class SmsHandler(BaseHandler):
    name = "sms"

    def handle(self, data):
        return True
