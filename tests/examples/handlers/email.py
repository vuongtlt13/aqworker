from aqworker.handler import BaseHandler


class EmailHandler(BaseHandler):
    name = "email"

    async def handle_async(self, data):
        return True
