from aqworker import Job


class EmailJob(Job):
    name = "email"
    queue_name = "email_queue"

    async def handle_async(self, data):
        return True
