import abc

from aqworker.handler.base import BaseHandler


# For once time job, normal job
class Job(BaseHandler):
    pass


# For cron job, repeated job
class CronJob(BaseHandler):
    @classmethod
    @abc.abstractmethod
    def cron(cls) -> str:
        raise NotImplementedError()
