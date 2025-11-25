import inspect

from aqworker.handler.registry import HandlerRegistry
from aqworker.job.models import Job
from aqworker.logger import logger


class HandlerDispatcher:
    """Resolve and execute handlers by name."""

    def __init__(self, handler_registry: HandlerRegistry):
        """
        Initialize dispatcher with a handler registry.

        Args:
            handler_registry: HandlerRegistry instance to use.
        """
        self.handler_registry = handler_registry

    async def execute(self, job: Job) -> bool:
        handler_name = job.handler
        handler_cls = self.handler_registry.get(handler_name)
        if not handler_cls:
            logger.error(f"Handler not found: {handler_name} for job {job.id}")
            return False

        handler = handler_cls()
        try:
            if inspect.iscoroutinefunction(handler.handle):
                return bool(await handler.handle(job.data or {}))

            return bool(handler.handle(job.data or {}))
        except Exception as exc:
            logger.exception(f"Handler {handler_name} error for job {job.id}: {exc}")
            return False
