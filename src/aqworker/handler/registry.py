from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:  # pragma: no cover - circular import guard for type checking
    from aqworker.handler.base import BaseHandler


class HandlerRegistry:
    """Central registry for handlers classes."""

    def __init__(self):
        self._registry: Dict[str, type["BaseHandler"]] = {}

    def register(self, handler_cls: type["BaseHandler"]):
        """
        Register a handlers class using its declared name.

        Validates that Job and CronJob handlers have queue_name defined.

        Raises:
            ValueError: If handler name collision or if Job/CronJob doesn't have queue_name
        """
        name = handler_cls.get_name()
        existing = self._registry.get(name)
        if existing and existing is not handler_cls:
            raise ValueError(
                f"Handler name collision: '{name}' already registered "
                f"by {existing.__module__}.{existing.__name__}"
            )

        # Validate queue_name for Job and CronJob
        self._validate_handler_queue_name(handler_cls, name)

        self._registry[name] = handler_cls
        return handler_cls

    def _validate_handler_queue_name(
        self, handler_cls: type["BaseHandler"], handler_name: str
    ) -> None:
        """
        Validate that Job and CronJob handlers have queue_name defined.

        Args:
            handler_cls: Handler class to validate
            handler_name: Handler name for error messages

        Raises:
            ValueError: If Job or CronJob doesn't have queue_name defined
        """
        # Check if it's Job or CronJob
        from aqworker.job.base import CronJob, Job

        if not (issubclass(handler_cls, Job) or issubclass(handler_cls, CronJob)):
            # BaseHandler doesn't require queue_name
            return

        # Check if handler has queue_name() method
        if hasattr(handler_cls, "queue_name"):
            queue_name_method = getattr(handler_cls, "queue_name")
            if callable(queue_name_method):
                # It's a method, check if it returns a value
                try:
                    queue_name = queue_name_method()
                    if queue_name:
                        return
                except Exception:
                    pass

        # Check handler's queue_name attribute
        handler_queue_name = getattr(handler_cls, "queue_name", None)
        if handler_queue_name:
            return

        # No queue_name found - raise error
        handler_type = "CronJob" if issubclass(handler_cls, CronJob) else "Job"
        raise ValueError(
            f"Handler '{handler_name}' ({handler_cls.__name__}) is a {handler_type} "
            f"but does not have 'queue_name' defined. "
            f"Please define 'queue_name' as a class attribute or implement 'queue_name()' method."
        )

    def get(self, name: str):
        return self._registry.get(name)

    def snapshot(self) -> Dict[str, type["BaseHandler"]]:
        return dict(self._registry)

    def is_empty(self) -> bool:
        return not self._registry
