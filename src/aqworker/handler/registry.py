from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:  # pragma: no cover - circular import guard for type checking
    from aqworker.handler.base import BaseHandler


class HandlerRegistry:
    """Central registry for handlers classes."""

    def __init__(self):
        self._registry: Dict[str, type["BaseHandler"]] = {}

    def register(self, handler_cls: type["BaseHandler"]):
        """Register a handlers class using its declared name."""
        name = handler_cls.get_name()
        existing = self._registry.get(name)
        if existing and existing is not handler_cls:
            raise ValueError(
                f"Handler name collision: '{name}' already registered "
                f"by {existing.__module__}.{existing.__name__}"
            )
        self._registry[name] = handler_cls
        return handler_cls

    def get(self, name: str):
        return self._registry.get(name)

    def snapshot(self) -> Dict[str, type["BaseHandler"]]:
        return dict(self._registry)

    def is_empty(self) -> bool:
        return not self._registry
