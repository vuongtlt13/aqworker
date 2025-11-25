import inspect
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

if TYPE_CHECKING:
    from aqworker.worker.base import BaseWorker


@dataclass(frozen=True)
class WorkerDefinition:
    """Metadata holder for aq_worker registrations."""

    name: str
    worker_class: type["BaseWorker"]
    queue_names: tuple[str, ...] = field(default_factory=tuple)


class WorkerRegistry:
    """Registry for available aq_worker classes."""

    def __init__(self):
        self._name_to_definition: Dict[str, WorkerDefinition] = {}

    @staticmethod
    def _camel_to_snake(name: str) -> str:
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        snake = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
        if snake.endswith("_worker"):
            snake = snake[: -len("_worker")]
        return snake

    @staticmethod
    def _extract_config_dict(worker_class: type["BaseWorker"]) -> Dict[str, Any]:
        config = getattr(worker_class, "worker_config", None)
        if config is None:
            return {}
        if inspect.isclass(config):
            try:
                config = config()
            except TypeError as exc:
                raise TypeError(
                    f"{worker_class.__name__}.worker_config must be instantiable without arguments"
                ) from exc
        if hasattr(config, "model_dump") and callable(getattr(config, "model_dump")):
            return cast(Dict[str, Any], config.model_dump())
        if isinstance(config, dict):
            return config
        return dict(getattr(config, "__dict__", {}))

    def _resolve_queue_names(self, worker_class: type["BaseWorker"]) -> tuple[str, ...]:
        explicit = getattr(worker_class, "queue_names", None)
        if explicit:
            return tuple(explicit)
        config = self._extract_config_dict(worker_class)
        queues = config.get("queue_names") or []
        return tuple(queues)

    def register(
        self, worker_class: type["BaseWorker"], name: Optional[str] = None
    ) -> None:
        if worker_class.__name__ == "BaseWorker":
            return

        worker_name = name or getattr(worker_class, "worker_name", None)
        if not worker_name:
            worker_name = self._camel_to_snake(worker_class.__name__)

        queue_names = self._resolve_queue_names(worker_class)

        self._name_to_definition[worker_name] = WorkerDefinition(
            name=worker_name,
            worker_class=worker_class,
            queue_names=queue_names,
        )

    def get(self, name: str) -> Optional[type["BaseWorker"]]:
        definition = self._name_to_definition.get(name)
        return definition.worker_class if definition else None

    def get_definition(self, name: str) -> Optional[WorkerDefinition]:
        return self._name_to_definition.get(name)

    def list_names(self) -> List[str]:
        return sorted(self._name_to_definition.keys())

    def list_definitions(self) -> List[WorkerDefinition]:
        return [self._name_to_definition[name] for name in self.list_names()]
