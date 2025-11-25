"""
Core AQWorker class - Main entry point for the aq_worker system.
"""

import importlib
import inspect
import pkgutil
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional, Type

from aqworker.handler.base import BaseHandler
from aqworker.handler.registry import HandlerRegistry
from aqworker.job.service import JobService
from aqworker.logger import logger
from aqworker.worker.dispatcher import HandlerDispatcher
from aqworker.worker.registry import WorkerRegistry

if TYPE_CHECKING:
    from worker.base import BaseWorker


class AQWorker:
    """
    Main AQWorker instance containing WorkerRegistry, HandlerRegistry, and JobService.

    This is the primary entry point for registering workers and handlers, and managing jobs.
    """

    def __init__(
        self,
        worker_registry: Optional[WorkerRegistry] = None,
        handler_registry: Optional[HandlerRegistry] = None,
        job_service: Optional[JobService] = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        include_packages: Optional[Iterable[str]] = None,
    ):
        """
        Initialize AQWorker with registries and job service.

        Args:
            worker_registry: Optional WorkerRegistry instance. Creates new one if not provided.
            handler_registry: Optional HandlerRegistry instance. Creates new one if not provided.
            job_service: Optional JobService instance. Creates new one if not provided.
            redis_host: Redis server hostname. Defaults to "localhost".
                       Only used when job_service is not provided.
            redis_port: Redis server port. Defaults to 6379.
                       Only used when job_service is not provided.
            redis_db: Redis database number. Defaults to 0.
                      Only used when job_service is not provided.
            include_packages: Optional iterable of package/module names to import
                               for handler auto-discovery.
        """
        self.worker_registry = worker_registry or WorkerRegistry()
        self.handler_registry = handler_registry or HandlerRegistry()
        self.job_service: Optional[JobService] = job_service or JobService(
            redis_host=redis_host, redis_port=redis_port, redis_db=redis_db
        )
        self.handler_dispatcher = HandlerDispatcher(self.handler_registry)
        self._workers: Dict[str, Optional[Type["BaseWorker"]]] = {}
        self._include_packages = list(include_packages or [])

        if self._include_packages:
            self.autodiscover_handlers(self._include_packages)

    def register_worker(
        self, worker_class: type["BaseWorker"], name: Optional[str] = None
    ) -> None:
        """
        Register a aq_worker class.

        Args:
            worker_class: Worker class to register
            name: Optional custom name for the aq_worker
        """
        self.worker_registry.register(worker_class, name)
        # Clear cached workers when new aq_worker is registered
        self._workers = {}

    def register_handler(self, handler_class: type["BaseHandler"]) -> None:
        """
        Register a handler class.

        Args:
            handler_class: Handler class to register
        """
        self.handler_registry.register(handler_class)

    def listen(self, job_service: JobService) -> None:
        """
            Register Job Service
        Args:
            job_service: JobService instance
        """
        self.job_service = job_service

    def create_worker(
        self, worker_type: str, custom_config: Optional[Dict[str, Any]] = None
    ) -> "BaseWorker":
        """
        Create a aq_worker instance.

        Args:
            worker_type: Type of aq_worker to create
            custom_config: Custom configuration to override defaults

        Returns:
            Worker instance
        """
        if not self._workers:
            # Copy snapshot of registered workers
            self._workers = {
                name: self.worker_registry.get(name)
                for name in self.worker_registry.list_names()
            }

        if worker_type not in self._workers:
            raise ValueError(
                f"Unknown aq_worker type: {worker_type}. Available: {list(self._workers.keys())}"
            )

        # Create aq_worker instance (will use worker_config from class)
        worker_class = self._workers[worker_type]
        if worker_class is None:
            raise ValueError(
                f"Unknown aq_worker type: {worker_type}. Available: {list(self._workers.keys())}"
            )
        return worker_class(
            custom_config,
            handler_dispatcher=self.handler_dispatcher,
            job_service=self.job_service,
        )

    def get_available_workers(self) -> list[str]:
        """Get list of available aq_worker types."""
        if not self._workers:
            self._workers = {
                name: self.worker_registry.get(name)
                for name in self.worker_registry.list_names()
            }
        return list(self._workers.keys())

    def handler(self, name: Optional[str] = None) -> Callable[..., Any]:
        """
        Decorator to convert a function into a handler and automatically register it.

        Usage:
            aq_worker = AQWorker()

            @aq_worker.handler(name='send_email')
            async def send_email(data: dict) -> bool:
                # Handler logic
                return True

            # Or with default name (function name)
            @aq_worker.handler()
            def process_data(data: dict) -> bool:
                return True

        Args:
            name: Optional handler name. If not provided, uses function name.

        Returns:
            The original function (decorator registers handler automatically)
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            # Determine if function is async
            is_async = inspect.iscoroutinefunction(func)

            # Get handler name
            handler_name = name or func.__name__

            # Create handler class dynamically
            class FunctionHandler(BaseHandler):
                """Handler class created from function decorator."""

                name = handler_name

                async def handle(self, data: Dict[str, Any]) -> bool:
                    """Process job data by calling the original function."""
                    if is_async:
                        return bool(await func(data))
                    else:
                        return bool(func(data))

            # Set class name for better debugging
            FunctionHandler.__name__ = f"{func.__name__}Handler"
            FunctionHandler.__qualname__ = f"{func.__qualname__}Handler"

            # Preserve original function for reference
            FunctionHandler._original_func = func  # type: ignore[attr-defined]

            # Automatically register handler
            self.register_handler(FunctionHandler)

            # Return original function so it can still be called directly if needed
            return func

        return decorator

    def autodiscover_handlers(self, packages: Iterable[str]) -> None:
        """
        Import every module inside the provided packages so handler decorators run.
        """
        before_snapshot = set(self.handler_registry.snapshot().keys())
        visited: set[str] = set()
        for package_name in packages:
            self._import_package(package_name, visited)

        current_handlers = sorted(self.handler_registry.snapshot().keys())
        if current_handlers:
            logger.info(
                "Available handlers after discovery: %s",
                ", ".join(current_handlers),
            )
        elif before_snapshot:
            logger.info("Handler registry unchanged during discovery.")
        else:
            logger.warning("No handlers registered after discovery.")

    def _import_package(self, module_name: str, visited: set[str]) -> None:
        if not module_name or module_name in visited:
            return

        visited.add(module_name)

        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            logger.warning("Include package '%s' not found: %s", module_name, exc)
            return
        except Exception as exc:
            logger.exception(
                "Failed to import include package '%s': %s", module_name, exc
            )
            return

        self._register_module_handlers(module)

        module_path = getattr(module, "__path__", None)
        if not module_path:
            return

        for _finder, name, _ispkg in pkgutil.walk_packages(
            module_path, prefix=f"{module.__name__}."
        ):
            self._import_package(name, visited)

    def _register_module_handlers(self, module) -> None:
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name, None)
            if (
                inspect.isclass(attribute)
                and issubclass(attribute, BaseHandler)
                and attribute is not BaseHandler
            ):
                try:
                    self.register_handler(attribute)
                except ValueError:
                    # Skip duplicates with informative log for debugging
                    logger.debug(
                        "Handler '%s' already registered", attribute.get_name()
                    )
