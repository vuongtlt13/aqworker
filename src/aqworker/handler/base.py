from abc import ABC
from typing import Any, Dict, Optional


class BaseHandler(ABC):
    """Abstract base for job handlers."""

    name: Optional[str] = None

    @classmethod
    def get_name(cls) -> str:
        return cls.name or cls.__name__

    async def handle(self, data: Dict[str, Any]) -> bool:
        """
        Process job data.

        Args:
            data: Job data dictionary containing all information needed for processing

        Returns:
            True if processing succeeded, False otherwise
        """
        raise NotImplementedError
