"""
Utility to load AQWorker instance from a Python file.
"""

import importlib.util
import os
import sys
from pathlib import Path
from typing import Optional

from aqworker.core import AQWorker


def load_aqworker_from_file(file_path: str) -> AQWorker:
    """
    Load AQWorker instance from a Python file.

    Args:
        file_path: Path to Python file containing AQWorker instance

    Returns:
        AQWorker instance found in the file

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If no AQWorker instance found in file
    """
    file_path_obj = Path(file_path)

    # Resolve absolute path
    if not file_path_obj.is_absolute():
        # Try relative to current working directory
        abs_path = Path.cwd() / file_path_obj
        if not abs_path.exists():
            # Try relative to project root (if we can detect it)
            abs_path = file_path_obj.resolve()
    else:
        abs_path = file_path_obj

    if not abs_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not abs_path.suffix == ".py":
        raise ValueError(f"File must be a Python file (.py): {file_path}")

    # Convert path to module path
    # e.g., examples/simple/aq_worker.py -> examples.simple.aq_worker
    module_name = str(file_path).replace(".py", "").replace("/", ".").replace("\\", ".")

    # Load the module
    spec = importlib.util.spec_from_file_location(module_name, abs_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from file: {file_path}")

    module = importlib.util.module_from_spec(spec)

    # Add parent directory to sys.path if needed for imports
    parent_dir = str(Path.cwd())
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ValueError(f"Error loading module from {file_path}: {e}") from e

    # Find AQWorker instance in the module
    # Try common names first
    common_names = ["aq_worker", "aq_worker", "aqworker", "aq"]
    for name in common_names:
        if hasattr(module, name):
            attr = getattr(module, name)
            if isinstance(attr, AQWorker):
                return attr

    # Search all attributes for AQWorker instances
    for attr_name in dir(module):
        if not attr_name.startswith("_"):  # Skip private attributes
            attr = getattr(module, attr_name)
            if isinstance(attr, AQWorker):
                return attr

    raise ValueError(
        f"No AQWorker instance found in {file_path}. "
        f"Expected an instance variable named one of: {', '.join(common_names)}, "
        f"or any attribute of type AQWorker."
    )


def get_aqworker_file() -> Optional[str]:
    """
    Get AQWorker file path from environment variable.

    Returns:
        File path if AQWORKER_FILE is set, None otherwise
    """
    return os.environ.get("AQWORKER_FILE")
