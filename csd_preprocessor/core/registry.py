"""Operation registry for preprocessing operations.

All preprocessing operations register themselves here.
The engine uses the registry to look up operations by name.
"""

from typing import Dict, List, Type

_REGISTRY: Dict[str, Type] = {}


def register_operation(name: str):
    """Decorator to register a preprocessing operation class."""
    def decorator(cls):
        _REGISTRY[name] = cls
        cls.operation_name = name
        return cls
    return decorator


def get_operation(name: str):
    """Get an operation class by name."""
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown operation '{name}'. Available: {available}")
    return _REGISTRY[name]


def list_operations() -> List[str]:
    """List all registered operation names."""
    return sorted(_REGISTRY.keys())
