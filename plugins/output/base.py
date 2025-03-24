"""
Base output plugin interface for the event generator.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class OutputPlugin(ABC):
    """Abstract base class for output plugins."""
    
    @abstractmethod
    def output(self, event: Dict[str, Any]) -> None:
        """Output an event."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Clean up resources."""
        pass
