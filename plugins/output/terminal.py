"""
Terminal output plugin for the event generator.
"""

import json
from typing import Dict, Any
from .base import OutputPlugin


class TerminalOutputPlugin(OutputPlugin):
    """Output plugin that prints events to the terminal."""
    
    def output(self, event: Dict[str, Any]) -> None:
        """Print the event to the terminal as formatted JSON."""
        print(json.dumps(event, indent=2))
        print("-" * 40)
    
    def close(self) -> None:
        """No cleanup needed for terminal output."""
        pass
