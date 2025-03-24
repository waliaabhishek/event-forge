"""
File output plugin for the event generator.
"""

import json
from typing import Dict, Any
from .base import OutputPlugin


class FileOutputPlugin(OutputPlugin):
    """Output plugin that writes events to a file."""
    
    def __init__(self, file_path: str) -> None:
        """Initialize with the output file path."""
        self.file_path = file_path
        self.file = open(file_path, 'w')
    
    def output(self, event: Dict[str, Any]) -> None:
        """Write the event to the file as JSON."""
        self.file.write(json.dumps(event) + "\n")
    
    def close(self) -> None:
        """Close the file."""
        if self.file:
            self.file.close()
