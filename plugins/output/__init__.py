"""
Output plugin system for the event generator.
This package contains plugins for outputting events to different destinations.
"""

from .base import OutputPlugin
from .terminal import TerminalOutputPlugin
from .file import FileOutputPlugin
from .kafka import KafkaOutputPlugin

__all__ = ['OutputPlugin', 'TerminalOutputPlugin', 'FileOutputPlugin', 'KafkaOutputPlugin']
