"""
Registry for output plugins.
"""

from typing import Dict, Type, Optional, Any
from .base import OutputPlugin


class OutputPluginRegistry:
    """Registry for output plugins."""
    
    _plugins: Dict[str, Type[OutputPlugin]] = {}
    
    @classmethod
    def register(cls, name: str, plugin_class: Type[OutputPlugin]) -> None:
        """Register a plugin with the given name."""
        cls._plugins[name] = plugin_class
    
    @classmethod
    def get_plugin(cls, name: str, **kwargs) -> Optional[OutputPlugin]:
        """Get a plugin instance by name with the given arguments."""
        if name not in cls._plugins:
            return None
        return cls._plugins[name](**kwargs)
    
    @classmethod
    def get_available_plugins(cls) -> Dict[str, Type[OutputPlugin]]:
        """Get all registered plugins."""
        return cls._plugins.copy()


# Register built-in plugins
from .terminal import TerminalOutputPlugin
from .file import FileOutputPlugin
from .kafka import KafkaOutputPlugin

OutputPluginRegistry.register("terminal", TerminalOutputPlugin)
OutputPluginRegistry.register("file", FileOutputPlugin)
OutputPluginRegistry.register("kafka", KafkaOutputPlugin)


def get_output_plugin(output_type: str, **kwargs) -> OutputPlugin:
    """Factory function to create the appropriate output plugin."""
    plugin = OutputPluginRegistry.get_plugin(output_type, **kwargs)
    if not plugin:
        raise ValueError(f"Unknown output type: {output_type}")
    return plugin
