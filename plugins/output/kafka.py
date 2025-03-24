"""
Kafka output plugin for the event generator.
"""

import json
import os
import warnings
from typing import Dict, Any, Optional

# Try to import Kafka, but make it optional
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    warnings.warn("kafka-python package not installed. KafkaOutputPlugin will be available but non-functional.")

from .base import OutputPlugin


class KafkaOutputPlugin(OutputPlugin):
    """Output plugin that sends events to a Kafka topic."""
    
    def __init__(self, 
                 config_file: str = None,
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'events',
                 key_field: Optional[str] = None,
                 **kafka_config) -> None:
        """
        Initialize the Kafka output plugin.
        
        Args:
            config_file: Path to a JSON file containing Kafka configuration
            bootstrap_servers: Comma-separated list of Kafka broker addresses (used if config_file not provided)
            topic: Kafka topic to send events to (used if config_file not provided)
            key_field: Optional field from the event to use as the message key (used if config_file not provided)
            **kafka_config: Additional configuration options for KafkaProducer (used if config_file not provided)
        """
        if not KAFKA_AVAILABLE:
            warnings.warn("kafka-python package not installed. KafkaOutputPlugin will not send messages.")
            self.producer = None
            self.topic = topic
            self.key_field = key_field
            return
            
        # Load configuration from file if provided
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                
            # Extract main settings
            self.bootstrap_servers = config.get('bootstrap_servers', bootstrap_servers)
            self.topic = config.get('topic', topic)
            self.key_field = config.get('key_field', key_field)
            
            # Extract Kafka producer config
            self.kafka_config = {k: v for k, v in config.items() 
                               if k not in ['bootstrap_servers', 'topic', 'key_field']}
            
            # Merge with any additional config passed directly
            self.kafka_config.update(kafka_config)
        else:
            # Use provided parameters
            self.bootstrap_servers = bootstrap_servers
            self.topic = topic
            self.key_field = key_field
            self.kafka_config = kafka_config
        
        # Set default serializers if not provided
        if 'value_serializer' not in self.kafka_config:
            self.kafka_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
        if 'key_serializer' not in self.kafka_config and self.key_field:
            self.kafka_config['key_serializer'] = lambda k: str(k).encode('utf-8')
            
        # Create the Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            **self.kafka_config
        )
    
    def output(self, event: Dict[str, Any]) -> None:
        """Send the event to the Kafka topic."""
        if not KAFKA_AVAILABLE or self.producer is None:
            # Just print a message if Kafka is not available
            print(f"[Kafka Output] Would send to topic '{self.topic}': {json.dumps(event)}")
            return
            
        key = None
        if self.key_field and self.key_field in event:
            key = event[self.key_field]
            
        self.producer.send(
            topic=self.topic,
            key=key,
            value=event
        )
        
        # Optional: Ensure the message is sent immediately
        # Uncomment for synchronous behavior (slower but guaranteed delivery)
        # self.producer.flush()
    
    def close(self) -> None:
        """Close the Kafka producer."""
        if KAFKA_AVAILABLE and self.producer:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
