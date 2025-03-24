#!/usr/bin/env python3
"""
Schema validation script to demonstrate validating data against a schema with union fields.
Supports both standard JSON and JSON Lines format (one JSON object per line).
Can also consume and validate messages from a Kafka topic.
"""

import json
import jsonschema
from jsonschema import validate
import sys
import os
import argparse
import time
import signal

# Try to import Kafka, but make it optional
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Warning: kafka-python package not installed. Kafka consumer functionality will not be available.")

def load_json_file(file_path):
    """
    Load JSON from a file.
    Supports both standard JSON and JSON Lines format (one JSON object per line).
    """
    try:
        with open(file_path, 'r') as file:
            # First try to load as standard JSON
            try:
                return json.load(file)
            except json.JSONDecodeError:
                # If that fails, try to load as JSON Lines (one object per line)
                file.seek(0)  # Reset file pointer to beginning
                data = []
                for line in file:
                    line = line.strip()
                    if line:  # Skip empty lines
                        data.append(json.loads(line))
                if not data:
                    raise ValueError("No valid JSON objects found in file")
                return data
    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        sys.exit(1)

def resolve_schema_references(schema, base_path):
    """Recursively resolve $ref references in the schema with actual schema content."""
    if isinstance(schema, dict):
        if "$ref" in schema and not schema["$ref"].startswith("#"):
            # This is an external reference
            ref_path = os.path.join(base_path, schema["$ref"])
            try:
                with open(ref_path, 'r') as f:
                    ref_schema = json.load(f)
                    # Replace the $ref with the actual schema
                    return resolve_schema_references(ref_schema, base_path)
            except Exception as e:
                print(f"Error resolving reference {schema['$ref']}: {e}")
                sys.exit(1)
        
        # Process all other properties in the schema
        return {k: resolve_schema_references(v, base_path) for k, v in schema.items()}
    elif isinstance(schema, list):
        return [resolve_schema_references(item, base_path) for item in schema]
    else:
        return schema

def validate_data(schema, data, schema_dir):
    """Validate data against the schema."""
    try:
        # Resolve all schema references
        resolved_schema = resolve_schema_references(schema, schema_dir)
        
        if isinstance(data, list):
            # If data is a list, validate each item
            for i, item in enumerate(data):
                validate(instance=item, schema=resolved_schema)
                # print(f"Item {i+1}: Valid")
        else:
            # If data is a single object
            validate(instance=data, schema=resolved_schema)
            # print("Data is valid")
        return True
    except jsonschema.exceptions.ValidationError as e:
        print(f"Validation error: {e}")
        return False

def load_kafka_config(config_file):
    """Load Kafka configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            
        # Extract main settings
        bootstrap_servers = config.get('bootstrap.servers', config.get('bootstrap_servers', 'localhost:9092'))
        topic = config.get('topic', 'events')
        
        # Prepare connection config
        connection_config = {}
        
        # Security settings for Confluent Cloud
        if 'security.protocol' in config:
            connection_config['security_protocol'] = config['security.protocol']
        
        if 'sasl.mechanisms' in config:
            connection_config['sasl_mechanism'] = config['sasl.mechanisms']
        
        if 'sasl.username' in config and 'sasl.password' in config:
            connection_config['sasl_plain_username'] = config['sasl.username']
            connection_config['sasl_plain_password'] = config['sasl.password']
            
        # Consumer-specific settings
        consumer_config = {}
        consumer_keys = [
            'group_id', 'client_id', 'auto_offset_reset', 
            'enable_auto_commit', 'auto_commit_interval_ms',
            'session_timeout_ms', 'heartbeat_interval_ms'
        ]
        
        for key in consumer_keys:
            if key in config:
                consumer_config[key] = config[key]
        
        # Set default group_id if not provided
        if 'group_id' not in consumer_config:
            consumer_config['group_id'] = 'schema-validator'
            
        # Set default auto_offset_reset if not provided
        if 'auto_offset_reset' not in consumer_config:
            consumer_config['auto_offset_reset'] = 'earliest'
        
        # Combine configs
        kafka_config = {**connection_config, **consumer_config}
        
        return bootstrap_servers, topic, kafka_config
    except Exception as e:
        print(f"Error loading Kafka config file {config_file}: {e}")
        sys.exit(1)

def validate_kafka_messages(schema, schema_dir, bootstrap_servers, topic, kafka_config, max_messages=None, timeout_ms=10000):
    """Consume messages from a Kafka topic and validate them against the schema."""
    if not KAFKA_AVAILABLE:
        print("Error: kafka-python package is not installed. Cannot consume from Kafka.")
        return False
    
    # Resolve schema references
    resolved_schema = resolve_schema_references(schema, schema_dir)
    
    print(f"Connecting to Kafka with bootstrap_servers: {bootstrap_servers}")
    print(f"Consuming from topic: {topic}")
    
    # Set up signal handling for graceful termination
    running = True
    
    def handle_signal(sig, frame):
        nonlocal running
        print("\nInterrupted. Stopping consumer...")
        running = False
    
    signal.signal(signal.SIGINT, handle_signal)
    
    try:
        # Create Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            **kafka_config
        )
        
        print(f"Successfully connected to Kafka. Waiting for messages...")
        
        message_count = 0
        valid_count = 0
        invalid_count = 0
        
        # Start consuming messages
        while running:
            # Poll for messages with a timeout
            messages = consumer.poll(timeout_ms=timeout_ms)
            
            if not messages:
                print("No messages received. Waiting...")
                
                # Check if we've reached the max messages limit
                if max_messages is not None and message_count >= max_messages:
                    print(f"Reached maximum message count ({max_messages}). Stopping.")
                    break
                
                continue
            
            # Process received messages
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    message_count += 1
                    
                    try:
                        # Validate the message value against the schema
                        validate(instance=message.value, schema=resolved_schema)
                        print(f"Message {message_count} (offset {message.offset}): Valid")
                        valid_count += 1
                    except jsonschema.exceptions.ValidationError as e:
                        print(f"Message {message_count} (offset {message.offset}): Invalid - {e}")
                        invalid_count += 1
                    
                    # Check if we've reached the max messages limit
                    if max_messages is not None and message_count >= max_messages:
                        print(f"Reached maximum message count ({max_messages}). Stopping.")
                        running = False
                        break
                
                if not running:
                    break
        
        # Print summary
        print("\nValidation Summary:")
        print(f"Total messages processed: {message_count}")
        print(f"Valid messages: {valid_count}")
        print(f"Invalid messages: {invalid_count}")
        
        # Close the consumer
        consumer.close()
        
        return valid_count > 0 and invalid_count == 0
    
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")
        return False

def main():
    """Main function to validate data against the schema."""
    parser = argparse.ArgumentParser(description="Validate JSON data against a schema with union fields")
    parser.add_argument("schema_file", help="Path to the schema file (default in schemas directory)")
    
    # Create a mutually exclusive group for data source
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--data-file", help="Path to the data file to validate")
    source_group.add_argument("--kafka", action="store_true", help="Consume and validate messages from Kafka")
    
    # Kafka-specific options
    kafka_group = parser.add_argument_group("Kafka Options")
    kafka_group.add_argument("--kafka-config", help="Path to Kafka configuration file")
    kafka_group.add_argument("--bootstrap-servers", help="Kafka bootstrap servers (comma-separated)")
    kafka_group.add_argument("--topic", help="Kafka topic to consume from")
    kafka_group.add_argument("--max-messages", type=int, help="Maximum number of messages to consume (default: unlimited)")
    kafka_group.add_argument("--timeout", type=int, default=10000, help="Timeout in milliseconds for polling messages (default: 10000)")
    
    args = parser.parse_args()
    
    # Handle default schema location in schemas directory
    schema_file = args.schema_file
    if not os.path.dirname(schema_file):
        schema_file = os.path.join("schemas", schema_file)
    
    # Get the directory of the schema file for resolving references
    schema_dir = os.path.dirname(os.path.abspath(schema_file))
    
    # Load the schema
    schema = load_json_file(schema_file)
    
    # Validate data from file or Kafka
    if args.data_file:
        data_file = args.data_file
        data = load_json_file(data_file)
        
        print(f"Validating {data_file} against schema {schema_file}...")
        
        if validate_data(schema, data, schema_dir):
            print("All data is valid according to the schema!")
            return 0
        else:
            print("Validation failed.")
            return 1
    elif args.kafka:
        if not KAFKA_AVAILABLE:
            print("Error: kafka-python package is not installed. Cannot consume from Kafka.")
            return 1
        
        # Get Kafka configuration
        if args.kafka_config:
            bootstrap_servers, topic, kafka_config = load_kafka_config(args.kafka_config)
            
            # Override with command-line arguments if provided
            if args.bootstrap_servers:
                bootstrap_servers = args.bootstrap_servers
            if args.topic:
                topic = args.topic
        else:
            # Use command-line arguments
            if not args.bootstrap_servers:
                print("Error: Either --kafka-config or --bootstrap-servers must be provided.")
                return 1
            
            bootstrap_servers = args.bootstrap_servers
            topic = args.topic or "events"
            kafka_config = {
                'group_id': 'schema-validator',
                'auto_offset_reset': 'earliest'
            }
        
        print(f"Validating Kafka messages from topic {topic} against schema {schema_file}...")
        
        if validate_kafka_messages(schema, schema_dir, bootstrap_servers, topic, kafka_config, args.max_messages, args.timeout):
            print("All consumed messages are valid according to the schema!")
            return 0
        else:
            print("Validation failed for some messages.")
            return 1

if __name__ == "__main__":
    sys.exit(main())
