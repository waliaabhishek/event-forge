#!/usr/bin/env python3
"""
Schema Registry Integration for JSON schemas with references.
This module provides functionality to register and manage JSON schemas in Confluent Cloud Schema Registry.
"""

import os
import json
import argparse
import logging
from typing import Dict, Any, List, Optional, Union
import requests
from requests.auth import HTTPBasicAuth

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaRegistryClient:
    """Client for interacting with Confluent Cloud Schema Registry."""
    
    def __init__(self, url: str, api_key: str, api_secret: str):
        """
        Initialize the Schema Registry client.
        
        Args:
            url: Schema Registry URL (e.g., https://psrc-xxxx.region.aws.confluent.cloud)
            api_key: API Key for authentication
            api_secret: API Secret for authentication
        """
        self.url = url.rstrip('/')
        self.auth = HTTPBasicAuth(api_key, api_secret)
        self.headers = {
            'Content-Type': 'application/json'
        }
        # Verify connection
        self._check_connection()
    
    def _check_connection(self) -> None:
        """Check connection to Schema Registry."""
        try:
            response = requests.get(f"{self.url}/subjects", auth=self.auth)
            response.raise_for_status()
            logger.info(f"Successfully connected to Schema Registry at {self.url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to Schema Registry: {e}")
            raise
    
    def get_subjects(self) -> List[str]:
        """
        Get a list of registered subjects.
        
        Returns:
            List of subject names
        """
        response = requests.get(f"{self.url}/subjects", auth=self.auth)
        response.raise_for_status()
        return response.json()
    
    def get_schema_versions(self, subject: str) -> List[int]:
        """
        Get all versions for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            List of version numbers
        """
        response = requests.get(f"{self.url}/subjects/{subject}/versions", auth=self.auth)
        response.raise_for_status()
        return response.json()
    
    def get_schema(self, subject: str, version: Union[int, str] = 'latest') -> Dict[str, Any]:
        """
        Get a specific schema version.
        
        Args:
            subject: Subject name
            version: Schema version or 'latest'
            
        Returns:
            Schema information including id, version, and schema
        """
        response = requests.get(
            f"{self.url}/subjects/{subject}/versions/{version}", 
            auth=self.auth
        )
        response.raise_for_status()
        return response.json()
    
    def register_schema(self, subject: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register a new schema under the specified subject.
        
        Args:
            subject: Subject name
            schema: JSON Schema to register
            
        Returns:
            Registration response including schema ID
        """
        # Prepare the schema in the format expected by Confluent Schema Registry
        schema_payload = {
            "schemaType": "JSON",
            "schema": json.dumps(schema)
        }
        
        try:
            response = requests.post(
                f"{self.url}/subjects/{subject}/versions",
                auth=self.auth,
                headers=self.headers,
                json=schema_payload
            )
            
            if response.status_code != 200:
                error_details = response.json() if response.text else {"error": "Unknown error"}
                logger.error(f"Failed to register schema: {response.status_code} - {error_details}")
                if response.status_code == 422:
                    logger.error("This is usually due to schema validation issues or incompatibility")
                    logger.debug(f"Schema payload: {json.dumps(schema_payload, indent=2)}")
            
            response.raise_for_status()
            logger.info(f"Successfully registered schema for subject '{subject}'")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    logger.error(f"Error details: {error_details}")
                except:
                    logger.error(f"Error response text: {e.response.text}")
            raise
    
    def check_compatibility(self, subject: str, schema: Dict[str, Any], 
                           version: Union[int, str] = 'latest') -> bool:
        """
        Check if a schema is compatible with a subject's registered schema.
        
        Args:
            subject: Subject name
            schema: Schema to check compatibility
            version: Version to check against or 'latest'
            
        Returns:
            True if compatible, False otherwise
        """
        payload = {
            "schema": json.dumps(schema)
        }
        response = requests.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/{version}",
            auth=self.auth,
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json().get('is_compatible', False)


class JsonSchemaProcessor:
    """Process JSON schemas with references for Schema Registry."""
    
    def __init__(self, registry_client: SchemaRegistryClient):
        """
        Initialize the JSON Schema processor.
        
        Args:
            registry_client: Schema Registry client
        """
        self.registry_client = registry_client
        self.registered_schemas = {}  # Cache for registered schemas
    
    def _resolve_references(self, schema: Dict[str, Any], base_path: str) -> Dict[str, Any]:
        """
        Resolve file references in a JSON schema by inlining them.
        
        Args:
            schema: JSON Schema with possible references
            base_path: Base path for resolving relative references
            
        Returns:
            Schema with resolved references
        """
        resolved_schema = schema.copy()
        
        # Process $ref keys
        if '$ref' in schema and isinstance(schema['$ref'], str):
            ref_path = schema['$ref']
            
            # Handle relative file references (not URLs)
            if not ref_path.startswith(('http://', 'https://')):
                # Construct absolute path for the reference
                if not os.path.isabs(ref_path):
                    ref_path = os.path.join(base_path, ref_path)
                
                logger.debug(f"Resolving reference: {ref_path}")
                
                try:
                    with open(ref_path, 'r') as f:
                        ref_schema = json.load(f)
                    
                    # Replace the reference with the actual schema content
                    ref_dir = os.path.dirname(ref_path)
                    return self._resolve_references(ref_schema, ref_dir)
                except FileNotFoundError:
                    logger.error(f"Reference file not found: {ref_path}")
                    raise
            
        # Process nested objects and arrays
        for key, value in schema.items():
            if isinstance(value, dict):
                resolved_schema[key] = self._resolve_references(value, base_path)
            elif isinstance(value, list):
                resolved_schema[key] = [
                    self._resolve_references(item, base_path) if isinstance(item, dict) else item
                    for item in value
                ]
        
        return resolved_schema
    
    def process_schema_file(self, schema_file: str) -> Dict[str, Any]:
        """
        Process a schema file, resolving all references.
        
        Args:
            schema_file: Path to the schema file
            
        Returns:
            Processed schema with all references resolved
        """
        logger.debug(f"Processing schema file: {schema_file}")
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        base_path = os.path.dirname(os.path.abspath(schema_file))
        resolved_schema = self._resolve_references(schema, base_path)
        
        # Log the resolved schema for debugging
        logger.debug(f"Resolved schema: {json.dumps(resolved_schema, indent=2)}")
        
        return resolved_schema
    
    def register_schema_with_references(self, subject: str, schema_file: str) -> Dict[str, Any]:
        """
        Process and register a schema with references.
        
        Args:
            subject: Subject name for registration
            schema_file: Path to the schema file
            
        Returns:
            Registration response
        """
        processed_schema = self.process_schema_file(schema_file)
        response = self.registry_client.register_schema(subject, processed_schema)
        self.registered_schemas[subject] = processed_schema
        return response
    
    def check_compatibility_with_references(self, subject: str, schema_file: str,
                                           version: Union[int, str] = 'latest') -> bool:
        """
        Check compatibility of a schema with references.
        
        Args:
            subject: Subject name
            schema_file: Path to the schema file
            version: Version to check against or 'latest'
            
        Returns:
            True if compatible, False otherwise
        """
        processed_schema = self.process_schema_file(schema_file)
        return self.registry_client.check_compatibility(subject, processed_schema, version)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    with open(config_file, 'r') as f:
        return json.load(f)


def main():
    """Main function to handle command line arguments and execute schema registry operations."""
    parser = argparse.ArgumentParser(description="JSON Schema Registry Manager")
    
    # Command subparsers
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Register schema command
    register_parser = subparsers.add_parser('register', help='Register a schema')
    register_parser.add_argument('--config', required=True, help='Path to registry configuration file')
    register_parser.add_argument('--subject', required=True, help='Subject name')
    register_parser.add_argument('--schema', required=True, help='Path to schema file')
    
    # List subjects command
    list_parser = subparsers.add_parser('list', help='List registered subjects')
    list_parser.add_argument('--config', required=True, help='Path to registry configuration file')
    
    # Get schema command
    get_parser = subparsers.add_parser('get', help='Get a schema')
    get_parser.add_argument('--config', required=True, help='Path to registry configuration file')
    get_parser.add_argument('--subject', required=True, help='Subject name')
    get_parser.add_argument('--version', default='latest', help='Schema version (default: latest)')
    get_parser.add_argument('--output', help='Output file path (if not specified, prints to stdout)')
    
    # Check compatibility command
    check_parser = subparsers.add_parser('check', help='Check schema compatibility')
    check_parser.add_argument('--config', required=True, help='Path to registry configuration file')
    check_parser.add_argument('--subject', required=True, help='Subject name')
    check_parser.add_argument('--schema', required=True, help='Path to schema file')
    check_parser.add_argument('--version', default='latest', help='Schema version to check against (default: latest)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Load configuration
    config = load_config(args.config)
    
    # Create Schema Registry client
    client = SchemaRegistryClient(
        url=config['url'],
        api_key=config['api_key'],
        api_secret=config['api_secret']
    )
    
    # Execute command
    if args.command == 'register':
        processor = JsonSchemaProcessor(client)
        response = processor.register_schema_with_references(args.subject, args.schema)
        print(f"Schema registered with ID: {response['id']}")
        
    elif args.command == 'list':
        subjects = client.get_subjects()
        print("Registered subjects:")
        for subject in subjects:
            print(f"- {subject}")
            
    elif args.command == 'get':
        schema_info = client.get_schema(args.subject, args.version)
        schema = json.loads(schema_info['schema'])
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(schema, f, indent=2)
            print(f"Schema saved to {args.output}")
        else:
            print(json.dumps(schema, indent=2))
            
    elif args.command == 'check':
        processor = JsonSchemaProcessor(client)
        is_compatible = processor.check_compatibility_with_references(
            args.subject, args.schema, args.version
        )
        print(f"Schema is {'compatible' if is_compatible else 'not compatible'}")


if __name__ == "__main__":
    main()
