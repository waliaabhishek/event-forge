#!/usr/bin/env python3
"""
Schema validation script to demonstrate validating data against a schema with union fields.
Supports both standard JSON and JSON Lines format (one JSON object per line).
"""

import json
import jsonschema
from jsonschema import validate
import sys
import os
import argparse

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

def main():
    """Main function to validate sample data against the schema."""
    parser = argparse.ArgumentParser(description="Validate JSON data against a schema with union fields")
    parser.add_argument("schema_file", help="Path to the schema file (default in schemas directory)")
    parser.add_argument("data_file", help="Path to the data file to validate")
    
    args = parser.parse_args()
    
    # Handle default schema location in schemas directory
    schema_file = args.schema_file
    if not os.path.dirname(schema_file):
        schema_file = os.path.join("schemas", schema_file)
    
    data_file = args.data_file
    
    # Get the directory of the schema file for resolving references
    schema_dir = os.path.dirname(os.path.abspath(schema_file))
    
    schema = load_json_file(schema_file)
    data = load_json_file(data_file)
    
    print(f"Validating {data_file} against schema {schema_file}...")
    
    if validate_data(schema, data, schema_dir):
        print("All data is valid according to the schema!")
    else:
        print("Validation failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
