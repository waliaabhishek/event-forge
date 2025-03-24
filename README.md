# JSON Schema with Union Fields Example

This project demonstrates how to create and use a JSON schema that includes a union field (implemented using `oneOf`), which can accept multiple different schema structures defined in separate files.

## Project Structure

```
.
├── data/                  # Sample data files
│   └── sample-data.json   # Sample data demonstrating different contact types
├── plugins/               # Plugin system
│   └── output/            # Output plugins
│       ├── __init__.py    # Plugin package initialization
│       ├── base.py        # Base output plugin interface
│       ├── terminal.py    # Terminal output plugin
│       ├── file.py        # File output plugin
│       ├── kafka.py       # Kafka output plugin
│       └── registry.py    # Plugin registry for dynamic discovery
├── schemas/               # JSON schema files
│   ├── schema.json        # Main schema with union field
│   ├── email-contact.schema.json
│   ├── phone-contact.schema.json
│   └── address-contact.schema.json
├── configs/               # Configuration files
│   └── kafka.json         # Sample Kafka configuration
├── tests/                 # Test-related files
│   └── results/           # Directory for test output files
├── generate_events.py     # Script to generate random events based on schema
├── validate_schema.py     # Script to validate data against schema
├── run_tests.py           # Comprehensive test script
└── requirements.txt       # Project dependencies
```

## Schema Structure

The project consists of multiple schema files in the `schemas/` directory:

1. **schema.json** - The main schema file that defines a person record with the following fields:
   - `id`: A unique identifier string (required)
   - `name`: The person's full name (required)
   - `age`: The person's age as an integer
   - `contactInfo`: A union field that references three different schemas (required)
   - `tags`: An array of string tags
   - `metadata`: A flexible object for additional information

2. **email-contact.schema.json** - Schema for email contact information
3. **phone-contact.schema.json** - Schema for phone contact information
4. **address-contact.schema.json** - Schema for physical address information

## Union Field Implementation

The `contactInfo` field in the main schema uses the `oneOf` keyword to implement a union type, but instead of defining the schemas inline, it references external schema files:

```json
"contactInfo": {
  "description": "Contact information that can be one of several formats",
  "oneOf": [
    { "$ref": "schemas/email-contact.schema.json" },
    { "$ref": "schemas/phone-contact.schema.json" },
    { "$ref": "schemas/address-contact.schema.json" }
  ]
}
```

Each referenced schema includes a `type` field that serves as a discriminator, making it clear which schema variant is being used.

## Benefits of Separate Schema Files

Using separate schema files provides several advantages:
- **Modularity**: Each schema can be maintained independently
- **Reusability**: Schemas can be reused across multiple parent schemas
- **Readability**: The main schema is more concise and easier to understand
- **Maintainability**: Changes to a specific schema type only require updating one file

## Sample Data

The `data/sample-data.json` file provides examples of valid data conforming to this schema, with each record using a different variant of the `contactInfo` field.

## Validation

The `validate_schema.py` script demonstrates how to validate JSON data against schemas with external references:

```bash
python validate_schema.py schemas/schema.json data/sample-data.json
```

The script uses the `jsonschema` library's `RefResolver` to properly handle the schema references.

## Event Generator

The event generator creates random person events based on the schema. It supports:

- Generating random data for all fields in the schema
- Outputting events to different destinations via a plugin system
- Controlling the rate of event generation
- Locale support for generating data in different languages/regions

### Output Plugins

The generator uses a modular output plugin system that allows for different output destinations:

- **Terminal**: Prints events to the terminal as formatted JSON
- **File**: Writes events to a file in JSON Lines format (one JSON object per line)
- **Kafka**: Sends events to a Kafka topic with configurable settings

The plugin system is designed to be extensible, making it easy to add new output plugins:

1. Create a new plugin file in the `plugins/output` directory
2. Implement the `OutputPlugin` interface
3. Register the plugin in the registry

Example usage:

```bash
# Output to terminal (default)
python generate_events.py --count 10

# Output to file
python generate_events.py --count 10 --output file --output-path events.json

# Output to Kafka
python generate_events.py --count 10 --output kafka --kafka-topic person-events

# Output to Kafka using configuration file
python generate_events.py --count 10 --output kafka --kafka-config configs/kafka.json
```

### Command-line Options

```bash
usage: generate_events.py [-h] [--count COUNT] [--rate RATE]
                          [--output {terminal,file,kafka}]
                          [--output-path OUTPUT_PATH]
                          [--schema-dir SCHEMA_DIR] [--locale LOCALE]
                          [--kafka-config KAFKA_CONFIG]
                          [--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS]
                          [--kafka-topic KAFKA_TOPIC]
                          [--kafka-key-field KAFKA_KEY_FIELD]

Generate random person events

optional arguments:
  -h, --help            show this help message and exit
  --count COUNT         Number of events to generate
  --rate RATE           Number of events per second (min: 1, max: unlimited)
  --output {terminal,file,kafka}
                        Output plugin to use
  --output-path OUTPUT_PATH
                        Path for file output
  --schema-dir SCHEMA_DIR
                        Directory containing the schema files
  --locale LOCALE       Locale for generating fake data (e.g., 'en_US', 'fr_FR')
  --kafka-config KAFKA_CONFIG
                        Path to Kafka configuration JSON file
  --kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS
                        Comma-separated list of Kafka broker addresses (overridden if --kafka-config is provided)
  --kafka-topic KAFKA_TOPIC
                        Kafka topic to send events to (overridden if --kafka-config is provided)
  --kafka-key-field KAFKA_KEY_FIELD
                        Field from the event to use as the message key (overridden if --kafka-config is provided)
```

## Testing

The project includes a comprehensive test script that verifies all functionality:

```bash
./run_tests.py
```

This script runs a series of tests to verify:
- Schema validation with sample data
- Basic event generation
- File output functionality
- Validation of generated events
- Locale support
- Rate control at different speeds (50/s, 100/s, 500/s)

The test results are displayed with color-coded output and include accuracy measurements for rate control tests. All test output files are stored in the `tests/results/` directory to keep the project root clean.

## Dependencies

The project requires the following Python packages:
- `jsonschema>=4.0.0`: For JSON schema validation
- `uuid>=1.30`: For generating unique identifiers
- `faker>=8.0.0`: For generating realistic random data
- `kafka-python>=2.0.2`: For Kafka output plugin

Install dependencies with:
```bash
pip install -r requirements.txt
