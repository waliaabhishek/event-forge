# JSON Schema with Union Fields Example

This project demonstrates how to create and use a JSON schema that includes a union field (implemented using `oneOf`), which can accept multiple different schema structures defined in separate files.

## Project Structure

```
.
├── data/                  # Sample data files
│   └── sample-data.json   # Sample data demonstrating different contact types
├── schemas/               # JSON schema files
│   ├── schema.json        # Main schema with union field
│   ├── email-contact.schema.json
│   ├── phone-contact.schema.json
│   └── address-contact.schema.json
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

The project includes an event generator that creates random person events based on the JSON schema:

### Features

- **Random Data Generation**: Uses the Faker library to generate realistic random data
- **Pluggable Output System**: Supports different output destinations
  - Terminal output (default)
  - File output
- **Internationalization**: Supports generating data in different locales
- **Rate Control**: Configurable events-per-second generation rate

### Usage

```bash
# Generate 10 events at 1 per second (default)
python generate_events.py

# Generate 20 events at 5 per second
python generate_events.py --count 20 --rate 5

# Generate events with French locale
python generate_events.py --locale fr_FR

# Output to a file
python generate_events.py --output file --output-path events.json
```

### Command-line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--count` | Number of events to generate | 10 |
| `--rate` | Number of events per second | 1.0 |
| `--output` | Output plugin to use (terminal or file) | terminal |
| `--output-path` | Path for file output (required with --output file) | None |
| `--schema-dir` | Directory containing the schema files | schemas |
| `--locale` | Locale for generating fake data (e.g., 'en_US', 'fr_FR') | None |

### Output Plugins

The event generator uses a pluggable output system that allows for different output destinations:

1. **TerminalOutputPlugin**: Outputs events to the terminal as formatted JSON
2. **FileOutputPlugin**: Writes events to a specified file (one JSON object per line)

Additional output plugins can be added by implementing the `OutputPlugin` abstract base class.

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
- `jsonschema>=4.17.3`: For JSON schema validation
- `uuid>=1.30`: For generating unique identifiers
- `faker>=18.13.0`: For generating realistic random data

Install dependencies with:
```bash
pip install -r requirements.txt
``` 
