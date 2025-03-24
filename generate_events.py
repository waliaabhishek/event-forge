#!/usr/bin/env python3
"""
Generate random person events based on the JSON schema.
Uses a pluggable output system to allow for different output destinations.
"""

import json
import random
import string
import time
import argparse
import os
import sys
from typing import Dict, List, Any, Optional
import uuid
from datetime import datetime, timedelta
from faker import Faker

# Import the output plugin system
from plugins.output.registry import get_output_plugin
from plugins.output.base import OutputPlugin

# Initialize the Faker generator
fake = Faker()

# Constants for generating random data
TAGS = ["employee", "contractor", "customer", "vendor", "partner", "manager", "developer", 
        "designer", "marketing", "sales", "support", "finance", "hr", "operations", "executive"]

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "Support", "Finance", "HR", "Operations", "Executive", 
               "Product", "Design", "Research", "Legal", "IT", "Customer Success"]


class EventGenerator:
    """Generate random person events based on the schema."""
    
    def __init__(self, schema_dir: str, locale: Optional[str] = None) -> None:
        """Initialize with the schema directory and optional locale."""
        self.schema_dir = schema_dir
        self.load_schemas()
        
        # Initialize Faker with the specified locale or default
        if locale:
            self.fake = Faker(locale)
            # Also keep an en_US faker for methods that might not be available in all locales
            self.fake_us = Faker('en_US')
        else:
            self.fake = Faker()
            self.fake_us = self.fake
    
    def load_schemas(self) -> None:
        """Load all schema files."""
        schema_path = os.path.join(self.schema_dir, "schema.json")
        email_schema_path = os.path.join(self.schema_dir, "email-contact.schema.json")
        phone_schema_path = os.path.join(self.schema_dir, "phone-contact.schema.json")
        address_schema_path = os.path.join(self.schema_dir, "address-contact.schema.json")
        
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        
        with open(email_schema_path, 'r') as f:
            self.email_schema = json.load(f)
        
        with open(phone_schema_path, 'r') as f:
            self.phone_schema = json.load(f)
        
        with open(address_schema_path, 'r') as f:
            self.address_schema = json.load(f)
    
    def generate_random_id(self) -> str:
        """Generate a random ID."""
        return f"p{random.randint(1000, 9999)}"
    
    def generate_random_name(self) -> str:
        """Generate a random full name using Faker."""
        return self.fake.name()
    
    def generate_random_age(self) -> int:
        """Generate a random age between 18 and 80."""
        return random.randint(18, 80)
    
    def generate_random_email_contact(self) -> Dict[str, Any]:
        """Generate random email contact info using Faker."""
        return {
            "type": "email",
            "email": self.fake.email(),
            "isVerified": random.choice([True, False])
        }
    
    def generate_random_phone_contact(self) -> Dict[str, Any]:
        """Generate random phone contact info using Faker."""
        contact = {
            "type": "phone",
            "phoneNumber": self.fake.numerify(text='##########'),  # 10 digits
            "countryCode": self.fake.country_calling_code()
        }
        
        # Add extension 50% of the time
        if random.random() > 0.5:
            contact["extension"] = self.fake.numerify(text='###')
        
        return contact
    
    def generate_random_address_contact(self) -> Dict[str, Any]:
        """Generate random address contact info using Faker."""
        # Use locale-specific methods if available, otherwise fall back to US faker
        try:
            state = self.fake.state_abbr()
        except AttributeError:
            # If state_abbr is not available in this locale, use a generic approach
            state = self.fake_us.state_abbr()
        
        return {
            "type": "address",
            "street": self.fake.street_address(),
            "city": self.fake.city(),
            "state": state,
            "postalCode": self.fake.postcode(),
            "country": self.fake.country()
        }
    
    def generate_random_contact_info(self) -> Dict[str, Any]:
        """Generate random contact info of one of the three types."""
        contact_type = random.choice(["email", "phone", "address"])
        
        if contact_type == "email":
            return self.generate_random_email_contact()
        elif contact_type == "phone":
            return self.generate_random_phone_contact()
        else:
            return self.generate_random_address_contact()
    
    def generate_random_tags(self) -> List[str]:
        """Generate a random list of tags."""
        num_tags = random.randint(1, 3)
        return random.sample(TAGS, num_tags)
    
    def generate_random_metadata(self) -> Dict[str, Any]:
        """Generate random metadata."""
        metadata = {}
        
        # Add join date or contract dates
        if random.random() > 0.5:
            # Employee join date
            join_date = self.fake.date_between(start_date='-10y', end_date='today')
            metadata["joinDate"] = join_date.strftime("%Y-%m-%d")
            metadata["department"] = random.choice(DEPARTMENTS)
        else:
            # Contractor dates
            start_date = self.fake.date_between(start_date='-1y', end_date='today')
            end_date = self.fake.date_between(start_date=start_date, end_date='+1y')
            metadata["contractStart"] = start_date.strftime("%Y-%m-%d")
            metadata["contractEnd"] = end_date.strftime("%Y-%m-%d")
        
        # Add other random metadata fields
        if random.random() > 0.7:
            metadata["status"] = random.choice(["active", "inactive", "pending"])
        
        if random.random() > 0.8:
            metadata["lastUpdated"] = self.fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
        
        return metadata
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate a complete random person event."""
        event = {
            "id": self.generate_random_id(),
            "name": self.generate_random_name(),
            "age": self.generate_random_age(),
            "contactInfo": self.generate_random_contact_info(),
            "tags": self.generate_random_tags(),
            "metadata": self.generate_random_metadata(),
            "timestamp": datetime.now().isoformat()
        }
        
        return event


def generate_events_at_rate(generator: EventGenerator, output_plugin: OutputPlugin, 
                           count: int, rate: float) -> None:
    """
    Generate events at a specified rate (events per second).
    
    Args:
        generator: The event generator
        output_plugin: The output plugin to use
        count: Total number of events to generate
        rate: Number of events per second (min: 1, max: unlimited)
    """
    print(f"Generating {count} random person events at a rate of {rate} events/second...")
    
    # Calculate the delay between events
    delay = 1.0 / rate if rate > 0 else 0
    
    # Track timing for rate control
    start_time = time.time()
    events_generated = 0
    
    try:
        for i in range(count):
            # Generate and output the event
            event = generator.generate_event()
            output_plugin.output(event)
            events_generated += 1
            
            # Calculate how much time should have passed for the desired rate
            target_time = start_time + (events_generated / rate)
            
            # Calculate how much time to sleep to maintain the rate
            current_time = time.time()
            sleep_time = max(0, target_time - current_time)
            
            if sleep_time > 0 and i < count - 1:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nEvent generation interrupted.")
    finally:
        # Calculate actual rate achieved
        end_time = time.time()
        elapsed = end_time - start_time
        actual_rate = events_generated / elapsed if elapsed > 0 else 0
        
        print(f"Generated {events_generated} events in {elapsed:.2f} seconds.")
        print(f"Actual rate: {actual_rate:.2f} events/second")


def main():
    """Main function to parse arguments and generate events."""
    parser = argparse.ArgumentParser(description="Generate random person events")
    parser.add_argument("--count", type=int, default=10, help="Number of events to generate")
    parser.add_argument("--rate", type=float, default=1.0, 
                        help="Number of events per second (min: 1, max: unlimited)")
    parser.add_argument("--output", type=str, default="terminal", choices=["terminal", "file", "kafka"], 
                        help="Output plugin to use")
    parser.add_argument("--output-path", type=str, help="Path for file output")
    parser.add_argument("--schema-dir", type=str, default="schemas", 
                        help="Directory containing the schema files")
    parser.add_argument("--locale", type=str, help="Locale for generating fake data (e.g., 'en_US', 'fr_FR')")
    
    # Kafka specific options
    parser.add_argument("--kafka-config", type=str, 
                        help="Path to Kafka configuration JSON file")
    parser.add_argument("--kafka-bootstrap-servers", type=str, default="localhost:9092",
                        help="Comma-separated list of Kafka broker addresses (overridden if --kafka-config is provided)")
    parser.add_argument("--kafka-topic", type=str, default="events",
                        help="Kafka topic to send events to (overridden if --kafka-config is provided)")
    parser.add_argument("--kafka-key-field", type=str, default="id",
                        help="Field from the event to use as the message key (overridden if --kafka-config is provided)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.output == "file" and not args.output_path:
        parser.error("--output-path is required when using file output")
    
    if args.rate <= 0:
        parser.error("Rate must be greater than 0")
    
    # Create the event generator
    generator = EventGenerator(args.schema_dir, args.locale)
    
    # Create the output plugin
    output_kwargs = {}
    if args.output == "file":
        output_kwargs["file_path"] = args.output_path
    elif args.output == "kafka":
        if args.kafka_config:
            output_kwargs["config_file"] = args.kafka_config
        else:
            output_kwargs["bootstrap_servers"] = args.kafka_bootstrap_servers
            output_kwargs["topic"] = args.kafka_topic
            output_kwargs["key_field"] = args.kafka_key_field
    
    output_plugin = get_output_plugin(args.output, **output_kwargs)
    
    try:
        # Generate events at the specified rate
        generate_events_at_rate(generator, output_plugin, args.count, args.rate)
    finally:
        output_plugin.close()


if __name__ == "__main__":
    main()
