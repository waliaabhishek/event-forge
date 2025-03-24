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
import psutil
import subprocess
from typing import Dict, List, Any, Optional
import uuid
from datetime import datetime, timedelta
from faker import Faker
import signal

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
        metadata_schema_path = os.path.join(self.schema_dir, "metadata.schema.json")
        
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        
        with open(email_schema_path, 'r') as f:
            self.email_schema = json.load(f)
        
        with open(phone_schema_path, 'r') as f:
            self.phone_schema = json.load(f)
        
        with open(address_schema_path, 'r') as f:
            self.address_schema = json.load(f)
            
        with open(metadata_schema_path, 'r') as f:
            self.metadata_schema = json.load(f)
    
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
        """Generate random metadata according to the metadata schema."""
        # Randomly choose between employee and contractor metadata
        is_employee = random.random() > 0.5
        
        if is_employee:
            # Generate employee metadata
            metadata = {
                "joinDate": self.fake.date_between(start_date='-10y', end_date='today').strftime("%Y-%m-%d"),
                "department": random.choice(DEPARTMENTS),
                "status": None,
                "lastUpdated": None
            }
        else:
            # Generate contractor metadata
            start_date = self.fake.date_between(start_date='-1y', end_date='today')
            end_date = self.fake.date_between(start_date=start_date, end_date='+1y')
            metadata = {
                "contractStart": start_date.strftime("%Y-%m-%d"),
                "contractEnd": end_date.strftime("%Y-%m-%d"),
                "status": None,
                "lastUpdated": None
            }
        
        # Add optional fields with some probability
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


def move_cursor_up_and_clear(lines):
    """Move cursor up and clear lines."""
    if not sys.stdout.isatty():
        return
    
    # Move up 'lines' lines
    sys.stdout.write(f"\033[{lines}A")
    
    # For each line, clear it and move down
    for i in range(lines):
        sys.stdout.write("\033[2K")  # Clear the entire line
        if i < lines - 1:
            sys.stdout.write("\033[1B")  # Move down 1 line
    
    # Move back up to the starting position
    if lines > 1:
        sys.stdout.write(f"\033[{lines-1}A")
    
    # Flush the output
    sys.stdout.flush()


def generate_events_at_rate(generator: EventGenerator, output_plugin: OutputPlugin, 
                           count: Optional[int], rate: float, stats_interval: float = 5.0) -> None:
    """
    Generate events at a specified rate (events per second).
    
    Args:
        generator: The event generator
        output_plugin: The output plugin to use
        count: Total number of events to generate, or None for continuous generation
        rate: Number of events per second (min: 1, max: unlimited)
        stats_interval: Interval in seconds between statistics reports (default: 5.0)
    """
    # Print initial messages
    if count is not None:
        print(f"Generating {count} random person events at a rate of {rate} events/second...")
    else:
        print(f"Generating continuous random person events at a rate of {rate} events/second...")
        print("Press Ctrl+C to stop generation")
    
    # Add a blank line before stats will appear
    print("")
    
    # Calculate the delay between events
    delay = 1.0 / rate if rate > 0 else 0
    
    # Track timing for rate control
    start_time = time.time()
    events_generated = 0
    
    # Track timing for stats display
    last_stats_time = start_time
    last_stats_count = 0
    
    # Get the process for memory tracking
    process = psutil.Process(os.getpid())
    
    # ANSI color codes for prettier output
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
    
    # Get output plugin type
    output_type = output_plugin.__class__.__name__
    
    # Check if we should display stats (only for non-terminal output)
    should_display_stats = output_type != "TerminalOutputPlugin"
    
    # Track if we've displayed stats before and how many lines were output
    stats_displayed = False
    stats_lines = 12  # Number of lines in the stats display (header + data + footer + activity indicator)
    
    # Activity indicator characters
    activity_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    activity_idx = 0
    last_activity_update = time.time()
    activity_update_interval = 0.1  # Update the activity indicator every 100ms
    
    # Flag to track if we're terminating
    terminating = False
    
    # Signal handler for graceful termination
    def signal_handler(sig, frame):
        nonlocal terminating
        if not terminating:
            terminating = True
            print("\nGeneration interrupted by signal.")
    
    # Register signal handlers
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Use an infinite loop if count is None, otherwise generate the specified number
        i = 0
        while (count is None or i < count) and not terminating:
            # Generate and output the event
            event = generator.generate_event()
            output_plugin.output(event)
            events_generated += 1
            i += 1
            
            # Calculate how much time should have passed for the desired rate
            target_time = start_time + (events_generated / rate)
            
            # Calculate how much time to sleep to maintain the rate
            current_time = time.time()
            sleep_time = max(0, target_time - current_time)
            
            # Update activity indicator if needed
            if should_display_stats and stats_displayed and current_time - last_activity_update >= activity_update_interval:
                # Move cursor up to the activity line and clear it
                sys.stdout.write("\033[1A\033[2K")
                
                # Print the activity indicator
                activity_char = activity_chars[activity_idx]
                activity_idx = (activity_idx + 1) % len(activity_chars)
                print(f"{BLUE}Activity:{RESET} {activity_char} Processing events... ({events_generated} total)")
                
                last_activity_update = current_time
            
            # Display stats at the specified interval (only for non-terminal output)
            if should_display_stats and current_time - last_stats_time >= stats_interval:
                elapsed_since_last = current_time - last_stats_time
                events_since_last = events_generated - last_stats_count
                current_rate = events_since_last / elapsed_since_last
                total_elapsed = current_time - start_time
                overall_rate = events_generated / total_elapsed
                
                # Calculate percentage of target rate achieved
                current_pct = (current_rate / rate) * 100
                overall_pct = (overall_rate / rate) * 100
                
                # Current timestamp
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Get memory usage in MB
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024
                
                # If we've displayed stats before, move cursor up to overwrite the previous stats
                if stats_displayed:
                    move_cursor_up_and_clear(stats_lines)
                
                # Print the stats
                print(f"{BOLD}━━━━━━━━━━━━━━━━ EVENT GENERATOR STATS ━━━━━━━━━━━━━━━━{RESET}")
                print(f"{BLUE}Timestamp:{RESET} {timestamp}")
                print(f"{BLUE}Output plugin:{RESET} {output_type}")
                print(f"{BLUE}Total events generated:{RESET} {events_generated}")
                print(f"{BLUE}Elapsed time:{RESET} {total_elapsed:.2f} seconds")
                print(f"{BLUE}Events per second:{RESET}")
                print(f"  {BLUE}Target rate:{RESET} {rate:.2f}")
                print(f"  {BLUE}Overall rate:{RESET} {overall_rate:.2f} {GREEN}({overall_pct:.1f}% of target){RESET}")
                print(f"  {BLUE}Current rate (last {elapsed_since_last:.1f}s):{RESET} {current_rate:.2f} {GREEN}({current_pct:.1f}% of target){RESET}")
                print(f"{BLUE}Memory usage:{RESET} {memory_usage_mb:.2f} MB")
                print(f"{BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
                
                # Add activity indicator line
                activity_char = activity_chars[activity_idx]
                print(f"{BLUE}Activity:{RESET} {activity_char} Processing events... ({events_generated} total)")
                
                # Set flag to indicate stats have been displayed
                stats_displayed = True
                
                # Reset stats counters
                last_stats_time = current_time
                last_stats_count = events_generated
            
            if sleep_time > 0 and (count is None or i < count) and not terminating:
                time.sleep(sleep_time)
                
            # If we've reached the count, break out of the loop
            if count is not None and i >= count:
                break
    
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\nGeneration interrupted by user.")
    
    finally:
        # Restore original signal handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)
        
        # Calculate final statistics
        end_time = time.time()
        total_time = end_time - start_time
        actual_rate = events_generated / total_time if total_time > 0 else 0
        
        # If we've been displaying stats, clear them before showing the final summary
        if should_display_stats and stats_displayed:
            # Add a newline after the activity indicator
            print("\n")
        else:
            # Just add a newline for spacing
            print("")
        
        # Print the final summary
        if count is not None:
            print(f"Generated {events_generated} events in {total_time:.2f} seconds.")
        else:
            print(f"Generated {events_generated} events in {total_time:.2f} seconds before interruption.")
        
        print(f"Actual rate: {actual_rate:.2f} events/second")
        
        # Clean up the output plugin
        output_plugin.close()


def main():
    """Main function to parse arguments and generate events."""
    parser = argparse.ArgumentParser(description="Generate random person events")
    parser.add_argument("--count", type=int, 
                        help="Number of events to generate (omit for continuous generation)")
    parser.add_argument("--rate", type=float, default=1.0, 
                        help="Number of events per second (min: 1, max: unlimited)")
    parser.add_argument("--output", type=str, default="terminal", choices=["terminal", "file", "kafka"], 
                        help="Output plugin to use")
    parser.add_argument("--output-path", type=str, help="Path for file output")
    parser.add_argument("--schema-dir", type=str, default="schemas", 
                        help="Directory containing the schema files")
    parser.add_argument("--locale", type=str, help="Locale for generating fake data (e.g., 'en_US', 'fr_FR')")
    parser.add_argument("--stats-interval", type=float, default=5.0,
                        help="Interval in seconds between statistics reports (default: 5.0)")
    
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
        
    if args.stats_interval <= 0:
        parser.error("Stats interval must be greater than 0")
    
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
        generate_events_at_rate(generator, output_plugin, args.count, args.rate, args.stats_interval)
    finally:
        output_plugin.close()


if __name__ == "__main__":
    main()
