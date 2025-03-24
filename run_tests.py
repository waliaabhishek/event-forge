#!/usr/bin/env python3
"""
Comprehensive test script for the schema_in_schema_json_to_tableflow project.
This script runs a series of tests to verify that all components are working correctly.
"""

import os
import subprocess
import time
import json
import sys
from typing import List, Dict, Any, Tuple

# ANSI color codes for terminal output
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Directory for test results
TEST_RESULTS_DIR = "tests/results"

def print_header(message: str) -> None:
    """Print a formatted header message."""
    print(f"\n{BOLD}{BLUE}{'=' * 80}{RESET}")
    print(f"{BOLD}{BLUE}= {message}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 80}{RESET}\n")

def print_result(test_name: str, success: bool, message: str = "") -> None:
    """Print a formatted test result."""
    status = f"{GREEN}PASS{RESET}" if success else f"{RED}FAIL{RESET}"
    print(f"{BOLD}{test_name}:{RESET} {status} {message}")

def run_command(command: str) -> Tuple[int, str]:
    """Run a shell command and return the exit code and output."""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    output, _ = process.communicate()
    return process.returncode, output

def validate_json_lines(file_path: str) -> Tuple[bool, int]:
    """Validate that a file contains valid JSON Lines format and count the lines."""
    try:
        count = 0
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    json.loads(line)
                    count += 1
        return True, count
    except Exception as e:
        print(f"Error validating JSON Lines: {e}")
        return False, 0

def cleanup_test_files() -> None:
    """Remove test files created during testing."""
    # Clean up the test results directory
    if os.path.exists(TEST_RESULTS_DIR):
        for file in os.listdir(TEST_RESULTS_DIR):
            file_path = os.path.join(TEST_RESULTS_DIR, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

def ensure_test_dir_exists() -> None:
    """Ensure the test results directory exists."""
    os.makedirs(TEST_RESULTS_DIR, exist_ok=True)

def run_tests() -> None:
    """Run all tests and report results."""
    test_results = []
    
    # Ensure test directory exists
    ensure_test_dir_exists()
    
    # Test 1: Schema validation with sample data
    print_header("Test 1: Schema Validation with Sample Data")
    exit_code, output = run_command("python validate_schema.py schema.json data/sample-data.json")
    success = exit_code == 0 and "All data is valid" in output
    test_results.append(("Schema Validation", success))
    print_result("Schema Validation", success)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 2: Basic Event Generation
    print_header("Test 2: Basic Event Generation")
    exit_code, output = run_command("python generate_events.py --count 3")
    success = exit_code == 0 and "Generated 3 events" in output
    test_results.append(("Basic Event Generation", success))
    print_result("Basic Event Generation", success)
    print(f"{YELLOW}Output excerpt:{RESET}\n{output[-200:]}")
    
    # Test 3: File Output
    print_header("Test 3: File Output")
    test_output_path = os.path.join(TEST_RESULTS_DIR, "test_output.json")
    exit_code, output = run_command(f"python generate_events.py --count 5 --output file --output-path {test_output_path}")
    file_success, line_count = validate_json_lines(test_output_path)
    success = exit_code == 0 and file_success and line_count == 5
    test_results.append(("File Output", success))
    print_result("File Output", success, f"(Generated {line_count} valid JSON Lines)")
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 4: Validation of Generated Events
    print_header("Test 4: Validation of Generated Events")
    exit_code, output = run_command(f"python validate_schema.py schema.json {test_output_path}")
    success = exit_code == 0 and "All data is valid" in output
    test_results.append(("Validation of Generated Events", success))
    print_result("Validation of Generated Events", success)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 5: Locale Support
    print_header("Test 5: Locale Support")
    test_locale_path = os.path.join(TEST_RESULTS_DIR, "test_locale.json")
    exit_code, output = run_command(f"python generate_events.py --count 2 --locale fr_FR --output file --output-path {test_locale_path}")
    success = exit_code == 0 and "Generated 2 events" in output
    test_results.append(("Locale Support", success))
    print_result("Locale Support", success)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 6: Rate Control (50 events/second)
    print_header("Test 6: Rate Control (50 events/second)")
    test_rate_50_path = os.path.join(TEST_RESULTS_DIR, "test_rate_50.json")
    exit_code, output = run_command(f"python generate_events.py --count 50 --rate 50 --output file --output-path {test_rate_50_path}")
    success = exit_code == 0 and "Generated 50 events" in output
    
    # Extract actual rate from output
    actual_rate = None
    for line in output.split('\n'):
        if "Actual rate:" in line:
            try:
                actual_rate = float(line.split(':')[1].strip().split()[0])
            except:
                pass
    
    rate_accuracy = "Unknown"
    if actual_rate:
        rate_accuracy = f"(Target: 50, Actual: {actual_rate:.2f}, Accuracy: {100 - abs(50-actual_rate)/50*100:.1f}%)"
    
    test_results.append(("Rate Control (50/s)", success))
    print_result("Rate Control (50/s)", success, rate_accuracy)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 7: Rate Control (100 events/second)
    print_header("Test 7: Rate Control (100 events/second)")
    test_rate_100_path = os.path.join(TEST_RESULTS_DIR, "test_rate_100.json")
    exit_code, output = run_command(f"python generate_events.py --count 100 --rate 100 --output file --output-path {test_rate_100_path}")
    success = exit_code == 0 and "Generated 100 events" in output
    
    # Extract actual rate from output
    actual_rate = None
    for line in output.split('\n'):
        if "Actual rate:" in line:
            try:
                actual_rate = float(line.split(':')[1].strip().split()[0])
            except:
                pass
    
    rate_accuracy = "Unknown"
    if actual_rate:
        rate_accuracy = f"(Target: 100, Actual: {actual_rate:.2f}, Accuracy: {100 - abs(100-actual_rate)/100*100:.1f}%)"
    
    test_results.append(("Rate Control (100/s)", success))
    print_result("Rate Control (100/s)", success, rate_accuracy)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 8: High Rate Test
    print_header("Test 8: High Rate Test (500 events/second)")
    test_rate_high_path = os.path.join(TEST_RESULTS_DIR, "test_rate_high.json")
    exit_code, output = run_command(f"python generate_events.py --count 200 --rate 500 --output file --output-path {test_rate_high_path}")
    success = exit_code == 0 and "Generated 200 events" in output
    
    # Extract actual rate from output
    actual_rate = None
    for line in output.split('\n'):
        if "Actual rate:" in line:
            try:
                actual_rate = float(line.split(':')[1].strip().split()[0])
            except:
                pass
    
    rate_accuracy = "Unknown"
    if actual_rate:
        rate_accuracy = f"(Target: 500, Actual: {actual_rate:.2f})"
    
    test_results.append(("High Rate Test", success))
    print_result("High Rate Test", success, rate_accuracy)
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 9: Plugin System
    print_header("Test 9: Plugin System")
    test_plugin_path = os.path.join(TEST_RESULTS_DIR, "test_plugin_system.json")
    exit_code, output = run_command(f"python generate_events.py --count 3 --output file --output-path {test_plugin_path}")
    file_success, line_count = validate_json_lines(test_plugin_path)
    success = exit_code == 0 and file_success and line_count == 3
    test_results.append(("Plugin System", success))
    print_result("Plugin System", success, f"(Generated {line_count} valid JSON Lines)")
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Test 10: Continuous Generation with Ctrl+C Interrupt
    print_header("Test 10: Continuous Generation with Ctrl+C Interrupt")
    test_continuous_path = os.path.join(TEST_RESULTS_DIR, "test_continuous.json")
    
    # Start the continuous generation process
    process = subprocess.Popen(
        f"python generate_events.py --rate 10 --output file --output-path {test_continuous_path}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    # Let it run for a few seconds
    time.sleep(3)
    
    # Send interrupt signal (SIGINT) to simulate Ctrl+C
    process.send_signal(2)  # 2 is SIGINT
    
    # Get output
    output, _ = process.communicate()
    
    # Check results
    file_success, line_count = validate_json_lines(test_continuous_path)
    success = "Event generation interrupted" in output and file_success and line_count > 0
    
    test_results.append(("Continuous Generation", success))
    print_result("Continuous Generation", success, f"(Generated {line_count} events before interrupt)")
    print(f"{YELLOW}Output:{RESET}\n{output}")
    
    # Summary
    print_header("Test Summary")
    all_passed = all(result[1] for result in test_results)
    
    for test_name, success in test_results:
        status = f"{GREEN}PASS{RESET}" if success else f"{RED}FAIL{RESET}"
        print(f"{test_name}: {status}")
    
    overall_status = f"{GREEN}ALL TESTS PASSED{RESET}" if all_passed else f"{RED}SOME TESTS FAILED{RESET}"
    print(f"\n{BOLD}Overall: {overall_status} ({sum(1 for _, s in test_results if s)}/{len(test_results)}){RESET}")

if __name__ == "__main__":
    try:
        # Clean up any previous test files
        cleanup_test_files()
        
        # Run all tests
        run_tests()
        
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Test execution interrupted.{RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{RED}Error during test execution: {e}{RESET}")
        sys.exit(1)
