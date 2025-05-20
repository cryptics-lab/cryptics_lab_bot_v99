#!/usr/bin/env python3
"""
Test runner for CrypticsLabBot Python Pipeline.
Runs all tests in the tests directory.
"""

import logging
import os
import sys
import unittest

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test-runner")

def run_tests():
    """Run all tests in the tests directory"""
    # Discover and run tests
    logger.info("Discovering tests...")
    
    # Get the directory of this script
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Run tests
    suite = unittest.defaultTestLoader.discover(test_dir, pattern='test_*.py')
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code based on test results
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    try:
        logger.info("Starting test run")
        exit_code = run_tests()
        logger.info(f"Test run completed with exit code {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Error running tests: {e}")
        sys.exit(1)
