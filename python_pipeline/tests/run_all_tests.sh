#!/bin/bash
# This script runs all Python tests in the CrypticsLabBot project

# Set environment variables
export PYTHONPATH=.

# Display header
echo "===================================================================="
echo "Running All Python Tests for CrypticsLabBot"
echo "===================================================================="

# Change to project directory
cd /Users/Borat/Desktop/code/cryptics_lab_bot_v99/python_pipeline

# Run all tests with verbose output
python -m pytest -v

# Check status
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Please check the output above."
fi

echo "===================================================================="
