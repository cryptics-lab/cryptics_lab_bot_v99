#!/bin/bash
# This script runs the schema migration tests with detailed output

# Set environment variables
export PYTHONPATH=.

# Display header
echo "===================================================================="
echo "Running Schema Migration Tests"
echo "===================================================================="

# Run tests with verbose output
cd /Users/Borat/Desktop/code/cryptics_lab_bot_v99/python_pipeline
python -m pytest tests/test_schema_migration.py -v

# Check status
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Please check the output above."
fi

echo "===================================================================="
