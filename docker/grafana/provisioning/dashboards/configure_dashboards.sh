#!/bin/bash

# Script to configure Grafana dashboards
# This script creates and updates Grafana dashboards using the Grafana API

# Wait for Grafana to be available
echo "Waiting for Grafana to be available..."
until $(curl --output /dev/null --silent --head --fail http://localhost:3000/api/health); do
    printf '.'
    sleep 2
done
echo "Grafana is up and running!"

# Paths to dashboard JSON files
CRYPTO_DASHBOARD="/var/lib/grafana/dashboards/cryptics-dashboard.json"
LATENCY_DASHBOARD="/var/lib/grafana/dashboards/latency-dashboard.json"

# Wait a bit more to ensure Grafana is fully initialized
sleep 10

echo "Checking for existing dashboards..."

# Create or update dashboards
echo "Importing dashboards..."

# Import the Cryptics Trading Dashboard
if [ -f "$CRYPTO_DASHBOARD" ]; then
    echo "Importing Cryptics Trading Dashboard..."
    curl -X POST --silent \
        -H "Content-Type: application/json" \
        -d @$CRYPTO_DASHBOARD \
        http://admin:admin@localhost:3000/api/dashboards/db
    echo "Cryptics Trading Dashboard imported successfully."
else
    echo "Warning: $CRYPTO_DASHBOARD file not found."
fi

# Import the Latency Dashboard
if [ -f "$LATENCY_DASHBOARD" ]; then
    echo "Importing Latency Dashboard..."
    curl -X POST --silent \
        -H "Content-Type: application/json" \
        -d @$LATENCY_DASHBOARD \
        http://admin:admin@localhost:3000/api/dashboards/db
    echo "Latency Dashboard imported successfully."
else
    echo "Warning: $LATENCY_DASHBOARD file not found."
fi

echo "Dashboard configuration complete."
