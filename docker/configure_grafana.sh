#!/bin/bash
# configure_grafana.sh - Ensure Grafana data source is properly configured

# Load environment variables if present
if [ -f .env ]; then
    source .env
fi

# Default values
GRAFANA_HOST=${GRAFANA_HOST:-http://localhost:3000}
GRAFANA_API_USER=${GRAFANA_USER:-admin}
GRAFANA_API_PASSWORD=${GRAFANA_PASSWORD:-admin}
DB_HOST=${DB_HOST:-timescaledb}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-cryptics}
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-postgres}

echo "Waiting for Grafana to be available..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s "$GRAFANA_HOST/api/health" | grep -q "ok"; then
        echo "Grafana is up and running!"
        break
    fi
    attempt=$((attempt+1))
    echo "Attempt $attempt/$max_attempts - Waiting for Grafana to start..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "Grafana did not start within the expected time."
    exit 1
fi

# Check if datasource exists
echo "Checking if TimescaleDB datasource exists..."
DATASOURCE_ID=$(curl -s -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/datasources/name/TimescaleDB" | grep -o '"id":[0-9]*' | grep -o '[0-9]*')

if [ -z "$DATASOURCE_ID" ]; then
    echo "TimescaleDB datasource not found. Creating..."
    
    # Create the datasource
    curl -X POST -H "Content-Type: application/json" -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/datasources" -d '{
        "name":"TimescaleDB",
        "type":"postgres",
        "url":"'"$DB_HOST"':'"$DB_PORT"'",
        "database":"'"$DB_NAME"'",
        "user":"'"$DB_USER"'",
        "secureJsonData": {
            "password":"'"$DB_PASSWORD"'"
        },
        "jsonData": {
            "sslmode":"disable",
            "postgresVersion":1500,
            "timescaledb":true
        }
    }'
    
    echo "Datasource created!"
else
    echo "TimescaleDB datasource already exists (ID: $DATASOURCE_ID)."
    
    # Let's update it to make sure it has the correct settings
    echo "Updating datasource to ensure correct configuration..."
    
    curl -X PUT -H "Content-Type: application/json" -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/datasources/$DATASOURCE_ID" -d '{
        "name":"TimescaleDB",
        "type":"postgres",
        "url":"'"$DB_HOST"':'"$DB_PORT"'",
        "database":"'"$DB_NAME"'",
        "user":"'"$DB_USER"'",
        "secureJsonData": {
            "password":"'"$DB_PASSWORD"'"
        },
        "jsonData": {
            "sslmode":"disable",
            "postgresVersion":1500,
            "timescaledb":true
        }
    }'
    
    echo "Datasource updated!"
fi

# Testing datasource connection
echo "Testing datasource connection..."
TEST_RESULT=$(curl -X POST -H "Content-Type: application/json" -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/datasources/proxy/1/query" -d '{"queries":[{"refId":"A","datasource":{"type":"postgres","uid":""},"rawSql":"SELECT 1","format":"table"}]}')

if echo "$TEST_RESULT" | grep -q "rows"; then
    echo "Datasource connection test successful!"
else
    echo "Datasource connection test failed. Response:"
    echo "$TEST_RESULT"
    exit 1
fi

# Configure dashboards
echo "Configuring Grafana dashboards..."
bash /etc/grafana/provisioning/dashboards/configure_dashboards.sh

echo ""
echo "===================================================================="
echo "Grafana is configured and ready to use!"
echo "You can access Grafana at $GRAFANA_HOST"
echo "Login with:"
echo "Username: $GRAFANA_API_USER"
echo "Password: $GRAFANA_API_PASSWORD"
echo "===================================================================="
