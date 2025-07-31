#!/bin/bash
# Script to configure Grafana dashboards for Cryptics Lab Bot

# Set default values
GRAFANA_HOST=${GRAFANA_HOST:-http://localhost:3000}
GRAFANA_API_USER=${GRAFANA_USER:-admin}
GRAFANA_API_PASSWORD=${GRAFANA_PASSWORD:-admin}

# Wait for Grafana to be available
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

# Function to check and create a folder if it doesn't exist
create_folder() {
    local folder_name=$1
    local folder_uid=$2
    
    # Check if folder exists
    folder_id=$(curl -s -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/folders" | grep -o "\"uid\":\"$folder_uid\"")
    
    if [ -z "$folder_id" ]; then
        echo "Creating folder: $folder_name"
        curl -X POST -H "Content-Type: application/json" -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/folders" -d "{
            \"uid\": \"$folder_uid\",
            \"title\": \"$folder_name\"
        }"
        echo ""
    else
        echo "Folder already exists: $folder_name"
    fi
}

# Create Cryptics folder
create_folder "Cryptics" "cryptics"

# Function to import or update a dashboard
import_dashboard() {
    local dashboard_json=$1
    local folder_uid=$2
    local dashboard_title=$(cat "$dashboard_json" | grep -o "\"title\":\"[^\"]*\"" | head -1 | cut -d'"' -f4)
    
    echo "Importing dashboard: $dashboard_title"
    
    # Read dashboard JSON
    dashboard_data=$(cat "$dashboard_json")
    
    # Prepare import data
    import_data="{
        \"dashboard\": $dashboard_data,
        \"overwrite\": true,
        \"folderUid\": \"$folder_uid\"
    }"
    
    # Import dashboard
    curl -X POST -H "Content-Type: application/json" -u "$GRAFANA_API_USER:$GRAFANA_API_PASSWORD" "$GRAFANA_HOST/api/dashboards/db" -d "$import_data"
    
    echo ""
    echo "Dashboard imported: $dashboard_title"
}

# Import dashboards
echo "Importing Cryptics dashboards..."
import_dashboard "/var/lib/grafana/dashboards/cryptics_dashboard.json" "cryptics"
import_dashboard "/var/lib/grafana/dashboards/latency-dashboard.json" "cryptics"

echo ""
echo "===================================================================="
echo "Grafana dashboards configured successfully!"
echo "You can access Grafana at $GRAFANA_HOST"
echo "Login with:"
echo "Username: $GRAFANA_API_USER"
echo "Password: $GRAFANA_API_PASSWORD"
echo "===================================================================="
