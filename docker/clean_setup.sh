#!/bin/bash
# clean_setup.sh
# Description: Remove all Docker containers and volumes related to the project and prepare for a fresh start

echo "Cleaning up existing Docker resources..."

# Stop all running containers from our docker-compose file
echo "Stopping all containers..."
docker-compose down -v --remove-orphans

# Find and remove any other containers with names that match our project
echo "Checking for other related containers..."
project_containers=$(docker ps -a --filter name='broker\|zk\|kafka\|schema\|connect\|timescale\|pgadmin\|grafana\|crypto\|cli' --format "{{.Names}}")
if [ -n "$project_containers" ]; then
    echo "Removing these containers: $project_containers"
    docker rm -f $project_containers 2>/dev/null || true
else
    echo "No other related containers found."
fi

# Remove all volumes related to the project
echo "Removing related Docker volumes..."
docker volume ls --filter name='docker\|timescale\|grafana\|kafka\|zk' --format "{{.Name}}" | xargs -r docker volume rm

# Remove any unused networks
echo "Cleaning up networks..."
docker network prune -f

# Clean up any dangling images
echo "Cleaning up dangling images..."
docker image prune -f

echo "Cleanup complete! The environment is ready for a fresh start."
echo "Run ./start_pipeline.sh to create a new environment."
