#!/bin/bash
# Initialize the Lyon Decisional Database Schema
#
# This script should be run AFTER both the MySQL and app containers are running.
# It calls the Python database loader with proper error handling.
#
# Usage:
#   bash scripts/init_database.sh
#   OR
#   ./scripts/init_database.sh

set -e  # Exit on error

echo "=========================================="
echo "Lyon Decisional Database Initialization"
echo "=========================================="
echo ""

# Check if we're in docker-compose context
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please ensure Docker is installed."
    exit 1
fi

echo "📋 Step 1: Verifying MySQL container is healthy..."
MYSQL_READY=0
MYSQL_ATTEMPTS=0
MYSQL_MAX_ATTEMPTS=30

while [ $MYSQL_READY -eq 0 ] && [ $MYSQL_ATTEMPTS -lt $MYSQL_MAX_ATTEMPTS ]; do
    if docker-compose exec -T mysql mysqladmin ping -h localhost >/dev/null 2>&1; then
        MYSQL_READY=1
        echo "✅ MySQL is ready."
    else
        MYSQL_ATTEMPTS=$((MYSQL_ATTEMPTS + 1))
        echo "⏳ MySQL not yet ready (attempt $MYSQL_ATTEMPTS/$MYSQL_MAX_ATTEMPTS). Waiting 2s..."
        sleep 2
    fi
done

if [ $MYSQL_READY -eq 0 ]; then
    echo "❌ MySQL failed to start after 60 seconds"
    echo "📋 Debugging info:"
    docker-compose logs mysql | tail -50
    exit 1
fi

echo ""

echo "📋 Step 2: Running database schema initialization..."
echo ""
set +e
INIT_OUTPUT=$(docker-compose exec app python3 -m src.database_loader 2>&1)
EXIT_CODE=$?
set -e

printf '%s\n' "$INIT_OUTPUT"

echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo "=========================================="
    echo "✅ Database schema created successfully!"
    echo "=========================================="
    echo ""
    echo "You can now verify the tables:"
    echo "  docker-compose exec mysql mysql -u lyon_user -p lyon_decisional -e 'SHOW TABLES;'"
    exit 0
else
    echo "=========================================="
    echo "❌ Database initialization failed!"
    echo "=========================================="
    echo ""
    if printf '%s\n' "$INIT_OUTPUT" | grep -q "Access denied for user"; then
        echo "This looks like a MySQL volume initialized with different credentials."
        echo "The safest fix is to recreate the database volume, then rerun initialization."
        echo ""
        echo "Recommended commands:"
        echo "  docker-compose down -v"
        echo "  docker-compose up -d"
        echo "  bash scripts/init_database.sh"
        echo ""
    fi
    echo "Troubleshooting tips:"
    echo "  1. Check MySQL is running: docker-compose ps mysql"
    echo "  2. Check app container logs: docker-compose logs app"
    echo "  3. Check MySQL container logs: docker-compose logs mysql"
    echo "  4. Verify credentials in docker-compose.yml match"
    exit 1
fi
