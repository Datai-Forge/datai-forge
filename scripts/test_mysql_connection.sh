#!/bin/bash
# Test MySQL connection and user authentication
#
# This script helps diagnose MySQL connection issues before running the full
# database initialization. It tests connectivity and user authentication.
#
# Usage:
#   bash scripts/test_mysql_connection.sh

set -e

echo "=========================================="
echo "MySQL Connection Diagnostic Test"
echo "=========================================="
echo ""

if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please ensure Docker is installed."
    exit 1
fi

echo "📋 Step 1: Checking MySQL container status..."
if docker-compose ps mysql | grep -q "mysql"; then
    echo "✅ MySQL container is running"
else
    echo "⚠️  MySQL container not running. Starting containers..."
    docker-compose up -d
    sleep 3
fi

echo ""
echo "📋 Step 2: Waiting for MySQL to be ready..."
MYSQL_READY=0
MYSQL_ATTEMPTS=0
while [ $MYSQL_READY -eq 0 ] && [ $MYSQL_ATTEMPTS -lt 30 ]; do
    if docker-compose exec -T mysql mysqladmin ping -h localhost >/dev/null 2>&1; then
        MYSQL_READY=1
        echo "✅ MySQL is responding to pings"
    else
        MYSQL_ATTEMPTS=$((MYSQL_ATTEMPTS + 1))
        echo "⏳ Waiting... (attempt $MYSQL_ATTEMPTS/30)"
        sleep 2
    fi
done

if [ $MYSQL_READY -eq 0 ]; then
    echo "❌ MySQL failed to respond after 60 seconds"
    echo ""
    echo "MySQL container logs (last 30 lines):"
    docker-compose logs mysql | tail -30
    exit 1
fi

echo ""
echo "📋 Step 3: Testing root user connection..."
if docker-compose exec -T mysql mysql -h localhost -u root -p"root_secure_password_2026" -e "SELECT 1" > /dev/null 2>&1; then
    echo "✅ Root user authentication successful"
else
    echo "❌ Root user authentication failed"
    echo "   Check MYSQL_ROOT_PASSWORD in docker-compose.yml"
fi

echo ""
echo "📋 Step 4: Testing lyon_user connection..."
if docker-compose exec -T mysql mysql -h localhost -u lyon_user -p"lyon_secure_password_2026" -e "SELECT 1" > /dev/null 2>&1; then
    echo "✅ lyon_user authentication successful"
else
    echo "❌ lyon_user authentication failed"
    echo "   Check MYSQL_USER and MYSQL_PASSWORD in docker-compose.yml"
    echo "   Users in MySQL:"
    docker-compose exec -T mysql mysql -h localhost -u root -p"root_secure_password_2026" -e "SELECT user, host FROM mysql.user;" || echo "   (Failed to list users)"
fi

echo ""
echo "📋 Step 5: Testing database access..."
if docker-compose exec -T mysql mysql -h localhost -u lyon_user -p"lyon_secure_password_2026" -e "SHOW DATABASES;" | grep -q "lyon_decisional"; then
    echo "✅ lyon_user can access lyon_decisional database"
else
    echo "⚠️  lyon_decisional database not found or permission denied"
fi

echo ""
echo "📋 Step 6: Testing app container environment variables..."
echo "Checking MYSQL_* environment variables in app container:"
docker-compose exec -T app env | grep MYSQL || echo "(No MYSQL_* variables found)"

echo ""
echo "=========================================="
echo "✅ Connection diagnostic complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Review any ❌ or ⚠️  messages above"
echo "  2. If credentials are wrong, update docker-compose.yml"
echo "  3. If containers need restart: docker-compose restart"
echo "  4. Run the full init: bash scripts/init_database.sh"
