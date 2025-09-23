#!/bin/bash

# ClickHouse Server Setup Script for Large Datasets
# This script configures ClickHouse server to handle large datasets with increased memory limits

echo "🚀 Setting up ClickHouse server for large datasets..."

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run this script as root (use sudo)"
    exit 1
fi

# Detect OS
if [ -f /etc/debian_version ]; then
    OS="debian"
elif [ -f /etc/redhat-release ]; then
    OS="redhat"
else
    echo "❌ Unsupported operating system"
    exit 1
fi

echo "📋 Detected OS: $OS"

# Install ClickHouse if not already installed
if ! command -v clickhouse &> /dev/null; then
    echo "📦 Installing ClickHouse..."
    
    if [ "$OS" = "debian" ]; then
        # Ubuntu/Debian installation
        apt-get update
        apt-get install -y apt-transport-https ca-certificates dirmngr
        apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
        echo "deb https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list
        apt-get update
        apt-get install -y clickhouse-server clickhouse-client
    elif [ "$OS" = "redhat" ]; then
        # CentOS/RHEL installation
        yum install -y yum-utils
        yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
        yum install -y clickhouse-server clickhouse-client
    fi
else
    echo "✅ ClickHouse is already installed"
fi

# Stop ClickHouse service
echo "⏹️ Stopping ClickHouse service..."
systemctl stop clickhouse-server

# Backup original config
if [ -f /etc/clickhouse-server/config.xml ]; then
    echo "💾 Backing up original config..."
    cp /etc/clickhouse-server/config.xml /etc/clickhouse-server/config.xml.backup.$(date +%Y%m%d_%H%M%S)
fi

# Copy new configuration
echo "⚙️ Installing new ClickHouse configuration..."
cp clickhouse-config.xml /etc/clickhouse-server/config.xml

# Set proper permissions
chown clickhouse:clickhouse /etc/clickhouse-server/config.xml
chmod 640 /etc/clickhouse-server/config.xml

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p /var/log/clickhouse-server
mkdir -p /var/lib/clickhouse/tmp
mkdir -p /var/lib/clickhouse/user_files

# Set proper ownership
chown -R clickhouse:clickhouse /var/log/clickhouse-server
chown -R clickhouse:clickhouse /var/lib/clickhouse

# Configure system limits for ClickHouse
echo "🔧 Configuring system limits..."
cat > /etc/security/limits.d/clickhouse.conf << EOF
clickhouse soft nofile 262144
clickhouse hard nofile 262144
clickhouse soft nproc 131072
clickhouse hard nproc 131072
clickhouse soft memlock 134217728
clickhouse hard memlock 134217728
EOF

# Configure systemd service limits
echo "🔧 Configuring systemd service limits..."
mkdir -p /etc/systemd/system/clickhouse-server.service.d
cat > /etc/systemd/system/clickhouse-server.service.d/override.conf << EOF
[Service]
LimitNOFILE=262144
LimitNPROC=131072
LimitMEMLOCK=134217728
EOF

# Reload systemd
systemctl daemon-reload

# Start ClickHouse service
echo "▶️ Starting ClickHouse service..."
systemctl start clickhouse-server

# Wait for service to start
sleep 10

# Check if ClickHouse is running
if systemctl is-active --quiet clickhouse-server; then
    echo "✅ ClickHouse server started successfully"
else
    echo "❌ Failed to start ClickHouse server"
    echo "📋 Checking logs..."
    journalctl -u clickhouse-server --no-pager -l
    exit 1
fi

# Test connection
echo "🧪 Testing ClickHouse connection..."
if clickhouse-client --query "SELECT 1"; then
    echo "✅ ClickHouse connection test successful"
else
    echo "❌ ClickHouse connection test failed"
    exit 1
fi

# Show current memory settings
echo "📊 Current ClickHouse memory settings:"
clickhouse-client --query "SELECT name, value FROM system.settings WHERE name LIKE '%memory%' OR name LIKE '%thread%' ORDER BY name"

echo ""
echo "🎉 ClickHouse server setup completed successfully!"
echo ""
echo "📋 Summary of changes:"
echo "   • Increased max_memory_usage to 64GB"
echo "   • Increased max_memory_usage_for_user to 48GB"
echo "   • Increased max_memory_usage_for_all_queries to 128GB"
echo "   • Increased external memory limits to 36GB"
echo "   • Increased max_threads to 8"
echo "   • Configured system limits for ClickHouse user"
echo ""
echo "🔧 To apply these settings to your application, restart your Node.js service:"
echo "   sudo systemctl restart your-app-service"
echo "   # or if using Docker:"
echo "   docker-compose restart"
echo ""
echo "📊 Monitor memory usage with:"
echo "   clickhouse-client --query \"SELECT * FROM system.processes\""
echo "   free -h"
