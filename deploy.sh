#!/bin/bash

# QDrant Search Service Deployment Script
# Run this script on your Linux server

echo "🚀 Starting QDrant Search Service Deployment..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# QDrant Search Service Configuration
OPENAI_API_KEY=your_openai_api_key_here
QDRANT_URL=http://qdrant:6333
QDRANT_API_KEY=
PORT=3001
NODE_ENV=production
COLLECTION_NAME=ads
EOF
    echo "⚠️  Please edit .env file with your actual values!"
    echo "⚠️  Especially set your OPENAI_API_KEY"
    read -p "Press Enter to continue after editing .env file..."
fi

# Build and start the services
echo "🏗️  Building Docker containers..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d

echo "⏰ Waiting for services to be ready..."
sleep 30

# Check if services are running
if curl -f http://localhost:3001/health > /dev/null 2>&1; then
    echo "✅ QDrant Search Service is running on port 3001!"
    echo "🔗 Health check: http://localhost:3001/health"
    echo "🔗 Service endpoints:"
    echo "   - POST /search/videos"
    echo "   - POST /search/videos/filtered"
    echo "   - GET /collection/info"
else
    echo "❌ Service health check failed. Checking logs..."
    docker-compose logs qdrant-search-service
fi

echo "📊 Service status:"
docker-compose ps 