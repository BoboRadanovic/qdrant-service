# Use Node.js 18 LTS Alpine image for smaller size
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install PM2 globally
RUN npm install -g pm2

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --omit=dev && npm cache clean --force

# Copy application files first
COPY . .

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nextjs -u 1001

# Create logs directory and set permissions
RUN mkdir -p logs && \
    chown -R nextjs:nodejs /app

USER nextjs

# Expose port
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3001/health', (res) => { process.exit(res.statusCode === 200 ? 0 : 1) })"

# Start the application directly with Node.js
CMD ["node", "server.js"] 