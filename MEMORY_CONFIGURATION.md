# ClickHouse Memory Configuration

## Problem

If you encounter "Query memory limit exceeded" errors like:

```
Query memory limit exceeded: would use 7.45 GiB (attempt to allocate chunk of 4.14 MiB bytes), maximum: 7.45 GiB
```

## Solutions

### 1. Environment Variables (Recommended)

Add these environment variables to your `.env` file:

```bash
# ClickHouse Memory Configuration (in bytes)
CLICKHOUSE_MAX_MEMORY=20000000000          # 20GB (increased from 16GB)
CLICKHOUSE_MAX_EXTERNAL_MEMORY=15000000000 # 15GB (increased from 12GB)
CLICKHOUSE_MAX_THREADS=2                   # Reduced threads to use less memory
```

### 2. Server Configuration

The server now uses these memory settings by default:

- `max_memory_usage`: 16GB (configurable via `CLICKHOUSE_MAX_MEMORY`)
- `max_bytes_before_external_group_by`: 12GB (configurable via `CLICKHOUSE_MAX_EXTERNAL_MEMORY`)
- `max_bytes_before_external_sort`: 12GB (configurable via `CLICKHOUSE_MAX_EXTERNAL_MEMORY`)
- `max_threads`: 4 (configurable via `CLICKHOUSE_MAX_THREADS`)

### 3. Query-Specific Settings

For brand search queries, additional memory settings are applied:

- `max_memory_usage`: 20GB
- `max_bytes_before_external_group_by`: 15GB
- `max_bytes_before_external_sort`: 15GB

- `max_threads`: 2

### 4. Query Optimization

The brand search query has been optimized to:

- Use a simpler JOIN structure
- Reduce the pre-filter limit from 1000 to 500
- Use more efficient window functions

### 5. Server Memory Requirements

Make sure your server has enough available memory:

- **Minimum**: 32GB RAM
- **Recommended**: 64GB RAM
- **For large datasets**: 128GB+ RAM

### 6. Monitoring

Monitor your server's memory usage:

```bash
# Check memory usage
free -h

# Check ClickHouse memory usage
ps aux | grep clickhouse
```

### 7. Troubleshooting

If you still get memory errors:

1. **Increase memory limits**:

   ```bash
   CLICKHOUSE_MAX_MEMORY=32000000000          # 32GB
   CLICKHOUSE_MAX_EXTERNAL_MEMORY=24000000000 # 24GB
   ```

2. **Reduce query complexity**:

   - Lower the `limit` parameter in your requests
   - Add more specific filters to reduce dataset size

3. **Check server resources**:

   - Ensure ClickHouse has enough available memory
   - Monitor for other processes consuming memory

4. **Optimize ClickHouse server settings**:
   - Increase `max_memory_usage` in ClickHouse server config
   - Adjust `max_bytes_before_external_group_by` and `max_bytes_before_external_sort`

## Example .env Configuration

```bash
# Qdrant Configuration
QDRANT_URL=http://localhost:6333
QDRANT_API_KEY=your_qdrant_api_key_here

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password_here
CLICKHOUSE_DATABASE=analytics

# Memory Configuration (for large queries)
CLICKHOUSE_MAX_MEMORY=20000000000          # 20GB
CLICKHOUSE_MAX_EXTERNAL_MEMORY=15000000000 # 15GB
CLICKHOUSE_MAX_THREADS=2                   # Reduced threads

# Server Configuration
PORT=3001
NODE_ENV=development
```
