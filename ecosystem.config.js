module.exports = {
  apps: [
    {
      name: "qdrant-search-service",
      script: "server.js",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      max_memory_restart: "1G",
      env: {
        NODE_ENV: "production",
        PORT: 3001,
      },
      env_production: {
        NODE_ENV: "production",
        PORT: 3001,
      },
      // Restart policy
      restart_delay: 4000,
      max_restarts: 10,
      min_uptime: "10s",

      // Logging - disabled for Docker
      log_file: "/dev/null",
      out_file: "/dev/null",
      error_file: "/dev/null",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
    },
  ],
};
