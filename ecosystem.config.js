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

      // Logging
      log_file: "./logs/combined.log",
      out_file: "./logs/out.log",
      error_file: "./logs/error.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
    },
  ],
};
