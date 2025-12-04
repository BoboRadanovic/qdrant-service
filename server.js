require("dotenv").config();
const express = require("express");
const axios = require("axios");
const { OpenAI } = require("openai");
const { createClient } = require("@clickhouse/client");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const compression = require("compression");
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccount.json");
const { private_key_id, private_key } = require("./firebasePrivateKey.json");

console.log("=== FIREBASE CONFIG DEBUG ===");
console.log("📋 Service Account project_id:", serviceAccount.project_id);
console.log("📋 Service Account client_email:", serviceAccount.client_email);
console.log("📋 Service Account client_id:", serviceAccount.client_id);
console.log("🔑 private_key_id from JSON:", private_key_id);
console.log(
  "🔑 private_key (first 50 chars):",
  private_key ? private_key.substring(0, 50) : "MISSING"
);
console.log(
  "🔑 private_key (last 50 chars):",
  private_key ? private_key.substring(private_key.length - 50) : "MISSING"
);
console.log("🔑 private_key length:", private_key ? private_key.length : 0);
console.log(
  "🔑 Contains \\n sequences:",
  private_key ? private_key.includes("\\n") : false
);

const config = {
  ...serviceAccount,
  private_key_id: private_key_id,
  private_key: private_key.replace(/\\n/g, "\n"),
};

console.log("🔧 Final config project_id:", config.project_id);
console.log("🔧 Final config private_key_id:", config.private_key_id);
console.log(
  "🔧 Final config private_key (first 50 chars):",
  config.private_key.substring(0, 50)
);
console.log(
  "🔧 Final config private_key contains actual newlines:",
  config.private_key.includes("\n")
);
console.log("=== END DEBUG ===\n");

admin.initializeApp({
  credential: admin.credential.cert(config),
});

const firebaseAuth = admin.auth();

// Validate required environment variables
if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY is required in environment variables");
  process.exit(1);
}

if (!process.env.QDRANT_URL) {
  console.error("❌ QDRANT_URL is required in environment variables");
  process.exit(1);
}

if (!process.env.CLICKHOUSE_HOST) {
  console.error("❌ CLICKHOUSE_HOST is required in environment variables");
  process.exit(1);
}

if (!process.env.CLICKHOUSE_USER) {
  console.error("❌ CLICKHOUSE_USER is required in environment variables");
  process.exit(1);
}

if (!process.env.CLICKHOUSE_PASSWORD) {
  console.error("❌ CLICKHOUSE_PASSWORD is required in environment variables");
  process.exit(1);
}

// Initialize OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Memory configuration (must be defined before ClickHouse client initialization)
const CLICKHOUSE_MAX_MEMORY =
  parseInt(process.env.CLICKHOUSE_MAX_MEMORY) || 48000000000; // 48GB default (increased for large datasets)
const CLICKHOUSE_MAX_EXTERNAL_MEMORY =
  parseInt(process.env.CLICKHOUSE_MAX_EXTERNAL_MEMORY) || 36000000000; // 36GB default (increased for large datasets)
const CLICKHOUSE_MAX_THREADS =
  parseInt(process.env.CLICKHOUSE_MAX_THREADS) || 8; // Increased threads for better performance

// Initialize ClickHouse client
const clickhouse = createClient({
  url: `http://${process.env.CLICKHOUSE_HOST}:${
    process.env.CLICKHOUSE_PORT || 8123
  }`,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DATABASE || "analytics",
  clickhouse_settings: {
    async_insert: 1,
    wait_for_async_insert: 1,
    // Increased memory limits for large queries
    max_threads: CLICKHOUSE_MAX_THREADS,
    max_memory_usage: CLICKHOUSE_MAX_MEMORY,
    max_bytes_before_external_group_by: CLICKHOUSE_MAX_EXTERNAL_MEMORY,
    max_bytes_before_external_sort: CLICKHOUSE_MAX_EXTERNAL_MEMORY,

    // Optimize for large IN clauses
    max_expanded_ast_elements: 1000000,
    max_ast_elements: 1000000,

    // Query optimizations
    optimize_aggregation_in_order: 1,
    optimize_read_in_order: 1,

    // Reduced parallel processing to use less memory
    parallel_replicas_count: 1,
    max_parallel_replicas: 1,

    // Network optimizations
    tcp_keep_alive_timeout: 300,

    // Additional memory optimizations
    max_memory_usage_for_user: 48000000000, // 48GB per user (increased for large datasets)
    max_memory_usage_for_all_queries: 64000000000, // 64GB total (increased for large datasets)
  },
});

// Test ClickHouse connection
async function testClickHouseConnection() {
  try {
    const result = await clickhouse.query({
      query: "SELECT 1 as test",
      format: "JSONEachRow",
    });
    const data = await result.json();
    console.log("✅ ClickHouse connection successful");
    return true;
  } catch (error) {
    console.error("❌ ClickHouse connection failed:", error.message);
    return false;
  }
}

// Check Qdrant collections
async function checkQdrantCollections() {
  try {
    // Check main collection
    const mainCollection = await axios.get(
      `${qdrantUrl}/collections/${COLLECTION_NAME}`,
      { headers: buildHeaders(), timeout: 10000 }
    );
    console.log(`✅ Main collection '${COLLECTION_NAME}' exists`);

    // Check brands collection
    const brandsCollection = await axios.get(
      `${qdrantUrl}/collections/${COLLECTION_NAME_BRANDS}`,
      { headers: buildHeaders(), timeout: 10000 }
    );
    console.log(`✅ Brands collection '${COLLECTION_NAME_BRANDS}' exists`);

    // Check companies collection
    try {
      const companiesCollection = await axios.get(
        `${qdrantUrl}/collections/${COLLECTION_NAME_COMPANIES}`,
        { headers: buildHeaders(), timeout: 10000 }
      );
      console.log(
        `✅ Companies collection '${COLLECTION_NAME_COMPANIES}' exists`
      );
    } catch (error) {
      console.error(
        `❌ Companies collection '${COLLECTION_NAME_COMPANIES}' does not exist`
      );
      console.error(
        "❌ Companies collection error:",
        error.response?.data || error.message
      );
    }
  } catch (error) {
    console.error("❌ Qdrant connection failed:", error.message);
  }
}

// Configuration
const PORT = process.env.PORT || 3001;
const qdrantUrl = process.env.QDRANT_URL;
const qdrantApiKey = process.env.QDRANT_API_KEY;
const COLLECTION_NAME = process.env.COLLECTION_NAME || "ads";
const COLLECTION_NAME_BRANDS = process.env.COLLECTION_NAME_BRANDS || "brands";
const COLLECTION_NAME_COMPANIES =
  process.env.COLLECTION_NAME_COMPANIES || "companies";
const DEFAULT_LIMIT = 200;

// Excluded categories - videos/brands/companies with these categories should not be returned
const EXCLUDED_CATEGORIES = [
  66, 957, 55, 71, 75, 68, 57, 73, 60, 70, 7, 53, 530, 1144, 1148, 29, 998, 708,
  97, 1030, 12, 34, 37, 93,
];

// Helper function to build headers with API key
function buildHeaders() {
  const headers = {
    "Content-Type": "application/json",
  };

  if (qdrantApiKey) {
    headers["api-key"] = qdrantApiKey;
  }

  return headers;
}

// Function to check if service is alive
async function checkServiceHealth(serviceUrl = `http://localhost:${PORT}`) {
  try {
    const response = await axios.get(`${serviceUrl}/health`, {
      timeout: 5000, // 5 second timeout
    });

    return {
      alive: true,
      status: response.status,
      data: response.data,
      responseTime: response.headers["x-response-time"] || "N/A",
    };
  } catch (error) {
    return {
      alive: false,
      error: error.message,
      code: error.code || "UNKNOWN_ERROR",
      status: error.response?.status || null,
    };
  }
}

// Create embedding function
async function createEmbedding(text, dimensions = 1536) {
  try {
    const response = await openai.embeddings.create({
      model: "text-embedding-3-small",
      input: text,
      encoding_format: "float",
      dimensions: dimensions, // Can be 256, 512, 1024, 1536, or 3072
    });
    return response.data[0].embedding;
  } catch (error) {
    console.error("Error creating embedding:", error.message);
    throw new Error("Failed to create embedding");
  }
}

// Helper function to build ClickHouse WHERE condition for duration ranges
// Input: durationRanges = [[0, 30], [60, 120], [600, null]] or null/undefined
// Returns: SQL condition string like "(duration >= 0 AND duration <= 30) OR (duration >= 60 AND duration <= 120) OR (duration >= 600)"
// Returns: null if no ranges provided
function buildClickHouseDurationFilter(durationRanges, fieldName = "duration") {
  if (
    !durationRanges ||
    !Array.isArray(durationRanges) ||
    durationRanges.length === 0
  ) {
    return null;
  }

  const conditions = durationRanges
    .filter(
      (range) =>
        Array.isArray(range) && range.length > 0 && typeof range[0] === "number"
    )
    .map((range) => {
      const min = range[0];
      const max = range.length > 1 ? range[1] : null;

      if (max === null) {
        // Open-ended range: [600, null] -> duration >= 600
        return `${fieldName} >= ${min}`;
      } else {
        // Closed range: [0, 30] -> duration >= 0 AND duration <= 30
        return `(${fieldName} >= ${min} AND ${fieldName} <= ${max})`;
      }
    });

  if (conditions.length === 0) {
    return null;
  }

  // If only one condition, return it without OR wrapper
  if (conditions.length === 1) {
    return conditions[0];
  }

  // Multiple conditions: wrap in parentheses with OR
  return `(${conditions.join(" OR ")})`;
}

// Helper function to build Qdrant filter for duration ranges
// Input: durationRanges = [[0, 30], [60, 120], [600, null]] or null/undefined
// Returns: Object with filter structure, or null if no ranges
// For single range: returns filter object to push to filters.must
// For multiple ranges: returns { should: [...] } to add at filters root level (filters.should)
function buildQdrantDurationFilter(durationRanges, fieldName = "duration") {
  if (
    !durationRanges ||
    !Array.isArray(durationRanges) ||
    durationRanges.length === 0
  ) {
    return null;
  }

  const rangeFilters = durationRanges
    .filter(
      (range) =>
        Array.isArray(range) && range.length > 0 && typeof range[0] === "number"
    )
    .map((range) => {
      const min = range[0];
      const max = range.length > 1 ? range[1] : null;

      const rangeFilter = { key: fieldName, range: {} };

      if (min !== null && typeof min === "number") {
        rangeFilter.range.gte = min;
      }

      if (max !== null && typeof max === "number") {
        rangeFilter.range.lte = max;
      }
      // If max is null, it's open-ended (only gte is set)

      return rangeFilter;
    });

  if (rangeFilters.length === 0) {
    return null;
  }

  // If only one range, return it as a filter object to push to must
  if (rangeFilters.length === 1) {
    return rangeFilters[0];
  }

  // Multiple ranges: return structure to merge into filters (should clause)
  // Note: should clause defaults to matching at least one condition, so min_should is not needed
  return {
    should: rangeFilters,
  };
}

// Helper to call external /api/videosPublic/search-ids service
async function fetchExternalVideoIds({
  searchTerm,
  categoryIds,
  languageId,
  showVideos,
  durationRanges = null, // New: Array of ranges: [[0, 30], [60, 120], [600, null]]
  durationMoreThen = null, // Legacy: kept for backward compatibility
  durationLessThen = null, // Legacy: kept for backward compatibility
  limit,
  token,
  dateFrom,
  dateTo,
  orientation,
}) {
  try {
    // Convert categoryIds=[0] to empty array
    const normalizedCategoryIds =
      categoryIds === 0 ||
      (Array.isArray(categoryIds) &&
        categoryIds.length === 1 &&
        categoryIds[0] === 0)
        ? []
        : categoryIds;
    const payload = {
      searchTerm: searchTerm || null,
      categoryIds: normalizedCategoryIds,
      language: languageId || null,
      showVideos, // Pass videoStatus directly (can be "all", "public", "unlisted", or null)
      limit: limit || 200,
      dateFrom: dateFrom || null,
      dateTo: dateTo || null,
      orientation: orientation || 0,
    };

    // Forward durationRanges if provided, otherwise fall back to legacy parameters
    if (
      durationRanges &&
      Array.isArray(durationRanges) &&
      durationRanges.length > 0
    ) {
      payload.durationRanges = durationRanges;
    } else {
      // Legacy format: only include if at least one is provided
      if (durationMoreThen !== null || durationLessThen !== null) {
        payload.durationMoreThen = durationMoreThen || null;
        payload.durationLessThen = durationLessThen || null;
      }
    }
    //console.log("payload", payload);
    // Remove undefined/null keys
    Object.keys(payload).forEach(
      (key) => payload[key] === null && delete payload[key]
    );

    const headers = {};
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
    console.log(payload);
    const response = await axios.post(
      "https://apiv1.vidtao.com/api/videosPublic/search-ids",
      payload,
      {
        timeout: 20000,
        headers,
      }
    );
    if (
      response.data &&
      response.data.data &&
      Array.isArray(response.data.data.videoIds)
    ) {
      return response.data.data.videoIds;
    }
    return [];
  } catch (e) {
    console.error("Error fetching external video IDs:", e.message);
    return [];
  }
}

async function fetchExternalBrandIds({
  searchTerm,
  categoryIds,
  countryId,
  softwareIds,
  durationMoreThen,
  durationLessThen,
  limit,
  token,
}) {
  try {
    const payload = {
      searchTerm: searchTerm || null,
      categoryIds: categoryIds || null,
      countryId: countryId || null,
      softwareIds: softwareIds || null,
      durationMoreThen: durationMoreThen || null,
      durationLessThen: durationLessThen || null,
      limit: limit || 200,
    };
    // Remove undefined/null keys
    Object.keys(payload).forEach(
      (key) => payload[key] === null && delete payload[key]
    );

    const headers = {};
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    const response = await axios.post(
      "https://apiv1.vidtao.com/api/videosPublic/search-brands-ids",
      payload,
      {
        timeout: 20000,
        headers,
      }
    );

    if (
      response.data &&
      response.data.data &&
      Array.isArray(response.data.data.brandIds)
    ) {
      return response.data.data.brandIds;
    }

    return [];
  } catch (e) {
    console.error("Error fetching external brand IDs:", e.message);
    if (e.response) {
      console.error("Error response status:", e.response.status);
      console.error(
        "Error response data:",
        JSON.stringify(e.response.data, null, 2)
      );
    }
    return [];
  }
}

async function fetchExternalCompanyIds({
  searchTerm,
  categoryIds,
  countryId,
  softwareIds,
  limit,
  token,
}) {
  try {
    const payload = {
      searchTerm: searchTerm || null,
      categoryIds: categoryIds || null,
      countryId: countryId || null,
      softwareIds: softwareIds || null,
      limit: limit || 200,
    };
    // Remove undefined/null keys
    Object.keys(payload).forEach(
      (key) => payload[key] === null && delete payload[key]
    );

    const headers = {};
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    const response = await axios.post(
      "https://apiv1.vidtao.com/api/videosPublic/search-companies-ids",
      payload,
      {
        timeout: 20000,
        headers,
      }
    );

    if (
      response.data &&
      response.data.data &&
      Array.isArray(response.data.data.companyIds)
    ) {
      return response.data.data.companyIds;
    }

    return [];
  } catch (e) {
    console.error("Error fetching external company IDs:", e.message);
    if (e.response) {
      console.error("Error response status:", e.response.status);
      console.error(
        "Error response data:",
        JSON.stringify(e.response.data, null, 2)
      );
    }
    return [];
  }
}

async function fetchSwipedCompanies(companyIds, userId) {
  if (!userId || !companyIds || companyIds.length === 0) {
    return [];
  }

  try {
    const companyIdList = companyIds
      .map((id) => `'${String(id).replace(/'/g, "''")}'`)
      .join(", ");

    const swipedQuery = `
      SELECT 
        firebase_id,
        company_id,
        created_at,
        swipe_board_ids
      FROM analytics.swiped_companies 
      WHERE firebase_id = '${userId.replace(/'/g, "''")}' 
      AND company_id IN (${companyIdList})
    `;

    const resultSet = await clickhouse.query({
      query: swipedQuery,
      format: "JSONEachRow",
    });

    const data = await resultSet.json();
    return data;
  } catch (error) {
    console.error("❌ Swiped companies query failed:", error.message);
    return [];
  }
}

const collapseOrientations = (orientations) => {
  if (!Array.isArray(orientations) || orientations.length === 0) {
    return 0;
  }

  const numericValues = orientations
    .map((code) => Number(code))
    .filter((code) => Number.isFinite(code));

  const nonZeroValues = numericValues.filter((code) => code > 0);

  if (nonZeroValues.length === 0) {
    return 0;
  }

  const uniqueNonZero = [...new Set(nonZeroValues)];

  if (uniqueNonZero.length === 1) {
    return uniqueNonZero[0];
  }

  return 3;
};

// Initialize Express app
const app = express();

// CORS configuration
const allowAllCors =
  process.env.ALLOW_ALL_CORS === "true" ||
  process.env.NODE_ENV === "development";
const corsOptions = allowAllCors
  ? {
      origin: true,
      methods: ["GET", "POST"],
      allowedHeaders: ["Content-Type", "Authorization"],
      credentials: true,
    }
  : {
      origin: [
        "https://vidtao.com",
        "https://www.vidtao.com",
        "https://v2.vidtao.com",
        "http://localhost:3000",
        "http://localhost:3001",
      ],
      methods: ["GET", "POST"],
      allowedHeaders: ["Content-Type", "Authorization"],
      credentials: true,
    };

app.use(cors(corsOptions));
app.use(express.json());

// Middleware
app.use(helmet()); // Security headers
app.use(compression()); // Gzip compression
app.use(express.json({ limit: "10mb" })); // Parse JSON bodies

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: "Too many requests from this IP, please try again later.",
  skip: (req) => {
    // Allow specific IP address
    const allowedIPs = ["199.36.158.100"];
    if (allowedIPs.includes(req.ip)) {
      return true;
    }

    // Allow requests from your website domain
    const referer = req.get("Referer") || "";
    const origin = req.get("Origin") || "";

    const allowedDomains = [
      "https://www.vidtao.com",
      "http://www.vidtao.com",
      "https://vidtao.com",
      "http://vidtao.com",
    ];

    return allowedDomains.some(
      (domain) => referer.startsWith(domain) || origin.startsWith(domain)
    );
  },
});
app.use(limiter);

/**
 * JWT token authentication middleware
 *
 * This middleware:
 * 1. Extracts the JWT token from the Authorization header (Bearer TOKEN format)
 * 2. Decodes the JWT token payload (without verification for simplicity)
 * 3. Extracts user_id from the token payload (supports user_id, userId, or sub fields)
 * 4. Sets req.user_id for use in route handlers
 * 5. Rejects requests without valid tokens
 *
 * Usage:
 * - Send requests with Authorization: Bearer <your-jwt-token> header
 * - Access user_id in routes via req.user_id
 *
 * Note: This is a simplified version that doesn't verify token signatures
 * For production use, consider implementing proper JWT verification
 */
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1]; // Bearer TOKEN

  if (!token) {
    return res.status(401).json({
      error: "Access token required",
      code: "MISSING_TOKEN",
      message: "Authorization header with Bearer token is required",
    });
  }

  try {
    // Simple JWT decode (without verification)
    const base64Url = token.split(".")[1];
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split("")
        .map(function (c) {
          return "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2);
        })
        .join("")
    );

    const decoded = JSON.parse(jsonPayload);

    // Extract user_id from the token payload
    req.user_id = decoded.user_id || decoded.userId || decoded.sub || null;

    if (!req.user_id) {
      return res.status(401).json({
        error: "Invalid token - user_id not found",
        code: "INVALID_TOKEN",
        message: "Token does not contain valid user_id",
      });
    }

    next();
  } catch (error) {
    return res.status(401).json({
      error: "Invalid token format",
      code: "INVALID_TOKEN",
      message: "Token could not be parsed",
    });
  }
};

// Health check endpoint (no authentication required)
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "qdrant-search-service",
  });
});

// Service alive check endpoint (no authentication required)
app.get("/check-alive", async (req, res) => {
  try {
    const result = await checkServiceHealth();
    res.status(200).json({
      service_check: result,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to check service health",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Apply authentication middleware to protected routes
app.use(authenticateToken);

// Test authentication endpoint
app.get("/auth/test", (req, res) => {
  res.status(200).json({
    message: "Authentication middleware test",
    user_id: req.user_id,
    has_token: !!req.headers["authorization"],
    timestamp: new Date().toISOString(),
  });
});

// Main search endpoint
app.post("/search", async (req, res) => {
  const startTime = performance.now();

  try {
    const {
      query,
      limit = DEFAULT_LIMIT,
      filters = null,
      includePayload = true,
      hnsw_ef = 32,
    } = req.body;

    // Validate required fields
    if (!query || typeof query !== "string" || query.trim() === "") {
      return res.status(400).json({
        error: "Query is required and must be a non-empty string",
        code: "INVALID_QUERY",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    console.log(`🔍 Search request: "${query}" (limit: ${limit})`);

    // Step 1: Create embedding
    const embeddingStartTime = performance.now();
    const embedding = await createEmbedding(query.trim());
    const embeddingTime = performance.now() - embeddingStartTime;

    // Step 2: Search in Qdrant
    const searchStartTime = performance.now();
    const searchBody = {
      vector: embedding,
      limit: parseInt(limit),
      with_payload: includePayload,
      with_vector: false,
      params: {
        hnsw_ef: parseInt(hnsw_ef),
      },
    };

    // Add filters if provided
    if (filters && typeof filters === "object") {
      searchBody.filter = filters;
    }

    const searchResponse = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
      searchBody,
      {
        headers: buildHeaders(),
        timeout: 30000, // 30 second timeout
      }
    );

    const searchTime = performance.now() - searchStartTime;
    const totalTime = performance.now() - startTime;

    const results = searchResponse.data.result;

    // Format response
    const response = {
      success: true,
      query: query,
      results: results.map((result) => ({
        id: result.id,
        score: result.score,
        video_id: result.payload?.yt_video_id || result.id,
        ...(includePayload && result.payload
          ? { payload: result.payload }
          : {}),
      })),
      metadata: {
        total_results: results.length,
        timing: {
          total_ms: Math.round(totalTime),
          embedding_ms: Math.round(embeddingTime),
          search_ms: Math.round(searchTime),
        },
        parameters: {
          collection: COLLECTION_NAME,
          limit: parseInt(limit),
          hnsw_ef: parseInt(hnsw_ef),
          filters_applied: !!filters,
        },
      },
    };

    console.log(
      `✅ Search completed in ${totalTime.toFixed(2)}ms - ${
        results.length
      } results`
    );

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Simple video search endpoint - returns just video IDs
app.post("/search/videos", async (req, res) => {
  const startTime = performance.now();

  try {
    const { keyword, limit = DEFAULT_LIMIT } = req.body;

    // Validate required fields
    if (!keyword || typeof keyword !== "string" || keyword.trim() === "") {
      return res.status(400).json({
        error: "Keyword is required and must be a non-empty string",
        code: "INVALID_KEYWORD",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    console.log(`🔍 Simple video search: "${keyword}" (limit: ${limit})`);

    // Create embedding
    const embedding = await createEmbedding(keyword.trim());

    // Search in Qdrant
    const searchBody = {
      vector: embedding,
      limit: parseInt(limit),
      with_payload: true,
      with_vector: false,
      params: {
        hnsw_ef: 32,
      },
    };

    const searchResponse = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
      searchBody,
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = searchResponse.data.result;
    const totalTime = performance.now() - startTime;

    // Extract just video IDs in order of similarity (highest to lowest)
    const videoIds = results
      .sort((a, b) => b.score - a.score) // Ensure highest similarity first
      .map((result) => result.payload?.yt_video_id || result.id);

    console.log(
      `✅ Simple search completed in ${totalTime.toFixed(2)}ms - ${
        videoIds.length
      } video IDs`
    );

    res.status(200).json({
      success: true,
      keyword: keyword,
      video_ids: videoIds,
      total_results: videoIds.length,
    });
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Simple search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Filtered video search endpoint - with category_id, language, is_unlisted, duration filters
app.post("/search/videos/filtered", async (req, res) => {
  const startTime = performance.now();

  try {
    const {
      keyword,
      limit = DEFAULT_LIMIT,
      category_id = null,
      language = null,
      is_unlisted = null,
      duration_min = null,
      duration_max = null,
      orientation = 0,
    } = req.body;

    // Validate required fields
    if (!keyword || typeof keyword !== "string" || keyword.trim() === "") {
      return res.status(400).json({
        error: "Keyword is required and must be a non-empty string",
        code: "INVALID_KEYWORD",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    console.log(`🔍 Filtered video search: "${keyword}" (limit: ${limit})`);

    // Create embedding
    const embedding = await createEmbedding(keyword.trim());

    // Build filters
    const filters = { must: [] };

    // Category ID filter (array of values)
    if (category_id && Array.isArray(category_id) && category_id.length > 0) {
      filters.must.push({
        key: "category_id",
        match: { any: category_id },
      });
    }

    // Language filter
    if (language && typeof language === "string") {
      filters.must.push({
        key: "language",
        match: { value: language },
      });
    }

    // Is unlisted filter
    if (is_unlisted !== null && typeof is_unlisted === "boolean") {
      filters.must.push({
        key: "unlisted",
        match: { value: is_unlisted },
      });
    }

    // Duration range filter
    if (duration_min !== null || duration_max !== null) {
      const durationFilter = { key: "duration", range: {} };

      if (duration_min !== null && typeof duration_min === "number") {
        durationFilter.range.gte = duration_min;
      }

      if (duration_max !== null && typeof duration_max === "number") {
        durationFilter.range.lte = duration_max;
      }

      filters.must.push(durationFilter);
    }

    // Orientation filter: only filter if orientation is 1 or 2
    // Orientation 3 represents multiple values, so include it in both cases
    const orientationFilter = Number(orientation);
    if (orientationFilter === 1 || orientationFilter === 2) {
      filters.must.push({
        key: "orientation",
        match: { any: [orientationFilter, 3] },
      });
    }

    // Search in Qdrant
    const searchBody = {
      vector: embedding,
      limit: parseInt(limit),
      with_payload: true,
      with_vector: false,
      params: {
        hnsw_ef: 32,
      },
    };

    // Add filters if any were specified
    if (filters.must.length > 0) {
      searchBody.filter = filters;
    }

    const searchResponse = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
      searchBody,
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = searchResponse.data.result;
    const totalTime = performance.now() - startTime;

    // Format results with video IDs and requested payload fields, ordered by similarity
    const formattedResults = results
      .sort((a, b) => b.score - a.score) // Ensure highest similarity first
      .map((result) => ({
        video_id: result.payload?.yt_video_id || result.id,
        score: result.score,
        category_id: result.payload?.category_id,
        language: result.payload?.language,
        is_unlisted: result.payload?.unlisted,
        duration: result.payload?.duration,
        orientation: result.payload?.orientation,
      }));

    console.log(
      `✅ Filtered search completed in ${totalTime.toFixed(2)}ms - ${
        formattedResults.length
      } results`
    );

    res.status(200).json({
      success: true,
      keyword: keyword,
      results: formattedResults,
      filters_applied: {
        category_id: category_id,
        language: language,
        is_unlisted: is_unlisted,
        duration_range: {
          min: duration_min,
          max: duration_max,
        },
      },
      total_results: formattedResults.length,
    });
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Filtered search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Filtered brand search endpoint - with category_id, country_id, software_id filters
app.post("/search/brands/filtered", async (req, res) => {
  const startTime = performance.now();

  try {
    const {
      keyword,
      limit = DEFAULT_LIMIT,
      category_id = null,
      country_id = null,
      software_id = null,
    } = req.body;

    // Validate required fields
    if (!keyword || typeof keyword !== "string" || keyword.trim() === "") {
      return res.status(400).json({
        error: "Keyword is required and must be a non-empty string",
        code: "INVALID_KEYWORD",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    console.log(
      `🔍 Filtered brand search: "${keyword}" (limit: ${limit}) in collection: ${COLLECTION_NAME_BRANDS}`
    );

    // Create embedding with 384 dimensions for brands collection
    const embedding = await createEmbedding(keyword.trim(), 384);

    // Build filters
    const filters = { must: [] };

    // Category ID filter (array of integers)
    if (category_id && Array.isArray(category_id) && category_id.length > 0) {
      // Validate all elements are integers
      if (!category_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "category_id must be an array of integers",
          code: "INVALID_CATEGORY_ID",
        });
      }
      filters.must.push({
        key: "category_id",
        match: { any: category_id },
      });
    }

    // Country ID filter (array of integers)
    if (country_id && Array.isArray(country_id) && country_id.length > 0) {
      // Validate all elements are integers
      if (!country_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "country_id must be an array of integers",
          code: "INVALID_COUNTRY_ID",
        });
      }
      filters.must.push({
        key: "country_id",
        match: { any: country_id },
      });
    }

    // Software ID filter (array of integers)
    if (software_id && Array.isArray(software_id) && software_id.length > 0) {
      // Validate all elements are integers
      if (!software_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "software_id must be an array of integers",
          code: "INVALID_SOFTWARE_ID",
        });
      }
      filters.must.push({
        key: "software_id",
        match: { any: software_id },
      });
    }

    // Search in Qdrant
    const searchBody = {
      vector: embedding,
      limit: parseInt(limit),
      with_payload: true,
      with_vector: false,
      params: {
        hnsw_ef: 32,
      },
    };

    // Add filters if any were specified
    if (filters.must.length > 0) {
      searchBody.filter = filters;
      //console.log("Applied filters:", JSON.stringify(filters, null, 2));
    }

    //console.log("Search body:", JSON.stringify(searchBody, null, 2));

    const searchResponse = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME_BRANDS}/points/search`,
      searchBody,
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = searchResponse.data.result;
    const totalTime = performance.now() - startTime;

    // Format results with all brand data from payload, ordered by similarity
    const formattedResults = results
      .sort((a, b) => b.score - a.score) // Ensure highest similarity first
      .map((result) => ({
        brand_id: result.payload?.brand_id || result.id,
        score: result.score,
        // Include all fields from the brands table payload
        ...result.payload,
      }));

    console.log(
      `✅ Filtered brand search completed in ${totalTime.toFixed(2)}ms - ${
        formattedResults.length
      } results`
    );

    res.status(200).json({
      success: true,
      keyword: keyword,
      results: formattedResults,
      filters_applied: {
        category_id: category_id,
        country_id: country_id,
        software_id: software_id,
      },
      total_results: formattedResults.length,
    });
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Filtered brand search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Log additional error details for debugging
    if (error.response?.data) {
      console.error(
        "Qdrant error details:",
        JSON.stringify(error.response.data, null, 2)
      );
    }

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME_BRANDS}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Filtered company search endpoint - with category_id, country_id, software_id filters
app.post("/search/companies/filtered", async (req, res) => {
  const startTime = performance.now();

  try {
    const {
      keyword,
      limit = DEFAULT_LIMIT,
      category_id = null,
      country_id = null,
      software_id = null,
    } = req.body;

    // Validate required fields
    if (!keyword || typeof keyword !== "string" || keyword.trim() === "") {
      return res.status(400).json({
        error: "Keyword is required and must be a non-empty string",
        code: "INVALID_KEYWORD",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    console.log(
      `🔍 Filtered company search: "${keyword}" (limit: ${limit}) in collection: ${COLLECTION_NAME_COMPANIES}`
    );

    // Create embedding with 384 dimensions for companies collection
    const embedding = await createEmbedding(keyword.trim(), 384);

    // Build filters
    const filters = { must: [] };

    // Category ID filter (array of integers)
    if (category_id && Array.isArray(category_id) && category_id.length > 0) {
      // Validate all elements are integers
      if (!category_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "category_id must be an array of integers",
          code: "INVALID_CATEGORY_ID",
        });
      }
      filters.must.push({
        key: "category_id",
        match: { any: category_id },
      });
    }

    // Country ID filter (array of integers)
    if (country_id && Array.isArray(country_id) && country_id.length > 0) {
      // Validate all elements are integers
      if (!country_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "country_id must be an array of integers",
          code: "INVALID_COUNTRY_ID",
        });
      }
      filters.must.push({
        key: "country_id",
        match: { any: country_id },
      });
    }

    // Software ID filter (array of integers)
    if (software_id && Array.isArray(software_id) && software_id.length > 0) {
      // Validate all elements are integers
      if (!software_id.every((id) => Number.isInteger(id))) {
        return res.status(400).json({
          error: "software_id must be an array of integers",
          code: "INVALID_SOFTWARE_ID",
        });
      }
      filters.must.push({
        key: "software_id",
        match: { any: software_id },
      });
    }

    // Search in Qdrant
    const searchBody = {
      vector: embedding,
      limit: parseInt(limit),
      with_payload: true,
      with_vector: false,
      params: {
        hnsw_ef: 32,
      },
    };

    // Add filters if any were specified
    if (filters.must.length > 0) {
      searchBody.filter = filters;
      console.log("Applied filters:", JSON.stringify(filters, null, 2));
    }

    console.log("Search body:", JSON.stringify(searchBody, null, 2));

    const searchResponse = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME_COMPANIES}/points/search`,
      searchBody,
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = searchResponse.data.result;
    const totalTime = performance.now() - startTime;

    // Format results with all company data from payload, ordered by similarity
    const formattedResults = results
      .sort((a, b) => b.score - a.score) // Ensure highest similarity first
      .map((result) => ({
        company_id: result.payload?.company_id || result.id,
        score: result.score,
        // Include all fields from the companies table payload
        ...result.payload,
      }));

    console.log(
      `✅ Filtered company search completed in ${totalTime.toFixed(2)}ms - ${
        formattedResults.length
      } results`
    );

    res.status(200).json({
      success: true,
      keyword: keyword,
      results: formattedResults,
      filters_applied: {
        category_id: category_id,
        country_id: country_id,
        software_id: software_id,
      },
      total_results: formattedResults.length,
    });
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Filtered company search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Log additional error details for debugging
    if (error.response?.data) {
      console.error(
        "Qdrant error details:",
        JSON.stringify(error.response.data, null, 2)
      );
    }

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME_COMPANIES}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Collection info endpoint
app.get("/collection/info", async (req, res) => {
  try {
    const response = await axios.get(
      `${qdrantUrl}/collections/${COLLECTION_NAME}`,
      { headers: buildHeaders() }
    );

    res.status(200).json({
      success: true,
      collection: COLLECTION_NAME,
      info: response.data.result,
    });
  } catch (error) {
    console.error("Error getting collection info:", error.message);
    res.status(500).json({
      error: "Failed to get collection info",
      code: "COLLECTION_INFO_ERROR",
    });
  }
});

// Enhanced video search endpoint - combines Qdrant search with ClickHouse summary data OR ClickHouse-only search
app.post("/search/videos/enhanced", async (req, res) => {
  const startTime = performance.now();
  console.log(`🚀 Enhanced search started at ${new Date().toISOString()}`);

  try {
    const {
      searchTerm = null, // Now optional
      limit = DEFAULT_LIMIT,
      categoryIds = null,
      language = null,
      showVideos = "all", // 'all', 'public', 'unlisted'
      durationRanges = null, // New: Array of ranges: [[0, 30], [60, 120], [600, null]]
      durationMoreThen = null, // Legacy: kept for backward compatibility
      durationLessThen = null, // Legacy: kept for backward compatibility
      sortProp = null, // Will default based on context
      orderAsc = false, // true = asc, false = desc
      similarityThreshold = 0.4, // Minimum similarity score (0.0 - 1.0)
      dateFrom = null, // New parameter for date range filtering
      dateTo = null, // New parameter for date range filtering
      orientation = 0,
    } = req.body;
    console.log("showVideos", showVideos);
    // Get user_id from authentication middleware
    const userId = req.user_id;
    axios
      .post("https://apiv1.vidtao.com/api/videos/insertSearchData", req.body, {
        headers: {
          authorization: req.headers["authorization"],
          "session-key": req.headers["session-key"],
        },
        timeout: 2000,
      })
      .catch((err) =>
        console.warn("History logging failed (non-critical):", err.message)
      );
    // Convert empty string searchTerm to null
    const normalizedSearchTerm = searchTerm === "" ? null : searchTerm;
    // Convert categoryIds=0 or [0] to null
    const normalizedCategoryIds =
      categoryIds === 0 ||
      (Array.isArray(categoryIds) &&
        categoryIds.length === 1 &&
        categoryIds[0] === 0)
        ? null
        : categoryIds;
    // Convert empty string sortProp to null
    const normalizedSortProp = sortProp === "" ? null : sortProp;

    // Convert sortProp "date" to "publishedAt"
    const finalSortProp =
      normalizedSortProp === "date" ? "publishedAt" : normalizedSortProp;

    // Orientation filter: 1 or 2 for specific orientation, otherwise 0 (no filter)
    const rawOrientation = Number(orientation);
    const orientationFilter =
      rawOrientation === 1 || rawOrientation === 2 ? rawOrientation : 0;

    // Validate searchTerm if provided
    if (
      normalizedSearchTerm !== null &&
      (typeof normalizedSearchTerm !== "string" ||
        normalizedSearchTerm.trim() === "")
    ) {
      return res.status(400).json({
        error: "searchTerm must be a non-empty string if provided",
        code: "INVALID_SEARCH_TERM",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }
    console.log("finalSortProp", finalSortProp);
    // Validate sortProp field (if provided)
    const validOrderFields = [
      "publishedAt",
      "last30Days",
      "last90Days",
      "total",
      "similarity_score",
    ];
    if (finalSortProp && !validOrderFields.includes(finalSortProp)) {
      return res.status(400).json({
        error: `sortProp must be one of: ${validOrderFields.join(
          ", "
        )} or null for auto-detection`,
        code: "INVALID_SORT_PROP",
      });
    }

    // Validate orderAsc (boolean)
    if (typeof orderAsc !== "boolean") {
      return res.status(400).json({
        error: "orderAsc must be a boolean (true = asc, false = desc)",
        code: "INVALID_ORDER_ASC",
      });
    }

    // Validate similarityThreshold
    if (similarityThreshold < 0 || similarityThreshold > 1) {
      return res.status(400).json({
        error: "similarityThreshold must be between 0.0 and 1.0",
        code: "INVALID_similarityThreshold",
      });
    }

    console.log(`📋 Request parameters:`, {
      searchTerm: normalizedSearchTerm ? `"${normalizedSearchTerm}"` : null,
      limit,
      categoryIds: normalizedCategoryIds,
      language,
      showVideos,
      durationMoreThen,
      durationLessThen,
      sortProp: finalSortProp,
      orderAsc,
      similarityThreshold,
      userId: userId ? "provided" : "none",
      dateFrom,
      dateTo,
      orientation: orientationFilter,
    });

    // Determine default sortProp based on context
    let effectiveSortProp = finalSortProp;
    if (!effectiveSortProp) {
      effectiveSortProp = normalizedSearchTerm
        ? "similarity_score"
        : "totalSpend";
    }

    // Map frontend sortProp names to database field names
    const sortPropMapping = {
      publishedAt: "published_at",
      last30Days: "last_30",
      last90Days: "last_90",
      total: "total",
      similarity_score: "similarity_score",
    };

    // Convert frontend sortProp to database field name
    const dbSortField = sortPropMapping[effectiveSortProp] || effectiveSortProp;

    // Convert orderAsc to order_direction string
    const order_direction = orderAsc ? "asc" : "desc";
    // For ClickHouse queries, use published_at as fallback when sorting by similarity_score
    const order_by =
      dbSortField === "similarity_score" ? "published_at" : dbSortField;

    console.log(
      `🔍 Enhanced video search: ${
        normalizedSearchTerm ? `"${normalizedSearchTerm}"` : "ClickHouse-only"
      } (limit: ${limit},  userId: ${userId || "none"})`
    );

    let qdrantResults = [];
    let qdrantVideoIds = [];
    let embeddingTime = 0;
    let qdrantTime = 0;
    let videoIds = [];
    let externalVideoIds = [];
    let externalTime = 0;
    let parallelExecutionTime = 0;

    // PATH 1: Keyword provided - optimized parallel execution
    if (normalizedSearchTerm) {
      console.log(
        `📡 Starting optimized parallel search operations at ${new Date().toISOString()}`
      );
      console.log(
        `📡 Performing Qdrant and external semantic search for: "${normalizedSearchTerm}"`
      );

      // Determine if external service should be used based on search term characteristics
      const trimmedSearchTerm = normalizedSearchTerm.trim();
      const wordCount = trimmedSearchTerm.split(/\s+/).length;
      const totalLength = trimmedSearchTerm.length;
      const shouldSkipExternalService = wordCount > 2 && totalLength > 10;
      const shouldUseExternalService = !shouldSkipExternalService;

      console.log(`🔍 Search term analysis:`);
      console.log(`   - Word count: ${wordCount} (max: 2)`);
      console.log(`   - Total length: ${totalLength} (max: 10)`);
      console.log(
        `   - Use external service: ${shouldUseExternalService ? "YES" : "NO"}`
      );
      if (shouldSkipExternalService) {
        console.log(
          `   - ❌ Skipping external service: complex query (${wordCount} words > 2 AND ${totalLength} chars > 10)`
        );
      } else {
        console.log(
          `   - ✅ Using external service: simple query (${wordCount} words, ${totalLength} chars)`
        );
      }

      // Step 1: Start embedding creation and external service call in parallel (if applicable)
      const parallelStartTime = performance.now();

      const embeddingPromise = createEmbedding(trimmedSearchTerm);

      let externalPromise = null;
      if (shouldUseExternalService) {
        console.log(
          `⚡ Starting embedding creation and external service call in parallel`
        );

        // Extract token from request headers
        const authHeader = req.headers["authorization"];
        const token = authHeader && authHeader.split(" ")[1]; // Bearer TOKEN

        const externalPayload = {
          searchTerm,
          categoryIds,
          language,
          showVideos: showVideos, // "all", "public", "unlisted", or null
          durationRanges, // Forward new format if provided
          durationMoreThen, // Legacy: kept for backward compatibility
          durationLessThen, // Legacy: kept for backward compatibility
          limit,
          token,
          dateFrom,
          dateTo,
          orientation: orientationFilter,
        };

        console.log(
          `🌐 External service payload:`,
          JSON.stringify(externalPayload, null, 2)
        );

        externalPromise = fetchExternalVideoIds(externalPayload);
      } else {
        console.log(
          `⚡ Starting embedding creation only (external service skipped - complex query)`
        );
      }

      // Step 2: Prepare Qdrant filters (can be done while embedding is being created)
      const filters = { must: [] };
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        filters.must.push({
          key: "category_id",
          match: { any: normalizedCategoryIds },
        });
      }
      if (language && typeof language === "string") {
        filters.must.push({ key: "language", match: { value: language } });
      }
      if (showVideos !== "all" && typeof showVideos === "string") {
        if (showVideos === "public") {
          filters.must.push({ key: "unlisted", match: { value: false } });
        } else if (showVideos === "unlisted") {
          filters.must.push({ key: "unlisted", match: { value: true } });
        }
      }
      // Duration filter: use new durationRanges if provided, otherwise fall back to legacy parameters
      if (
        durationRanges &&
        Array.isArray(durationRanges) &&
        durationRanges.length > 0
      ) {
        // New format: multiple ranges support
        const durationFilter = buildQdrantDurationFilter(
          durationRanges,
          "duration"
        );
        if (durationFilter) {
          // If single range, push to must. If multiple ranges, add should clause at filter root level
          if (durationFilter.should) {
            // Multiple ranges: add should clause at the same level as must (not nested inside must)
            // should clause defaults to matching at least one condition
            filters.should = durationFilter.should;
          } else {
            // Single range: push to must
            filters.must.push(durationFilter);
          }
        }
      } else if (durationMoreThen !== null || durationLessThen !== null) {
        // Legacy format: single range
        const durationFilter = { key: "duration", range: {} };
        if (durationMoreThen !== null && typeof durationMoreThen === "number") {
          durationFilter.range.gte = durationMoreThen;
        }
        if (durationLessThen !== null && typeof durationLessThen === "number") {
          durationFilter.range.lte = durationLessThen;
        }
        filters.must.push(durationFilter);
      }

      // Date range filter for Qdrant
      if (dateFrom !== null || dateTo !== null) {
        const dateFilter = { key: "published_at", range: {} };
        if (dateFrom !== null && dateFrom !== undefined) {
          // Convert date to ISO format for Qdrant
          const fromDate = new Date(dateFrom);
          if (!isNaN(fromDate.getTime())) {
            dateFilter.range.gte = fromDate.toISOString();
          }
        }
        if (dateTo !== null && dateTo !== undefined) {
          // Convert date to ISO format for Qdrant
          const toDate = new Date(dateTo);
          if (!isNaN(toDate.getTime())) {
            dateFilter.range.lte = toDate.toISOString();
          }
        }
        if (dateFilter.range.gte || dateFilter.range.lte) {
          filters.must.push(dateFilter);
        }
      }

      // Orientation filter for Qdrant: only filter if orientation is 1 or 2
      // Orientation 3 represents multiple values, so include it in both cases
      if (orientationFilter === 1 || orientationFilter === 2) {
        filters.must.push({
          key: "orientation",
          match: { any: [orientationFilter, 3] },
        });
      }

      // Step 3: Wait for embedding to be ready, then start Qdrant search immediately
      console.log(`🔍 Waiting for embedding to be ready...`);
      const embedding = await embeddingPromise;
      embeddingTime = performance.now() - parallelStartTime;
      console.log(`✅ Embedding created in ${embeddingTime.toFixed(2)}ms`);

      // Step 4: Start Qdrant search immediately (don't wait for external service)
      const qdrantStartTime = performance.now();
      console.log(`🔍 Starting Qdrant search at ${new Date().toISOString()}`);
      const searchBody = {
        vector: embedding,
        limit: parseInt(limit),
        with_payload: true,
        with_vector: false,
        params: { hnsw_ef: 32 },
      };

      // Add filter if we have any must clauses or should clauses
      if (filters.must.length > 0 || filters.should) {
        searchBody.filter = filters;
      }
      console.log(
        "searchBody filter:",
        JSON.stringify(searchBody.filter, null, 2)
      );
      try {
        const searchResponse = await axios.post(
          `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
          searchBody,
          { headers: buildHeaders(), timeout: 30000 }
        );
        qdrantResults = searchResponse.data.result;
      } catch (error) {
        console.error(
          "❌ Qdrant search error:",
          error.response?.data || error.message
        );
        console.error("❌ Search body:", JSON.stringify(searchBody, null, 2));
        throw error;
      }
      qdrantTime = performance.now() - qdrantStartTime;
      console.log(
        `✅ Qdrant search completed in ${qdrantTime.toFixed(2)}ms, returned ${
          qdrantResults.length
        } results`
      );

      // Step 5: Process Qdrant results
      // Filter by similarity threshold
      const originalResultsCount = qdrantResults.length;
      qdrantResults = qdrantResults.filter(
        (result) => result.score >= similarityThreshold
      );
      if (originalResultsCount > qdrantResults.length) {
        console.log(
          `🔍 Filtered ${
            originalResultsCount - qdrantResults.length
          } results below similarity threshold ${similarityThreshold}`
        );
      }
      // Extract video IDs from Qdrant results
      qdrantVideoIds = qdrantResults
        .sort((a, b) => b.score - a.score)
        .map((result) => result.payload?.yt_video_id || result.id);

      // Step 6: Wait for external service to complete (if applicable)
      if (externalPromise) {
        console.log(`🌐 Waiting for external service to complete...`);
        const extIds = await externalPromise;
        externalTime = performance.now() - parallelStartTime;
        externalVideoIds = extIds || [];
        console.log(
          `✅ External service completed in ${externalTime.toFixed(
            2
          )}ms, returned ${externalVideoIds.length} video IDs`
        );
      } else {
        console.log(`🌐 External service skipped (complex query)`);
        externalTime = 0;
        externalVideoIds = [];
      }

      parallelExecutionTime = Math.max(embeddingTime, externalTime);
      console.log(
        `📊 Parallel execution completed in ${parallelExecutionTime.toFixed(
          2
        )}ms (embedding: ${embeddingTime.toFixed(
          2
        )}ms, external: ${externalTime.toFixed(2)}ms)`
      );

      // Step 7: Merge Qdrant and external video IDs - use ALL results for ClickHouse processing
      console.log(`🔄 Merging ALL results for ClickHouse processing`);
      console.log(`   - Qdrant results: ${qdrantVideoIds.length}`);
      console.log(`   - External results: ${externalVideoIds.length}`);

      // Start with all Qdrant results (they have priority)
      const mergedVideoIds = [...qdrantVideoIds];
      const seen = new Set(qdrantVideoIds);

      // Add ALL external results (no limit here - we'll apply limit after ClickHouse processing)
      let addedFromExternal = 0;
      for (const vid of externalVideoIds) {
        if (!seen.has(vid)) {
          mergedVideoIds.push(vid);
          seen.add(vid);
          addedFromExternal++;
        }
      }
      console.log(
        `   - Added ${addedFromExternal} unique results from external service`
      );

      videoIds = mergedVideoIds;
      console.log(
        `✅ Final merged result: ${videoIds.length} videos (Qdrant: ${
          qdrantVideoIds.length
        }, External used: ${
          videoIds.length - qdrantVideoIds.length
        }) - will be processed in chunks and limited to ${limit} at the end`
      );
    }

    // PATH 2: Get data from ClickHouse (parallel with swiped videos if userId provided)
    let clickhouseData = [];
    let clickhouseTime = 0;
    let swipedVideosData = [];
    let swipedVideosTime = 0;
    const clickhouseStartTime = performance.now();

    // Function to fetch swiped videos data
    async function fetchSwipedVideos(videoIds, userId) {
      //console.log("fetchSwipedVideos", videoIds, userId);
      if (
        !userId ||
        typeof userId !== "string" ||
        !videoIds ||
        videoIds.length === 0
      ) {
        return [];
      }

      try {
        // Filter out any undefined/null video IDs and ensure they're strings
        const validVideoIds = videoIds.filter(
          (id) => id && typeof id === "string"
        );
        if (validVideoIds.length === 0) {
          return [];
        }

        const videoIdList = validVideoIds
          .map((id) => `'${id.replace(/'/g, "''")}'`)
          .join(", ");

        const swipedQuery = `
          SELECT 
            firebase_id,
            yt_video_id,
            created_at,
            swipe_board_ids
          FROM analytics.swiped_videos 
          WHERE firebase_id = '${userId.replace(/'/g, "''")}' 
          AND yt_video_id IN (${videoIdList})
        `;

        const resultSet = await clickhouse.query({
          query: swipedQuery,
          format: "JSONEachRow",
        });

        const data = await resultSet.json();
        return data;
      } catch (error) {
        console.error("❌ Swiped videos query failed:", error.message);
        return [];
      }
    }

    // Function to fetch video orientations data
    async function fetchVideoOrientations(videoIds) {
      if (!videoIds || videoIds.length === 0) {
        return [];
      }

      try {
        // Filter out any undefined/null video IDs and ensure they're strings
        const validVideoIds = videoIds.filter(
          (id) => id && typeof id === "string"
        );
        if (validVideoIds.length === 0) {
          return [];
        }

        const videoIdList = validVideoIds
          .map((id) => `'${id.replace(/'/g, "''")}'`)
          .join(", ");

        const orientationQuery = `
          SELECT 
            yt_video_id,
            groupArray(orientation) AS orientations
          FROM analytics.video_company_relations
          WHERE yt_video_id IN (${videoIdList})
          GROUP BY yt_video_id
        `;

        const resultSet = await clickhouse.query({
          query: orientationQuery,
          format: "JSONEachRow",
        });

        const data = await resultSet.json();
        return data;
      } catch (error) {
        console.error("❌ Orientation query failed:", error.message);
        return [];
      }
    }

    if (normalizedSearchTerm && videoIds.length > 0) {
      // Case 1: We have video IDs from Qdrant search - query specific videos using chunked processing
      console.log(
        `📊 Starting chunked ClickHouse queries for ${
          videoIds.length
        } video IDs at ${new Date().toISOString()}`
      );

      // Helper function to split array into chunks
      const chunkArray = (array, chunkSize) => {
        const chunks = [];
        for (let i = 0; i < array.length; i += chunkSize) {
          chunks.push(array.slice(i, i + chunkSize));
        }
        return chunks;
      };

      // Split video IDs into chunks of 5000 (optimized for ClickHouse query limits)
      const CHUNK_SIZE = 5000;
      const videoIdChunks = chunkArray(videoIds, CHUNK_SIZE);
      console.log(
        `🔍 Split ${videoIds.length} video IDs into ${videoIdChunks.length} chunks of ${CHUNK_SIZE}`
      );

      // Function to execute ClickHouse query for a single chunk
      const executeClickHouseChunk = async (chunk, chunkIndex) => {
        const videoIdList = chunk
          .map((id) => `'${id.replace(/'/g, "''")}'`)
          .join(", ");

        let query;
        if (effectiveSortProp === "similarity_score") {
          // When sorting by similarity_score, preserve Qdrant order by not using ORDER BY
          // Using argMax with HAVING to avoid ClickHouse bug with NOT IN + aggregation
          query = `
            SELECT 
              vs.yt_video_id,
              argMax(vs.last_30, vs.updated_at) as last_30,
              argMax(vs.last_60, vs.updated_at) as last_60,
              argMax(vs.last_90, vs.updated_at) as last_90,
              argMax(vs.total, vs.updated_at) as total,
              argMax(vs.category_id, vs.updated_at) as category_id,
              argMax(vs.language, vs.updated_at) as language,
              argMax(vs.listed, vs.updated_at) as listed,
              argMax(vs.duration, vs.updated_at) as duration,
              argMax(vs.title, vs.updated_at) as title,
              argMax(vs.frame, vs.updated_at) as frame,
              argMax(vs.brand_name, vs.updated_at) as brand_name,
              argMax(vs.published_at, vs.updated_at) as published_at,
              argMax(vs.brand_id, vs.updated_at) as brand_id,
              max(vs.updated_at) as updated_at
            FROM analytics.yt_video_summary vs
            WHERE vs.yt_video_id IN (${videoIdList})
            GROUP BY vs.yt_video_id
            HAVING category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})
          `;
        } else {
          // For other sort fields, use ClickHouse ordering
          // Using argMax with HAVING to avoid ClickHouse bug with NOT IN + aggregation
          query = `
            SELECT 
              vs.yt_video_id,
              argMax(vs.last_30, vs.updated_at) as last_30,
              argMax(vs.last_60, vs.updated_at) as last_60,
              argMax(vs.last_90, vs.updated_at) as last_90,
              argMax(vs.total, vs.updated_at) as total,
              argMax(vs.category_id, vs.updated_at) as category_id,
              argMax(vs.language, vs.updated_at) as language,
              argMax(vs.listed, vs.updated_at) as listed,
              argMax(vs.duration, vs.updated_at) as duration,
              argMax(vs.title, vs.updated_at) as title,
              argMax(vs.frame, vs.updated_at) as frame,
              argMax(vs.brand_name, vs.updated_at) as brand_name,
              argMax(vs.published_at, vs.updated_at) as published_at,
              argMax(vs.brand_id, vs.updated_at) as brand_id,
              max(vs.updated_at) as updated_at
            FROM analytics.yt_video_summary vs
            WHERE vs.yt_video_id IN (${videoIdList})
            GROUP BY vs.yt_video_id
            HAVING category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})
            ORDER BY ${order_by} ${order_direction.toUpperCase()}
          `;
        }

        // Only log every 5th chunk to reduce console spam
        if ((chunkIndex + 1) % 5 === 0 || chunkIndex === 0) {
          console.log(
            `🔍 Executing ClickHouse chunk ${chunkIndex + 1}/${
              videoIdChunks.length
            } with ${chunk.length} video IDs`
          );
        }

        try {
          const resultSet = await clickhouse.query({
            query: query,
            format: "JSONEachRow",
          });

          const chunkData = await resultSet.json();
          console.log(
            `✅ ClickHouse chunk ${chunkIndex + 1} returned ${
              chunkData.length
            } records`
          );
          return chunkData;
        } catch (error) {
          console.error(
            `❌ ClickHouse chunk ${chunkIndex + 1} failed:`,
            error.message
          );
          console.error(`❌ Video IDs count:`, chunk.length);
          console.error(`❌ Sample video IDs:`, chunk.slice(0, 5));
          return [];
        }
      };

      // Execute chunks with controlled concurrency (max 10 concurrent)
      console.log(
        `🔄 Executing ${videoIdChunks.length} ClickHouse chunks with controlled concurrency`
      );

      const CONCURRENT_CHUNKS = 3;
      const chunkResults = [];

      for (let i = 0; i < videoIdChunks.length; i += CONCURRENT_CHUNKS) {
        const batch = videoIdChunks.slice(i, i + CONCURRENT_CHUNKS);
        const batchPromises = batch.map((chunk, batchIndex) =>
          executeClickHouseChunk(chunk, i + batchIndex)
        );

        const batchResults = await Promise.all(batchPromises);
        chunkResults.push(...batchResults);

        console.log(
          `✅ Completed batch ${
            Math.floor(i / CONCURRENT_CHUNKS) + 1
          }/${Math.ceil(videoIdChunks.length / CONCURRENT_CHUNKS)}`
        );
      }

      // Merge all chunk results
      clickhouseData = chunkResults.flat();
      console.log(
        `🔍 Merged ${chunkResults.length} chunks into ${clickhouseData.length} total records`
      );

      // Note: Orientation and swiped videos data will be fetched AFTER filtering/limiting
      // to avoid fetching data for videos that won't be in the final result

      clickhouseTime = performance.now() - clickhouseStartTime;

      console.log(
        `✅ ClickHouse query completed in ${clickhouseTime.toFixed(
          2
        )}ms, returned ${clickhouseData.length} records`
      );
      console.log(
        `ℹ️  Orientation and swiped videos data will be fetched after filtering to final ${limit} results`
      );
    } else if (!normalizedSearchTerm) {
      // Case 2: No keyword - direct ClickHouse query with filters
      console.log(
        `📊 Starting direct ClickHouse search with filters at ${new Date().toISOString()}`
      );

      // Build WHERE clause for filters
      const whereConditions = [];

      // Category ID filter
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        const categoryList = normalizedCategoryIds.join(", ");
        whereConditions.push(`category_id IN (${categoryList})`);
      }

      // Language filter
      if (language && typeof language === "string") {
        whereConditions.push(`language = '${language.replace(/'/g, "''")}'`);
      }

      // Video status filter (note: ClickHouse uses 'listed', opposite of 'unlisted')
      if (showVideos !== "all" && typeof showVideos === "string") {
        if (showVideos === "public") {
          // Public videos are listed (listed = 1)
          whereConditions.push(`listed = 1`);
        } else if (showVideos === "unlisted") {
          // Unlisted videos are not listed (listed = 0)
          whereConditions.push(`listed = 0`);
        }
      }

      // Duration filter: use new durationRanges if provided, otherwise fall back to legacy parameters
      if (
        durationRanges &&
        Array.isArray(durationRanges) &&
        durationRanges.length > 0
      ) {
        // New format: multiple ranges support
        const durationCondition = buildClickHouseDurationFilter(
          durationRanges,
          "duration"
        );
        if (durationCondition) {
          console.log("durationCondition", durationCondition);
          whereConditions.push(durationCondition);
        }
      } else {
        // Legacy format: single range
        if (durationMoreThen !== null && typeof durationMoreThen === "number") {
          whereConditions.push(`duration >= ${durationMoreThen}`);
        }
        if (durationLessThen !== null && typeof durationLessThen === "number") {
          whereConditions.push(`duration <= ${durationLessThen}`);
        }
      }

      // Date range filter
      if (dateFrom !== null && dateFrom !== undefined) {
        // Convert date to ClickHouse format (YYYY-MM-DD)
        const fromDate = new Date(dateFrom);
        if (!isNaN(fromDate.getTime())) {
          const formattedFromDate = fromDate.toISOString().split("T")[0]; // YYYY-MM-DD format
          whereConditions.push(`published_at >= '${formattedFromDate}'`);
        }
      }
      if (dateTo !== null && dateTo !== undefined) {
        // Convert date to ClickHouse format (YYYY-MM-DD)
        const toDate = new Date(dateTo);
        if (!isNaN(toDate.getTime())) {
          const formattedToDate = toDate.toISOString().split("T")[0]; // YYYY-MM-DD format
          whereConditions.push(`published_at <= '${formattedToDate}'`);
        }
      }

      if (orientationFilter > 0) {
        whereConditions.push(`yt_video_id IN (
          SELECT DISTINCT yt_video_id
          FROM analytics.video_company_relations
          WHERE orientation IN (${orientationFilter}, 3)
        )`);
      }

      // Category exclusion filter
      whereConditions.push(
        `total > 100 AND category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
      );

      const whereClause =
        whereConditions.length > 0
          ? `WHERE ${whereConditions.join(" AND ")}`
          : "";

      // Optimize query: pre-filter top 1000 records before expensive deduplication
      const preFilterLimit = 1000; // Take top 1000 records, then deduplicate and limit

      const query = `
        WITH latest_records AS (
          SELECT
              yt_video_id,
              last_30,
              last_60,
              last_90,
              total,
              category_id,
              language,
              listed,
              duration,
              title,
              frame,
              brand_name,
              published_at,
              updated_at,
              brand_id
          FROM (
              SELECT *,
                  ROW_NUMBER() OVER (
                      PARTITION BY yt_video_id
                      ORDER BY updated_at DESC
                  ) AS rn
              FROM analytics.yt_video_summary
              ${whereClause}
          )
          WHERE rn = 1
      ),
      top_videos AS (
          SELECT *
          FROM latest_records
          ORDER BY ${order_by} ${order_direction}
          LIMIT ${limit}
      )
      SELECT
          tv.yt_video_id as yt_video_id,
          tv.last_30,
          tv.last_60,
          tv.last_90,
          tv.total,
          tv.category_id,
          tv.language,
          tv.listed,
          tv.duration,
          tv.title,
          tv.frame,
          tv.brand_name,
          tv.brand_id,
          tv.published_at,
          tv.updated_at,
          vcr.orientations
      FROM top_videos tv
      LEFT JOIN (
          SELECT
              yt_video_id,
              groupArray(orientation) AS orientations
          FROM analytics.video_company_relations
          WHERE yt_video_id IN (SELECT yt_video_id FROM top_videos)
          GROUP BY yt_video_id
      ) vcr ON tv.yt_video_id = vcr.yt_video_id
      ORDER BY ${order_by} ${order_direction}

      `;
      console.log("query", query);
      console.log(`📊 Executing direct ClickHouse query with filters`);

      try {
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
        });

        const rawData = await resultSet.json();
        clickhouseTime = performance.now() - clickhouseStartTime;

        console.log(
          `✅ ClickHouse returned ${
            rawData.length
          } records in ${clickhouseTime.toFixed(2)}ms`
        );

        clickhouseData = rawData.map((row) => {
          const orientationValues = Array.isArray(row.orientations)
            ? row.orientations
            : row.orientations !== undefined && row.orientations !== null
            ? [row.orientations]
            : [];

          const numericValues = orientationValues
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value));

          const orientation = collapseOrientations(numericValues);
          const { orientations, ...rest } = row;

          return {
            ...rest,
            orientation,
          };
        });

        // If userId is provided, fetch swiped videos data for the returned video IDs
        if (userId && clickhouseData.length > 0) {
          const returnedVideoIds = clickhouseData.map(
            (item) => item.yt_video_id
          );

          swipedVideosData = await fetchSwipedVideos(returnedVideoIds, userId);
          swipedVideosTime = performance.now() - clickhouseStartTime;
          console.log(
            `✅ Swiped videos query completed in ${swipedVideosTime.toFixed(
              2
            )}ms, found ${swipedVideosData.length} records`
          );
        }
      } catch (clickhouseError) {
        console.error("❌ ClickHouse query failed:", clickhouseError.message);
        clickhouseData = [];
      }
    }

    const totalTime = performance.now() - startTime;

    // Step 3: Format results based on search type
    const enhancedResults = [];
    let filteredOutCount = 0;

    if (normalizedSearchTerm) {
      // Case 1: Keyword search - combine Qdrant similarity scores with ClickHouse data
      // Note: swiped videos and orientation data will be fetched AFTER limiting
      const clickhouseMap = new Map();
      clickhouseData.forEach((item) => {
        clickhouseMap.set(item.yt_video_id, item);
      });

      // Process ALL video IDs (both Qdrant and external results)
      // Create a map of Qdrant results for quick lookup
      const qdrantMap = new Map();
      qdrantResults.forEach((qdrantResult) => {
        const videoId = qdrantResult.payload?.yt_video_id || qdrantResult.id;
        qdrantMap.set(videoId, qdrantResult);
      });

      // Process all video IDs from the merged list
      videoIds.forEach((videoId) => {
        const clickhouseInfo = clickhouseMap.get(videoId);
        const qdrantResult = qdrantMap.get(videoId);

        // Only include videos that have ClickHouse data
        if (clickhouseInfo) {
          const enhancedResult = {
            ytVideoId: videoId,
            similarity_score: qdrantResult ? qdrantResult.score : null,
            // Qdrant payload data (if available)
            qdrant_data: qdrantResult
              ? {
                  category_id: qdrantResult.payload?.category_id,
                  language: qdrantResult.payload?.language,
                  is_unlisted: qdrantResult.payload?.unlisted,
                  duration: qdrantResult.payload?.duration,
                  orientation: qdrantResult.payload?.orientation,
                }
              : null,
            // ClickHouse summary data
            summary_data: clickhouseInfo,
            // Swiped videos data - will be populated after limiting
            swiped_data: null,
          };

          enhancedResults.push(enhancedResult);
        } else {
          filteredOutCount++;
        }
      });
    } else {
      // Case 2: ClickHouse-only search - format ClickHouse data directly
      // Create a map of swiped videos for quick lookup (only for non-searchTerm queries)
      const swipedVideosMap = new Map();
      swipedVideosData.forEach((item) => {
        swipedVideosMap.set(item.yt_video_id, item);
      });

      clickhouseData.forEach((clickhouseResult) => {
        const swipedInfo = swipedVideosMap.get(clickhouseResult.yt_video_id);

        const enhancedResult = {
          ytVideoId: clickhouseResult.yt_video_id,
          similarity_score: null, // No similarity score for direct ClickHouse search
          // No Qdrant data available
          qdrant_data: null,
          // ClickHouse summary data
          summary_data: clickhouseResult,
          // Swiped videos data (if available)
          swiped_data: swipedInfo || null,
        };

        enhancedResults.push(enhancedResult);
      });
    }

    // Sort results based on the effective sort property
    if (normalizedSearchTerm) {
      // When we have searchTerm, we need to sort the combined results
      if (effectiveSortProp === "similarity_score") {
        // Only sort by similarity when no sortProp was provided or explicitly requested
        console.log(
          `🔄 Sorting by similarity_score (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          const scoreA = a.similarity_score || 0;
          const scoreB = b.similarity_score || 0;
          return orderAsc ? scoreA - scoreB : scoreB - scoreA;
        });
      } else {
        // Sort by the specified field (prioritize user choice)
        console.log(
          `🔄 Sorting by ${effectiveSortProp} (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          let valueA, valueB;

          switch (dbSortField) {
            case "last_30":
              valueA = a.summary_data?.last_30 ?? 0;
              valueB = b.summary_data?.last_30 ?? 0;
              break;
            case "last_90":
              valueA = a.summary_data?.last_90 ?? 0;
              valueB = b.summary_data?.last_90 ?? 0;
              break;
            case "total":
              valueA = a.summary_data?.total ?? 0;
              valueB = b.summary_data?.total ?? 0;
              break;
            case "published_at":
              valueA = new Date(a.summary_data?.published_at || 0);
              valueB = new Date(b.summary_data?.published_at || 0);
              break;
            default:
              valueA = a.summary_data?.[dbSortField] ?? 0;
              valueB = b.summary_data?.[dbSortField] ?? 0;
          }

          if (dbSortField === "published_at") {
            return orderAsc ? valueA - valueB : valueB - valueA;
          } else {
            return orderAsc ? valueA - valueB : valueB - valueA;
          }
        });
      }
    } else {
      console.log(
        `🔄 ClickHouse-only search, sorted by ${order_by} (${order_direction})`
      );
    }

    // Log filtering results
    if (filteredOutCount > 0) {
      console.log(
        `🔍 Filtered out ${filteredOutCount} videos without ClickHouse data`
      );
    }

    // Apply final limit after sorting
    const finalResults = enhancedResults.slice(0, parseInt(limit));
    console.log(
      `🔍 Applied final limit: ${enhancedResults.length} → ${finalResults.length} results`
    );

    // NOW fetch orientation and swiped videos data only for the final results (for searchTerm queries)
    if (normalizedSearchTerm && finalResults.length > 0) {
      console.log(
        `🔄 Fetching orientation and swiped videos for ${finalResults.length} final results`
      );
      const finalVideoIds = finalResults.map((r) => r.ytVideoId);

      const finalParallelQueries = [];

      if (userId) {
        console.log(`🔄 Fetching swiped videos data for final results`);
        finalParallelQueries.push(fetchSwipedVideos(finalVideoIds, userId));
      } else {
        finalParallelQueries.push(Promise.resolve([]));
      }

      console.log(
        `🔄 Fetching orientation data for ${finalVideoIds.length} final videos`
      );
      finalParallelQueries.push(fetchVideoOrientations(finalVideoIds));

      const [finalSwipedData, finalOrientationData] = await Promise.all(
        finalParallelQueries
      );
      swipedVideosData = finalSwipedData;

      // Create orientation map
      const orientationMap = new Map();
      finalOrientationData.forEach((item) => {
        const orientationValues = Array.isArray(item.orientations)
          ? item.orientations
          : item.orientations !== undefined && item.orientations !== null
          ? [item.orientations]
          : [];

        const numericValues = orientationValues
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value));

        const orientation = collapseOrientations(numericValues);
        orientationMap.set(item.yt_video_id, orientation);
      });

      // Create swiped videos map
      const swipedVideosMap = new Map();
      swipedVideosData.forEach((item) => {
        swipedVideosMap.set(item.yt_video_id, item);
      });

      // Enrich final results with orientation and swiped data
      finalResults.forEach((result) => {
        // Add orientation to summary_data
        if (result.summary_data) {
          result.summary_data.orientation =
            orientationMap.get(result.ytVideoId) || 0;
        }
        // Add swiped data
        result.swiped_data = swipedVideosMap.get(result.ytVideoId) || null;
      });

      console.log(
        `✅ Orientation data fetched for ${finalOrientationData.length} final videos`
      );
      console.log(
        `✅ Swiped videos data fetched, found ${swipedVideosData.length} records`
      );
    }

    // Format response to match frontend expectations
    const data = {
      hasMore: enhancedResults.length > parseInt(limit),
      results: finalResults.map((r) => {
        const isSwiped = r.swiped_data ? true : false;

        return {
          ytVideoId: r.ytVideoId,
          isSwiped: isSwiped, // Set based on swiped_videos table
          title: r.summary_data?.title || r.qdrant_data?.title || null,
          duration: r.summary_data?.duration || r.qdrant_data?.duration || null,
          orientation: r.summary_data?.orientation ?? 0,
          description: null, // Not available in current data structure
          thumbnail: r.summary_data?.frame || null,
          publishedAt: r.summary_data?.published_at
            ? new Date(r.summary_data.published_at)
            : null,
          totalSpend: r.summary_data?.total || null,
          is_unlisted:
            r.summary_data?.listed === false ||
            r.qdrant_data?.is_unlisted ||
            false,
          language: r.summary_data?.language || r.qdrant_data?.language || null,
          category:
            r.summary_data?.category_id || r.qdrant_data?.category_id || null,
          last30Days: r.summary_data?.last_30 ?? null,
          last90Days: r.summary_data?.last_90 ?? null,
          affiliateOfferId: null, // Not available in current data structure
          brandName: r.summary_data?.brand_name || null,
          brandId: r.summary_data?.brand_id || null,
          // Additional fields for debugging/backward compatibility
          similarity_score: r.similarity_score,
          qdrant_data: r.qdrant_data,
          summary_data: r.summary_data,
          swiped_data: r.swiped_data, // Include swiped data for debugging
        };
      }),
    };

    const response = {
      success: true,
      search_type: normalizedSearchTerm
        ? "semantic_plus_clickhouse"
        : "clickhouse_only",
      keyword: normalizedSearchTerm || null,
      total_results: finalResults.length,
      total_available: enhancedResults.length,
      qdrant_matches: qdrantResults.length,
      external_matches: externalVideoIds.length,
      external_matches_used: videoIds.length - qdrantVideoIds.length,
      external_service_used: normalizedSearchTerm
        ? externalVideoIds.length > 0 || externalTime > 0
        : false,
      clickhouse_matches: clickhouseData.length,
      swiped_matches: swipedVideosData.length,
      parameters: {
        search_filters: {
          category_id: normalizedCategoryIds,
          language: language,
          is_unlisted: showVideos !== "all",
          duration_range: {
            min: durationMoreThen,
            max: durationLessThen,
          },
          date_range: {
            from: dateFrom,
            to: dateTo,
          },
          orientation: orientationFilter,
        },
        search_term_analysis: normalizedSearchTerm
          ? {
              word_count: normalizedSearchTerm.trim().split(/\s+/).length,
              total_length: normalizedSearchTerm.trim().length,
              should_use_external: !(
                normalizedSearchTerm.trim().split(/\s+/).length <= 2 &&
                normalizedSearchTerm.trim().length <= 10
              ),
            }
          : null,
        similarityThreshold: normalizedSearchTerm ? similarityThreshold : null,
        clickhouse_ordering: {
          order_by: order_by,
          order_direction: order_direction,
        },
        limit: parseInt(limit),
        userId: userId,
        dateFrom,
        dateTo,
      },
      timing: {
        total_ms: Math.round(totalTime),
        parallel_execution_ms: Math.round(parallelExecutionTime),
        embedding_ms: Math.round(embeddingTime),
        qdrant_search_ms: Math.round(qdrantTime),
        clickhouse_query_ms: Math.round(clickhouseTime),
        swiped_videos_query_ms: Math.round(swipedVideosTime),
        external_service_ms: Math.round(externalTime),
      },
      data: data,
    };

    const searchTypeDesc = normalizedSearchTerm
      ? `semantic + ClickHouse`
      : `ClickHouse-only`;
    console.log(
      `✅ Enhanced search (${searchTypeDesc}) completed in ${totalTime.toFixed(
        2
      )}ms - ${enhancedResults.length} results (Qdrant: ${
        qdrantResults.length
      }, External available: ${externalVideoIds.length}, External used: ${
        videoIds.length - qdrantVideoIds.length
      }, ClickHouse: ${clickhouseData.length}, Swiped: ${
        swipedVideosData.length
      })`
    );
    console.log(`📊 Final timing breakdown:`);
    console.log(`   - Total time: ${totalTime.toFixed(2)}ms`);
    console.log(
      `   - Parallel execution: ${parallelExecutionTime.toFixed(2)}ms`
    );
    console.log(`   - Embedding: ${embeddingTime.toFixed(2)}ms`);
    console.log(`   - External service: ${externalTime.toFixed(2)}ms`);
    console.log(`   - Qdrant search: ${qdrantTime.toFixed(2)}ms`);
    console.log(`   - ClickHouse query: ${clickhouseTime.toFixed(2)}ms`);
    console.log(`   - Swiped videos: ${swipedVideosTime.toFixed(2)}ms`);
    console.log(`🏁 Response sent at ${new Date().toISOString()}`);

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Enhanced search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );
    // Log Qdrant error details if available
    if (error.response?.data) {
      console.error(
        "❌ Qdrant error details:",
        JSON.stringify(error.response.data, null, 2)
      );
    }
    console.error(`🚨 Error occurred at ${new Date().toISOString()}`);

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// YouTube video summary endpoint with ordering support
app.post("/videos/summary", async (req, res) => {
  const startTime = performance.now();

  try {
    const {
      video_ids,
      order_by = "published_at",
      order_direction = "desc",
      limit = 100,
      offset = 0,
    } = req.body;

    // Validate required fields
    if (!video_ids || !Array.isArray(video_ids) || video_ids.length === 0) {
      return res.status(400).json({
        error: "video_ids is required and must be a non-empty array",
        code: "INVALID_VIDEO_IDS",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    // Validate offset
    if (offset < 0) {
      return res.status(400).json({
        error: "Offset must be >= 0",
        code: "INVALID_OFFSET",
      });
    }

    // Validate order_by field
    const validOrderFields = [
      "published_at",
      "last_30",
      "last_60",
      "last_90",
      "total",
    ];
    if (!validOrderFields.includes(order_by)) {
      return res.status(400).json({
        error: `order_by must be one of: ${validOrderFields.join(", ")}`,
        code: "INVALID_ORDER_BY",
      });
    }

    // Validate order_direction
    if (!["asc", "desc"].includes(order_direction.toLowerCase())) {
      return res.status(400).json({
        error: "order_direction must be 'asc' or 'desc'",
        code: "INVALID_ORDER_DIRECTION",
      });
    }

    console.log(
      `🎬 YouTube video summary request: ${video_ids.length} video IDs (order: ${order_by} ${order_direction})`
    );

    // Prepare video IDs for SQL IN clause (escape for security)
    const videoIdList = video_ids
      .map((id) => `'${id.replace(/'/g, "''")}'`)
      .join(", ");

    // Build the query with deduplication to ensure latest record per video
    // Using argMax with HAVING to avoid ClickHouse bug with NOT IN + aggregation
    const query = `
      SELECT 
        yt_video_id,
        argMax(last_30, updated_at) as last_30,
        argMax(last_60, updated_at) as last_60,
        argMax(last_90, updated_at) as last_90,
        argMax(total, updated_at) as total,
        argMax(category_id, updated_at) as category_id,
        argMax(language, updated_at) as language,
        argMax(listed, updated_at) as listed,
        argMax(duration, updated_at) as duration,
        argMax(title, updated_at) as title,
        argMax(frame, updated_at) as frame,
        argMax(brand_name, updated_at) as brand_name,
        argMax(published_at, updated_at) as published_at,
        max(updated_at) as updated_at
      FROM analytics.yt_video_summary 
      WHERE yt_video_id IN (${videoIdList})
      GROUP BY yt_video_id
      HAVING category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})
      ORDER BY ${order_by} ${order_direction.toUpperCase()}
      LIMIT ${parseInt(limit)}
      OFFSET ${parseInt(offset)}
    `;

    console.log(
      `📊 Executing ClickHouse query for ${video_ids.length} video IDs`
    );

    // Execute the query
    const resultSet = await clickhouse.query({
      query: query,
      format: "JSONEachRow",
    });

    const data = await resultSet.json();
    const totalTime = performance.now() - startTime;

    // Format response
    const response = {
      success: true,
      total_results: data.length,
      parameters: {
        order_by: order_by,
        order_direction: order_direction,
        limit: parseInt(limit),
        offset: parseInt(offset),
        requested_video_ids: video_ids.length,
      },
      timing: {
        total_ms: Math.round(totalTime),
      },
      data: data,
    };

    console.log(
      `✅ ClickHouse query completed in ${totalTime.toFixed(2)}ms - ${
        data.length
      } records found`
    );

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ ClickHouse query error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );

    // Handle specific error types
    if (error.code === "ECONNREFUSED" || error.code === "NETWORK_ERROR") {
      return res.status(503).json({
        error: "Unable to connect to ClickHouse database",
        code: "CLICKHOUSE_UNAVAILABLE",
      });
    }

    if (error.message.includes("SYNTAX_ERROR")) {
      return res.status(500).json({
        error: "Database query syntax error",
        code: "QUERY_SYNTAX_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Debug endpoint to check actual data structure
app.get("/debug/sample-data", async (req, res) => {
  try {
    const response = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
      {
        vector: Array(1536).fill(0), // Dummy vector to get any results
        limit: 5,
        with_payload: true,
        with_vector: false,
      },
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = response.data.result;

    res.status(200).json({
      success: true,
      message: "Sample data from collection",
      total_found: results.length,
      samples: results.map((result) => ({
        id: result.id,
        score: result.score,
        payload_keys: result.payload ? Object.keys(result.payload) : [],
        payload_sample: result.payload,
      })),
    });
  } catch (error) {
    console.error("Error getting sample data:", error.message);
    res.status(500).json({
      error: "Failed to get sample data",
      code: "SAMPLE_DATA_ERROR",
      message: error.message,
    });
  }
});

// Debug endpoint to check brands collection data structure
app.get("/debug/brands-data", async (req, res) => {
  try {
    const response = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME_BRANDS}/points/search`,
      {
        vector: Array(384).fill(0), // Dummy vector to get any results (384 dimensions for brands)
        limit: 5,
        with_payload: true,
        with_vector: false,
      },
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = response.data.result;

    res.status(200).json({
      success: true,
      message: "Sample data from brands collection",
      collection: COLLECTION_NAME_BRANDS,
      total_found: results.length,
      samples: results.map((result) => ({
        id: result.id,
        score: result.score,
        payload_keys: result.payload ? Object.keys(result.payload) : [],
        payload_sample: result.payload,
      })),
    });
  } catch (error) {
    console.error("Error getting brands sample data:", error.message);
    res.status(500).json({
      error: "Failed to get brands sample data",
      code: "BRANDS_SAMPLE_DATA_ERROR",
      message: error.message,
    });
  }
});

// Debug endpoint to check companies collection data structure
app.get("/debug/companies-data", async (req, res) => {
  try {
    const response = await axios.post(
      `${qdrantUrl}/collections/${COLLECTION_NAME_COMPANIES}/points/search`,
      {
        vector: Array(384).fill(0), // Dummy vector to get any results (384 dimensions for companies)
        limit: 5,
        with_payload: true,
        with_vector: false,
      },
      {
        headers: buildHeaders(),
        timeout: 30000,
      }
    );

    const results = response.data.result;

    res.status(200).json({
      success: true,
      message: "Sample data from companies collection",
      collection: COLLECTION_NAME_COMPANIES,
      total_found: results.length,
      samples: results.map((result) => ({
        id: result.id,
        score: result.score,
        payload_keys: result.payload ? Object.keys(result.payload) : [],
        payload_sample: result.payload,
      })),
    });
  } catch (error) {
    console.error("Error getting companies sample data:", error.message);
    res.status(500).json({
      error: "Failed to get companies sample data",
      code: "COMPANIES_SAMPLE_DATA_ERROR",
      message: error.message,
    });
  }
});

// Enhanced brand search endpoint - combines Qdrant search with ClickHouse summary data OR ClickHouse-only search
app.post("/search/brands/enhanced", async (req, res) => {
  const startTime = performance.now();
  console.log(
    `🚀 Enhanced brand search started at ${new Date().toISOString()}`
  );

  try {
    const {
      searchTerm = null,
      limit = DEFAULT_LIMIT,
      categoryIds = null,
      countryId = null,
      softwareIds = null,
      durationMoreThen = null,
      durationLessThen = null,
      sortProp = "totalSpend",
      orderAsc = false,
      similarityThreshold = 0.3,
    } = req.body;
    // Fire-and-forget: store search history without blocking the response
    axios
      .post("https://apiv1.vidtao.com/api/brands/insertSearchData", req.body, {
        headers: {
          authorization: req.headers["authorization"],
          "session-key": req.headers["session-key"],
        },
        timeout: 2000,
      })
      .catch((err) =>
        console.warn("History logging failed (non-critical):", err.message)
      );
    // Get user_id from authentication middleware
    const userId = req.user_id;

    // Convert empty string searchTerm to null
    const normalizedSearchTerm = searchTerm === "" ? null : searchTerm;
    // Convert categoryIds=0 or [0] to null
    const normalizedCategoryIds =
      categoryIds === 0 ||
      (Array.isArray(categoryIds) &&
        categoryIds.length === 1 &&
        categoryIds[0] === 0)
        ? null
        : categoryIds;
    // Convert countryId=0 to null
    const normalizedCountryId = countryId === 0 ? null : countryId;
    // Convert empty string sortProp to totalSpend and 'total' to 'totalSpend'
    const normalizedSortProp =
      sortProp === ""
        ? "totalSpend"
        : sortProp === "total"
        ? "totalSpend"
        : sortProp;

    console.log("normalizedSortProp", normalizedSortProp);

    // Validate searchTerm if provided
    if (
      normalizedSearchTerm !== null &&
      (typeof normalizedSearchTerm !== "string" ||
        normalizedSearchTerm.trim() === "")
    ) {
      return res.status(400).json({
        error: "searchTerm must be a non-empty string if provided",
        code: "INVALID_SEARCH_TERM",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    // Validate sortProp field (if provided)
    console.log("normalizedSortProp", normalizedSortProp);
    const validOrderFields = ["totalSpend", "similarity_score"];
    if (normalizedSortProp && !validOrderFields.includes(normalizedSortProp)) {
      return res.status(400).json({
        error: `sortProp must be one of: ${validOrderFields.join(
          ", "
        )} or null for auto-detection`,
        code: "INVALID_SORT_PROP",
      });
    }

    // Validate orderAsc (boolean)
    if (typeof orderAsc !== "boolean") {
      return res.status(400).json({
        error: "orderAsc must be a boolean (true = asc, false = desc)",
        code: "INVALID_ORDER_ASC",
      });
    }

    // Validate similarityThreshold
    if (similarityThreshold < 0 || similarityThreshold > 1) {
      return res.status(400).json({
        error: "similarityThreshold must be between 0.0 and 1.0",
        code: "INVALID_similarityThreshold",
      });
    }

    console.log(`📋 Request parameters:`, {
      searchTerm: normalizedSearchTerm ? `"${normalizedSearchTerm}"` : null,
      limit,
      categoryIds: normalizedCategoryIds,
      countryId: normalizedCountryId,
      softwareIds,
      durationMoreThen,
      durationLessThen,
      sortProp: normalizedSortProp,
      orderAsc,
      similarityThreshold,
      userId: userId ? "provided" : "none",
    });

    // Determine default sortProp based on context
    let effectiveSortProp = normalizedSortProp;
    if (!effectiveSortProp) {
      effectiveSortProp = normalizedSearchTerm
        ? "similarity_score"
        : "totalSpend";
    }

    // Map frontend sortProp names to database field names
    const sortPropMapping = {
      totalSpend: "total_spend",
      similarity_score: "similarity_score",
    };

    // Convert frontend sortProp to database field name
    const dbSortField = sortPropMapping[effectiveSortProp] || effectiveSortProp;

    // Convert orderAsc to order_direction string
    const order_direction = orderAsc ? "asc" : "desc";
    // For ClickHouse queries, use total_spend as fallback when sorting by similarity_score
    const order_by =
      dbSortField === "similarity_score" ? "total_spend" : dbSortField;

    console.log(
      `🔍 Enhanced brand search: ${
        normalizedSearchTerm ? `"${normalizedSearchTerm}"` : "ClickHouse-only"
      } (limit: ${limit}, userId: ${userId || "none"})`
    );

    let qdrantResults = [];
    let qdrantBrandIds = [];
    let embeddingTime = 0;
    let qdrantTime = 0;
    let externalVideoIds = [];
    let externalTime = 0;
    let parallelExecutionTime = 0;
    let brandIds = [];

    // PATH 1: Keyword provided - optimized parallel execution
    if (normalizedSearchTerm) {
      console.log(
        `📡 Starting optimized parallel search operations at ${new Date().toISOString()}`
      );
      console.log(
        `📡 Performing Qdrant and external semantic search for: "${normalizedSearchTerm}"`
      );

      // Step 1: Start embedding creation and external service call in parallel
      const parallelStartTime = performance.now();

      const embeddingPromise = createEmbedding(
        normalizedSearchTerm.trim(),
        384
      ); // 384 dimensions for brands

      console.log(
        `⚡ Starting embedding creation and external service call in parallel`
      );
      // Extract token from request headers
      const authHeader = req.headers["authorization"];
      const token = authHeader && authHeader.split(" ")[1]; // Bearer TOKEN

      const externalPromise = fetchExternalBrandIds({
        searchTerm: normalizedSearchTerm,
        categoryIds: normalizedCategoryIds,
        countryId: normalizedCountryId,
        softwareIds,
        durationMoreThen,
        durationLessThen,
        limit,
        token,
      });

      // Step 2: Prepare Qdrant filters (can be done while embedding is being created)
      const filters = { must: [] };
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        filters.must.push({
          key: "category_id",
          match: { any: normalizedCategoryIds },
        });
      }
      if (normalizedCountryId && typeof normalizedCountryId === "number") {
        filters.must.push({
          key: "country_id",
          match: { value: normalizedCountryId },
        });
      }
      if (softwareIds && Array.isArray(softwareIds) && softwareIds.length > 0) {
        filters.must.push({ key: "software_id", match: { any: softwareIds } });
      }
      if (durationMoreThen !== null || durationLessThen !== null) {
        const durationFilter = { key: "average_video_duration", range: {} };
        if (durationMoreThen !== null && typeof durationMoreThen === "number") {
          durationFilter.range.gte = durationMoreThen;
        }
        if (durationLessThen !== null && typeof durationLessThen === "number") {
          durationFilter.range.lte = durationLessThen;
        }
        filters.must.push(durationFilter);
      }

      // Step 3: Wait for embedding to be ready, then start Qdrant search immediately
      console.log(`🔍 Waiting for embedding to be ready...`);
      const embedding = await embeddingPromise;
      embeddingTime = performance.now() - parallelStartTime;
      console.log(`✅ Embedding created in ${embeddingTime.toFixed(2)}ms`);

      // Step 4: Start Qdrant search immediately (don't wait for external service)
      const qdrantStartTime = performance.now();
      console.log(
        `🔍 Starting Qdrant brand search at ${new Date().toISOString()}`
      );
      const searchBody = {
        vector: embedding,
        limit: parseInt(limit),
        with_payload: true,
        with_vector: false,
        params: { hnsw_ef: 32 },
      };
      if (filters.must.length > 0) {
        searchBody.filter = filters;
      }
      console.log(
        "searchBody filter:",
        JSON.stringify(searchBody.filter, null, 2)
      );
      const searchResponse = await axios.post(
        `${qdrantUrl}/collections/${COLLECTION_NAME_BRANDS}/points/search`,
        searchBody,
        { headers: buildHeaders(), timeout: 30000 }
      );
      qdrantResults = searchResponse.data.result;
      qdrantTime = performance.now() - qdrantStartTime;
      console.log(
        `✅ Qdrant brand search completed in ${qdrantTime.toFixed(
          2
        )}ms, returned ${qdrantResults.length} results`
      );

      // Step 5: Process Qdrant results
      // Filter by similarity threshold
      const originalResultsCount = qdrantResults.length;
      qdrantResults = qdrantResults.filter(
        (result) => result.score >= similarityThreshold
      );
      if (originalResultsCount > qdrantResults.length) {
        console.log(
          `🔍 Filtered ${
            originalResultsCount - qdrantResults.length
          } results below similarity threshold ${similarityThreshold}`
        );
      }
      // Extract brand IDs from Qdrant results
      qdrantBrandIds = qdrantResults
        .sort((a, b) => b.score - a.score)
        .map((result) => result.payload?.brand_id || result.id);

      // Step 6: Wait for external service to complete
      console.log(`🌐 Waiting for external service to complete...`);
      const extIds = await externalPromise;
      externalTime = performance.now() - parallelStartTime;
      externalVideoIds = extIds || [];
      console.log(
        `✅ External service completed in ${externalTime.toFixed(
          2
        )}ms, returned ${externalVideoIds.length} brand IDs`
      );

      // Debug: Show first few brand IDs from external service
      if (externalVideoIds.length > 0) {
        console.log(
          `🔍 First 5 external brand IDs:`,
          externalVideoIds.slice(0, 5)
        );
      }

      parallelExecutionTime = Math.max(embeddingTime, externalTime);
      console.log(
        `📊 Parallel execution completed in ${parallelExecutionTime.toFixed(
          2
        )}ms (embedding: ${embeddingTime.toFixed(
          2
        )}ms, external: ${externalTime.toFixed(2)}ms)`
      );

      // Step 7: Merge Qdrant and external brand IDs - use ALL results for ClickHouse processing
      console.log(`🔄 Merging ALL results for ClickHouse processing`);
      console.log(`   - Qdrant results: ${qdrantBrandIds.length}`);
      console.log(`   - External results: ${externalVideoIds.length}`);

      // Start with all Qdrant results (they have priority)
      const mergedBrandIds = [...qdrantBrandIds];
      const seen = new Set(qdrantBrandIds);

      // Add ALL external results (no limit here - we'll apply limit after ClickHouse processing)
      let addedFromExternal = 0;
      for (const bid of externalVideoIds) {
        if (!seen.has(bid)) {
          mergedBrandIds.push(bid);
          seen.add(bid);
          addedFromExternal++;
        }
      }
      console.log(
        `   - Added ${addedFromExternal} unique results from external service`
      );

      brandIds = mergedBrandIds;
      console.log(
        `✅ Final merged result: ${brandIds.length} brands (Qdrant: ${
          qdrantBrandIds.length
        }, External used: ${
          brandIds.length - qdrantBrandIds.length
        }) - will be processed in chunks and limited to ${limit} at the end`
      );

      // Debug: Show final brand IDs that will be queried
      if (brandIds.length > 0) {
        console.log(`🔍 Final brand IDs to query:`, brandIds.slice(0, 5));
      }
    }

    // PATH 2: Get data from ClickHouse (parallel with swiped brands if userId provided)
    let clickhouseData = [];
    let clickhouseTime = 0;
    let swipedBrandsData = [];
    let swipedBrandsTime = 0;
    const clickhouseStartTime = performance.now();

    // Function to fetch swiped brands data
    async function fetchSwipedBrands(brandIds, userId) {
      if (!userId || !brandIds || brandIds.length === 0) {
        return [];
      }

      try {
        const brandIdList = brandIds
          .map((id) => `'${String(id).replace(/'/g, "''")}'`)
          .join(", ");

        const swipedQuery = `
          SELECT 
            firebase_id,
            brand_id,
            created_at,
            swipe_board_ids
          FROM analytics.swiped_brands 
          WHERE firebase_id = '${userId.replace(/'/g, "''")}' 
          AND brand_id IN (${brandIdList})
        `;

        const resultSet = await clickhouse.query({
          query: swipedQuery,
          format: "JSONEachRow",
        });

        const data = await resultSet.json();
        return data;
      } catch (error) {
        console.error("❌ Swiped brands query failed:", error.message);
        return [];
      }
    }

    // Function to fetch swiped companies data
    async function fetchSwipedCompanies(companyIds, userId) {
      if (!userId || !companyIds || companyIds.length === 0) {
        return [];
      }

      try {
        const companyIdList = companyIds
          .map((id) => `'${String(id).replace(/'/g, "''")}'`)
          .join(", ");

        const swipedQuery = `
          SELECT 
            firebase_id,
            company_id,
            created_at,
            swipe_board_ids
          FROM analytics.swiped_companies 
          WHERE firebase_id = '${userId.replace(/'/g, "''")}' 
          AND company_id IN (${companyIdList})
        `;

        const resultSet = await clickhouse.query({
          query: swipedQuery,
          format: "JSONEachRow",
        });

        const data = await resultSet.json();
        return data;
      } catch (error) {
        console.error("❌ Swiped companies query failed:", error.message);
        return [];
      }
    }

    if (normalizedSearchTerm && brandIds.length > 0) {
      // Case 1: We have brand IDs from Qdrant search - query specific brands using chunked processing
      console.log(
        `📊 Starting chunked ClickHouse queries for ${
          brandIds.length
        } brand IDs at ${new Date().toISOString()}`
      );

      // Helper function to split array into chunks
      const chunkArray = (array, chunkSize) => {
        const chunks = [];
        for (let i = 0; i < array.length; i += chunkSize) {
          chunks.push(array.slice(i, i + chunkSize));
        }
        return chunks;
      };

      // Split brand IDs into chunks of 5000 (optimized for ClickHouse query limits)
      const CHUNK_SIZE = 500;
      const brandIdChunks = chunkArray(brandIds, CHUNK_SIZE);
      console.log(
        `🔍 Split ${brandIds.length} brand IDs into ${brandIdChunks.length} chunks of ${CHUNK_SIZE}`
      );

      // Function to execute ClickHouse query for a single chunk
      const executeClickHouseChunk = async (chunk, chunkIndex) => {
        const brandIdList = chunk
          .map((id) => `'${String(id).replace(/'/g, "''")}'`)
          .join(", ");

        let query;
        if (effectiveSortProp === "similarity_score") {
          // When sorting by similarity_score, preserve Qdrant order by not using ORDER BY
          query = `
            SELECT 
              bb.brand_id,
              COALESCE(bs.views_7, 0) as views_7,
              COALESCE(bs.views_14, 0) as views_14,
              COALESCE(bs.views_21, 0) as views_21,
              COALESCE(bs.views_30, 0) as views_30,
              COALESCE(bs.views_90, 0) as views_90,
              COALESCE(bs.views_365, 0) as views_365,
              COALESCE(bs.views_720, 0) as views_720,
              COALESCE(bs.total_views, 0) as total_views,
              COALESCE(bs.spend_7, 0) as spend_7,
              COALESCE(bs.spend_14, 0) as spend_14,
              COALESCE(bs.spend_21, 0) as spend_21,
              COALESCE(bs.spend_30, 0) as spend_30,
              COALESCE(bs.spend_90, 0) as spend_90,
              COALESCE(bs.spend_365, 0) as spend_365,
              COALESCE(bs.spend_720, 0) as spend_720,
              COALESCE(bs.total_spend, 0) as total_spend,
              bs.latest_date as summary_date,
              bb.name,
              bb.description,
              bb.thumbnail,
              bb.category_id,
              bb.country_id,
              bb.avg_duration,
              bb.updated_at
            FROM analytics.brand_basic bb
            LEFT JOIN (
                SELECT *
                FROM analytics.brand_summary_latest_view
                WHERE brand_id IN (${brandIdList})
            ) bs ON bb.brand_id = bs.brand_id
            WHERE bb.brand_id IN (${brandIdList})
            AND ${
              normalizedCategoryIds &&
              Array.isArray(normalizedCategoryIds) &&
              normalizedCategoryIds.length > 0
                ? `bb.category_id IN (${normalizedCategoryIds.join(", ")})`
                : `bb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
            }
          `;
        } else {
          // For other sort fields, use ClickHouse ordering
          const whereClause = `WHERE bb.brand_id IN (${brandIdList}) AND ${
            normalizedCategoryIds &&
            Array.isArray(normalizedCategoryIds) &&
            normalizedCategoryIds.length > 0
              ? `bb.category_id IN (${normalizedCategoryIds.join(", ")})`
              : `bb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
          }`;
          query = `
            SELECT 
              bb.brand_id,
              COALESCE(bs.views_7, 0) as views_7,
              COALESCE(bs.views_14, 0) as views_14,
              COALESCE(bs.views_21, 0) as views_21,
              COALESCE(bs.views_30, 0) as views_30,
              COALESCE(bs.views_90, 0) as views_90,
              COALESCE(bs.views_365, 0) as views_365,
              COALESCE(bs.views_720, 0) as views_720,
              COALESCE(bs.total_views, 0) as total_views,
              COALESCE(bs.spend_7, 0) as spend_7,
              COALESCE(bs.spend_14, 0) as spend_14,
              COALESCE(bs.spend_21, 0) as spend_21,
              COALESCE(bs.spend_30, 0) as spend_30,
              COALESCE(bs.spend_90, 0) as spend_90,
              COALESCE(bs.spend_365, 0) as spend_365,
              COALESCE(bs.spend_720, 0) as spend_720,
              COALESCE(bs.total_spend, 0) as total_spend,
              bs.latest_date as summary_date,
              bb.name,
              bb.description,
              bb.thumbnail,
              bb.category_id,
              bb.country_id,
              bb.avg_duration,
              bb.updated_at
            FROM analytics.brand_basic bb
            LEFT JOIN (
                SELECT *
                FROM analytics.brand_summary_latest_view
                WHERE brand_id IN (${brandIdList})
            ) bs ON bb.brand_id = bs.brand_id
            ${whereClause}
            ORDER BY ${order_by} ${order_direction.toUpperCase()}
          `;
        }

        // Only log every 5th chunk to reduce console spam
        if ((chunkIndex + 1) % 5 === 0 || chunkIndex === 0) {
          console.log(
            `🔍 Executing ClickHouse chunk ${chunkIndex + 1}/${
              brandIdChunks.length
            } with ${chunk.length} brand IDs`
          );
        }

        try {
          const resultSet = await clickhouse.query({
            query: query,
            format: "JSONEachRow",
          });

          const chunkData = await resultSet.json();
          console.log(
            `✅ ClickHouse chunk ${chunkIndex + 1} returned ${
              chunkData.length
            } records`
          );
          return chunkData;
        } catch (error) {
          console.error(
            `❌ ClickHouse chunk ${chunkIndex + 1} failed:`,
            error.message
          );
          return [];
        }
      };

      // Execute chunks with controlled concurrency (max 10 concurrent)
      console.log(
        `🔄 Executing ${brandIdChunks.length} ClickHouse chunks with controlled concurrency`
      );

      const CONCURRENT_CHUNKS = 10;
      const chunkResults = [];

      for (let i = 0; i < brandIdChunks.length; i += CONCURRENT_CHUNKS) {
        const batch = brandIdChunks.slice(i, i + CONCURRENT_CHUNKS);
        const batchPromises = batch.map((chunk, batchIndex) =>
          executeClickHouseChunk(chunk, i + batchIndex)
        );

        const batchResults = await Promise.all(batchPromises);
        chunkResults.push(...batchResults);

        console.log(
          `✅ Completed batch ${
            Math.floor(i / CONCURRENT_CHUNKS) + 1
          }/${Math.ceil(brandIdChunks.length / CONCURRENT_CHUNKS)}`
        );
      }

      // Merge all chunk results
      clickhouseData = chunkResults.flat();
      console.log(
        `🔍 Merged ${chunkResults.length} chunks into ${clickhouseData.length} total records`
      );

      // Execute swiped brands query in parallel if userId is provided
      if (userId) {
        console.log(
          `🔄 Executing swiped brands query in parallel with ClickHouse chunks`
        );
        swipedBrandsData = await fetchSwipedBrands(brandIds, userId);
      }

      // Note: Deduplication is handled at SQL level using argMax aggregate function

      clickhouseTime = performance.now() - clickhouseStartTime;

      console.log(
        `✅ ClickHouse query completed in ${clickhouseTime.toFixed(
          2
        )}ms, returned ${clickhouseData.length} records`
      );

      // Log only if no results found
      if (clickhouseData.length === 0 && brandIds.length > 0) {
        console.log(
          `⚠️ No ClickHouse data found for ${brandIds.length} brand IDs`
        );
      }

      if (userId) {
        console.log(
          `✅ Swiped brands query completed, found ${swipedBrandsData.length} records`
        );
      }
    } else if (!normalizedSearchTerm) {
      // Case 2: No keyword - direct ClickHouse query with filters
      console.log(
        `📊 Starting direct ClickHouse brand search with filters at ${new Date().toISOString()}`
      );

      // Build WHERE clause for filters
      // Note: brand_summary table only contains brand_id, views, spend, and date
      // Category, country, software, and duration filters are not available in this table
      // Build WHERE clause for filters
      const whereConditions = [];

      // Category ID filter
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        // If categoryIds are provided, filter to include only those categories
        const categoryList = normalizedCategoryIds.join(", ");
        whereConditions.push(`bb.category_id IN (${categoryList})`);
      } else {
        // If no categoryIds provided, exclude the standard excluded categories
        whereConditions.push(
          `bb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
        );
      }

      // Country ID filter
      if (normalizedCountryId && typeof normalizedCountryId === "number") {
        whereConditions.push(`bb.country_id = ${normalizedCountryId}`);
      }

      // Combine all conditions
      const whereClause =
        whereConditions.length > 0
          ? `WHERE ${whereConditions.join(" AND ")}`
          : "";

      // Take 1000 brands based on WHERE and ORDER clause, then get first 200 distinct
      const preFilterLimit = 1000; // Take top 1000, then get first 200 distinct

      console.log(
        `🔍 whereClause: ${whereClause}, preFilterLimit: ${preFilterLimit}`
      );

      // Fast approach: get 1000 records, then deduplicate programmatically
      const query = `
        SELECT 
          bb.brand_id,
          COALESCE(bs.views_7, 0) as views_7,
          COALESCE(bs.views_14, 0) as views_14,
          COALESCE(bs.views_21, 0) as views_21,
          COALESCE(bs.views_30, 0) as views_30,
          COALESCE(bs.views_90, 0) as views_90,
          COALESCE(bs.views_365, 0) as views_365,
          COALESCE(bs.views_720, 0) as views_720,
          COALESCE(bs.total_views, 0) as total_views,
          COALESCE(bs.spend_7, 0) as spend_7,
          COALESCE(bs.spend_14, 0) as spend_14,
          COALESCE(bs.spend_21, 0) as spend_21,
          COALESCE(bs.spend_30, 0) as spend_30,
          COALESCE(bs.spend_90, 0) as spend_90,
          COALESCE(bs.spend_365, 0) as spend_365,
          COALESCE(bs.spend_720, 0) as spend_720,
          COALESCE(bs.total_spend, 0) as total_spend,
          bs.latest_date as summary_date,
          bb.name,
          bb.description,
          bb.category_id,
          bb.country_id,
          bb.avg_duration,
          bb.thumbnail,
          bb.updated_at
        FROM analytics.brand_basic bb
        LEFT JOIN analytics.brand_summary_latest_view AS bs ON bb.brand_id = bs.brand_id
        ${whereClause}
        ORDER BY COALESCE(bs.${order_by}, 0) ${order_direction.toUpperCase()}, bb.brand_id
        LIMIT ${preFilterLimit}
      `;

      console.log(
        `📊 Executing direct ClickHouse brand query with filters`,
        query
      );

      try {
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
          clickhouse_settings: {
            // Query-specific memory settings for brand search (increased for large datasets)
            max_memory_usage: 48000000000, // 48GB for this specific query (increased from 20GB)
            max_bytes_before_external_group_by: 36000000000, // 36GB (increased from 15GB)
            max_bytes_before_external_sort: 36000000000, // 36GB (increased from 15GB)
            max_threads: 4, // Increased threads for better performance
          },
        });

        const rawData = await resultSet.json();
        clickhouseTime = performance.now() - clickhouseStartTime;

        console.log(
          `✅ ClickHouse returned ${
            rawData.length
          } raw records in ${clickhouseTime.toFixed(2)}ms`
        );

        // Programmatic deduplication: keep first occurrence of each brand_id
        const seenBrandIds = new Set();
        clickhouseData = [];

        for (const record of rawData) {
          if (!seenBrandIds.has(record.brand_id)) {
            seenBrandIds.add(record.brand_id);
            clickhouseData.push(record);

            // Stop when we have enough distinct brands
            if (clickhouseData.length >= parseInt(limit)) {
              break;
            }
          }
        }

        console.log(
          `🔍 Deduplicated ${rawData.length} → ${clickhouseData.length} distinct brands`
        );

        // If userId is provided, fetch swiped brands data for the returned brand IDs
        if (userId && clickhouseData.length > 0) {
          const returnedBrandIds = clickhouseData.map((item) => item.brand_id);

          swipedBrandsData = await fetchSwipedBrands(returnedBrandIds, userId);
          swipedBrandsTime = performance.now() - clickhouseStartTime;
          console.log(
            `✅ Swiped brands query completed in ${swipedBrandsTime.toFixed(
              2
            )}ms, found ${swipedBrandsData.length} records`
          );
        }
      } catch (clickhouseError) {
        console.error(
          "❌ ClickHouse brand query failed:",
          clickhouseError.message
        );
        clickhouseData = [];
      }
    }

    const totalTime = performance.now() - startTime;

    // Step 3: Format results based on search type
    const enhancedResults = [];

    // Create a map of swiped brands for quick lookup
    const swipedBrandsMap = new Map();
    swipedBrandsData.forEach((item) => {
      swipedBrandsMap.set(String(item.brand_id), item);
    });

    if (normalizedSearchTerm) {
      // Case 1: Keyword search - combine Qdrant similarity scores with ClickHouse data
      const clickhouseMap = new Map();
      clickhouseData.forEach((item) => {
        // Convert brand_id to string to ensure consistent key matching
        clickhouseMap.set(String(item.brand_id), item);
      });

      // Process ALL brand IDs (both Qdrant and external results)
      // Create a map of Qdrant results for quick lookup
      const qdrantMap = new Map();
      qdrantResults.forEach((qdrantResult) => {
        const brandId = qdrantResult.payload?.brand_id || qdrantResult.id;
        qdrantMap.set(brandId, qdrantResult);
      });

      console.log(
        `🔍 Processing ${brandIds.length} brand IDs (Qdrant: ${
          qdrantResults.length
        }, External: ${brandIds.length - qdrantResults.length})`
      );

      // Process all brand IDs from the merged list
      brandIds.forEach((brandId) => {
        const clickhouseInfo = clickhouseMap.get(String(brandId));
        const swipedInfo = swipedBrandsMap.get(String(brandId));
        const qdrantResult = qdrantMap.get(brandId);

        const enhancedResult = {
          brandId: brandId,
          similarity_score: qdrantResult ? qdrantResult.score : null,
          // Qdrant payload data (if available)
          qdrant_data: qdrantResult ? qdrantResult.payload : null,
          // ClickHouse summary data
          summary_data: clickhouseInfo || null,
          // Swiped brands data (if available)
          swiped_data: swipedInfo || null,
        };

        enhancedResults.push(enhancedResult);
      });
    } else {
      // Case 2: ClickHouse-only search - format ClickHouse data directly
      clickhouseData.forEach((clickhouseResult) => {
        const swipedInfo = swipedBrandsMap.get(
          String(clickhouseResult.brand_id)
        );

        const enhancedResult = {
          brandId: clickhouseResult.brand_id,
          similarity_score: null, // No similarity score for direct ClickHouse search
          // No Qdrant data available
          qdrant_data: null,
          // ClickHouse summary data
          summary_data: clickhouseResult,
          // Swiped brands data (if available)
          swiped_data: swipedInfo || null,
        };

        enhancedResults.push(enhancedResult);
      });
    }

    // Sort results based on the effective sort property
    if (normalizedSearchTerm && brandIds.length > 0) {
      // When we have searchTerm, we need to sort the combined results
      if (effectiveSortProp === "similarity_score") {
        // Only sort by similarity when no sortProp was provided or explicitly requested
        console.log(
          `🔄 Sorting by similarity_score (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          const scoreA = a.similarity_score || 0;
          const scoreB = b.similarity_score || 0;
          return orderAsc ? scoreA - scoreB : scoreB - scoreA;
        });
      } else {
        // Sort by the specified field (prioritize user choice)
        console.log(
          `🔄 Sorting by ${effectiveSortProp} (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          let valueA, valueB;

          switch (dbSortField) {
            case "total_spend":
              valueA = a.summary_data?.total_spend ?? 0;
              valueB = b.summary_data?.total_spend ?? 0;
              break;
            default:
              valueA = a.summary_data?.[dbSortField] ?? 0;
              valueB = b.summary_data?.[dbSortField] ?? 0;
          }

          return orderAsc ? valueA - valueB : valueB - valueA;
        });
      }
    } else {
      console.log(
        `🔄 ClickHouse-only search, sorted by ${order_by} (${order_direction})`
      );
    }

    // Apply final limit after sorting
    const finalResults = enhancedResults.slice(0, parseInt(limit));
    console.log(
      `🔍 Applied final limit: ${enhancedResults.length} → ${finalResults.length} results`
    );

    // Format response to match frontend expectations (based on original fullSearch structure)
    console.log(
      `🔍 Final response formatting: ${finalResults.length} enhanced results`
    );

    const data = {
      results: finalResults.map((r, index) => {
        const isSwiped = r.swiped_data ? true : false;

        const result = {
          brandId: r.brandId,
          isSwiped: isSwiped, // Set based on swiped_brands table
          name: r.summary_data?.name || r.qdrant_data?.name || null,
          description: r.summary_data?.description || null,
          thumbnail:
            r.summary_data?.thumbnail || r.qdrant_data?.thumbnail || null,
          countryId:
            r.summary_data?.country_id || r.qdrant_data?.country_id || null,
          categoryId:
            r.summary_data?.category_id || r.qdrant_data?.category_id || null,
          totalSpend: r.summary_data?.total_spend || null,
          averageVideoDuration:
            r.summary_data?.avg_duration ||
            r.qdrant_data?.average_video_duration ||
            null,
          // Additional fields for debugging/backward compatibility
          similarity_score: r.similarity_score,
          qdrant_data: r.qdrant_data,
          summary_data: r.summary_data,
          swiped_data: r.swiped_data, // Include swiped data for debugging
        };

        // Log only first result for debugging
        if (index === 0) {
          console.log(`🔍 Sample result:`, {
            brandId: result.brandId,
            name: result.name,
            totalSpend: result.totalSpend,
            has_summary_data: !!result.summary_data,
            has_qdrant_data: !!result.qdrant_data,
          });
        }

        return result;
      }),
    };

    const response = {
      success: true,
      search_type: normalizedSearchTerm
        ? "semantic_plus_clickhouse"
        : "clickhouse_only",
      keyword: normalizedSearchTerm || null,
      total_results: finalResults.length,
      total_available: enhancedResults.length,
      qdrant_matches: qdrantResults.length,
      external_matches: externalVideoIds.length,
      external_matches_used: brandIds.length - qdrantBrandIds.length,
      external_service_used: normalizedSearchTerm
        ? externalVideoIds.length > 0 || externalTime > 0
        : false,
      clickhouse_matches: clickhouseData.length,
      swiped_matches: swipedBrandsData.length,
      parameters: {
        search_filters: {
          category_id: categoryIds,
          country_id: countryId,
          software_id: softwareIds,
          duration_range: {
            min: durationMoreThen,
            max: durationLessThen,
          },
        },
        similarityThreshold: normalizedSearchTerm ? similarityThreshold : null,
        clickhouse_ordering: {
          order_by: order_by,
          order_direction: order_direction,
        },
        limit: parseInt(limit),
        userId: userId,
      },
      timing: {
        total_ms: Math.round(totalTime),
        parallel_execution_ms: Math.round(parallelExecutionTime),
        embedding_ms: Math.round(embeddingTime),
        qdrant_search_ms: Math.round(qdrantTime),
        clickhouse_query_ms: Math.round(clickhouseTime),
        swiped_brands_query_ms: Math.round(swipedBrandsTime),
        external_service_ms: Math.round(externalTime),
      },
      data: data,
    };

    const searchTypeDesc = normalizedSearchTerm
      ? `semantic + ClickHouse`
      : `ClickHouse-only`;
    console.log(
      `✅ Enhanced brand search (${searchTypeDesc}) completed in ${totalTime.toFixed(
        2
      )}ms - ${enhancedResults.length} results (Qdrant: ${
        qdrantResults.length
      }, External available: ${externalVideoIds.length}, External used: ${
        brandIds.length - qdrantBrandIds.length
      }, ClickHouse: ${clickhouseData.length}, Swiped: ${
        swipedBrandsData.length
      })`
    );
    console.log(`📊 Final timing breakdown:`);
    console.log(`   - Total time: ${totalTime.toFixed(2)}ms`);
    console.log(
      `   - Parallel execution: ${parallelExecutionTime.toFixed(2)}ms`
    );
    console.log(`   - Embedding: ${embeddingTime.toFixed(2)}ms`);
    console.log(`   - External service: ${externalTime.toFixed(2)}ms`);
    console.log(`   - Qdrant search: ${qdrantTime.toFixed(2)}ms`);
    console.log(`   - ClickHouse query: ${clickhouseTime.toFixed(2)}ms`);
    console.log(`   - Swiped brands: ${swipedBrandsTime.toFixed(2)}ms`);
    console.log(`🏁 Response sent at ${new Date().toISOString()}`);

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Enhanced brand search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );
    console.error(`🚨 Error occurred at ${new Date().toISOString()}`);

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME_BRANDS}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// Enhanced company search endpoint - combines Qdrant search with ClickHouse summary data OR ClickHouse-only search
app.post("/search/companies/enhanced", async (req, res) => {
  const startTime = performance.now();
  console.log(
    `🚀 Enhanced company search started at ${new Date().toISOString()}`
  );

  try {
    const {
      searchTerm = null,
      limit = DEFAULT_LIMIT,
      categoryIds = null,
      countryId = null,
      softwareIds = null,
      sortProp = "totalSpend",
      orderAsc = false,
      similarityThreshold = 0.4,
    } = req.body;
    axios
      .post(
        "https://apiv1.vidtao.com/api/companies/insertSearchData",
        req.body,
        {
          headers: {
            authorization: req.headers["authorization"],
            "session-key": req.headers["session-key"],
          },
          timeout: 2000,
        }
      )
      .catch((err) =>
        console.warn("History logging failed (non-critical):", err.message)
      );
    // Get user_id from authentication middleware
    const userId = req.user_id;

    // Convert empty string searchTerm to null
    const normalizedSearchTerm = searchTerm === "" ? null : searchTerm;
    // Convert categoryIds=0 or [0] to null
    const normalizedCategoryIds =
      categoryIds === 0 ||
      (Array.isArray(categoryIds) &&
        categoryIds.length === 1 &&
        categoryIds[0] === 0)
        ? null
        : categoryIds;
    // Convert countryId=0 to null
    const normalizedCountryId = countryId === 0 ? null : countryId;
    // Convert empty string sortProp to totalSpend and 'total' to 'totalSpend'
    const normalizedSortProp =
      sortProp === ""
        ? "totalSpend"
        : sortProp === "total"
        ? "totalSpend"
        : sortProp;

    console.log("normalizedSortProp", normalizedSortProp);

    // Validate searchTerm if provided
    if (
      normalizedSearchTerm !== null &&
      (typeof normalizedSearchTerm !== "string" ||
        normalizedSearchTerm.trim() === "")
    ) {
      return res.status(400).json({
        error: "searchTerm must be a non-empty string if provided",
        code: "INVALID_SEARCH_TERM",
      });
    }

    // Validate limit
    if (limit < 1 || limit > 1000) {
      return res.status(400).json({
        error: "Limit must be between 1 and 1000",
        code: "INVALID_LIMIT",
      });
    }

    // Validate sortProp field (if provided)
    console.log("normalizedSortProp", normalizedSortProp);
    const validOrderFields = ["totalSpend", "similarity_score"];
    if (normalizedSortProp && !validOrderFields.includes(normalizedSortProp)) {
      return res.status(400).json({
        error: `sortProp must be one of: ${validOrderFields.join(
          ", "
        )} or null for auto-detection`,
        code: "INVALID_SORT_PROP",
      });
    }

    // Validate orderAsc (boolean)
    if (typeof orderAsc !== "boolean") {
      return res.status(400).json({
        error: "orderAsc must be a boolean (true = asc, false = desc)",
        code: "INVALID_ORDER_ASC",
      });
    }

    // Validate similarityThreshold
    if (similarityThreshold < 0 || similarityThreshold > 1) {
      return res.status(400).json({
        error: "similarityThreshold must be between 0.0 and 1.0",
        code: "INVALID_similarityThreshold",
      });
    }

    console.log(`📋 Request parameters:`, {
      searchTerm: normalizedSearchTerm ? `"${normalizedSearchTerm}"` : null,
      limit,
      categoryIds: normalizedCategoryIds,
      countryId: normalizedCountryId,
      softwareIds,
      sortProp: normalizedSortProp,
      orderAsc,
      similarityThreshold,
      userId: userId ? "provided" : "none",
    });

    // Determine default sortProp based on context
    let effectiveSortProp = normalizedSortProp;
    if (!effectiveSortProp) {
      effectiveSortProp = normalizedSearchTerm
        ? "similarity_score"
        : "totalSpend";
    }

    // Map frontend sortProp names to database field names
    const sortPropMapping = {
      totalSpend: "total_spend",
      similarity_score: "similarity_score",
    };

    // Convert frontend sortProp to database field name
    const dbSortField = sortPropMapping[effectiveSortProp] || effectiveSortProp;

    // Convert orderAsc to order_direction string
    const order_direction = orderAsc ? "asc" : "desc";
    const order_by = dbSortField;

    console.log(
      `🔍 Enhanced company search: ${
        normalizedSearchTerm ? `"${normalizedSearchTerm}"` : "ClickHouse-only"
      } (limit: ${limit},  userId: ${userId || "none"})`
    );

    let qdrantResults = [];
    let qdrantCompanyIds = [];
    let embeddingTime = 0;
    let qdrantTime = 0;
    let companyIds = [];
    let externalCompanyIds = [];
    let externalTime = 0;
    let parallelExecutionTime = 0;
    let clickhouseData = [];
    let clickhouseTime = 0;
    let swipedCompaniesData = [];
    let swipedCompaniesTime = 0;

    // PATH 1: Keyword provided - optimized parallel execution
    if (normalizedSearchTerm) {
      console.log(
        `📡 Starting optimized parallel search operations at ${new Date().toISOString()}`
      );
      console.log(
        `📡 Performing Qdrant and external semantic search for: "${normalizedSearchTerm}"`
      );

      // Determine if external service should be used based on search term characteristics
      const trimmedSearchTerm = normalizedSearchTerm.trim();
      const wordCount = trimmedSearchTerm.split(/\s+/).length;
      const totalLength = trimmedSearchTerm.length;
      const shouldSkipExternalService = wordCount > 2 && totalLength > 10;
      const shouldUseExternalService = !shouldSkipExternalService;

      console.log(`🔍 Search term analysis:`);
      console.log(`   - Word count: ${wordCount} (max: 2)`);
      console.log(`   - Total length: ${totalLength} (max: 10)`);
      console.log(
        `   - Use external service: ${shouldUseExternalService ? "YES" : "NO"}`
      );
      if (shouldSkipExternalService) {
        console.log(
          `   - ❌ Skipping external service: complex query (${wordCount} words > 2 AND ${totalLength} chars > 10)`
        );
      } else {
        console.log(
          `   - ✅ Using external service: simple query (${wordCount} words, ${totalLength} chars)`
        );
      }

      // Step 1: Start embedding creation and external service call in parallel (if applicable)
      const parallelStartTime = performance.now();

      const embeddingPromise = createEmbedding(trimmedSearchTerm);

      let externalPromise = null;
      if (shouldUseExternalService) {
        console.log(
          `⚡ Starting embedding creation and external service call in parallel`
        );

        // Extract token from request headers
        const authHeader = req.headers["authorization"];
        const token = authHeader && authHeader.split(" ")[1]; // Bearer TOKEN

        externalPromise = fetchExternalCompanyIds({
          searchTerm,
          categoryIds,
          countryId,
          softwareIds,
          limit,
          token,
        });
      } else {
        console.log(
          `⚡ Starting embedding creation only (external service skipped - complex query)`
        );
      }

      // Step 2: Prepare Qdrant filters (can be done while embedding is being created)
      const filters = { must: [] };

      // Only add filters if they are valid and the collection supports them
      // For now, let's start with basic filters and add more as needed
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        filters.must.push({
          key: "category_id",
          match: { any: normalizedCategoryIds },
        });
      }
      if (normalizedCountryId && typeof normalizedCountryId === "number") {
        filters.must.push({
          key: "country_id",
          match: { value: normalizedCountryId },
        });
      }

      // Log the filters being used
      console.log(
        "🔍 Company search filters:",
        JSON.stringify(filters, null, 2)
      );

      // Step 3: Wait for embedding to be ready, then start Qdrant search immediately
      console.log(`🔍 Waiting for embedding to be ready...`);
      const embedding = await embeddingPromise;
      embeddingTime = performance.now() - parallelStartTime;
      console.log(`✅ Embedding created in ${embeddingTime.toFixed(2)}ms`);

      // Step 4: Start Qdrant search immediately (don't wait for external service)
      const qdrantStartTime = performance.now();
      console.log(`🔍 Starting Qdrant search at ${new Date().toISOString()}`);
      const searchBody = {
        vector: embedding,
        limit: parseInt(limit),
        with_payload: true,
        with_vector: false,
        params: { hnsw_ef: 32 },
      };

      if (filters.must.length > 0) {
        searchBody.filter = filters;
      }
      // console.log(
      //   "searchBody filter:",
      //   JSON.stringify(searchBody.filter, null, 2)
      // );
      try {
        const searchResponse = await axios.post(
          `${qdrantUrl}/collections/${COLLECTION_NAME_COMPANIES}/points/search`,
          searchBody,
          { headers: buildHeaders(), timeout: 30000 }
        );
        qdrantResults = searchResponse.data.result;
      } catch (error) {
        console.error(
          "❌ Qdrant search error:",
          error.response?.data || error.message
        );
        console.error("❌ Search body:", JSON.stringify(searchBody, null, 2));
        console.error("❌ Collection name:", COLLECTION_NAME_COMPANIES);
        console.error("❌ Qdrant URL:", qdrantUrl);

        // Check if collection exists
        try {
          const collectionInfo = await axios.get(
            `${qdrantUrl}/collections/${COLLECTION_NAME_COMPANIES}`,
            { headers: buildHeaders(), timeout: 10000 }
          );
          console.log("✅ Collection exists:", collectionInfo.data);
          // If collection exists but search failed, throw the error
          throw error;
        } catch (collectionError) {
          console.error("❌ Collection does not exist or is not accessible");
          console.error(
            "❌ Collection error:",
            collectionError.response?.data || collectionError.message
          );
          console.log(
            "🔄 Falling back to ClickHouse-only search for companies"
          );

          // Set empty results and continue with ClickHouse-only search
          qdrantResults = [];
          qdrantCompanyIds = [];
          qdrantTime = 0;
        }
      }
      qdrantTime = performance.now() - qdrantStartTime;
      console.log(
        `✅ Qdrant search completed in ${qdrantTime.toFixed(2)}ms, returned ${
          qdrantResults.length
        } results`
      );

      // Step 5: Process Qdrant results
      // Filter by similarity threshold
      const originalResultsCount = qdrantResults.length;
      qdrantResults = qdrantResults.filter(
        (result) => result.score >= similarityThreshold
      );
      if (originalResultsCount > qdrantResults.length) {
        console.log(
          `🔍 Filtered ${
            originalResultsCount - qdrantResults.length
          } results below similarity threshold ${similarityThreshold}`
        );
      }
      // Extract company IDs from Qdrant results
      qdrantCompanyIds = qdrantResults
        .sort((a, b) => b.score - a.score)
        .map((result) => result.payload?.company_id || result.id);

      // Step 6: Wait for external service to complete (if applicable)
      if (externalPromise) {
        console.log(`🌐 Waiting for external service to complete...`);
        const extIds = await externalPromise;
        externalTime = performance.now() - parallelStartTime;
        externalCompanyIds = extIds || [];
        console.log(
          `✅ External service completed in ${externalTime.toFixed(
            2
          )}ms, returned ${externalCompanyIds.length} company IDs`
        );
      } else {
        console.log(`🌐 External service skipped (complex query)`);
        externalTime = 0;
        externalCompanyIds = [];
      }

      parallelExecutionTime = Math.max(embeddingTime, externalTime);
      console.log(
        `📊 Parallel execution completed in ${parallelExecutionTime.toFixed(
          2
        )}ms (embedding: ${embeddingTime.toFixed(
          2
        )}ms, external: ${externalTime.toFixed(2)}ms)`
      );

      // Step 7: Merge Qdrant and external company IDs - use ALL results for ClickHouse processing
      console.log(`🔄 Merging ALL results for ClickHouse processing`);
      console.log(`   - Qdrant results: ${qdrantCompanyIds.length}`);
      console.log(`   - External results: ${externalCompanyIds.length}`);

      // Start with all Qdrant results (they have priority)
      const mergedCompanyIds = [...qdrantCompanyIds];
      const seen = new Set(qdrantCompanyIds);

      // Add ALL external results (no limit here - we'll apply limit after ClickHouse processing)
      let addedFromExternal = 0;
      for (const companyId of externalCompanyIds) {
        if (!seen.has(companyId)) {
          mergedCompanyIds.push(companyId);
          seen.add(companyId);
          addedFromExternal++;
        }
      }

      companyIds = mergedCompanyIds;
      console.log(
        `✅ Final merged result: ${companyIds.length} companies (Qdrant: ${
          qdrantCompanyIds.length
        }, External used: ${
          companyIds.length - qdrantCompanyIds.length
        }) - will be processed in chunks and limited to ${limit} at the end`
      );

      // Step 8: Fetch ClickHouse data for the merged company IDs using chunked processing
      if (companyIds.length > 0) {
        const clickhouseStartTime = performance.now();
        console.log(
          `📊 Starting chunked ClickHouse queries for ${
            companyIds.length
          } company IDs at ${new Date().toISOString()}`
        );

        // Helper function to split array into chunks
        const chunkArray = (array, chunkSize) => {
          const chunks = [];
          for (let i = 0; i < array.length; i += chunkSize) {
            chunks.push(array.slice(i, i + chunkSize));
          }
          return chunks;
        };

        // Split company IDs into chunks of 5000 (optimized for ClickHouse query limits)
        const CHUNK_SIZE = 5000;
        const companyIdChunks = chunkArray(companyIds, CHUNK_SIZE);
        console.log(
          `🔍 Split ${companyIds.length} company IDs into ${companyIdChunks.length} chunks of ${CHUNK_SIZE}`
        );

        // Function to execute ClickHouse query for a single chunk
        const executeClickHouseChunk = async (chunk, chunkIndex) => {
          const companyIdList = chunk
            .map((id) => `'${String(id).replace(/'/g, "''")}'`)
            .join(", ");

          let query;
          if (effectiveSortProp === "similarity_score") {
            // When sorting by similarity_score, preserve Qdrant order by not using ORDER BY
            query = `
              SELECT 
                cb.company_id,
                COALESCE(cs.views_7, 0) as views_7,
                COALESCE(cs.views_14, 0) as views_14,
                COALESCE(cs.views_21, 0) as views_21,
                COALESCE(cs.views_30, 0) as views_30,
                COALESCE(cs.views_90, 0) as views_90,
                COALESCE(cs.views_365, 0) as views_365,
                COALESCE(cs.views_720, 0) as views_720,
                COALESCE(cs.total_views, 0) as total_views,
                COALESCE(cs.spend_7, 0) as spend_7,
                COALESCE(cs.spend_14, 0) as spend_14,
                COALESCE(cs.spend_21, 0) as spend_21,
                COALESCE(cs.spend_30, 0) as spend_30,
                COALESCE(cs.spend_90, 0) as spend_90,
                COALESCE(cs.spend_365, 0) as spend_365,
                COALESCE(cs.spend_720, 0) as spend_720,
                COALESCE(cs.total_spend, 0) as total_spend,
                cs.latest_date as summary_date,
                cb.legal_name,
                cb.country_id,
                cb.category_id,
                cb.is_affiliate,
                cb.updated_at
              FROM analytics.company_basic cb
              LEFT JOIN (
                  SELECT
                    company_id,
                    argMax(views_7, date) AS views_7,
                    argMax(views_14, date) AS views_14,
                    argMax(views_21, date) AS views_21,
                    argMax(views_30, date) AS views_30,
                    argMax(views_90, date) AS views_90,
                    argMax(views_365, date) AS views_365,
                    argMax(views_720, date) AS views_720,
                    argMax(total_views, date) AS total_views,
                    argMax(spend_7, date) AS spend_7,
                    argMax(spend_14, date) AS spend_14,
                    argMax(spend_21, date) AS spend_21,
                    argMax(spend_30, date) AS spend_30,
                    argMax(spend_90, date) AS spend_90,
                    argMax(spend_365, date) AS spend_365,
                    argMax(spend_720, date) AS spend_720,
                    argMax(total_spend, date) AS total_spend,
                    argMax(date, date) AS latest_date
                FROM analytics.company_summary
                WHERE company_id IN (${companyIdList})
                GROUP BY company_id
              ) cs ON cb.company_id = cs.company_id
              WHERE cb.company_id IN (${companyIdList})
              AND ${
                normalizedCategoryIds &&
                Array.isArray(normalizedCategoryIds) &&
                normalizedCategoryIds.length > 0
                  ? `cb.category_id IN (${normalizedCategoryIds.join(", ")})`
                  : `cb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
              }
            `;
          } else {
            // For other sort fields, use ClickHouse ordering
            const whereClause = `WHERE cb.company_id IN (${companyIdList}) AND ${
              normalizedCategoryIds &&
              Array.isArray(normalizedCategoryIds) &&
              normalizedCategoryIds.length > 0
                ? `cb.category_id IN (${normalizedCategoryIds.join(", ")})`
                : `cb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
            }`;
            query = `
              SELECT 
                cb.company_id,
                COALESCE(cs.views_7, 0) as views_7,
                COALESCE(cs.views_14, 0) as views_14,
                COALESCE(cs.views_21, 0) as views_21,
                COALESCE(cs.views_30, 0) as views_30,
                COALESCE(cs.views_90, 0) as views_90,
                COALESCE(cs.views_365, 0) as views_365,
                COALESCE(cs.views_720, 0) as views_720,
                COALESCE(cs.total_views, 0) as total_views,
                COALESCE(cs.spend_7, 0) as spend_7,
                COALESCE(cs.spend_14, 0) as spend_14,
                COALESCE(cs.spend_21, 0) as spend_21,
                COALESCE(cs.spend_30, 0) as spend_30,
                COALESCE(cs.spend_90, 0) as spend_90,
                COALESCE(cs.spend_365, 0) as spend_365,
                COALESCE(cs.spend_720, 0) as spend_720,
                COALESCE(cs.total_spend, 0) as total_spend,
                cs.latest_date as summary_date,
                cb.legal_name,
                cb.country_id,
                cb.category_id,
                cb.is_affiliate,
                cb.updated_at
              FROM analytics.company_basic cb
              LEFT JOIN (
                SELECT
                    company_id,
                    argMax(views_7, date) AS views_7,
                    argMax(views_14, date) AS views_14,
                    argMax(views_21, date) AS views_21,
                    argMax(views_30, date) AS views_30,
                    argMax(views_90, date) AS views_90,
                    argMax(views_365, date) AS views_365,
                    argMax(views_720, date) AS views_720,
                    argMax(total_views, date) AS total_views,
                    argMax(spend_7, date) AS spend_7,
                    argMax(spend_14, date) AS spend_14,
                    argMax(spend_21, date) AS spend_21,
                    argMax(spend_30, date) AS spend_30,
                    argMax(spend_90, date) AS spend_90,
                    argMax(spend_365, date) AS spend_365,
                    argMax(spend_720, date) AS spend_720,
                    argMax(total_spend, date) AS total_spend,
                    argMax(date, date) AS latest_date
                FROM analytics.company_summary
                WHERE company_id IN (${companyIdList})
                GROUP BY company_id
              ) cs ON cb.company_id = cs.company_id
              ${whereClause}
              ORDER BY ${order_by} ${order_direction.toUpperCase()}
            `;
          }

          // Only log every 5th chunk to reduce console spam
          if ((chunkIndex + 1) % 5 === 0 || chunkIndex === 0) {
            console.log(
              `🔍 Executing ClickHouse chunk ${chunkIndex + 1}/${
                companyIdChunks.length
              } with ${chunk.length} company IDs`
            );
          }

          try {
            const resultSet = await clickhouse.query({
              query: query,
              format: "JSONEachRow",
            });

            const chunkData = await resultSet.json();
            console.log(
              `✅ ClickHouse chunk ${chunkIndex + 1} returned ${
                chunkData.length
              } records`
            );
            return chunkData;
          } catch (error) {
            console.error(
              `❌ ClickHouse chunk ${chunkIndex + 1} failed:`,
              error.message
            );
            return [];
          }
        };

        // Execute chunks with controlled concurrency (max 10 concurrent)
        console.log(
          `🔄 Executing ${companyIdChunks.length} ClickHouse chunks with controlled concurrency`
        );

        const CONCURRENT_CHUNKS = 10;
        const chunkResults = [];

        for (let i = 0; i < companyIdChunks.length; i += CONCURRENT_CHUNKS) {
          const batch = companyIdChunks.slice(i, i + CONCURRENT_CHUNKS);
          const batchPromises = batch.map((chunk, batchIndex) =>
            executeClickHouseChunk(chunk, i + batchIndex)
          );

          const batchResults = await Promise.all(batchPromises);
          chunkResults.push(...batchResults);

          console.log(
            `✅ Completed batch ${
              Math.floor(i / CONCURRENT_CHUNKS) + 1
            }/${Math.ceil(companyIdChunks.length / CONCURRENT_CHUNKS)}`
          );
        }

        // Merge all chunk results
        clickhouseData = chunkResults.flat();
        console.log(
          `🔍 Merged ${chunkResults.length} chunks into ${clickhouseData.length} total records`
        );

        // Execute swiped companies query in parallel if userId is provided
        if (userId) {
          console.log(
            `🔄 Executing swiped companies query in parallel with ClickHouse chunks`
          );
          swipedCompaniesData = await fetchSwipedCompanies(companyIds, userId);
        }

        // Note: Deduplication is handled at SQL level using argMax aggregate function

        clickhouseTime = performance.now() - clickhouseStartTime;

        console.log(
          `✅ ClickHouse query completed in ${clickhouseTime.toFixed(
            2
          )}ms, returned ${clickhouseData.length} records`
        );

        if (userId) {
          console.log(
            `✅ Swiped companies query completed, found ${swipedCompaniesData.length} records`
          );
        }
      }
    }

    // Handle ClickHouse-only search (no keyword provided)
    if (!normalizedSearchTerm) {
      // Case 2: No keyword - direct ClickHouse query with filters
      console.log(
        `📊 Starting direct ClickHouse company search with filters at ${new Date().toISOString()}`
      );

      const clickhouseStartTime = performance.now();

      // Build WHERE clause for filters
      // Build WHERE clause for filters
      const whereConditions = [];

      // Category ID filter
      if (
        normalizedCategoryIds &&
        Array.isArray(normalizedCategoryIds) &&
        normalizedCategoryIds.length > 0
      ) {
        // If categoryIds are provided, filter to include only those categories
        const categoryList = normalizedCategoryIds.join(", ");
        whereConditions.push(`cb.category_id IN (${categoryList})`);
      } else {
        // If no categoryIds provided, exclude the standard excluded categories
        whereConditions.push(
          `cb.category_id NOT IN (${EXCLUDED_CATEGORIES.join(", ")})`
        );
      }

      // Country ID filter
      if (normalizedCountryId && typeof normalizedCountryId === "number") {
        whereConditions.push(`cb.country_id = ${normalizedCountryId}`);
      }

      // Combine all conditions
      const whereClause =
        whereConditions.length > 0
          ? `WHERE ${whereConditions.join(" AND ")}`
          : "";

      // Optimize query: pre-filter top 500 companies before expensive deduplication
      //const preFilterLimit = 500; // Take top 500 companies, then deduplicate and limit

      const query = `
        SELECT
          cb.company_id,
          cb.legal_name,
          cb.country_id,
          cb.category_id,
          cb.is_affiliate,
          cb.updated_at,
          COALESCE(cs.views_7, 0) AS views_7,
          COALESCE(cs.views_14, 0) AS views_14,
          COALESCE(cs.views_21, 0) AS views_21,
          COALESCE(cs.views_30, 0) AS views_30,
          COALESCE(cs.views_90, 0) AS views_90,
          COALESCE(cs.views_365, 0) AS views_365,
          COALESCE(cs.views_720, 0) AS views_720,
          COALESCE(cs.total_views, 0) AS total_views,
          COALESCE(cs.spend_7, 0) AS spend_7,
          COALESCE(cs.spend_14, 0) AS spend_14,
          COALESCE(cs.spend_21, 0) AS spend_21,
          COALESCE(cs.spend_30, 0) AS spend_30,
          COALESCE(cs.spend_90, 0) AS spend_90,
          COALESCE(cs.spend_365, 0) AS spend_365,
          COALESCE(cs.spend_720, 0) AS spend_720,
          COALESCE(cs.total_spend, 0) AS total_spend,
          cs.latest_date AS summary_date
      FROM analytics.company_basic AS cb
      LEFT JOIN (
          SELECT
              company_id,
              argMax(views_7, date) AS views_7,
              argMax(views_14, date) AS views_14,
              argMax(views_21, date) AS views_21,
              argMax(views_30, date) AS views_30,
              argMax(views_90, date) AS views_90,
              argMax(views_365, date) AS views_365,
              argMax(views_720, date) AS views_720,
              argMax(total_views, date) AS total_views,
              argMax(spend_7, date) AS spend_7,
              argMax(spend_14, date) AS spend_14,
              argMax(spend_21, date) AS spend_21,
              argMax(spend_30, date) AS spend_30,
              argMax(spend_90, date) AS spend_90,
              argMax(spend_365, date) AS spend_365,
              argMax(spend_720, date) AS spend_720,
              argMax(total_spend, date) AS total_spend,
              argMax(date, date) AS latest_date
          FROM analytics.company_summary
          GROUP BY company_id
      ) AS cs
          ON cb.company_id = cs.company_id
      ${whereClause}
      ORDER BY COALESCE(cs.${order_by}, 0) ${order_direction.toUpperCase()}, cb.company_id
      LIMIT ${parseInt(limit)};

      `;

      console.log(`📊 Executing direct ClickHouse company query with filters`);

      try {
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
        });

        clickhouseData = await resultSet.json();
        clickhouseTime = performance.now() - clickhouseStartTime;

        console.log(
          `✅ ClickHouse returned ${
            clickhouseData.length
          } records in ${clickhouseTime.toFixed(2)}ms`
        );

        // If userId is provided, fetch swiped companies data for the returned company IDs
        if (userId && clickhouseData.length > 0) {
          const returnedCompanyIds = clickhouseData.map(
            (item) => item.company_id
          );

          swipedCompaniesData = await fetchSwipedCompanies(
            returnedCompanyIds,
            userId
          );
          swipedCompaniesTime = performance.now() - clickhouseStartTime;
          console.log(
            `✅ Swiped companies query completed in ${swipedCompaniesTime.toFixed(
              2
            )}ms, found ${swipedCompaniesData.length} records`
          );
        }
      } catch (clickhouseError) {
        console.error(
          "❌ ClickHouse company query failed:",
          clickhouseError.message
        );
        clickhouseData = [];
      }
    }

    const totalTime = performance.now() - startTime;

    // Step 3: Format results based on search type
    const enhancedResults = [];
    let filteredOutCount = 0;

    // Create a map of swiped companies for quick lookup
    const swipedCompaniesMap = new Map();
    swipedCompaniesData.forEach((item) => {
      swipedCompaniesMap.set(String(item.company_id), item);
    });

    if (normalizedSearchTerm) {
      // Case 1: Keyword search - combine Qdrant similarity scores with ClickHouse data
      const clickhouseMap = new Map();
      clickhouseData.forEach((item) => {
        clickhouseMap.set(item.company_id, item);
      });

      // Process ALL company IDs (both Qdrant and external results)
      // Create a map of Qdrant results for quick lookup
      const qdrantMap = new Map();
      qdrantResults.forEach((qdrantResult) => {
        const companyId = qdrantResult.payload?.company_id || qdrantResult.id;
        qdrantMap.set(companyId, qdrantResult);
      });

      // Process all company IDs from the merged list
      companyIds.forEach((companyId) => {
        const clickhouseInfo = clickhouseMap.get(companyId);
        const swipedInfo = swipedCompaniesMap.get(String(companyId));
        const qdrantResult = qdrantMap.get(companyId);

        // Only include companies that have ClickHouse data
        if (clickhouseInfo) {
          const enhancedResult = {
            companyId: companyId,
            similarity_score: qdrantResult ? qdrantResult.score : null,
            // Qdrant payload data (if available)
            qdrant_data: qdrantResult ? qdrantResult.payload : null,
            // ClickHouse summary data
            summary_data: clickhouseInfo,
            // Swiped companies data (if available)
            swiped_data: swipedInfo || null,
          };

          enhancedResults.push(enhancedResult);
        } else {
          filteredOutCount++;
        }
      });
    } else {
      // Case 2: ClickHouse-only search - format ClickHouse data directly
      clickhouseData.forEach((clickhouseResult) => {
        const swipedInfo = swipedCompaniesMap.get(
          String(clickhouseResult.company_id)
        );

        const enhancedResult = {
          companyId: clickhouseResult.company_id,
          similarity_score: null, // No similarity score for direct ClickHouse search
          // No Qdrant data available
          qdrant_data: null,
          // ClickHouse summary data
          summary_data: clickhouseResult,
          // Swiped companies data (if available)
          swiped_data: swipedInfo || null,
        };

        enhancedResults.push(enhancedResult);
      });
    }

    // Sort results based on the effective sort property
    if (normalizedSearchTerm) {
      // When we have searchTerm, we need to sort the combined results
      if (effectiveSortProp === "similarity_score") {
        // Only sort by similarity when no sortProp was provided or explicitly requested
        console.log(
          `🔄 Sorting by similarity_score (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          const scoreA = a.similarity_score || 0;
          const scoreB = b.similarity_score || 0;
          return orderAsc ? scoreA - scoreB : scoreB - scoreA;
        });
      } else {
        // Sort by the specified field (prioritize user choice)
        console.log(
          `🔄 Sorting by ${effectiveSortProp} (${orderAsc ? "asc" : "desc"})`
        );
        enhancedResults.sort((a, b) => {
          let valueA, valueB;

          switch (dbSortField) {
            case "total_spend":
              valueA = a.summary_data?.total_spend ?? 0;
              valueB = b.summary_data?.total_spend ?? 0;
              break;
            default:
              valueA = a.summary_data?.[dbSortField] ?? 0;
              valueB = b.summary_data?.[dbSortField] ?? 0;
          }

          return orderAsc ? valueA - valueB : valueB - valueA;
        });
      }
    } else {
      console.log(
        `🔄 ClickHouse-only search, sorted by ${order_by} (${order_direction})`
      );
    }

    // Log filtering results
    if (filteredOutCount > 0) {
      console.log(
        `🔍 Filtered out ${filteredOutCount} companies without ClickHouse data`
      );
    }

    // Apply final limit after sorting
    const finalResults = enhancedResults.slice(0, parseInt(limit));
    console.log(
      `🔍 Applied final limit: ${enhancedResults.length} → ${finalResults.length} results`
    );

    // Format response to match frontend expectations (based on original fullSearch structure)
    console.log(
      `🔍 Final response formatting: ${finalResults.length} enhanced results`
    );

    const data = {
      results: finalResults.map((r, index) => {
        const isSwiped = r.swiped_data ? true : false;

        const result = {
          companyId: r.companyId,
          isSwiped: isSwiped, // Set based on swiped_companies table
          legalName:
            r.summary_data?.legal_name || r.qdrant_data?.legal_name || null,
          countryId:
            r.summary_data?.country_id || r.qdrant_data?.country_id || null,
          categoryId:
            r.summary_data?.category_id || r.qdrant_data?.category_id || null,
          isAffiliate: r.summary_data?.is_affiliate || false,
          totalSpend: r.summary_data?.total_spend || null,
          // Additional fields for debugging/backward compatibility
          similarity_score: r.similarity_score,
          qdrant_data: r.qdrant_data,
          summary_data: r.summary_data,
          swiped_data: r.swiped_data, // Include swiped data for debugging
        };

        // Log first 3 results to see what's being formatted
        if (index < 3) {
          console.log(`🔍 Final result ${index + 1}:`, {
            companyId: result.companyId,
            legalName: result.legalName,
            totalSpend: result.totalSpend,
            has_summary_data: !!result.summary_data,
            has_qdrant_data: !!result.qdrant_data,
          });
        }

        return result;
      }),
    };

    const response = {
      success: true,
      search_type: normalizedSearchTerm
        ? "semantic_plus_clickhouse"
        : "clickhouse_only",
      keyword: normalizedSearchTerm || null,
      total_results: finalResults.length,
      total_available: enhancedResults.length,
      qdrant_matches: qdrantResults.length,
      external_matches: externalCompanyIds.length,
      external_matches_used: companyIds.length - qdrantCompanyIds.length,
      external_service_used: normalizedSearchTerm
        ? externalCompanyIds.length > 0 || externalTime > 0
        : false,
      clickhouse_matches: clickhouseData.length,
      swiped_matches: swipedCompaniesData.length,
      parameters: {
        search_filters: {
          category_id: categoryIds,
          country_id: countryId,
          software_id: softwareIds,
        },
        similarityThreshold: normalizedSearchTerm ? similarityThreshold : null,
        clickhouse_ordering: {
          order_by: order_by,
          order_direction: order_direction,
        },
        limit: parseInt(limit),
        userId: userId,
      },
      timing: {
        total_ms: Math.round(totalTime),
        parallel_execution_ms: Math.round(parallelExecutionTime),
        embedding_ms: Math.round(embeddingTime),
        qdrant_search_ms: Math.round(qdrantTime),
        clickhouse_query_ms: Math.round(clickhouseTime),
        swiped_companies_query_ms: Math.round(swipedCompaniesTime),
        external_service_ms: Math.round(externalTime),
      },
      data: data,
    };

    const searchTypeDesc = normalizedSearchTerm
      ? `semantic + ClickHouse`
      : `ClickHouse-only`;
    console.log(
      `✅ Enhanced company search (${searchTypeDesc}) completed in ${totalTime.toFixed(
        2
      )}ms - ${enhancedResults.length} results (Qdrant: ${
        qdrantResults.length
      }, External available: ${externalCompanyIds.length}, External used: ${
        companyIds.length - qdrantCompanyIds.length
      }, ClickHouse: ${clickhouseData.length}, Swiped: ${
        swipedCompaniesData.length
      })`
    );
    console.log(`📊 Final timing breakdown:`);
    console.log(`   - Total time: ${totalTime.toFixed(2)}ms`);
    console.log(
      `   - Parallel execution: ${parallelExecutionTime.toFixed(2)}ms`
    );
    console.log(`   - Embedding: ${embeddingTime.toFixed(2)}ms`);
    console.log(`   - External service: ${externalTime.toFixed(2)}ms`);
    console.log(`   - Qdrant search: ${qdrantTime.toFixed(2)}ms`);
    console.log(`   - ClickHouse query: ${clickhouseTime.toFixed(2)}ms`);
    console.log(`   - Swiped companies: ${swipedCompaniesTime.toFixed(2)}ms`);
    console.log(`🏁 Response sent at ${new Date().toISOString()}`);

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Enhanced company search error (after ${totalTime.toFixed(2)}ms):`,
      error.message
    );
    console.error(`🚨 Error occurred at ${new Date().toISOString()}`);

    // Handle specific error types
    if (error.code === "ECONNREFUSED") {
      return res.status(503).json({
        error: "Unable to connect to Qdrant service",
        code: "QDRANT_UNAVAILABLE",
      });
    }

    if (error.response?.status === 404) {
      return res.status(404).json({
        error: `Collection '${COLLECTION_NAME_COMPANIES}' not found`,
        code: "COLLECTION_NOT_FOUND",
      });
    }

    if (error.message.includes("Failed to create embedding")) {
      return res.status(503).json({
        error: "Failed to create embedding",
        code: "EMBEDDING_ERROR",
      });
    }

    res.status(500).json({
      error: "Internal server error",
      code: "INTERNAL_ERROR",
      message:
        process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});

// ========================================
// AUTHENTICATION MIDDLEWARE
// ========================================

// Simple auth middleware to extract user_id from token
// Token format expected: "Bearer <user_id>" or just "<user_id>"
// For production, use proper JWT verification with Firebase Admin SDK
async function authenticateUser(req, res, next) {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];
  req.token = token;

  if (!token) return res.sendStatus(401);

  // DEBUG: Decode token without verification to see what's inside
  try {
    const tokenParts = token.split(".");
    if (tokenParts.length === 3) {
      const payload = JSON.parse(
        Buffer.from(tokenParts[1], "base64").toString()
      );
      console.log("🔍 TOKEN DEBUG:");
      console.log("  - Token aud (audience/project):", payload.aud);
      console.log("  - Token iss (issuer):", payload.iss);
      console.log(
        "  - Token exp (expires):",
        new Date(payload.exp * 1000).toISOString()
      );
      console.log("  - Token uid:", payload.uid || payload.user_id);
      console.log("  - Expected project: swipetube-3c5c6");
      console.log(
        "  - Token from correct project?",
        payload.aud === "swipetube-3c5c6"
      );
    }
  } catch (e) {
    console.log("❌ Could not decode token:", e.message);
  }

  firebaseAuth
    .verifyIdToken(token)
    .then(async (decodedToken) => {
      req.user = decodedToken;
      req.user_id = decodedToken.uid;
      req.firebase_id = decodedToken.uid;

      next();
    })
    .catch((error) => {
      console.log("❌ Token verification failed:", error.message);
      res.sendStatus(403);
    });
}

// ========================================
// SWIPE BOARDS ENDPOINTS
// ========================================

// Insert swipe board(s) - supports single or array of swipe boards
app.post("/swipe-boards", authenticateUser, async (req, res) => {
  try {
    const user_id = req.user.uid; // Get from token
    const payload = req.body;

    // Validate input
    if (!payload || (Array.isArray(payload) && payload.length === 0)) {
      return res.status(400).json({
        error: "Request body is required",
        code: "INVALID_INPUT",
      });
    }

    // Normalize to array
    const boards = Array.isArray(payload) ? payload : [payload];

    // Validate each board
    for (const board of boards) {
      if (!board.swipe_board_id || !board.name) {
        return res.status(400).json({
          error: "Each swipe board must have swipe_board_id and name",
          code: "INVALID_INPUT",
        });
      }
    }

    // Prepare values for bulk insert - use user_id from token
    const values = boards.map((board) => ({
      swipe_board_id: board.swipe_board_id,
      user_id: user_id,
      name: board.name,
      created:
        board.created ||
        new Date()
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .split(".")[0],
      share_id: board.share_id || "",
    }));

    // Insert into ClickHouse
    await clickhouse.insert({
      table: "analytics.swipe_boards",
      values: values,
      format: "JSONEachRow",
    });

    console.log(
      `✅ Inserted ${boards.length} swipe board(s) for user ${user_id}`
    );

    res.json({
      success: true,
      message: `Successfully inserted ${boards.length} swipe board(s)`,
      count: boards.length,
    });
  } catch (error) {
    console.error("❌ Error inserting swipe boards:", error);
    res.status(500).json({
      error: "Failed to insert swipe boards",
      details: error.message,
      code: "INSERT_ERROR",
    });
  }
});

// Update swipe board - ReplacingMergeTree will replace based on swipe_board_id
app.put("/swipe-boards/:swipe_board_id", authenticateUser, async (req, res) => {
  try {
    const { swipe_board_id } = req.params;
    const { name, share_id } = req.body;
    const user_id = req.user.uid; // Get from token

    // Validate input
    if (!name) {
      return res.status(400).json({
        error: "name is required",
        code: "INVALID_INPUT",
      });
    }

    // Insert new row (ReplacingMergeTree will eventually replace the old one)
    await clickhouse.insert({
      table: "analytics.swipe_boards",
      values: [
        {
          swipe_board_id: parseInt(swipe_board_id),
          user_id: user_id,
          name: name,
          created: new Date()
            .toISOString()
            .replace("T", " ")
            .replace("Z", "")
            .split(".")[0],
          share_id: share_id || "",
        },
      ],
      format: "JSONEachRow",
    });

    // Force merge to deduplicate immediately
    await clickhouse.query({
      query: "OPTIMIZE TABLE analytics.swipe_boards FINAL",
    });

    console.log(`✅ Updated swipe board ${swipe_board_id} for user ${user_id}`);

    res.json({
      success: true,
      message: `Successfully updated swipe board ${swipe_board_id}`,
    });
  } catch (error) {
    console.error("❌ Error updating swipe board:", error);
    res.status(500).json({
      error: "Failed to update swipe board",
      details: error.message,
      code: "UPDATE_ERROR",
    });
  }
});

// Delete swipe board
app.delete("/swipe-boards", authenticateUser, async (req, res) => {
  try {
    const { swipe_board_id } = req.body;
    const user_id = req.user.uid; // Get from token

    if (!swipe_board_id) {
      return res.status(400).json({
        error: "swipe_board_id is required",
        code: "INVALID_INPUT",
      });
    }

    await clickhouse.query({
      query: `ALTER TABLE analytics.swipe_boards DELETE WHERE swipe_board_id = {swipe_board_id:Int64} AND user_id = {user_id:String}`,
      query_params: {
        swipe_board_id: parseInt(swipe_board_id),
        user_id: user_id,
      },
    });

    // Force deletion to take effect immediately
    await clickhouse.query({
      query: "OPTIMIZE TABLE analytics.swipe_boards FINAL",
    });

    console.log(`✅ Deleted swipe board ${swipe_board_id} for user ${user_id}`);

    res.json({
      success: true,
      message: `Successfully deleted swipe board ${swipe_board_id}`,
    });
  } catch (error) {
    console.error("❌ Error deleting swipe board:", error);
    res.status(500).json({
      error: "Failed to delete swipe board",
      details: error.message,
      code: "DELETE_ERROR",
    });
  }
});

// ========================================
// SWIPED VIDEOS ENDPOINTS
// ========================================

// Insert swiped video(s) - supports single or array, with array of swipe_board_ids
app.post("/swiped-videos", authenticateUser, async (req, res) => {
  try {
    const firebase_id = req.user.uid; // Get from token
    const payload = req.body;

    if (!payload || (Array.isArray(payload) && payload.length === 0)) {
      return res.status(400).json({
        error: "Request body is required",
        code: "INVALID_INPUT",
      });
    }

    // Normalize to array
    const videos = Array.isArray(payload) ? payload : [payload];

    // Validate each video
    for (const video of videos) {
      if (!video.yt_video_id) {
        return res.status(400).json({
          error: "Each swiped video must have yt_video_id",
          code: "INVALID_INPUT",
        });
      }
    }

    // Prepare values - handle swipe_board_ids as array, use firebase_id from token
    const values = videos.map((video) => ({
      id: video.id || Date.now() + Math.floor(Math.random() * 1000000),
      firebase_id: firebase_id,
      yt_video_id: video.yt_video_id,
      created_at:
        video.created_at ||
        new Date()
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .split(".")[0],
      swipe_board_ids: Array.isArray(video.swipe_board_ids)
        ? video.swipe_board_ids
        : video.swipe_board_ids
        ? [video.swipe_board_ids]
        : [],
    }));

    // Insert into ClickHouse (ReplacingMergeTree will handle duplicates)
    await clickhouse.insert({
      table: "analytics.swiped_videos",
      values: values,
      format: "JSONEachRow",
    });

    console.log(
      `✅ Inserted ${videos.length} swiped video(s) for user ${firebase_id}`
    );

    res.json({
      success: true,
      message: `Successfully inserted ${videos.length} swiped video(s)`,
      count: videos.length,
    });
  } catch (error) {
    console.error("❌ Error inserting swiped videos:", error);
    res.status(500).json({
      error: "Failed to insert swiped videos",
      details: error.message,
      code: "INSERT_ERROR",
    });
  }
});

// Delete swiped video
app.delete("/swiped-videos", authenticateUser, async (req, res) => {
  try {
    const { yt_video_id } = req.body;
    const firebase_id = req.user.uid; // Get from token

    if (!yt_video_id) {
      return res.status(400).json({
        error: "yt_video_id is required",
        code: "INVALID_INPUT",
      });
    }

    await clickhouse.query({
      query: `
        ALTER TABLE analytics.swiped_videos DELETE 
        WHERE firebase_id = {firebase_id:String} 
        AND yt_video_id = {yt_video_id:String}
      `,
      query_params: {
        firebase_id: firebase_id,
        yt_video_id: yt_video_id,
      },
    });

    // Force deletion to take effect immediately
    await clickhouse.query({
      query: "OPTIMIZE TABLE analytics.swiped_videos FINAL",
    });

    console.log(
      `✅ Deleted swiped video for ${firebase_id}, video: ${yt_video_id}`
    );

    res.json({
      success: true,
      message: "Successfully deleted swiped video",
    });
  } catch (error) {
    console.error("❌ Error deleting swiped video:", error);
    res.status(500).json({
      error: "Failed to delete swiped video",
      details: error.message,
      code: "DELETE_ERROR",
    });
  }
});

// ========================================
// SWIPED COMPANIES ENDPOINTS
// ========================================

// Insert swiped company(ies) - supports single or array, with array of swipe_board_ids
app.post("/swiped-companies", authenticateUser, async (req, res) => {
  try {
    const firebase_id = req.user.uid; // Get from token
    const payload = req.body;

    if (!payload || (Array.isArray(payload) && payload.length === 0)) {
      return res.status(400).json({
        error: "Request body is required",
        code: "INVALID_INPUT",
      });
    }

    // Normalize to array
    const companies = Array.isArray(payload) ? payload : [payload];

    // Validate each company
    for (const company of companies) {
      if (!company.company_id) {
        return res.status(400).json({
          error: "Each swiped company must have company_id",
          code: "INVALID_INPUT",
        });
      }
    }

    // Prepare values - use firebase_id from token
    const values = companies.map((company) => ({
      id: company.id || Date.now() + Math.floor(Math.random() * 1000000),
      firebase_id: firebase_id,
      company_id: company.company_id,
      created_at:
        company.created_at ||
        new Date()
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .split(".")[0],
      swipe_board_ids: Array.isArray(company.swipe_board_ids)
        ? company.swipe_board_ids
        : company.swipe_board_ids
        ? [company.swipe_board_ids]
        : [],
    }));

    // Insert into ClickHouse
    await clickhouse.insert({
      table: "analytics.swiped_companies",
      values: values,
      format: "JSONEachRow",
    });

    console.log(
      `✅ Inserted ${companies.length} swiped company(ies) for user ${firebase_id}`
    );

    res.json({
      success: true,
      message: `Successfully inserted ${companies.length} swiped company(ies)`,
      count: companies.length,
    });
  } catch (error) {
    console.error("❌ Error inserting swiped companies:", error);
    res.status(500).json({
      error: "Failed to insert swiped companies",
      details: error.message,
      code: "INSERT_ERROR",
    });
  }
});

// Delete swiped company
app.delete("/swiped-companies", authenticateUser, async (req, res) => {
  try {
    const { company_id } = req.body;
    const firebase_id = req.user.uid; // Get from token

    if (!company_id) {
      return res.status(400).json({
        error: "company_id is required",
        code: "INVALID_INPUT",
      });
    }

    await clickhouse.query({
      query: `
        ALTER TABLE analytics.swiped_companies DELETE 
        WHERE firebase_id = {firebase_id:String} 
        AND company_id = {company_id:Int64}
      `,
      query_params: {
        firebase_id: firebase_id,
        company_id: parseInt(company_id),
      },
    });

    // Force deletion to take effect immediately
    await clickhouse.query({
      query: "OPTIMIZE TABLE analytics.swiped_companies FINAL",
    });

    console.log(
      `✅ Deleted swiped company for ${firebase_id}, company: ${company_id}`
    );

    res.json({
      success: true,
      message: "Successfully deleted swiped company",
    });
  } catch (error) {
    console.error("❌ Error deleting swiped company:", error);
    res.status(500).json({
      error: "Failed to delete swiped company",
      details: error.message,
      code: "DELETE_ERROR",
    });
  }
});

// ========================================
// SWIPED BRANDS ENDPOINTS
// ========================================

// Insert swiped brand(s) - supports single or array, with array of swipe_board_ids
app.post("/swiped-brands", authenticateUser, async (req, res) => {
  try {
    console.log("DEBUG: Full req object keys:", Object.keys(req));
    console.log("DEBUG: req.user:", JSON.stringify(req.user, null, 2));
    console.log("DEBUG: req.user.uid:", req.user?.uid);
    console.log(
      "DEBUG: req.headers.authorization:",
      req.headers.authorization?.substring(0, 50) + "..."
    );

    const firebase_id = req.user.uid; // Get from token
    const payload = req.body;

    if (!payload || (Array.isArray(payload) && payload.length === 0)) {
      return res.status(400).json({
        error: "Request body is required",
        code: "INVALID_INPUT",
      });
    }

    // Normalize to array
    const brands = Array.isArray(payload) ? payload : [payload];

    // Validate each brand
    for (const brand of brands) {
      if (!brand.brand_id) {
        return res.status(400).json({
          error: "Each swiped brand must have brand_id",
          code: "INVALID_INPUT",
        });
      }
    }

    // Prepare values - use firebase_id from token
    const values = brands.map((brand) => ({
      id: brand.id || Date.now() + Math.floor(Math.random() * 1000000),
      firebase_id: firebase_id,
      brand_id: brand.brand_id,
      created_at:
        brand.created_at ||
        new Date()
          .toISOString()
          .replace("T", " ")
          .replace("Z", "")
          .split(".")[0],
      swipe_board_ids: Array.isArray(brand.swipe_board_ids)
        ? brand.swipe_board_ids
        : brand.swipe_board_ids
        ? [brand.swipe_board_ids]
        : [],
    }));

    // Insert into ClickHouse
    await clickhouse.insert({
      table: "analytics.swiped_brands",
      values: values,
      format: "JSONEachRow",
    });

    // console.log(
    //   `✅ Inserted ${brands.length} swiped brand(s) for user ${firebase_id}`
    // );

    res.json({
      success: true,
      message: `Successfully inserted ${brands.length} swiped brand(s)`,
      count: brands.length,
    });
  } catch (error) {
    console.error("❌ Error inserting swiped brands:", error);
    res.status(500).json({
      error: "Failed to insert swiped brands",
      details: error.message,
      code: "INSERT_ERROR",
    });
  }
});

// Delete swiped brand
app.delete("/swiped-brands", authenticateUser, async (req, res) => {
  try {
    const { brand_id } = req.body;
    const firebase_id = req.user.uid; // Get from token

    if (!brand_id) {
      return res.status(400).json({
        error: "brand_id is required",
        code: "INVALID_INPUT",
      });
    }

    await clickhouse.query({
      query: `
        ALTER TABLE analytics.swiped_brands DELETE 
        WHERE firebase_id = {firebase_id:String} 
        AND brand_id = {brand_id:Int64}
      `,
      query_params: {
        firebase_id: firebase_id,
        brand_id: parseInt(brand_id),
      },
    });

    // Force deletion to take effect immediately
    await clickhouse.query({
      query: "OPTIMIZE TABLE analytics.swiped_brands FINAL",
    });

    console.log(
      `✅ Deleted swiped brand for ${firebase_id}, brand: ${brand_id}`
    );

    res.json({
      success: true,
      message: "Successfully deleted swiped brand",
    });
  } catch (error) {
    console.error("❌ Error deleting swiped brand:", error);
    res.status(500).json({
      error: "Failed to delete swiped brand",
      details: error.message,
      code: "DELETE_ERROR",
    });
  }
});

// Start server
app.listen(PORT, async () => {
  console.log(`🚀 QDrant Search Service started on port ${PORT}`);
  console.log(`🔗 QDrant URL: ${qdrantUrl}`);
  console.log(
    `🔗 ClickHouse URL: http://${process.env.CLICKHOUSE_HOST}:${
      process.env.CLICKHOUSE_PORT || 8123
    }`
  );
  console.log(`📊 Collection: ${COLLECTION_NAME}`);
  console.log(`📊 Brands Collection: ${COLLECTION_NAME_BRANDS}`);
  console.log(`📊 Companies Collection: ${COLLECTION_NAME_COMPANIES}`);
  console.log(
    `🔑 API Key: ${qdrantApiKey ? "✅ Configured" : "❌ Not configured"}`
  );

  // Test ClickHouse connection
  await testClickHouseConnection();

  // Check Qdrant collections
  await checkQdrantCollections();
  console.log(`\n📚 Available endpoints:`);
  console.log(`  POST /search - Vector similarity search`);
  console.log(
    `  POST /search/videos - Simple video ID search (returns 200 video IDs)`
  );
  console.log(
    `  POST /search/videos/filtered - Filtered video search with category_id, language, is_unlisted, duration`
  );
  console.log(
    `  POST /search/videos/enhanced - Dual-mode: Semantic search (with keyword) OR ClickHouse-only (without keyword)`
  );
  console.log(
    `  POST /search/brands/filtered - Filtered brand search with category_id, country_id, software_id`
  );
  console.log(
    `  POST /search/brands/enhanced - Dual-mode: Semantic search (with keyword) OR ClickHouse-only (without keyword)`
  );
  console.log(
    `  POST /search/companies/filtered - Filtered company search with category_id, country_id, software_id`
  );
  console.log(
    `  POST /search/companies/enhanced - Dual-mode: Semantic search (with keyword) OR ClickHouse-only (without keyword)`
  );
  console.log(
    `  POST /videos/summary - Get YouTube video summary data from ClickHouse with ordering`
  );
  console.log(`  GET /collection/info - Collection information`);
  console.log(`  GET /health - Health check`);
  console.log(`  GET /check-alive - Service alive check`);
  console.log(`  GET /debug/sample-data - Sample data from collection`);
  console.log(`  GET /debug/brands-data - Sample data from brands collection`);
  console.log(
    `  GET /debug/companies-data - Sample data from companies collection`
  );
  console.log(
    `\n📋 Swipe Boards Management (9 endpoints, requires auth token):`
  );
  console.log(`  POST /swipe-boards - Insert swipe board(s)`);
  console.log(`  PUT /swipe-boards/:swipe_board_id - Update swipe board`);
  console.log(`  DELETE /swipe-boards - Delete swipe board`);
  console.log(`  POST /swiped-videos - Insert swiped video(s)`);
  console.log(`  DELETE /swiped-videos - Delete swiped video`);
  console.log(`  POST /swiped-companies - Insert swiped company(ies)`);
  console.log(`  DELETE /swiped-companies - Delete swiped company`);
  console.log(`  POST /swiped-brands - Insert swiped brand(s)`);
  console.log(`  DELETE /swiped-brands - Delete swiped brand`);
  console.log(`VERSION 07082025`);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("🛑 Received SIGTERM, shutting down gracefully");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("🛑 Received SIGINT, shutting down gracefully");
  process.exit(0);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("🛑 Received SIGTERM, shutting down gracefully");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("🛑 Received SIGINT, shutting down gracefully");
  process.exit(0);
});
