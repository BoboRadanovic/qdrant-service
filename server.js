require("dotenv").config();
const express = require("express");
const axios = require("axios");
const { OpenAI } = require("openai");
const { createClient } = require("@clickhouse/client");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const compression = require("compression");

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

// Configuration
const PORT = process.env.PORT || 3001;
const qdrantUrl = process.env.QDRANT_URL;
const qdrantApiKey = process.env.QDRANT_API_KEY;
const COLLECTION_NAME = process.env.COLLECTION_NAME || "ads";
const COLLECTION_NAME_BRANDS = process.env.COLLECTION_NAME_BRANDS || "brands";
const COLLECTION_NAME_COMPANIES =
  process.env.COLLECTION_NAME_COMPANIES || "companies";
const DEFAULT_LIMIT = 200;

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

// Helper to call external /api/videosPublic/search-ids service
async function fetchExternalVideoIds({
  searchTerm,
  categoryIds,
  languageId,
  videoStatus,
  durationMoreThen,
  durationLessThen,
  limit,
}) {
  try {
    const payload = {
      searchTerm: searchTerm || null,
      categoryIds: categoryIds || null,
      language: languageId || null,
      showVideos:
        videoStatus !== undefined && videoStatus !== null ? videoStatus : null,
      durationMoreThen: durationMoreThen || null,
      durationLessThen: durationLessThen || null,
      limit: limit || 200,
    };
    // Remove undefined/null keys
    Object.keys(payload).forEach(
      (key) => payload[key] === null && delete payload[key]
    );
    const response = await axios.post(
      "http://apiv1.vidtao.com/api/videosPublic/search-ids",
      payload,
      { timeout: 20000 }
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

    const response = await axios.post(
      "http://apiv1.vidtao.com/api/videosPublic/search-brands-ids",
      payload,
      { timeout: 20000 }
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

// Initialize Express app
const app = express();

// CORS configuration
const corsOptions = {
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
});
app.use(limiter);

// Health check endpoint
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "qdrant-search-service",
  });
});

// Service alive check endpoint
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
      page = 1,
      categoryIds = null,
      languageId = null,
      videoStatus = "all", // 'all', 'public', 'unlisted'
      durationMoreThen = null,
      durationLessThen = null,
      sortProp = null, // Will default based on context
      orderAsc = false, // true = asc, false = desc
      similarityThreshold = 0.4, // Minimum similarity score (0.0 - 1.0)
      userId = null, // New parameter for user identification
    } = req.body;

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

    // Validate userId if provided
    if (
      userId !== null &&
      (typeof userId !== "string" || userId.trim() === "")
    ) {
      return res.status(400).json({
        error: "userId must be a non-empty string if provided",
        code: "INVALID_USER_ID",
      });
    }

    // Validate sortProp field (if provided)
    const validOrderFields = [
      "publishedAt",
      "last30Days",
      "last90Days",
      "total",
      "similarity_score",
    ];
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
      page,
      categoryIds: normalizedCategoryIds,
      languageId,
      videoStatus,
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
      } (limit: ${limit}, page: ${page}, userId: ${userId || "none"})`
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

        externalPromise = fetchExternalVideoIds({
          searchTerm,
          categoryIds,
          languageId,
          videoStatus: videoStatus === "all" ? null : videoStatus,
          durationMoreThen,
          durationLessThen,
          limit,
        });
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
      if (languageId && typeof languageId === "string") {
        filters.must.push({ key: "language", match: { value: languageId } });
      }
      if (videoStatus !== "all" && typeof videoStatus === "string") {
        if (videoStatus === "public") {
          filters.must.push({ key: "unlisted", match: { value: false } });
        } else if (videoStatus === "unlisted") {
          filters.must.push({ key: "unlisted", match: { value: true } });
        }
      }
      if (durationMoreThen !== null || durationLessThen !== null) {
        const durationFilter = { key: "duration", range: {} };
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
      console.log(
        "searchBody filter:",
        JSON.stringify(searchBody.filter, null, 2)
      );
      const searchResponse = await axios.post(
        `${qdrantUrl}/collections/${COLLECTION_NAME}/points/search`,
        searchBody,
        { headers: buildHeaders(), timeout: 30000 }
      );
      qdrantResults = searchResponse.data.result;
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

      // Step 7: Merge Qdrant and external video IDs with priority logic
      console.log(`🔄 Merging results with priority logic (limit: ${limit})`);
      console.log(`   - Qdrant results: ${qdrantVideoIds.length}`);
      console.log(`   - External results: ${externalVideoIds.length}`);

      // Start with all Qdrant results (they have priority)
      const mergedVideoIds = [...qdrantVideoIds];
      const seen = new Set(qdrantVideoIds);

      // If we have less than the limit, add external results to fill up
      if (mergedVideoIds.length < limit) {
        const neededFromExternal = limit - mergedVideoIds.length;
        console.log(
          `   - Need ${neededFromExternal} more results from external service`
        );

        let addedFromExternal = 0;
        for (const vid of externalVideoIds) {
          if (!seen.has(vid) && addedFromExternal < neededFromExternal) {
            mergedVideoIds.push(vid);
            seen.add(vid);
            addedFromExternal++;
          }
        }
        console.log(
          `   - Added ${addedFromExternal} unique results from external service`
        );
      } else {
        console.log(
          `   - Qdrant results already meet the limit, skipping external results`
        );
      }

      videoIds = mergedVideoIds;
      console.log(
        `✅ Final merged result: ${videoIds.length} videos (Qdrant: ${
          qdrantVideoIds.length
        }, External used: ${videoIds.length - qdrantVideoIds.length})`
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
      if (!userId || !videoIds || videoIds.length === 0) {
        return [];
      }

      try {
        const videoIdList = videoIds
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

    if (normalizedSearchTerm && videoIds.length > 0) {
      // Case 1: We have video IDs from Qdrant search - query specific videos
      const videoIdList = videoIds
        .map((id) => `'${id.replace(/'/g, "''")}'`)
        .join(", ");

      let query;

      if (effectiveSortProp === "similarity_score") {
        // When sorting by similarity_score, preserve Qdrant order by not using ORDER BY
        query = `
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
            updated_at
          FROM analytics.yt_video_summary 
          WHERE yt_video_id IN (${videoIdList})
        `;
      } else {
        // For other sort fields, use ClickHouse ordering
        query = `
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
            updated_at
          FROM analytics.yt_video_summary 
          WHERE yt_video_id IN (${videoIdList})
          ORDER BY ${order_by} ${order_direction.toUpperCase()}
          LIMIT ${parseInt(limit)}
        `;
      }

      console.log(
        `📊 Starting ClickHouse query for ${
          videoIds.length
        } specific video IDs at ${new Date().toISOString()}`
      );

      // Execute both queries in parallel if userId is provided
      if (userId) {
        console.log(
          `🔄 Executing ClickHouse and swiped videos queries in parallel`
        );
        const [clickhouseResult, swipedResult] = await Promise.all([
          clickhouse.query({
            query: query,
            format: "JSONEachRow",
          }),
          fetchSwipedVideos(videoIds, userId),
        ]);

        clickhouseData = await clickhouseResult.json();
        swipedVideosData = swipedResult;
      } else {
        // Only fetch ClickHouse data if no userId
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
        });

        clickhouseData = await resultSet.json();
      }

      clickhouseTime = performance.now() - clickhouseStartTime;

      console.log(
        `✅ ClickHouse query completed in ${clickhouseTime.toFixed(
          2
        )}ms, returned ${clickhouseData.length} records`
      );
      if (userId) {
        console.log(
          `✅ Swiped videos query completed, found ${swipedVideosData.length} records`
        );
      }
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
      if (languageId && typeof languageId === "string") {
        whereConditions.push(`language = '${languageId.replace(/'/g, "''")}'`);
      }

      // Video status filter (note: ClickHouse uses 'listed', opposite of 'unlisted')
      if (videoStatus !== "all" && typeof videoStatus === "string") {
        if (videoStatus === "public") {
          // Public videos are listed (listed = 1)
          whereConditions.push(`listed = 1`);
        } else if (videoStatus === "unlisted") {
          // Unlisted videos are not listed (listed = 0)
          whereConditions.push(`listed = 0`);
        }
      }

      // Duration range filter
      if (durationMoreThen !== null && typeof durationMoreThen === "number") {
        whereConditions.push(`duration >= ${durationMoreThen}`);
      }
      if (durationLessThen !== null && typeof durationLessThen === "number") {
        whereConditions.push(`duration <= ${durationLessThen}`);
      }

      const whereClause =
        whereConditions.length > 0
          ? `WHERE ${whereConditions.join(" AND ")}`
          : "";

      const query = `
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
          updated_at
        FROM analytics.yt_video_summary 
        ${whereClause}
        ORDER BY ${order_by} ${order_direction.toUpperCase()}
        LIMIT ${parseInt(limit)}
      `;

      console.log(`📊 Executing direct ClickHouse query with filters`);

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

    // Create a map of swiped videos for quick lookup
    const swipedVideosMap = new Map();
    swipedVideosData.forEach((item) => {
      swipedVideosMap.set(item.yt_video_id, item);
    });

    if (normalizedSearchTerm) {
      // Case 1: Keyword search - combine Qdrant similarity scores with ClickHouse data
      const clickhouseMap = new Map();
      clickhouseData.forEach((item) => {
        clickhouseMap.set(item.yt_video_id, item);
      });

      // If we have no Qdrant results but have external results, use those
      if (qdrantResults.length === 0 && videoIds.length > 0) {
        videoIds.forEach((videoId) => {
          const clickhouseInfo = clickhouseMap.get(videoId);
          const swipedInfo = swipedVideosMap.get(videoId);

          const enhancedResult = {
            ytVideoId: videoId,
            similarity_score: null,
            qdrant_data: null,
            summary_data: clickhouseInfo || null,
            swiped_data: swipedInfo || null,
          };

          enhancedResults.push(enhancedResult);
        });
      } else {
        // Combine Qdrant results with ClickHouse data
        qdrantResults.forEach((qdrantResult) => {
          const videoId = qdrantResult.payload?.yt_video_id || qdrantResult.id;
          const clickhouseInfo = clickhouseMap.get(videoId);
          const swipedInfo = swipedVideosMap.get(videoId);

          const enhancedResult = {
            ytVideoId: videoId,
            similarity_score: qdrantResult.score,
            // Qdrant payload data
            qdrant_data: {
              category_id: qdrantResult.payload?.category_id,
              language: qdrantResult.payload?.language,
              is_unlisted: qdrantResult.payload?.unlisted,
              duration: qdrantResult.payload?.duration,
            },
            // ClickHouse summary data (if available)
            summary_data: clickhouseInfo || null,
            // Swiped videos data (if available)
            swiped_data: swipedInfo || null,
          };

          enhancedResults.push(enhancedResult);
        });
      }
    } else {
      // Case 2: ClickHouse-only search - format ClickHouse data directly
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

    // Format response to match frontend expectations
    const data = {
      page: page,
      hasMore: clickhouseData.length === limit,
      results: enhancedResults.map((r) => {
        const isSwiped = r.swiped_data ? true : false;

        return {
          ytVideoId: r.ytVideoId,
          isSwiped: isSwiped, // Set based on swiped_videos table
          title: r.summary_data?.title || r.qdrant_data?.title || null,
          duration: r.summary_data?.duration || r.qdrant_data?.duration || null,
          description: null, // Not available in current data structure
          thumbnail: r.summary_data?.frame || null,
          publishedAt: r.summary_data?.published_at || null,
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
          brandId: null, // Not available in current data structure
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
      total_results: enhancedResults.length,
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
          language: languageId,
          is_unlisted: videoStatus !== "all",
          duration_range: {
            min: durationMoreThen,
            max: durationLessThen,
          },
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

    // Build the query
    const query = `
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
        updated_at
      FROM analytics.yt_video_summary 
      WHERE yt_video_id IN (${videoIdList})
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
      similarityThreshold = 0.4,
      userId = null,
    } = req.body;

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

    // Validate userId if provided
    if (
      userId !== null &&
      (typeof userId !== "string" || userId.trim() === "")
    ) {
      return res.status(400).json({
        error: "userId must be a non-empty string if provided",
        code: "INVALID_USER_ID",
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
      const externalPromise = fetchExternalBrandIds({
        searchTerm: normalizedSearchTerm,
        categoryIds: normalizedCategoryIds,
        countryId: normalizedCountryId,
        softwareIds,
        durationMoreThen,
        durationLessThen,
        limit,
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

      // Step 7: Merge Qdrant and external brand IDs with priority logic
      console.log(`🔄 Merging results with priority logic (limit: ${limit})`);
      console.log(`   - Qdrant results: ${qdrantBrandIds.length}`);
      console.log(`   - External results: ${externalVideoIds.length}`);

      // Start with all Qdrant results (they have priority)
      const mergedBrandIds = [...qdrantBrandIds];
      const seen = new Set(qdrantBrandIds);

      // If we have less than the limit, add external results to fill up
      if (mergedBrandIds.length < limit) {
        const neededFromExternal = limit - mergedBrandIds.length;
        console.log(
          `   - Need ${neededFromExternal} more results from external service`
        );

        let addedFromExternal = 0;
        for (const bid of externalVideoIds) {
          if (!seen.has(bid) && addedFromExternal < neededFromExternal) {
            mergedBrandIds.push(bid);
            seen.add(bid);
            addedFromExternal++;
          }
        }
        console.log(
          `   - Added ${addedFromExternal} unique results from external service`
        );
      } else {
        console.log(
          `   - Qdrant results already meet the limit, skipping external results`
        );
      }

      brandIds = mergedBrandIds;
      console.log(
        `✅ Final merged result: ${brandIds.length} brands (Qdrant: ${
          qdrantBrandIds.length
        }, External used: ${brandIds.length - qdrantBrandIds.length})`
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

    if (normalizedSearchTerm && brandIds.length > 0) {
      // Case 1: We have brand IDs from Qdrant search - query specific brands
      const brandIdList = brandIds
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
            bs.date as summary_date,
            bb.name,
            bb.description,
            bb.category_id,
            bb.country_id,
            bb.avg_duration,
            bb.updated_at
          FROM analytics.brand_basic bb
          LEFT JOIN analytics.brand_summary bs ON bb.brand_id = bs.brand_id
          WHERE bb.brand_id IN (${brandIdList})
        `;
      } else {
        // For other sort fields, use ClickHouse ordering
        const whereClause = `WHERE bb.brand_id IN (${brandIdList})`;
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
            bs.date as summary_date,
            bb.name,
            bb.description,
            bb.category_id,
            bb.country_id,
            bb.avg_duration,
            bb.updated_at
          FROM analytics.brand_basic bb
          LEFT JOIN analytics.brand_summary bs ON bb.brand_id = bs.brand_id
          ${whereClause}
          ORDER BY ${order_by} ${order_direction.toUpperCase()}
          LIMIT ${parseInt(limit)}
        `;
      }

      console.log(
        `📊 Starting ClickHouse query for ${
          brandIds.length
        } specific brand IDs at ${new Date().toISOString()}`
      );
      console.log(`🔍 ClickHouse query to execute:`, query);

      // Execute both queries in parallel if userId is provided
      if (userId) {
        console.log(
          `🔄 Executing ClickHouse and swiped brands queries in parallel`
        );
        const [clickhouseResult, swipedResult] = await Promise.all([
          clickhouse.query({
            query: query,
            format: "JSONEachRow",
          }),
          fetchSwipedBrands(brandIds, userId),
        ]);

        clickhouseData = await clickhouseResult.json();
        swipedBrandsData = swipedResult;
      } else {
        // Only fetch ClickHouse data if no userId
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
        });

        clickhouseData = await resultSet.json();
      }

      clickhouseTime = performance.now() - clickhouseStartTime;

      console.log(
        `✅ ClickHouse query completed in ${clickhouseTime.toFixed(
          2
        )}ms, returned ${clickhouseData.length} records`
      );

      // DEBUG: Log the actual data returned from ClickHouse
      console.log(
        `🔍 ClickHouse raw data sample (first 3 records):`,
        clickhouseData.slice(0, 3).map((item) => ({
          brand_id: item.brand_id,
          name: item.name,
          total_spend: item.total_spend,
          has_data: !!item.name,
        }))
      );

      // Debug: If no results, check the brand_summary table
      if (clickhouseData.length === 0 && brandIds.length > 0) {
        console.log(
          `🔍 No results found in brand_summary table. Checking table status...`
        );
        try {
          // Check brand_basic table first
          const basicCountQuery = `SELECT count() as total FROM analytics.brand_basic LIMIT 1`;
          const basicCountResult = await clickhouse.query({
            query: basicCountQuery,
            format: "JSONEachRow",
          });
          const basicCountData = await basicCountResult.json();
          console.log(
            `🔍 brand_basic table has ${
              basicCountData[0]?.total || 0
            } total rows`
          );

          // Check brand_summary table
          const summaryCountQuery = `SELECT count() as total FROM analytics.brand_summary LIMIT 1`;
          const summaryCountResult = await clickhouse.query({
            query: summaryCountQuery,
            format: "JSONEachRow",
          });
          const summaryCountData = await summaryCountResult.json();
          console.log(
            `🔍 brand_summary table has ${
              summaryCountData[0]?.total || 0
            } total rows`
          );

          // Check if our specific brand IDs exist in brand_basic table
          const brandIdList = brandIds
            .slice(0, 10)
            .map((id) => `'${String(id).replace(/'/g, "''")}'`)
            .join(", ");
          const checkBrandsQuery = `SELECT brand_id, name FROM analytics.brand_basic WHERE brand_id IN (${brandIdList}) LIMIT 10`;
          const checkBrandsResult = await clickhouse.query({
            query: checkBrandsQuery,
            format: "JSONEachRow",
          });
          const checkBrandsData = await checkBrandsResult.json();
          console.log(
            `🔍 Found ${checkBrandsData.length} of our brand IDs in brand_basic table:`,
            checkBrandsData.map((r) => ({ brand_id: r.brand_id, name: r.name }))
          );

          // Show a few sample brand_ids from brand_basic table
          const sampleQuery = `SELECT brand_id, name FROM analytics.brand_basic LIMIT 5`;
          const sampleResult = await clickhouse.query({
            query: sampleQuery,
            format: "JSONEachRow",
          });
          const sampleData = await sampleResult.json();
          console.log(
            `🔍 Sample brand_ids in brand_basic table:`,
            sampleData.map((r) => ({ brand_id: r.brand_id, name: r.name }))
          );

          // Show the exact query we're trying to execute
          console.log(`🔍 Query we executed:`, query);
          console.log(`🔍 Brand IDs we searched for:`, brandIds.slice(0, 10));
        } catch (error) {
          console.error(`❌ Error checking brand tables:`, error.message);
        }
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
      const whereClause = "";

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
          bs.date as summary_date,
          bb.name,
          bb.description,
          bb.category_id,
          bb.country_id,
          bb.avg_duration,
          bb.updated_at
        FROM analytics.brand_basic bb
        LEFT JOIN analytics.brand_summary bs ON bb.brand_id = bs.brand_id
        ${whereClause}
        ORDER BY ${order_by} ${order_direction.toUpperCase()}
        LIMIT ${parseInt(limit)}
      `;

      console.log(`📊 Executing direct ClickHouse brand query with filters`);

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

      console.log(`🔍 ClickHouse data types check:`, {
        sample_brand_id: clickhouseData[0]?.brand_id,
        sample_brand_id_type: typeof clickhouseData[0]?.brand_id,
        brand_ids_from_external: brandIds.slice(0, 3),
        brand_ids_from_external_type: typeof brandIds[0],
      });

      // If we have no Qdrant results but have external results, use those
      if (qdrantResults.length === 0 && brandIds.length > 0) {
        // If ClickHouse returned no data, try to fetch basic brand info
        if (clickhouseData.length === 0) {
          console.log(
            `🔍 No ClickHouse data found, attempting to fetch basic brand info...`
          );
          try {
            const brandIdList = brandIds
              .slice(0, 25)
              .map((id) => `'${String(id).replace(/'/g, "''")}'`)
              .join(", ");
            const basicBrandQuery = `SELECT brand_id, name, description, category_id, country_id, avg_duration, updated_at FROM analytics.brand_basic WHERE brand_id IN (${brandIdList})`;
            const basicBrandResult = await clickhouse.query({
              query: basicBrandQuery,
              format: "JSONEachRow",
            });
            const basicBrandData = await basicBrandResult.json();
            console.log(
              `🔍 Found ${basicBrandData.length} brands in brand_basic table`
            );

            // Create a map of basic brand data
            const basicBrandMap = new Map();
            basicBrandData.forEach((item) => {
              basicBrandMap.set(item.brand_id, item);
            });

            brandIds.forEach((brandId) => {
              const basicBrandInfo = basicBrandMap.get(String(brandId));
              const swipedInfo = swipedBrandsMap.get(String(brandId));

              const enhancedResult = {
                brandId: brandId,
                similarity_score: null,
                qdrant_data: null,
                summary_data: basicBrandInfo || null,
                swiped_data: swipedInfo || null,
              };

              enhancedResults.push(enhancedResult);
            });
          } catch (error) {
            console.error(`❌ Error fetching basic brand info:`, error.message);
            // Fallback to just brand IDs
            brandIds.forEach((brandId) => {
              const swipedInfo = swipedBrandsMap.get(String(brandId));

              const enhancedResult = {
                brandId: brandId,
                similarity_score: null,
                qdrant_data: null,
                summary_data: null,
                swiped_data: swipedInfo || null,
              };

              enhancedResults.push(enhancedResult);
            });
          }
        } else {
          // Use ClickHouse data as before
          console.log(
            `🔍 Using ClickHouse data for ${brandIds.length} brand IDs`
          );
          console.log(`🔍 ClickHouse map has ${clickhouseMap.size} entries`);

          brandIds.forEach((brandId) => {
            const clickhouseInfo = clickhouseMap.get(String(brandId));
            const swipedInfo = swipedBrandsMap.get(String(brandId));

            console.log(`🔍 Processing brand ${brandId}:`, {
              has_clickhouse_data: !!clickhouseInfo,
              has_swiped_data: !!swipedInfo,
              clickhouse_name: clickhouseInfo?.name || "null",
              brand_id_lookup_key: String(brandId),
            });

            const enhancedResult = {
              brandId: brandId,
              similarity_score: null,
              qdrant_data: null,
              summary_data: clickhouseInfo || null,
              swiped_data: swipedInfo || null,
            };

            enhancedResults.push(enhancedResult);
          });
        }
      } else {
        // Combine Qdrant results with ClickHouse data
        qdrantResults.forEach((qdrantResult) => {
          const brandId = qdrantResult.payload?.brand_id || qdrantResult.id;
          const clickhouseInfo = clickhouseMap.get(String(brandId));
          const swipedInfo = swipedBrandsMap.get(String(brandId));

          const enhancedResult = {
            brandId: brandId,
            similarity_score: qdrantResult.score,
            qdrant_data: qdrantResult.payload || null,
            summary_data: clickhouseInfo || null,
            swiped_data: swipedInfo || null,
          };

          enhancedResults.push(enhancedResult);
        });
      }
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

    // Format response to match frontend expectations (based on original fullSearch structure)
    console.log(
      `🔍 Final response formatting: ${enhancedResults.length} enhanced results`
    );

    const data = {
      results: enhancedResults.map((r, index) => {
        const isSwiped = r.swiped_data ? true : false;

        const result = {
          brandId: r.brandId,
          isSwiped: isSwiped, // Set based on swiped_brands table
          name: r.summary_data?.name || r.qdrant_data?.name || null,
          description: r.summary_data?.description || null,
          thumbnail: r.qdrant_data?.thumbnail || null,
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

        // Log first 3 results to see what's being formatted
        if (index < 3) {
          console.log(`🔍 Final result ${index + 1}:`, {
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
      total_results: enhancedResults.length,
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
