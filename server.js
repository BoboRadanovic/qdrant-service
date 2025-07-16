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

// Initialize Express app
const app = express();

// Middleware
app.use(helmet()); // Security headers
app.use(compression()); // Gzip compression
app.use(cors()); // CORS support
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
      console.log("Applied filters:", JSON.stringify(filters, null, 2));
    }

    console.log("Search body:", JSON.stringify(searchBody, null, 2));

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

  try {
    const {
      keyword = null, // Now optional
      limit = DEFAULT_LIMIT,
      category_id = null,
      language = null,
      is_unlisted = null,
      duration_min = null,
      duration_max = null,
      order_by = "published_at",
      order_direction = "desc",
    } = req.body;

    // Validate keyword if provided
    if (
      keyword !== null &&
      (typeof keyword !== "string" || keyword.trim() === "")
    ) {
      return res.status(400).json({
        error: "Keyword must be a non-empty string if provided",
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
      `🔍 Enhanced video search: ${
        keyword ? `"${keyword}"` : "ClickHouse-only"
      } (limit: ${limit})`
    );

    let qdrantResults = [];
    let embeddingTime = 0;
    let qdrantTime = 0;
    let videoIds = [];

    // PATH 1: Keyword provided - do Qdrant search first
    if (keyword) {
      console.log(`📡 Performing Qdrant semantic search for: "${keyword}"`);

      // Step 1: Create embedding and search in Qdrant
      const embeddingStartTime = performance.now();
      const embedding = await createEmbedding(keyword.trim());
      embeddingTime = performance.now() - embeddingStartTime;

      // Build filters for Qdrant search
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
      const qdrantStartTime = performance.now();
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

      qdrantResults = searchResponse.data.result;
      qdrantTime = performance.now() - qdrantStartTime;

      // Extract video IDs from Qdrant results
      videoIds = qdrantResults
        .sort((a, b) => b.score - a.score)
        .map((result) => result.payload?.yt_video_id || result.id);

      console.log(`✅ Found ${videoIds.length} videos from Qdrant search`);
    }

    // PATH 2: Get data from ClickHouse
    let clickhouseData = [];
    let clickhouseTime = 0;
    const clickhouseStartTime = performance.now();

    if (keyword && videoIds.length > 0) {
      // Case 1: We have video IDs from Qdrant search - query specific videos
      const videoIdList = videoIds
        .map((id) => `'${id.replace(/'/g, "''")}'`)
        .join(", ");

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
      `;

      console.log(
        `📊 Querying ClickHouse for ${videoIds.length} specific video IDs`
      );

      try {
        const resultSet = await clickhouse.query({
          query: query,
          format: "JSONEachRow",
        });

        clickhouseData = await resultSet.json();
        clickhouseTime = performance.now() - clickhouseStartTime;

        console.log(`✅ ClickHouse returned ${clickhouseData.length} records`);
      } catch (clickhouseError) {
        console.error("❌ ClickHouse query failed:", clickhouseError.message);
        clickhouseData = [];
      }
    } else if (!keyword) {
      // Case 2: No keyword - direct ClickHouse query with filters
      console.log(`📊 Performing direct ClickHouse search with filters`);

      // Build WHERE clause for filters
      const whereConditions = [];

      // Category ID filter
      if (category_id && Array.isArray(category_id) && category_id.length > 0) {
        const categoryList = category_id.join(", ");
        whereConditions.push(`category_id IN (${categoryList})`);
      }

      // Language filter
      if (language && typeof language === "string") {
        whereConditions.push(`language = '${language.replace(/'/g, "''")}'`);
      }

      // Is unlisted filter (note: ClickHouse uses 'listed', opposite of 'unlisted')
      if (is_unlisted !== null && typeof is_unlisted === "boolean") {
        whereConditions.push(`listed = ${!is_unlisted}`);
      }

      // Duration range filter
      if (duration_min !== null && typeof duration_min === "number") {
        whereConditions.push(`duration >= ${duration_min}`);
      }
      if (duration_max !== null && typeof duration_max === "number") {
        whereConditions.push(`duration <= ${duration_max}`);
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

        console.log(`✅ ClickHouse returned ${clickhouseData.length} records`);
      } catch (clickhouseError) {
        console.error("❌ ClickHouse query failed:", clickhouseError.message);
        clickhouseData = [];
      }
    }

    const totalTime = performance.now() - startTime;

    // Step 3: Format results based on search type
    const enhancedResults = [];

    if (keyword) {
      // Case 1: Keyword search - combine Qdrant similarity scores with ClickHouse data
      const clickhouseMap = new Map();
      clickhouseData.forEach((item) => {
        clickhouseMap.set(item.yt_video_id, item);
      });

      // Combine Qdrant results with ClickHouse data
      qdrantResults.forEach((qdrantResult) => {
        const videoId = qdrantResult.payload?.yt_video_id || qdrantResult.id;
        const clickhouseInfo = clickhouseMap.get(videoId);

        const enhancedResult = {
          video_id: videoId,
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
        };

        enhancedResults.push(enhancedResult);
      });
    } else {
      // Case 2: ClickHouse-only search - format ClickHouse data directly
      clickhouseData.forEach((clickhouseResult) => {
        const enhancedResult = {
          video_id: clickhouseResult.yt_video_id,
          similarity_score: null, // No similarity score for direct ClickHouse search
          // No Qdrant data available
          qdrant_data: null,
          // ClickHouse summary data
          summary_data: clickhouseResult,
        };

        enhancedResults.push(enhancedResult);
      });
    }

    // Format response
    const response = {
      success: true,
      search_type: keyword ? "semantic_plus_clickhouse" : "clickhouse_only",
      keyword: keyword || null,
      total_results: enhancedResults.length,
      qdrant_matches: qdrantResults.length,
      clickhouse_matches: clickhouseData.length,
      parameters: {
        search_filters: {
          category_id: category_id,
          language: language,
          is_unlisted: is_unlisted,
          duration_range: {
            min: duration_min,
            max: duration_max,
          },
        },
        clickhouse_ordering: {
          order_by: order_by,
          order_direction: order_direction,
        },
        limit: parseInt(limit),
      },
      timing: {
        total_ms: Math.round(totalTime),
        embedding_ms: Math.round(embeddingTime),
        qdrant_search_ms: Math.round(qdrantTime),
        clickhouse_query_ms: Math.round(clickhouseTime),
      },
      results: enhancedResults,
    };

    const searchTypeDesc = keyword
      ? `semantic + ClickHouse`
      : `ClickHouse-only`;
    console.log(
      `✅ Enhanced search (${searchTypeDesc}) completed in ${totalTime.toFixed(
        2
      )}ms - ${enhancedResults.length} results`
    );

    res.status(200).json(response);
  } catch (error) {
    const totalTime = performance.now() - startTime;
    console.error(
      `❌ Enhanced search error (after ${totalTime.toFixed(2)}ms):`,
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
