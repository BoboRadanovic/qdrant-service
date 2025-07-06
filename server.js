require("dotenv").config();
const express = require("express");
const axios = require("axios");
const { OpenAI } = require("openai");
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

// Initialize OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Configuration
const PORT = process.env.PORT || 3001;
const qdrantUrl = process.env.QDRANT_URL;
const qdrantApiKey = process.env.QDRANT_API_KEY;
const COLLECTION_NAME = process.env.COLLECTION_NAME || "ads";
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
async function createEmbedding(text) {
  try {
    const response = await openai.embeddings.create({
      model: "text-embedding-3-small",
      input: text,
      encoding_format: "float",
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

// Start server
app.listen(PORT, () => {
  console.log(`🚀 QDrant Search Service started on port ${PORT}`);
  console.log(`🔗 QDrant URL: ${qdrantUrl}`);
  console.log(`📊 Collection: ${COLLECTION_NAME}`);
  console.log(
    `🔑 API Key: ${qdrantApiKey ? "✅ Configured" : "❌ Not configured"}`
  );
  console.log(`\n📚 Available endpoints:`);
  console.log(`  POST /search - Vector similarity search`);
  console.log(
    `  POST /search/videos - Simple video ID search (returns 200 video IDs)`
  );
  console.log(
    `  POST /search/videos/filtered - Filtered video search with category_id, language, is_unlisted, duration`
  );
  console.log(`  GET /collection/info - Collection information`);
  console.log(`  GET /health - Health check`);
  console.log(`  GET /check-alive - Service alive check`);
  console.log(`  GET /debug/sample-data - Sample data from collection`);
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
