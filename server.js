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
const PORT = process.env.PORT || 3000;
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
  console.log(`  GET /collection/info - Collection information`);
  console.log(`  GET /health - Health check`);
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
