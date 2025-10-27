# Swipe Boards API Documentation

## Overview

This API provides **9 endpoints** for managing swipe boards and swiped items (videos, companies, brands) in ClickHouse.

**⚠️ All endpoints require authentication via Authorization header.**

## Authentication

All endpoints require an `Authorization` header with a user token:

```
Authorization: Bearer <user_id>
```

The `user_id`/`firebase_id` is automatically extracted from the token - you don't need to include it in the request body.

---

## Important Notes

### ClickHouse Upsert Behavior

- **ReplacingMergeTree**: Used for `swipe_boards` and `swiped_videos` tables. This engine automatically deduplicates rows based on the ORDER BY keys during background merges.
- **Upsert Pattern**: Since ClickHouse doesn't support PostgreSQL-style `ON CONFLICT`, we simply insert new rows. ReplacingMergeTree will eventually deduplicate based on:
  - `swipe_boards`: `swipe_board_id`
  - `swiped_videos`: `(firebase_id, yt_video_id)`

### Array Support

- All endpoints support both single items and arrays of items
- `swipe_board_ids` can be an array (one item can belong to multiple boards)
- Bulk operations are supported for efficient batch inserts

---

## Swipe Boards Endpoints (3 endpoints)

### 1. Insert Swipe Board(s)

**Endpoint**: `POST /swipe-boards`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body** (single board):

```json
{
  "swipe_boards": {
    "swipe_board_id": 1,
    "name": "My Favorites",
    "share_id": "abc123",
    "created": "2024-01-01T00:00:00Z"
  }
}
```

**Request Body** (bulk insert):

```json
{
  "swipe_boards": [
    {
      "swipe_board_id": 1,
      "name": "Favorites",
      "share_id": "share123"
    },
    {
      "swipe_board_id": 2,
      "name": "Watch Later",
      "share_id": "share456"
    }
  ]
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully inserted 2 swipe board(s)",
  "count": 2
}
```

**Notes**:

- `user_id` is automatically extracted from the Authorization token
- `created` and `share_id` are optional
- Supports both single board and array of boards

---

### 2. Update Swipe Board

**Endpoint**: `PUT /swipe-boards/:swipe_board_id`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body**:

```json
{
  "name": "Updated Board Name",
  "share_id": "new_share_id"
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully updated swipe board 1"
}
```

**Notes**:

- `user_id` is automatically extracted from the Authorization token
- Only `name` is required, `share_id` is optional
- ReplacingMergeTree will eventually replace the old row

---

### 3. Delete Swipe Board

**Endpoint**: `DELETE /swipe-boards/:swipe_board_id`

**Headers**:

```
Authorization: Bearer <user_id>
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully deleted swipe board 1"
}
```

**Notes**:

- Only deletes boards owned by the authenticated user
- Uses `user_id` from token to ensure user can only delete their own boards

---

## Swiped Videos Endpoints (2 endpoints)

### 1. Insert Swiped Video(s)

**Endpoint**: `POST /swiped-videos`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body** (single video):

```json
{
  "swiped_videos": {
    "yt_video_id": "dQw4w9WgXcQ",
    "swipe_board_ids": [1, 2, 3],
    "created_at": "2024-01-01T00:00:00Z"
  }
}
```

**Request Body** (bulk insert):

```json
{
  "swiped_videos": [
    {
      "yt_video_id": "video1",
      "swipe_board_ids": [1]
    },
    {
      "yt_video_id": "video2",
      "swipe_board_ids": [1, 2]
    }
  ]
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully inserted 2 swiped video(s)",
  "count": 2
}
```

**Notes**:

- `firebase_id` is automatically extracted from the Authorization token
- `swipe_board_ids` can be an array to add video to multiple boards
- If the same video is inserted again, it will update the `swipe_board_ids` (upsert behavior)
- `created_at` is optional

---

### 2. Delete Swiped Video

**Endpoint**: `DELETE /swiped-videos`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body**:

```json
{
  "yt_video_id": "dQw4w9WgXcQ"
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully deleted swiped video"
}
```

**Notes**:

- `firebase_id` is automatically extracted from token
- Only deletes videos owned by the authenticated user

---

## Swiped Companies Endpoints (2 endpoints)

### 1. Insert Swiped Company(ies)

**Endpoint**: `POST /swiped-companies`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body** (single):

```json
{
  "swiped_companies": {
    "company_id": 456,
    "swipe_board_ids": [1, 2]
  }
}
```

**Request Body** (bulk):

```json
{
  "swiped_companies": [
    {
      "company_id": 456,
      "swipe_board_ids": [1]
    },
    {
      "company_id": 789,
      "swipe_board_ids": [1, 2]
    }
  ]
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully inserted 2 swiped company(ies)",
  "count": 2
}
```

**Notes**:

- `firebase_id` is automatically extracted from token
- Supports bulk operations

---

### 2. Delete Swiped Company

**Endpoint**: `DELETE /swiped-companies`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body**:

```json
{
  "company_id": 456
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully deleted swiped company"
}
```

**Notes**:

- `firebase_id` is automatically extracted from token

---

## Swiped Brands Endpoints (2 endpoints)

### 1. Insert Swiped Brand(s)

**Endpoint**: `POST /swiped-brands`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body** (single):

```json
{
  "swiped_brands": {
    "brand_id": 789,
    "swipe_board_ids": [1, 2]
  }
}
```

**Request Body** (bulk):

```json
{
  "swiped_brands": [
    {
      "brand_id": 789,
      "swipe_board_ids": [1]
    },
    {
      "brand_id": 101,
      "swipe_board_ids": [1, 2]
    }
  ]
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully inserted 2 swiped brand(s)",
  "count": 2
}
```

**Notes**:

- `firebase_id` is automatically extracted from token
- Supports bulk operations

---

### 2. Delete Swiped Brand

**Endpoint**: `DELETE /swiped-brands`

**Headers**:

```
Authorization: Bearer <user_id>
Content-Type: application/json
```

**Request Body**:

```json
{
  "brand_id": 789
}
```

**Response**:

```json
{
  "success": true,
  "message": "Successfully deleted swiped brand"
}
```

**Notes**:

- `firebase_id` is automatically extracted from token

---

## Error Handling

All endpoints return consistent error responses:

```json
{
  "error": "Error message",
  "details": "Detailed error information",
  "code": "ERROR_CODE"
}
```

**Error Codes**:

- `UNAUTHORIZED`: Missing or invalid authorization token (HTTP 401)
- `INVALID_INPUT`: Missing or invalid required fields (HTTP 400)
- `INSERT_ERROR`: Failed to insert data (HTTP 500)
- `UPDATE_ERROR`: Failed to update data (HTTP 500)
- `DELETE_ERROR`: Failed to delete data (HTTP 500)

---

## Usage Examples

### Example 1: Create a swipe board and add videos to it

```bash
# Set your user token
USER_TOKEN="user123"

# 1. Create swipe board
curl -X POST http://localhost:8000/swipe-boards \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "swipe_boards": {
      "swipe_board_id": 1,
      "name": "My Favorites"
    }
  }'

# 2. Add videos to the board
curl -X POST http://localhost:8000/swiped-videos \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "swiped_videos": [
      {
        "yt_video_id": "video1",
        "swipe_board_ids": [1]
      },
      {
        "yt_video_id": "video2",
        "swipe_board_ids": [1]
      }
    ]
  }'
```

### Example 2: Add one video to multiple boards

```bash
curl -X POST http://localhost:8000/swiped-videos \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "swiped_videos": {
      "yt_video_id": "awesome_video",
      "swipe_board_ids": [1, 2, 3]
    }
  }'
```

### Example 3: Update swipe_board_ids (upsert pattern)

```bash
# To update swipe_board_ids for a video, just insert again with new board IDs
# ReplacingMergeTree will handle the deduplication
curl -X POST http://localhost:8000/swiped-videos \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "swiped_videos": {
      "yt_video_id": "video1",
      "swipe_board_ids": [1, 2, 4, 5]
    }
  }'
```

### Example 4: Update a swipe board name

```bash
curl -X PUT http://localhost:8000/swipe-boards/1 \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Updated Favorites"
  }'
```

### Example 5: Delete a video from swipes

```bash
curl -X DELETE http://localhost:8000/swiped-videos \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "yt_video_id": "video1"
  }'
```

---

## Summary of 9 Endpoints

### Swipe Boards (3 endpoints)

1. ✅ `POST /swipe-boards` - Insert swipe board(s)
2. ✅ `PUT /swipe-boards/:swipe_board_id` - Update swipe board
3. ✅ `DELETE /swipe-boards/:swipe_board_id` - Delete swipe board

### Swiped Videos (2 endpoints)

4. ✅ `POST /swiped-videos` - Insert swiped video(s)
5. ✅ `DELETE /swiped-videos` - Delete swiped video

### Swiped Companies (2 endpoints)

6. ✅ `POST /swiped-companies` - Insert swiped company(ies)
7. ✅ `DELETE /swiped-companies` - Delete swiped company

### Swiped Brands (2 endpoints)

8. ✅ `POST /swiped-brands` - Insert swiped brand(s)
9. ✅ `DELETE /swiped-brands` - Delete swiped brand

---

## Performance Considerations

1. **Bulk Operations**: Always prefer bulk inserts when adding multiple items
2. **Background Merges**: ReplacingMergeTree performs deduplication during background merges, so there might be a slight delay before duplicates are removed
3. **Array Operations**: ClickHouse efficiently handles arrays, so using `swipe_board_ids` as an array is optimal
4. **Authentication**: Token validation is lightweight (currently just extracts user_id). For production, integrate with Firebase Admin SDK for proper JWT verification.

---

## Production Recommendations

The current authentication middleware is simple and extracts the user_id directly from the token. For production environments:

1. **Use Firebase Admin SDK** to verify JWT tokens
2. **Add rate limiting** for each user
3. **Add validation** for swipe_board_id ownership
4. **Add indexes** in ClickHouse for frequently queried fields
5. **Monitor** ReplacingMergeTree merge operations
6. **Add caching** for frequently accessed data

### Example Firebase Admin Integration

```javascript
const admin = require("firebase-admin");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

async function authenticateUser(req, res, next) {
  try {
    const token = req.headers.authorization?.replace("Bearer ", "");

    // Verify Firebase token
    const decodedToken = await admin.auth().verifyIdToken(token);

    // Set all three to the same value: user_id = req.user.uid = firebase_id
    req.user = { uid: decodedToken.uid };
    req.user_id = decodedToken.uid;
    req.firebase_id = decodedToken.uid;

    next();
  } catch (error) {
    return res.status(401).json({
      error: "Authentication failed",
      code: "UNAUTHORIZED",
    });
  }
}
```

**Important**: All three values are the same:

- `req.user.uid` (Firebase standard - **used in all endpoints**)
- `req.user_id` (convenience alias - set by middleware)
- `req.firebase_id` (convenience alias - set by middleware)

All endpoints use `req.user.uid` to ensure consistency with Firebase conventions, where `user_id = req.user.uid = firebase_id`.
