# API Quick Reference

**Base URL:** `http://your-server:8000`  
**Auth Header (ALL endpoints):** `Authorization: Bearer <user_id>`

---

## 1. POST `/swipe-boards`

```json
{
  "swipe_boards": {
    "swipe_board_id": 123,
    "name": "Board Name",
    "share_id": "optional"
  }
}
```

**Or array:** `"swipe_boards": [...]`

---

## 2. PUT `/swipe-boards/:swipe_board_id`

```json
{
  "name": "Updated Name",
  "share_id": "optional"
}
```

---

## 3. DELETE `/swipe-boards/:swipe_board_id`

**No payload** (ID in URL)

---

## 4. POST `/swiped-videos`

```json
{
  "swiped_videos": {
    "yt_video_id": "video123",
    "swipe_board_ids": [1, 2, 3]
  }
}
```

**Or array:** `"swiped_videos": [...]`

---

## 5. DELETE `/swiped-videos`

```json
{
  "yt_video_id": "video123"
}
```

---

## 6. POST `/swiped-companies`

```json
{
  "swiped_companies": {
    "company_id": 456,
    "swipe_board_ids": [1, 2]
  }
}
```

**Or array:** `"swiped_companies": [...]`

---

## 7. DELETE `/swiped-companies`

```json
{
  "company_id": 456
}
```

---

## 8. POST `/swiped-brands`

```json
{
  "swiped_brands": {
    "brand_id": 789,
    "swipe_board_ids": [1, 2]
  }
}
```

**Or array:** `"swiped_brands": [...]`

---

## 9. DELETE `/swiped-brands`

```json
{
  "brand_id": 789
}
```

---

## Response (All endpoints)

```json
{
  "success": true,
  "message": "...",
  "count": 1
}
```

## Error Response

```json
{
  "error": "...",
  "code": "UNAUTHORIZED|INVALID_INPUT|...",
  "details": "..."
}
```
