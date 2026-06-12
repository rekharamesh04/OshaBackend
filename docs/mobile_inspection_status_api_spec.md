# Mobile Inspection Status API — Backend ↔ Frontend Contract
**Date:** June 12, 2026  
**From:** Backend Team  
**To:** Frontend / Mobile Team  
**Status:** Draft — Please review and confirm before we start coding

---

## What we're building

A single new API endpoint that powers the mobile app's "inspection home screen." The inspector opens the app, picks a location, and sees all stations grouped by category with their inspection status and a progress summary.

This endpoint reads from our existing tables — no new database tables are needed.

---

## Endpoint

```
GET /api/mobile/inspection-status?location_key={location_key}&category={category}
```

### Query Parameters

| Parameter      | Required | Type   | Description |
|----------------|----------|--------|-------------|
| `location_key` | Yes      | string | The location slug from the dashboard (e.g. `"austin-tx"`) |
| `category`     | No       | string | Filter to a single category. If omitted, returns all 6 categories. Values: `eyewash`, `fire`, `exitdoor`, `racking`, `hra`, `recordkeeping` |

### Authentication

Same as all other endpoints — `x-api-key` header required.

---

## Important: Use our existing ID formats

Our backend already has established ID formats. Please use these instead of the placeholder IDs from the earlier mockup.

| Entity   | Our format (use this)                | Mockup format (don't use) |
|----------|--------------------------------------|---------------------------|
| Location | `"austin-tx"` (slug)                 | `"LOC_AUSTIN_01"`         |
| Category | `"exitdoor"` (typeKey)               | `"CAT_EXIT_DOOR"`         |
| Station  | `"austin-tx-exitdoor-a2b3c4"` (slug) | `"ST_A2"`                 |

The backend returns these IDs. The frontend should store and send them back as-is when starting an inspection.

---

## The 6 categories (fixed list)

| typeKey          | Display Name       |
|------------------|--------------------|
| `eyewash`        | Eyewash            |
| `fire`           | Fire Extinguisher  |
| `exitdoor`       | Exit Door          |
| `racking`        | Monthly Racking    |
| `hra`            | Quarterly HRA      |
| `recordkeeping`  | Recordkeeping      |

---

## Response JSON

```json
{
  "location_key": "austin-tx",
  "location_name": "Austin Plant",
  "date": "2026-06-12",
  "summary": {
    "total": 50,
    "completed": 20,
    "started": 3,
    "pending": 27,
    "percent_complete": 40
  },
  "categories": [
    {
      "category_key": "exitdoor",
      "category_name": "Exit Door",
      "counts": {
        "total": 3,
        "completed": 1,
        "started": 1,
        "pending": 1
      },
      "stations": [
        {
          "station_id": "austin-tx-exitdoor-a2b3c4",
          "station_name": "Exit Door Station #1",
          "status": "completed",
          "inspection_id": "550e8400-e29b-41d4-a716-446655440000",
          "started_at": "2026-06-12T08:30:00Z",
          "completed_at": "2026-06-12T09:14:00Z"
        },
        {
          "station_id": "austin-tx-exitdoor-d5e6f7",
          "station_name": "Exit Door Station #2",
          "status": "started",
          "inspection_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
          "started_at": "2026-06-12T11:02:00Z",
          "completed_at": null
        },
        {
          "station_id": "austin-tx-exitdoor-g8h9i0",
          "station_name": "Exit Door Station #3",
          "status": "pending",
          "inspection_id": null,
          "started_at": null,
          "completed_at": null
        }
      ]
    },
    {
      "category_key": "eyewash",
      "category_name": "Eyewash",
      "counts": {
        "total": 2,
        "completed": 2,
        "started": 0,
        "pending": 0
      },
      "stations": [
        {
          "station_id": "austin-tx-eyewash-x1y2z3",
          "station_name": "Eyewash Station #1",
          "status": "completed",
          "inspection_id": "a1b2c3d4-5678-90ab-cdef-111111111111",
          "started_at": "2026-06-12T08:00:00Z",
          "completed_at": "2026-06-12T08:40:00Z"
        },
        {
          "station_id": "austin-tx-eyewash-m4n5o6",
          "station_name": "Eyewash Station #2",
          "status": "completed",
          "inspection_id": "b2c3d4e5-6789-01bc-defg-222222222222",
          "started_at": "2026-06-12T08:45:00Z",
          "completed_at": "2026-06-12T08:52:00Z"
        }
      ]
    },
    {
      "category_key": "fire",
      "category_name": "Fire Extinguisher",
      "counts": {
        "total": 2,
        "completed": 0,
        "started": 0,
        "pending": 2
      },
      "stations": [
        {
          "station_id": "austin-tx-fire-p7q8r9",
          "station_name": "Fire Ext Station #1",
          "status": "pending",
          "inspection_id": null,
          "started_at": null,
          "completed_at": null
        },
        {
          "station_id": "austin-tx-fire-s0t1u2",
          "station_name": "Fire Ext Station #2",
          "status": "pending",
          "inspection_id": null,
          "started_at": null,
          "completed_at": null
        }
      ]
    },
    {
      "category_key": "racking",
      "category_name": "Monthly Racking",
      "counts": { "total": 0, "completed": 0, "started": 0, "pending": 0 },
      "stations": []
    },
    {
      "category_key": "hra",
      "category_name": "Quarterly HRA",
      "counts": { "total": 0, "completed": 0, "started": 0, "pending": 0 },
      "stations": []
    },
    {
      "category_key": "recordkeeping",
      "category_name": "Recordkeeping",
      "counts": { "total": 0, "completed": 0, "started": 0, "pending": 0 },
      "stations": []
    }
  ]
}
```

---

## How status is determined

Status is not stored as a flag. It is calculated per station based on whether an inspection record exists for the current month.

| Status      | Meaning | How it's determined |
|-------------|---------|---------------------|
| `pending`   | Not started | No inspection record exists for this station this month |
| `started`   | In progress | An inspection record exists but not all checklist items are answered |
| `completed` | Done | An inspection record exists and all checklist items are answered |

Since inspections happen monthly, the endpoint checks if a station has been inspected in the current month (not just today). A station that became due on June 1 and hasn't been inspected by June 12 will still show as "pending."

---

## Timestamps

- All timestamps are UTC in ISO 8601 format (e.g. `"2026-06-12T09:14:00Z"`)
- `started_at` = when the inspection record was first created
- `completed_at` = when all checklist items were answered (null if not finished)
- Frontend should convert to local timezone for display

---

## User identity

This endpoint does NOT require a `user_id` parameter. It shows all stations for the selected location regardless of who is inspecting. The logged-in user's name (for the header display like "Rahul Kumar") is handled on the frontend side — the backend doesn't need it for this endpoint.

---

## Starting an inspection from the home screen

When the user taps a "pending" station to start an inspection, the frontend should create a session with these fields:

```json
{
  "auditor_name": "Rahul Kumar Yadav",
  "facility_area": "Austin Plant",
  "date_of_audit": "2026-06-12",
  "location": "Austin Plant",
  "station": "Exit Door Station #3",
  "station_id": "austin-tx-exitdoor-g8h9i0"
}
```

The key new field is `station_id`. This is the dashboard station ID that allows us to link the inspection back to the station for progress tracking. Please include it when creating a session.

When the user taps a "started" station, use the existing `inspection_id` from the response to resume that inspection.

---

## How to get the list of locations (for the dropdown)

Use the existing endpoint:

```
GET /api/companies
```

This returns the full hierarchy. Extract the location names and keys from the response to populate the location dropdown.

---

## Error responses

| Status | Body | When |
|--------|------|------|
| 400 | `{"error": "location_key is required"}` | Missing location_key param |
| 400 | `{"error": "Invalid category. Must be one of: eyewash, fire, exitdoor, racking, hra, recordkeeping"}` | Bad category param |
| 404 | `{"error": "Location 'xxx' not found"}` | location_key doesn't exist |
| 403 | `{"error": "Forbidden", "message": "Invalid or missing API key"}` | Bad API key |

---

## Changes from the earlier mockup JSON

Here's what changed from the JSON that was shared earlier and why:

| Earlier mockup | This spec | Why |
|----------------|-----------|-----|
| `location_id: "LOC_AUSTIN_01"` | `location_key: "austin-tx"` | Using our existing backend format |
| `category_id: "CAT_EXIT_DOOR"` | `category_key: "exitdoor"` | Using our existing typeKey format |
| `station_id: "ST_A2"` | `station_id: "austin-tx-exitdoor-a2b3c4"` | Using our existing station ID format |
| `user_id`, `user_name` in response | Removed | Not needed — this is a location-level view, not user-level |
| `inspection_id: "INS_88421"` | `inspection_id: "550e8400-..."` (UUID) | Our inspections use UUIDs |
| Timestamps in IST | Timestamps in UTC | Backend stores UTC, frontend converts |

---

## Questions for the frontend team

1. Does this response shape work for your mockup? Any fields missing?
2. Are you okay using our existing ID formats instead of the LOC/CAT/ST prefixes?
3. For the category dropdown — do you want us to also return the category icon (e.g. `"fa-door-open"`) in the response, or will you hardcode those on your side?
4. When resuming a "started" inspection, will you use the `inspection_id` we return to fetch the full inspection details?

---

**Please review and confirm. Once we agree on this contract, we'll build the endpoint and let you know when it's ready to test.**
