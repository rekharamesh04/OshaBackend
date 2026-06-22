# Mobile Inspection Status API — How It Works

## What this API does

This API powers the mobile app's "Start Inspection" home screen. When an inspector opens the app, picks a location, and lands on the home screen — this API tells them:

- How many stations need inspection this month
- Which ones are done, which are in progress, which haven't been touched yet
- A progress bar (e.g., "12 of 50 complete — 24%")

---

## The core concept: Dashboard = Work Order

Think of it like this:

```
Dashboard Table (osha-dashboard)
  └── Location: Austin Plant
        ├── Eyewash Station #1     ← Must be inspected every month
        ├── Eyewash Station #2     ← Must be inspected every month
        ├── Fire Ext Station #1    ← Must be inspected every month
        └── Exit Door Station #1   ← Must be inspected every month

Inspection Tables (6 separate tables)
  └── Records of inspections that have actually been performed
        ├── Eyewash Station #1 — inspected on June 5th ✅
        ├── Fire Ext Station #1 — started on June 10th, incomplete ⏳
        └── (nothing for Eyewash #2 or Exit Door #1)
```

The API does one thing: **compares the two lists**.

---

## How status is calculated

For each station in the dashboard:

| Condition | Status | Meaning |
|-----------|--------|---------|
| No inspection record found for this station in the current month | `pending` | Nobody has inspected it yet |
| Inspection record exists but checklist has unanswered items | `started` | Someone began but didn't finish |
| Inspection record exists and ALL checklist items are answered | `completed` | Done for this month |

### Important distinction

| Field | Admin Dashboard (`GET /api/companies`) | Mobile API (`GET /api/mobile/inspection-status`) |
|-------|---------------------------------------|------------------------------------------------|
| **`status`** | `ok` / `warn` / `overdue` | `pending` / `started` / `completed` |
| **Meaning** | Is the station's equipment healthy? | Has someone inspected it this month? |
| **Source** | Stored in dashboard DB | Calculated on-the-fly |

A station can be `"ok"` (equipment is fine) AND `"pending"` (nobody has inspected it yet this month). These are two different questions.

---

## How matching works (station → inspection)

When the mobile API tries to find an inspection for a station, it uses this logic:

```
Step 1: Look for an inspection record where station_id matches
        (e.g., station_id = "austin-tx-eyewash-a1b2c3")

Step 2: If no station_id match, fall back to station name match
        (e.g., station name = "Eyewash Station #1")

Step 3: Only consider inspections from the CURRENT MONTH
        (date_of_audit between month start and month end)

Step 4: If auditor_name filter is provided, only count that person's inspections
```

### Why station_id matters

Old inspections don't have `station_id` because it didn't exist before. The name fallback handles those.

New inspections (created from the mobile flow) will include `station_id` in the session, which flows into the inspection record automatically. This makes matching 100% reliable.

---

## The full flow (step by step)

```
1. Inspector opens mobile app
2. App calls GET /api/companies → gets location list → shows dropdown
3. Inspector picks "Austin Plant" (location_key = "austin-tx")
4. App calls GET /api/mobile/inspection-status?location_key=austin-tx
5. API does this internally:
   a. Query dashboard table → find all stations under "austin-tx"
   b. Query all 6 inspection tables in parallel → find this month's inspections
   c. For each station, check if a matching inspection exists
   d. Calculate status (pending/started/completed) for each station
   e. Group stations by category (eyewash, fire, exitdoor, etc.)
   f. Calculate summary (total/completed/started/pending/percent_complete)
   g. Return the response
6. App renders the home screen with progress bar + station cards
```

---

## What happens when the inspector taps a station

### Tapping a "pending" station (start new inspection):
```
POST /inspection-session
Body: {
  "auditor_name": "Rahul Kumar Yadav",
  "facility_area": "Austin Plant",
  "date_of_audit": "2026-06-16",
  "location": "Austin Plant",
  "station": "Eyewash Station #1",
  "station_id": "austin-tx-eyewash-a1b2c3"   ← links it back to dashboard
}
→ Returns session_id

Then POST to the appropriate inspection Lambda with the session_id
```

### Tapping a "started" station (resume inspection):
```
Use the inspection_id from the mobile API response to load and continue
the existing inspection via the inspection Lambda's GET endpoint
```

### Tapping a "completed" station:
```
Show read-only view of the completed inspection using the inspection_id
```

---

## Query parameters summary

```
GET /api/mobile/inspection-status
  ?location_key=austin-tx          (required — which location)
  &category=eyewash                (optional — filter to one category)
  &auditor_name=Rahul Kumar Yadav  (optional — filter to one person's progress)
```

---

## Response shape (abbreviated)

```json
{
  "location_key": "austin-tx",
  "location_name": "Austin Plant",
  "date": "2026-06-16",
  "summary": {
    "total": 12,
    "completed": 3,
    "started": 1,
    "pending": 8,
    "percent_complete": 25
  },
  "categories": [
    {
      "category_key": "eyewash",
      "category_name": "Eyewash",
      "counts": { "total": 3, "completed": 1, "started": 0, "pending": 2 },
      "stations": [
        {
          "station_id": "austin-tx-eyewash-a1b2c3",
          "station_name": "Eyewash Station #1",
          "status": "completed",
          "inspection_id": "uuid-here",
          "started_at": "2026-06-05T...",
          "completed_at": "2026-06-05T..."
        },
        {
          "station_id": "austin-tx-eyewash-d4e5f6",
          "station_name": "Eyewash Station #2",
          "status": "pending",
          "inspection_id": null,
          "started_at": null,
          "completed_at": null
        }
      ]
    }
  ]
}
```
