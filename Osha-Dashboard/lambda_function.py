"""
OSHA Safety Hub — Dashboard Lambda
Manages the Reseller → Company → Location → StationType → Station hierarchy
for the frontend dashboard sidebar.

Endpoints:
    --- Reseller CRUD ---
    GET    /api/resellers                                     → Full nested tree (Reseller→Company→Location→Station)
    POST   /api/resellers                                     → Create reseller
    PUT    /api/resellers/{reseller_key}                      → Update reseller name
    DELETE /api/resellers/{reseller_key}                      → Delete reseller + cascade all companies/locations/stations

    --- Company CRUD ---
    GET    /api/companies                                     → Full nested tree (optionally filtered by ?reseller_key=)
    POST   /api/companies                                     → Create company (optional reseller_key in body)
    POST   /api/companies/{ck}/locations                      → Create location (auto-creates 5 station types)
    POST   /api/companies/{ck}/locations/{lk}/stations        → Create station
    PUT    /api/companies/{company_key}                        → Update company name/state
    PUT    /api/companies/{ck}/locations/{lk}                  → Update location details
    GET    /api/companies/{ck}/locations/{lk}/inspection-categories → List category toggles for location
    PUT    /api/companies/{ck}/locations/{lk}/toggle-category  → Enable/disable inspection category at location
    PUT    /api/stations/{station_id}                          → Update station status/notes
    DELETE /api/companies/{company_key}                        → Delete company + all locations & stations
    DELETE /api/companies/{ck}/locations/{lk}                  → Delete location + all stations
    DELETE /api/stations/{station_id}                          → Delete a single station
    GET    /api/alerts                                         → All stations with status != "ok"
    GET    /admin/inspections                                  → Unified inspection list (all 5 types)

DynamoDB Table: osha-dashboard (PK + SK single-table design)
    Reseller:          PK=RESELLER#{rk}        SK=METADATA
    Reseller→Company:  PK=RESELLER#{rk}        SK=COMPANY#{ck}
    Company:           PK=COMPANY#{key}         SK=METADATA
    Location:          PK=COMPANY#{ck}          SK=LOCATION#{lk}
    Station:           PK=LOCATION#{lk}         SK=STATION#{id}
"""

import json
import os
import re
import uuid
import logging
import time as _time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from boto3.dynamodb.conditions import Key, Attr

# ─────────────────────────────────────────────
# Lambda In-Memory Cache (survives container reuse)
# ─────────────────────────────────────────────
_DASHBOARD_CACHE = {"data": None, "ts": 0}
_CACHE_TTL = 30  # seconds

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────
# AWS Clients
# ─────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb")
dashboard_table = dynamodb.Table(os.getenv("DASHBOARD_TABLE_NAME", "osha-dashboard"))

# API Key Authentication
EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()

# Inspection tables (for admin/inspections endpoint)
inspection_tables = {
    "Recordkeeping":      dynamodb.Table(os.getenv("RECORDKEEPING_TABLE", "osha-inspections")),
    "Eyewash":            dynamodb.Table(os.getenv("EYEWASH_TABLE", "osha-eyewash-inspections")),
    "Fire Extinguisher":  dynamodb.Table(os.getenv("FIRE_EXT_TABLE", "osha-fire-extinguisher-inspections")),
    "Exit Door":          dynamodb.Table(os.getenv("EXIT_DOOR_TABLE", "osha-exit-door-inspections")),
    "Monthly Racking":    dynamodb.Table(os.getenv("RACKING_TABLE", "osha-racking-inspections")),
    "Quarterly HRA":      dynamodb.Table(os.getenv("HRA_TABLE", "osha-hra-inspections")),
}

# Centralized session table (shared across all inspection types)
session_table = dynamodb.Table(os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions"))

# Maps category key (dashboard/mobile) → inspection_type value stored in session table
CATEGORY_TO_INSPECTION_TYPE = {
    "eyewash": "eyewash",
    "fire": "fire-extinguisher",
    "exitdoor": "exit-door",
    "racking": "racking",
    "hra": "hra",
    "recordkeeping": "recordkeeping",
}

# Maps inspection_type (session table) → inspection_tables label
INSPECTION_TYPE_TO_LABEL = {
    "eyewash": "Eyewash",
    "fire-extinguisher": "Fire Extinguisher",
    "exit-door": "Exit Door",
    "racking": "Monthly Racking",
    "hra": "Quarterly HRA",
    "recordkeeping": "Recordkeeping",
}

# Auto-calculated summary items to skip during progress/next-item computation
SUMMARY_ITEM_IDS = {
    "fire-extinguisher": {11, 12},
    "exit-door": {18, 19},
}

# ─────────────────────────────────────────────
# Fixed Station Type Categories
# ─────────────────────────────────────────────
STATION_TYPES = [
    {"key": "eyewash",       "label": "Eyewash",           "icon": "fa-eye"},
    {"key": "fire",          "label": "Fire Extinguisher",  "icon": "fa-fire-extinguisher"},
    {"key": "exitdoor",      "label": "Exit Door",          "icon": "fa-door-open"},
    {"key": "racking",       "label": "Monthly Racking",    "icon": "fa-th-large"},
    {"key": "hra",           "label": "Quarterly HRA",      "icon": "fa-clipboard-list"},
    {"key": "recordkeeping", "label": "Recordkeeping",      "icon": "fa-folder-open"},
]

VALID_CATEGORY_KEYS = {st["key"] for st in STATION_TYPES}

CATEGORY_KEY_TO_CHECKLIST_TYPE = {
    "fire": "fire-extinguisher",
    "eyewash": "eyewash",
    "exitdoor": "exit-door",
    "racking": "racking",
    "hra": "hra",
    "recordkeeping": "recordkeeping",
}


# ═══════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════

def _disabled_categories_from_item(location_item):
    """Return disabled category keys from a location DynamoDB item."""
    if not location_item:
        return set()
    raw = location_item.get("disabled_categories") or []
    return {str(x) for x in raw}


def _get_location_item(company_key, location_key):
    """Single get_item for a company location (O(1))."""
    return dashboard_table.get_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"},
    ).get("Item")


def _get_disabled_categories_set(company_key, location_key):
    """Load disabled category keys for a location."""
    item = _get_location_item(company_key, location_key)
    return _disabled_categories_from_item(item)


def _list_inspection_categories(disabled_categories):
    """Build admin/mobile category list with is_enabled flags."""
    disabled = disabled_categories or set()
    return [
        {
            "category_key": st["key"],
            "checklist_type": CATEGORY_KEY_TO_CHECKLIST_TYPE.get(st["key"], st["key"]),
            "label": st["label"],
            "is_enabled": st["key"] not in disabled,
        }
        for st in STATION_TYPES
    ]


def _resolve_location_item(company_key, location_key):
    """
    Resolve location metadata for mobile APIs.
    Fast path: get_item when company_key is provided.
    Slow path: filtered scan (backward compat).
    Returns (item, resolved_company_key) or (None, company_key).
    """
    if company_key:
        item = dashboard_table.get_item(
            Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"},
            ProjectionExpression="PK, SK, #n, disabled_categories",
            ExpressionAttributeNames={"#n": "name"},
        ).get("Item")
        return item, company_key

    logger.warning(
        "[MOBILE] company_key omitted for location '%s' — using slow scan; "
        "pass company_key for 200-500ms performance target",
        location_key,
    )
    loc_scan_kwargs = {
        "FilterExpression": Attr("SK").eq(f"LOCATION#{location_key}"),
        "ProjectionExpression": "PK, SK, #n, disabled_categories",
        "ExpressionAttributeNames": {"#n": "name"},
    }
    loc_resp = dashboard_table.scan(**loc_scan_kwargs)
    loc_items = loc_resp.get("Items", [])
    while "LastEvaluatedKey" in loc_resp:
        loc_scan_kwargs["ExclusiveStartKey"] = loc_resp["LastEvaluatedKey"]
        loc_resp = dashboard_table.scan(**loc_scan_kwargs)
        loc_items.extend(loc_resp.get("Items", []))

    for item in loc_items:
        pk = item.get("PK", "")
        if pk.startswith("COMPANY#"):
            return item, pk[8:]
    return None, ""


def _resolve_company_location_from_station(station_id):
    """Resolve company_key and location_key from a station_id."""
    station = _find_station_by_id(station_id)
    location_key = ""
    company_key = ""
    if station:
        pk = str(station.get("PK", ""))
        if pk.startswith("LOCATION#"):
            location_key = pk[9:]
        company_key = str(station.get("company_key", "")).strip()
    if not location_key:
        location_key = _extract_location_key_from_station_id(station_id) or ""
    return company_key, location_key


def _category_disabled_response(category_key):
    return build_response(403, {
        "error": f"Inspection category '{category_key}' is disabled at this location",
    })


def _check_category_enabled_at_station(station_id, category_key):
    """Return error response if category disabled at station's location, else None."""
    company_key, location_key = _resolve_company_location_from_station(station_id)
    if not company_key or not location_key:
        return None
    disabled = _get_disabled_categories_set(company_key, location_key)
    if category_key in disabled:
        return _category_disabled_response(category_key)
    return None

def _normalized_headers(event):
    """Normalize header keys to lowercase for case-insensitive lookup."""
    headers = event.get("headers") or {}
    return {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in headers.items()}


def require_api_key(event):
    """Validate the x-api-key header or query param. Returns None if valid, or an error response."""
    if not EXPECTED_API_KEY:
        return build_response(500, {"error": "Server API_KEY env var is not configured"})
    headers = _normalized_headers(event)
    provided = (headers.get("x-api-key") or headers.get("x_api_key") or headers.get("apikey") or "").strip()
    # Fallback: also check query string parameters (for browser URL testing)
    if not provided:
        qsp = event.get("queryStringParameters") or {}
        provided = (qsp.get("x-api-key") or qsp.get("api_key") or qsp.get("apikey") or "").strip()
    if not provided or provided != EXPECTED_API_KEY:
        return build_response(403, {"error": "Forbidden", "message": "Invalid or missing API key"})
    return None


def build_response(status_code, body):
    """Standard API Gateway response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key,X-Amz-Date",
        },
        "body": json.dumps(body, default=str),
    }



def parse_body(event):
    """Parse JSON body from API Gateway event."""
    body = event.get("body", "{}")
    if not body:
        return {}
    if event.get("isBase64Encoded"):
        import base64
        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def get_query(event, key, default=""):
    """Get a query string parameter."""
    params = event.get("queryStringParameters") or {}
    return params.get(key, default)


def slugify(text):
    """Convert text to URL-safe slug: 'Dallas, TX' → 'dallas-tx'."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)   # Remove special chars
    text = re.sub(r"[\s_]+", "-", text)     # Spaces/underscores → hyphens
    text = re.sub(r"-+", "-", text)         # Collapse multiple hyphens
    return text.strip("-")


def now_iso():
    """Current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def convert_decimals(obj):
    """Convert DynamoDB Decimal types to native Python types."""
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    return obj


def scan_full_table(ddb_table):
    """Scan a DynamoDB table handling pagination."""
    items = []
    resp = ddb_table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = ddb_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items


def parallel_scan_table(ddb_table, total_segments=4, **extra_kwargs):
    """Parallel segmented scan — splits the table read across N threads."""
    all_items = []

    def _scan_segment(segment):
        items = []
        kwargs = {"TotalSegments": total_segments, "Segment": segment}
        kwargs.update(extra_kwargs)
        resp = ddb_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            resp = ddb_table.scan(**kwargs)
            items.extend(resp.get("Items", []))
        return items

    with ThreadPoolExecutor(max_workers=total_segments) as executor:
        futures = [executor.submit(_scan_segment, i) for i in range(total_segments)]
        for future in as_completed(futures):
            all_items.extend(future.result())

    return all_items


def _load_dashboard_items_cached():
    """Return all dashboard table items with 30-second in-memory cache.
    On Lambda container reuse this avoids re-scanning the table on every request."""
    global _DASHBOARD_CACHE
    now = _time.time()
    if _DASHBOARD_CACHE["data"] is not None and (now - _DASHBOARD_CACHE["ts"]) < _CACHE_TTL:
        return _DASHBOARD_CACHE["data"]

    items = convert_decimals(parallel_scan_table(dashboard_table, total_segments=4))
    _DASHBOARD_CACHE = {"data": items, "ts": now}
    return items


def _invalidate_dashboard_cache():
    """Clear the in-memory dashboard cache after any write operation."""
    global _DASHBOARD_CACHE
    _DASHBOARD_CACHE = {"data": None, "ts": 0}


def _extract_location_key_from_station_id(station_id):
    """Extract location_key from station_id format: {location_key}-{type_key}-{6hex}.
    Returns None if the format is unrecognized."""
    type_keys = sorted(
        ["eyewash", "fire", "exitdoor", "racking", "hra", "recordkeeping"],
        key=len, reverse=True,
    )
    if not station_id or len(station_id) < 8:
        return None
    prefix = station_id[:-7]  # strip "-{6hex}"
    for tk in type_keys:
        suffix = f"-{tk}"
        if prefix.endswith(suffix):
            return prefix[:-len(suffix)]
    return None


# ═══════════════════════════════════════════════
# Centralized Session Helpers
# ═══════════════════════════════════════════════

def _get_inspection_table(inspection_type):
    """Resolve an inspection_type string to its DynamoDB table object."""
    label = INSPECTION_TYPE_TO_LABEL.get(inspection_type)
    if label:
        return inspection_tables.get(label)
    return None


def _get_inspection_table_by_category(category_key):
    """Resolve a dashboard category key to its DynamoDB table object."""
    inspection_type = CATEGORY_TO_INSPECTION_TYPE.get(category_key)
    if inspection_type:
        return _get_inspection_table(inspection_type)
    return None


def _resolve_session_inspection_type(session, session_id, inspection_id, body_hint=None):
    """
    Resolve inspection_type for a session.

    Order:
      1. Existing session.inspection_type
      2. Optional body hint (inspection_type or category from client)
      3. Probe all inspection tables by inspection_id and backfill session
    """
    inspection_type = str((session or {}).get("inspection_type", "") or "").strip()
    if inspection_type and _get_inspection_table(inspection_type):
        return inspection_type

    hint = str(body_hint or "").strip()
    if hint:
        if hint in INSPECTION_TYPE_TO_LABEL:
            inspection_type = hint
        elif hint in CATEGORY_TO_INSPECTION_TYPE:
            inspection_type = CATEGORY_TO_INSPECTION_TYPE[hint]
        if inspection_type and _get_inspection_table(inspection_type):
            try:
                session_table.update_item(
                    Key={"session_id": session_id},
                    UpdateExpression="SET inspection_type = :it",
                    ExpressionAttributeValues={":it": inspection_type},
                )
                logger.info(
                    "[SESSION] Backfilled inspection_type='%s' from body hint for session %s",
                    inspection_type, session_id,
                )
            except Exception as update_err:
                logger.warning("[SESSION] Failed to backfill inspection_type from hint: %s", update_err)
            return inspection_type

    inspection_id = str(inspection_id or "").strip()
    if not inspection_id:
        return ""

    label_to_type = {v: k for k, v in INSPECTION_TYPE_TO_LABEL.items()}
    for label, tbl in inspection_tables.items():
        try:
            probe = tbl.get_item(Key={"inspection_id": inspection_id}).get("Item")
            if not probe:
                continue
            inspection_type = label_to_type.get(label, "")
            if not inspection_type:
                continue
            try:
                session_table.update_item(
                    Key={"session_id": session_id},
                    UpdateExpression="SET inspection_type = :it",
                    ExpressionAttributeValues={":it": inspection_type},
                )
                logger.info(
                    "[SESSION] Fallback backfilled inspection_type='%s' for session %s",
                    inspection_type, session_id,
                )
            except Exception as update_err:
                logger.warning("[SESSION] Failed to backfill inspection_type: %s", update_err)
            return inspection_type
        except Exception:
            continue
    return ""


def _ids_match(a, b):
    """Compare checklist/category IDs across int, Decimal, and custom string forms."""
    if a is None or b is None:
        return False
    if a == b:
        return True
    try:
        return int(a) == int(b)
    except (ValueError, TypeError):
        return str(a) == str(b)


def _numeric_id(raw):
    """Return int ID for default items/categories, or None for custom string IDs."""
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _session_progress(inspection, inspection_type=""):
    """Compute progress for any inspection type. Returns {total, answered, percentage}."""
    skip_ids = SUMMARY_ITEM_IDS.get(inspection_type, set())
    total = 0
    answered = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = _numeric_id(item.get("id"))
            if iid is not None and iid in skip_ids:
                continue
            total += 1
            if str(item.get("answer") or "").strip():
                answered += 1
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                total += 1
                if str(item.get("answer") or "").strip():
                    answered += 1
    pct = round((answered / total * 100), 1) if total > 0 else 0
    return {"total": total, "answered": answered, "percentage": pct}


def _session_next_unanswered(inspection, inspection_type=""):
    """Find the next unanswered item ID. Returns None if all answered."""
    skip_ids = SUMMARY_ITEM_IDS.get(inspection_type, set())
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = _numeric_id(item.get("id"))
            if iid is not None and iid in skip_ids:
                continue
            if not str(item.get("answer") or "").strip():
                return item.get("id")
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                if not str(item.get("answer") or "").strip():
                    return item.get("id")
    return None


def _session_compute_status(inspection, inspection_type=""):
    """Compute inspection status from answers. Returns in_progress/completed/pending."""
    progress = _session_progress(inspection, inspection_type)
    if progress["total"] == 0:
        return "pending"
    if progress["answered"] == progress["total"]:
        return "completed"
    if progress["answered"] > 0:
        return "in_progress"
    return "pending"


def _sanitize_dynamodb(obj):
    """Recursively sanitize a Python object for DynamoDB put_item."""
    if isinstance(obj, dict):
        return {k: _sanitize_dynamodb(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_sanitize_dynamodb(i) for i in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj


# ═══════════════════════════════════════════════
# RESELLER API 1: GET /api/resellers — Full Nested Tree
# ═══════════════════════════════════════════════
def get_resellers(event):
    """
    Returns the full Reseller → Company → Location → StationType → Station tree.
    OPTIMIZED: Uses parallel scan + in-memory cache (30s TTL).
    """
    all_items = _load_dashboard_items_cached()

    resellers = {}
    reseller_companies = {}
    companies = {}
    locations = {}
    stations = {}
    _st_type_keys = {st["key"] for st in STATION_TYPES}

    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if pk.startswith("RESELLER#"):
            rk = pk[9:]  # len("RESELLER#") == 9
            if sk == "METADATA":
                resellers[rk] = {"key": rk, "name": item.get("name", ""), "companies": []}
            elif sk.startswith("COMPANY#"):
                reseller_companies.setdefault(rk, []).append(sk[8:])

        elif pk.startswith("COMPANY#"):
            ck = pk[8:]
            if sk == "METADATA":
                companies[ck] = {"key": ck, "name": item.get("name", ""), "state": item.get("state", ""), "locations": []}
            elif sk.startswith("LOCATION#"):
                lk = sk[9:]
                locations.setdefault(ck, []).append({
                    "key": lk, "name": item.get("name", ""),
                    "state": item.get("state", ""), "address": item.get("address", ""),
                    "city": item.get("city", ""), "zip": item.get("zip", ""),
                    "phone": item.get("phone", ""),
                    "disabled_categories": list(_disabled_categories_from_item(item)),
                })

        elif pk.startswith("LOCATION#") and sk.startswith("STATION#"):
            lk = pk[9:]
            stations.setdefault(lk, []).append({
                "id": item.get("station_id", sk[8:]),
                "name": item.get("name", ""), "route": item.get("route", ""),
                "status": item.get("status", "ok"),
                "lastInspected": item.get("lastInspected", ""),
                "nextDue": item.get("nextDue", ""),
                "notes": item.get("notes", ""), "typeKey": item.get("typeKey", ""),
            })

    def _build_location(loc):
        lk = loc["key"]
        loc_stations = stations.get(lk, [])
        type_buckets = {tk: [] for tk in _st_type_keys}
        for s in loc_stations:
            tk = s.get("typeKey", "")
            if tk in type_buckets:
                type_buckets[tk].append(s)
        loc_copy = {k: v for k, v in loc.items() if k != "disabled_categories"}
        disabled_set = set(loc.get("disabled_categories") or [])
        loc_copy["stationTypes"] = [
            {
                "key": st["key"], "label": st["label"], "icon": st["icon"],
                "is_enabled": st["key"] not in disabled_set,
                "stations": type_buckets.get(st["key"], []),
            }
            for st in STATION_TYPES
        ]
        return loc_copy

    def _build_company(ck):
        company = companies.get(ck)
        if not company:
            return None
        return {
            "key": company["key"], "name": company["name"], "state": company["state"],
            "locations": [_build_location(loc) for loc in locations.get(ck, [])],
        }

    result = []
    for rk, reseller in resellers.items():
        for ck in reseller_companies.get(rk, []):
            built = _build_company(ck)
            if built:
                reseller["companies"].append(built)
        result.append(reseller)

    return build_response(200, {"resellers": result})


# ═══════════════════════════════════════════════
# RESELLER API 2: POST /api/resellers — Create Reseller
# ═══════════════════════════════════════════════
def create_reseller(event):
    """Creates a new reseller."""
    body = parse_body(event)
    name = str(body.get("name", "")).strip()

    if not name:
        return build_response(400, {"error": "name is required"})

    key = slugify(name)

    existing = dashboard_table.get_item(Key={"PK": f"RESELLER#{key}", "SK": "METADATA"}).get("Item")
    if existing:
        return build_response(409, {"error": f"Reseller '{name}' already exists with key '{key}'"})

    item = {
        "PK": f"RESELLER#{key}",
        "SK": "METADATA",
        "name": name,
        "created_at": now_iso(),
    }
    dashboard_table.put_item(Item=item)
    _invalidate_dashboard_cache()

    return build_response(201, {"key": key, "name": name, "companies": []})


# ═══════════════════════════════════════════════
# RESELLER API 3: PUT /api/resellers/{reseller_key} — Update Reseller
# ═══════════════════════════════════════════════
def update_reseller(event):
    """Updates a reseller's name."""
    path_params = event.get("pathParameters") or {}
    reseller_key = str(path_params.get("reseller_key", "")).strip()
    body = parse_body(event)

    if not reseller_key:
        return build_response(400, {"error": "reseller_key is required"})

    existing = dashboard_table.get_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Reseller '{reseller_key}' not found"})

    new_name = str(body.get("name", "")).strip()
    if not new_name:
        return build_response(400, {"error": "name is required"})

    dashboard_table.update_item(
        Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"},
        UpdateExpression="SET #name = :name, #updated_at = :updated_at",
        ExpressionAttributeNames={"#name": "name", "#updated_at": "updated_at"},
        ExpressionAttributeValues={":name": new_name, ":updated_at": now_iso()},
    )
    _invalidate_dashboard_cache()

    return build_response(200, {"key": reseller_key, "name": new_name})


# ═══════════════════════════════════════════════
# RESELLER API 4: DELETE /api/resellers/{reseller_key} — Cascade Delete
# ═══════════════════════════════════════════════
def delete_reseller(event):
    """
    Deletes a reseller and CASCADE-DELETES all associated companies,
    their locations, and their stations.
    OPTIMIZED: Uses targeted DynamoDB queries by PK instead of full table scan.
    """
    path_params = event.get("pathParameters") or {}
    reseller_key = str(path_params.get("reseller_key", "")).strip()

    if not reseller_key:
        return build_response(400, {"error": "reseller_key is required"})

    existing = dashboard_table.get_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Reseller '{reseller_key}' not found"})

    # Query company associations under this reseller (PK=RESELLER#rk, SK begins_with COMPANY#)
    assoc_resp = dashboard_table.query(
        KeyConditionExpression=Key("PK").eq(f"RESELLER#{reseller_key}") & Key("SK").begins_with("COMPANY#"),
    )
    company_keys = []
    for assoc in assoc_resp.get("Items", []):
        ck = assoc["SK"][8:]  # strip "COMPANY#"
        company_keys.append(ck)
        dashboard_table.delete_item(Key={"PK": assoc["PK"], "SK": assoc["SK"]})

    deleted_companies = []
    deleted_locations = []
    deleted_stations = []

    for ck in company_keys:
        # Query locations under this company
        loc_resp = dashboard_table.query(
            KeyConditionExpression=Key("PK").eq(f"COMPANY#{ck}") & Key("SK").begins_with("LOCATION#"),
        )
        for loc_item in loc_resp.get("Items", []):
            lk = loc_item["SK"][9:]
            deleted_locations.append(lk)
            dashboard_table.delete_item(Key={"PK": loc_item["PK"], "SK": loc_item["SK"]})

            # Query stations under this location
            st_resp = dashboard_table.query(
                KeyConditionExpression=Key("PK").eq(f"LOCATION#{lk}") & Key("SK").begins_with("STATION#"),
            )
            for st_item in st_resp.get("Items", []):
                dashboard_table.delete_item(Key={"PK": st_item["PK"], "SK": st_item["SK"]})
                deleted_stations.append(st_item.get("station_id", ""))

        dashboard_table.delete_item(Key={"PK": f"COMPANY#{ck}", "SK": "METADATA"})
        deleted_companies.append(ck)

    dashboard_table.delete_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"})
    _invalidate_dashboard_cache()

    return build_response(200, {
        "message": "Reseller deleted successfully (cascade)",
        "reseller_key": reseller_key,
        "deleted_companies": deleted_companies,
        "deleted_locations": deleted_locations,
        "deleted_stations": deleted_stations,
    })



# ═══════════════════════════════════════════════
# API 1: GET /api/companies — Full Nested Tree
# ═══════════════════════════════════════════════
def get_companies(event):
    """
    Returns the full Company → Location → StationType → Station tree.
    OPTIMIZED: Uses parallel scan + in-memory cache (30s TTL).
    """
    all_items = _load_dashboard_items_cached()

    companies = {}
    locations = {}
    stations = {}

    # Single pass — classify every item
    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if pk.startswith("COMPANY#"):
            ck = pk[8:]  # len("COMPANY#") == 8
            if sk == "METADATA":
                companies[ck] = {
                    "key": ck,
                    "name": item.get("name", ""),
                    "state": item.get("state", ""),
                    "locations": [],
                }
            elif sk.startswith("LOCATION#"):
                lk = sk[9:]  # len("LOCATION#") == 9
                locations.setdefault(ck, []).append({
                    "key": lk,
                    "name": item.get("name", ""),
                    "state": item.get("state", ""),
                    "address": item.get("address", ""),
                    "city": item.get("city", ""),
                    "zip": item.get("zip", ""),
                    "phone": item.get("phone", ""),
                    "disabled_categories": list(_disabled_categories_from_item(item)),
                })

        elif pk.startswith("LOCATION#") and sk.startswith("STATION#"):
            lk = pk[9:]
            stations.setdefault(lk, []).append({
                "id": item.get("station_id", sk[8:]),
                "name": item.get("name", ""),
                "route": item.get("route", ""),
                "status": item.get("status", "ok"),
                "lastInspected": item.get("lastInspected", ""),
                "nextDue": item.get("nextDue", ""),
                "notes": item.get("notes", ""),
                "typeKey": item.get("typeKey", ""),
            })

    # Pre-index stations by (location_key, typeKey) for O(1) grouping
    _st_type_keys = {st["key"] for st in STATION_TYPES}

    result = []
    for ck, company in companies.items():
        for loc in locations.get(ck, []):
            lk = loc["key"]
            loc_stations = stations.get(lk, [])

            # Group by typeKey (pre-bucket to avoid N*M loop)
            type_buckets = {tk: [] for tk in _st_type_keys}
            for s in loc_stations:
                tk = s.get("typeKey", "")
                if tk in type_buckets:
                    type_buckets[tk].append(s)

            disabled_set = set(loc.get("disabled_categories") or [])

            station_types = [
                {
                    "key": st["key"],
                    "label": st["label"],
                    "icon": st["icon"],
                    "is_enabled": st["key"] not in disabled_set,
                    "stations": type_buckets.get(st["key"], []),
                }
                for st in STATION_TYPES
            ]

            loc_copy = {k: v for k, v in loc.items() if k != "disabled_categories"}
            loc_copy["stationTypes"] = station_types
            company["locations"].append(loc_copy)

        result.append(company)

    return build_response(200, {"companies": result})


# ═══════════════════════════════════════════════
# API 2: POST /api/companies — Create Company
# ═══════════════════════════════════════════════
def create_company(event):
    """Creates a new company. Optionally associates it with a reseller."""
    body = parse_body(event)
    name = str(body.get("name", "")).strip()
    state = str(body.get("state", "")).strip()
    reseller_key = str(body.get("reseller_key", "")).strip()

    if not name:
        return build_response(400, {"error": "name is required"})

    key = slugify(name)

    # Check if already exists
    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{key}", "SK": "METADATA"}).get("Item")
    if existing:
        return build_response(409, {"error": f"Company '{name}' already exists with key '{key}'"})

    # If reseller_key provided, verify reseller exists
    if reseller_key:
        reseller = dashboard_table.get_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"}).get("Item")
        if not reseller:
            return build_response(404, {"error": f"Reseller '{reseller_key}' not found"})

    item = {
        "PK": f"COMPANY#{key}",
        "SK": "METADATA",
        "name": name,
        "state": state,
        "created_at": now_iso(),
    }
    dashboard_table.put_item(Item=item)

    # Auto-associate with reseller if provided
    if reseller_key:
        dashboard_table.put_item(Item={
            "PK": f"RESELLER#{reseller_key}",
            "SK": f"COMPANY#{key}",
            "associated_at": now_iso(),
        })

    _invalidate_dashboard_cache()

    return build_response(201, {
        "key": key,
        "name": name,
        "state": state,
        "reseller_key": reseller_key or None,
        "locations": [],
    })


# ═══════════════════════════════════════════════
# API 3: POST /api/companies/{ck}/locations — Create Location
# ═══════════════════════════════════════════════
def create_location(event):
    """
    Creates a new location under a company.
    Auto-creates the 5 fixed station type categories with empty station arrays.
    """
    path_params = event.get("pathParameters") or {}
    company_key = path_params.get("company_key", "")
    body = parse_body(event)

    name = str(body.get("name", "")).strip()
    state = str(body.get("state", "")).strip()
    address = str(body.get("address", "")).strip()
    city = str(body.get("city", "")).strip()
    zip_code = str(body.get("zip", "")).strip()
    phone = str(body.get("phone", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required in URL path"})
    if not name:
        return build_response(400, {"error": "name is required"})

    # Verify company exists
    company = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"}).get("Item")
    if not company:
        return build_response(404, {"error": f"Company '{company_key}' not found"})

    location_key = slugify(f"{name}-{state}") if state else slugify(name)

    # Check if location already exists
    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}).get("Item")
    if existing:
        return build_response(409, {"error": f"Location '{name}' already exists under this company"})

    item = {
        "PK": f"COMPANY#{company_key}",
        "SK": f"LOCATION#{location_key}",
        "name": name,
        "state": state,
        "address": address,
        "city": city,
        "zip": zip_code,
        "phone": phone,
        "created_at": now_iso(),
    }
    dashboard_table.put_item(Item=item)

    # Return with empty station types
    station_types = [
        {"key": st["key"], "label": st["label"], "icon": st["icon"], "stations": []}
        for st in STATION_TYPES
    ]

    _invalidate_dashboard_cache()

    return build_response(201, {
        "key": location_key,
        "name": name,
        "state": state,
        "address": address,
        "city": city,
        "zip": zip_code,
        "phone": phone,
        "stationTypes": station_types,
    })


# ═══════════════════════════════════════════════
# API 4: POST /api/companies/{ck}/locations/{lk}/stations — Create Station
# ═══════════════════════════════════════════════
def create_station(event):
    """Creates a new station under a location."""
    path_params = event.get("pathParameters") or {}
    company_key = path_params.get("company_key", "")
    location_key = path_params.get("location_key", "")
    body = parse_body(event)

    name = str(body.get("name", "")).strip()
    type_key = str(body.get("typeKey", "")).strip()
    status = str(body.get("status", "ok")).strip()
    last_inspected = str(body.get("lastInspected", "")).strip()
    next_due = str(body.get("nextDue", "")).strip()
    notes = str(body.get("notes", "")).strip()

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key required in URL path"})
    if not name:
        return build_response(400, {"error": "name is required"})
    if not type_key:
        return build_response(400, {"error": "typeKey is required"})

    # Validate typeKey
    valid_types = {st["key"] for st in STATION_TYPES}
    if type_key not in valid_types:
        return build_response(400, {"error": f"Invalid typeKey. Must be one of: {', '.join(valid_types)}"})

    # Validate status
    if status not in ("ok", "warn", "fail"):
        return build_response(400, {"error": "status must be 'ok', 'warn', or 'fail'"})

    # Verify location exists
    location = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}).get("Item")
    if not location:
        return build_response(404, {"error": f"Location '{location_key}' not found under company '{company_key}'"})

    # Generate station ID: location-typeKey-random
    short_id = uuid.uuid4().hex[:6]
    station_id = f"{location_key}-{type_key}-{short_id}"

    # Count existing stations of this type for route numbering
    existing_stations = dashboard_table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        ExpressionAttributeValues={
            ":pk": f"LOCATION#{location_key}",
            ":prefix": "STATION#",
        },
    ).get("Items", [])
    type_count = sum(1 for s in existing_stations if s.get("typeKey") == type_key)
    station_number = type_count + 1

    # Build route path
    route = f"/{type_key}/{location_key}/{station_number}"

    item = {
        "PK": f"LOCATION#{location_key}",
        "SK": f"STATION#{station_id}",
        "station_id": station_id,
        "name": name,
        "typeKey": type_key,
        "route": route,
        "status": status,
        "lastInspected": last_inspected,
        "nextDue": next_due,
        "notes": notes,
        "company_key": company_key,
        "created_at": now_iso(),
    }
    dashboard_table.put_item(Item=item)

    _invalidate_dashboard_cache()

    return build_response(201, {
        "id": station_id,
        "name": name,
        "route": route,
        "status": status,
        "lastInspected": last_inspected,
        "nextDue": next_due,
        "notes": notes,
    })


# ═══════════════════════════════════════════════
# API 5: PUT /api/stations/{station_id} — Update Station
# ═══════════════════════════════════════════════
def _find_station_by_id(station_id):
    """Locate a station item by station_id using the fastest available method:
    1) StationIdIndex GSI query (O(1), requires GSI to be active)
    2) Direct get_item via parsed location_key from station_id format
    3) Filtered scan fallback (slowest)
    """
    # Strategy 1: GSI query — guaranteed O(1) if GSI exists
    try:
        gsi_resp = dashboard_table.query(
            IndexName="StationIdIndex",
            KeyConditionExpression=Key("station_id").eq(station_id),
            Limit=1,
        )
        gsi_items = gsi_resp.get("Items", [])
        if gsi_items:
            return gsi_items[0]
    except Exception as gsi_err:
        err_code = getattr(gsi_err, "response", {}).get("Error", {}).get("Code", "")
        if err_code == "ValidationException" and "StationIdIndex" in str(gsi_err):
            pass  # GSI not yet created
        else:
            logger.warning(f"StationIdIndex query failed: {gsi_err}")

    # Strategy 2: parse station_id format → direct get_item
    location_key = _extract_location_key_from_station_id(station_id)
    if location_key:
        result = dashboard_table.get_item(
            Key={"PK": f"LOCATION#{location_key}", "SK": f"STATION#{station_id}"}
        )
        item = result.get("Item")
        if item:
            return item

    # Strategy 3: filtered scan fallback
    resp = dashboard_table.scan(
        FilterExpression=Attr("SK").eq(f"STATION#{station_id}") & Attr("PK").begins_with("LOCATION#"),
    )
    items = resp.get("Items", [])
    while not items and "LastEvaluatedKey" in resp:
        resp = dashboard_table.scan(
            FilterExpression=Attr("SK").eq(f"STATION#{station_id}") & Attr("PK").begins_with("LOCATION#"),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items = resp.get("Items", [])
    return items[0] if items else None


def update_station(event):
    """Updates a station's status, notes, dates, or name.
    OPTIMIZED: Uses StationIdIndex GSI → parsed get_item → scan fallback."""
    path_params = event.get("pathParameters") or {}
    station_id = path_params.get("station_id", "")
    body = parse_body(event)

    if not station_id:
        return build_response(400, {"error": "station_id is required in URL path"})

    station_item = _find_station_by_id(station_id)

    if not station_item:
        return build_response(404, {"error": f"Station '{station_id}' not found"})

    # Update allowed fields
    updatable_fields = ["name", "status", "lastInspected", "nextDue", "notes"]
    update_expr_parts = ["#updated_at = :updated_at"]
    attr_names = {"#updated_at": "updated_at"}
    attr_values = {":updated_at": now_iso()}

    for field in updatable_fields:
        if field in body:
            safe_name = f"#{field}"
            safe_value = f":{field}"
            update_expr_parts.append(f"{safe_name} = {safe_value}")
            attr_names[safe_name] = field
            attr_values[safe_value] = body[field]

    # Validate status if provided
    if "status" in body and body["status"] not in ("ok", "warn", "fail"):
        return build_response(400, {"error": "status must be 'ok', 'warn', or 'fail'"})

    if len(update_expr_parts) <= 1:
        return build_response(400, {"error": "No valid fields to update"})

    dashboard_table.update_item(
        Key={"PK": station_item["PK"], "SK": station_item["SK"]},
        UpdateExpression="SET " + ", ".join(update_expr_parts),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
    )

    updated = dashboard_table.get_item(Key={"PK": station_item["PK"], "SK": station_item["SK"]}).get("Item", {})
    updated = convert_decimals(updated)
    _invalidate_dashboard_cache()

    return build_response(200, {
        "id": updated.get("station_id", station_id),
        "name": updated.get("name", ""),
        "route": updated.get("route", ""),
        "status": updated.get("status", "ok"),
        "lastInspected": updated.get("lastInspected", ""),
        "nextDue": updated.get("nextDue", ""),
        "notes": updated.get("notes", ""),
    })


# ═══════════════════════════════════════════════
# API 6: GET /api/alerts — Stations with Issues
# ═══════════════════════════════════════════════
def get_alerts(event):
    """Returns all stations where status is 'warn' or 'fail', enriched with reseller/company/location context.
    OPTIMIZED: Uses parallel scan + in-memory cache."""
    all_items = _load_dashboard_items_cached()

    # Build lookup maps
    resellers = {}               # reseller_key → {name}
    company_to_reseller = {}     # company_key → reseller_key
    companies = {}               # key → {name, state}
    locations = {}               # location_key → {name, company_key, ...}
    alerts = []

    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if sk == "METADATA" and pk.startswith("RESELLER#"):
            rk = pk.replace("RESELLER#", "")
            resellers[rk] = {"name": item.get("name", "")}

        elif sk.startswith("COMPANY#") and pk.startswith("RESELLER#"):
            rk = pk.replace("RESELLER#", "")
            ck = sk.replace("COMPANY#", "")
            company_to_reseller[ck] = rk

        elif sk == "METADATA" and pk.startswith("COMPANY#"):
            key = pk.replace("COMPANY#", "")
            companies[key] = {"name": item.get("name", ""), "state": item.get("state", "")}

        elif sk.startswith("LOCATION#") and pk.startswith("COMPANY#"):
            company_key = pk.replace("COMPANY#", "")
            location_key = sk.replace("LOCATION#", "")
            locations[location_key] = {
                "name": item.get("name", ""),
                "company_key": company_key,
            }

        elif sk.startswith("STATION#") and pk.startswith("LOCATION#"):
            status = item.get("status", "ok")
            if status in ("warn", "fail"):
                location_key = pk.replace("LOCATION#", "")
                type_key = item.get("typeKey", "")
                type_label = next((st["label"] for st in STATION_TYPES if st["key"] == type_key), type_key)

                alerts.append({
                    "id": item.get("station_id", ""),
                    "name": item.get("name", ""),
                    "route": item.get("route", ""),
                    "status": status,
                    "lastInspected": item.get("lastInspected", ""),
                    "nextDue": item.get("nextDue", ""),
                    "notes": item.get("notes", ""),
                    "_location_key": location_key,
                    "typeKey": type_key,
                    "typeLabel": type_label,
                })

    # Enrich with reseller/company/location context
    enriched_alerts = []
    for alert in alerts:
        loc_key = alert.pop("_location_key", "")
        loc_info = locations.get(loc_key, {})
        company_key = loc_info.get("company_key", "")
        company_info = companies.get(company_key, {})
        reseller_key = company_to_reseller.get(company_key, "")
        reseller_info = resellers.get(reseller_key, {})

        alert["resellerKey"] = reseller_key
        alert["resellerName"] = reseller_info.get("name", "")
        alert["companyKey"] = company_key
        alert["companyName"] = company_info.get("name", "")
        alert["locationKey"] = loc_key
        alert["locationName"] = loc_info.get("name", "")
        enriched_alerts.append(alert)

    return build_response(200, {
        "total": len(enriched_alerts),
        "alerts": enriched_alerts,
    })


# ═══════════════════════════════════════════════
# API 7: GET /admin/inspections — Unified Inspection List
# ═══════════════════════════════════════════════
def admin_list_inspections(event):
    """
    Returns a unified list of ALL inspections from all 6 types,
    with computed status, evidence count, and aggregate stats.

    OPTIMIZED:
      1. Server-side FilterExpression for date_of_audit and location/facility_area
         → DynamoDB filters BEFORE sending data over the network
      2. Parallel scan across 6 tables (6 threads)
      3. Reduced Python-side filtering (already done by DynamoDB)

    Query Parameters (all optional):
        location    — Filter by facility_area (case-insensitive)
        start_date  — Filter inspections on or after this date (YYYY-MM-DD)
        end_date    — Filter inspections on or before this date (YYYY-MM-DD)
    """
    try:
        params = event.get("queryStringParameters", {}) or {}
        filter_location = str(params.get("location", "") or "").strip()
        filter_start = str(params.get("start_date", "") or "").strip()
        filter_end = str(params.get("end_date", "") or "").strip()

        all_inspections = []

        def _query_or_scan_table(type_label, ddb_table):
            """Use GSI query() when location filter is present, otherwise scan with FilterExpression.
            The LocationDateIndex GSI (PK=facility_area, SK=date_of_audit) enables
            O(K) reads instead of O(N) full table scans."""
            try:
                items = []

                # FAST PATH: GSI query when location + date filters are provided
                if filter_location and (filter_start or filter_end):
                    try:
                        kce = Key("facility_area").eq(filter_location)
                        if filter_start and filter_end:
                            kce = kce & Key("date_of_audit").between(filter_start, filter_end)
                        elif filter_start:
                            kce = kce & Key("date_of_audit").gte(filter_start)
                        else:
                            kce = kce & Key("date_of_audit").lte(filter_end)

                        query_kwargs = {
                            "IndexName": "LocationDateIndex",
                            "KeyConditionExpression": kce,
                        }
                        resp = ddb_table.query(**query_kwargs)
                        items.extend(resp.get("Items", []))
                        while "LastEvaluatedKey" in resp:
                            query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
                            resp = ddb_table.query(**query_kwargs)
                            items.extend(resp.get("Items", []))

                        items = convert_decimals(items)
                        logger.info(f"[ADMIN] GSI query {type_label}: {len(items)} items")
                        if items:
                            return [(item, type_label) for item in items]
                        else:
                            logger.info(
                                f"[ADMIN] GSI returned 0 for facility_area='{filter_location}' "
                                f"on {type_label}, falling back to scan"
                            )
                    except ddb_table.meta.client.exceptions.ResourceNotFoundException:
                        logger.warning(f"[ADMIN] LocationDateIndex not found on {type_label}, falling back to scan")
                    except Exception as gsi_err:
                        err_code = getattr(gsi_err, "response", {}).get("Error", {}).get("Code", "")
                        if err_code == "ValidationException" and "LocationDateIndex" in str(gsi_err):
                            logger.warning(f"[ADMIN] LocationDateIndex not ready on {type_label}, falling back to scan")
                        else:
                            raise

                # FALLBACK: scan with FilterExpression
                filter_parts = []
                attr_names = {}
                attr_values = {}

                if filter_start and filter_end:
                    filter_parts.append("#doa BETWEEN :ds AND :de")
                    attr_names["#doa"] = "date_of_audit"
                    attr_values[":ds"] = filter_start
                    attr_values[":de"] = filter_end
                elif filter_start:
                    filter_parts.append("#doa >= :ds")
                    attr_names["#doa"] = "date_of_audit"
                    attr_values[":ds"] = filter_start
                elif filter_end:
                    filter_parts.append("#doa <= :de")
                    attr_names["#doa"] = "date_of_audit"
                    attr_values[":de"] = filter_end

                if filter_location:
                    filter_parts.append("(#loc = :loc OR #fa = :loc)")
                    attr_names["#loc"] = "location"
                    attr_names["#fa"] = "facility_area"
                    attr_values[":loc"] = filter_location

                scan_kwargs = {}
                if filter_parts:
                    scan_kwargs["FilterExpression"] = " AND ".join(filter_parts)
                    scan_kwargs["ExpressionAttributeNames"] = attr_names
                    scan_kwargs["ExpressionAttributeValues"] = attr_values

                items = []
                resp = ddb_table.scan(**scan_kwargs)
                items.extend(resp.get("Items", []))
                while "LastEvaluatedKey" in resp:
                    scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
                    resp = ddb_table.scan(**scan_kwargs)
                    items.extend(resp.get("Items", []))

                items = convert_decimals(items)
                logger.info(f"[ADMIN] Scanned {type_label}: {len(items)} items (filtered server-side)")
                return [(item, type_label) for item in items]
            except Exception as e:
                logger.error(f"Error scanning table for {type_label}: {str(e)}")
                return []

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(_query_or_scan_table, label, table): label
                for label, table in inspection_tables.items()
            }
            for future in as_completed(futures):
                all_inspections.extend(future.result())

        logger.info(f"[ADMIN] Total inspections after server-side filter: {len(all_inspections)}")

        result_list = []
        for raw, type_label in all_inspections:
            try:
                categories = raw.get("categories") or []
                general_results = raw.get("general_results") or []
                date_of_audit = str(raw.get("date_of_audit") or "")
                location = str(raw.get("location") or "")
                facility_area = str(raw.get("facility_area") or "")

                stored_status = str(raw.get("status") or "").strip()
                if stored_status in ("paused", "in_progress"):
                    status = stored_status
                else:
                    status = compute_inspection_status(categories, general_results)

                evidence = count_evidence(categories)

                progress = None
                if status in ("in_progress", "paused", "pending"):
                    progress = compute_progress(categories)

                entry = {
                    "inspection_id": raw.get("inspection_id"),
                    "session_id": raw.get("session_id"),
                    "location": location,
                    "facility_area": facility_area,
                    "station": str(raw.get("station") or ""),
                    "type": type_label,
                    "date": date_of_audit,
                    "inspector": str(raw.get("auditor_name") or ""),
                    "evidence_count": evidence,
                    "status": status,
                    "created_at": str(raw.get("created_at") or ""),
                }
                if progress is not None:
                    entry["progress"] = progress
                result_list.append(entry)
            except Exception as e:
                logger.error(f"Error processing inspection record: {str(e)}")
                continue

        result_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        stats = {
            "total": len(result_list),
            "completed":   sum(1 for i in result_list if i["status"] == "completed"),
            "in_progress": sum(1 for i in result_list if i["status"] == "in_progress"),
            "paused":      sum(1 for i in result_list if i["status"] == "paused"),
            "pending":     sum(1 for i in result_list if i["status"] == "pending"),
            "overdue":     sum(1 for i in result_list if i["status"] == "overdue"),
        }

        return build_response(200, {"stats": stats, "inspections": result_list})

    except Exception as e:
        logger.exception(f"admin_list_inspections failed: {str(e)}")
        return build_response(500, {"error": f"Internal error: {str(e)}"})


def compute_inspection_status(categories, general_results):
    """Calculate inspection status: completed, in_progress, pending, or overdue."""
    answered = 0
    total = 0
    if not isinstance(categories, list):
        return "pending"
    for cat in categories:
        if not isinstance(cat, dict):
            continue
        for item in cat.get("items", []):
            if not isinstance(item, dict):
                continue
            iid = item.get("id")
            if isinstance(iid, int) and iid in (11, 12):
                continue
            total += 1
            answer = str(item.get("answer") or "").strip()
            if answer:
                answered += 1

    if total == 0:
        return "pending"
    if answered == total:
        return "completed"
    if answered > 0:
        return "in_progress"
    return "pending"


def count_evidence(categories):
    """Count total evidence items across all checklist items."""
    count = 0
    if not isinstance(categories, list):
        return 0
    for cat in categories:
        if not isinstance(cat, dict):
            continue
        for item in cat.get("items", []):
            if not isinstance(item, dict):
                continue
            evidence = item.get("evidence", [])
            count += len(evidence) if isinstance(evidence, list) else 0
    return count


def compute_progress(categories):
    """Compute completion progress for an inspection's categories."""
    total = 0
    answered = 0
    if not isinstance(categories, list):
        return {"total": 0, "answered": 0, "percentage": 0}
    for cat in categories:
        if not isinstance(cat, dict):
            continue
        for item in cat.get("items", []):
            if not isinstance(item, dict):
                continue
            iid = item.get("id")
            # Skip auto-calculated summary items (fire extinguisher items 11, 12)
            if isinstance(iid, int) and iid in (11, 12):
                continue
            total += 1
            if str(item.get("answer") or "").strip():
                answered += 1
        # Also handle sub_sections (OSHA checklist)
        for sub in cat.get("sub_sections", []):
            if not isinstance(sub, dict):
                continue
            for item in sub.get("items", []):
                if not isinstance(item, dict):
                    continue
                total += 1
                if str(item.get("answer") or "").strip():
                    answered += 1
    percentage = round((answered / total * 100)) if total > 0 else 0
    return {"total": total, "answered": answered, "percentage": percentage}


# ═══════════════════════════════════════════════
# API 8: PUT /api/companies/{company_key} — Update Company
# ═══════════════════════════════════════════════
def update_company(event):
    """Updates a company's name and/or state."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    body = parse_body(event)

    if not company_key:
        return build_response(400, {"error": "company_key is required"})

    # Check company exists
    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Company '{company_key}' not found"})

    # Update allowed fields
    updatable_fields = ["name", "state"]
    update_expr_parts = ["#updated_at = :updated_at"]
    attr_names = {"#updated_at": "updated_at"}
    attr_values = {":updated_at": now_iso()}

    for field in updatable_fields:
        if field in body:
            safe_name = f"#{field}"
            safe_value = f":{field}"
            update_expr_parts.append(f"{safe_name} = {safe_value}")
            attr_names[safe_name] = field
            attr_values[safe_value] = str(body[field]).strip()

    if len(update_expr_parts) <= 1:
        return build_response(400, {"error": "No valid fields to update. Allowed: name, state"})

    dashboard_table.update_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"},
        UpdateExpression="SET " + ", ".join(update_expr_parts),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
    )

    updated = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"}).get("Item", {})
    _invalidate_dashboard_cache()
    return build_response(200, {
        "key": company_key,
        "name": updated.get("name", ""),
        "state": updated.get("state", ""),
    })


# ═══════════════════════════════════════════════
# API 9: PUT /api/companies/{ck}/locations/{lk} — Update Location
# ═══════════════════════════════════════════════
def update_location(event):
    """Updates a location's details (name, state, address, city, zip, phone)."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    location_key = str(path_params.get("location_key", "")).strip()
    body = parse_body(event)

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key are required"})

    # Check location exists
    existing = dashboard_table.get_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}
    ).get("Item")
    if not existing:
        return build_response(404, {"error": f"Location '{location_key}' not found"})

    # Update allowed fields
    updatable_fields = ["name", "state", "address", "city", "zip", "phone"]
    update_expr_parts = ["#updated_at = :updated_at"]
    attr_names = {"#updated_at": "updated_at"}
    attr_values = {":updated_at": now_iso()}

    for field in updatable_fields:
        if field in body:
            safe_name = f"#{field}"
            safe_value = f":{field}"
            update_expr_parts.append(f"{safe_name} = {safe_value}")
            attr_names[safe_name] = field
            attr_values[safe_value] = str(body[field]).strip()

    if len(update_expr_parts) <= 1:
        return build_response(400, {"error": "No valid fields to update. Allowed: name, state, address, city, zip, phone"})

    dashboard_table.update_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"},
        UpdateExpression="SET " + ", ".join(update_expr_parts),
        ExpressionAttributeNames=attr_names,
        ExpressionAttributeValues=attr_values,
    )

    updated = dashboard_table.get_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}
    ).get("Item", {})
    _invalidate_dashboard_cache()

    return build_response(200, {
        "key": location_key,
        "name": updated.get("name", ""),
        "state": updated.get("state", ""),
        "address": updated.get("address", ""),
        "city": updated.get("city", ""),
        "zip": updated.get("zip", ""),
        "phone": updated.get("phone", ""),
    })


# ═══════════════════════════════════════════════
# GET /api/companies/{ck}/locations/{lk}/inspection-categories
# ═══════════════════════════════════════════════
def get_location_inspection_categories(event):
    """Returns all inspection categories with is_enabled for a location (admin toggles)."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    location_key = str(path_params.get("location_key", "")).strip()

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key are required"})

    item = _get_location_item(company_key, location_key)
    if not item:
        return build_response(404, {"error": f"Location '{location_key}' not found"})

    disabled = _disabled_categories_from_item(item)
    return build_response(200, {
        "company_key": company_key,
        "location_key": location_key,
        "disabled_categories": sorted(disabled),
        "inspection_categories": _list_inspection_categories(disabled),
    })


# ═══════════════════════════════════════════════
# PUT /api/companies/{ck}/locations/{lk}/toggle-category
# ═══════════════════════════════════════════════
def toggle_location_category(event):
    """
    Enable or disable an inspection category for a specific location.

    Body: {"category_key": "eyewash", "enabled": false}
    """
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    location_key = str(path_params.get("location_key", "")).strip()
    body = parse_body(event)

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key are required"})

    category_key = str(body.get("category_key", "")).strip()
    enabled = body.get("enabled")

    if not category_key:
        return build_response(400, {"error": "category_key is required"})
    if category_key not in VALID_CATEGORY_KEYS:
        return build_response(400, {
            "error": f"Invalid category_key. Must be one of: {sorted(VALID_CATEGORY_KEYS)}",
        })
    if enabled is None or not isinstance(enabled, bool):
        return build_response(400, {"error": "enabled must be true or false"})

    item = _get_location_item(company_key, location_key)
    if not item:
        return build_response(404, {"error": f"Location '{location_key}' not found"})

    disabled = list(_disabled_categories_from_item(item))
    if enabled:
        disabled = [x for x in disabled if x != category_key]
    elif category_key not in disabled:
        disabled.append(category_key)

    now = now_iso()
    dashboard_table.update_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"},
        UpdateExpression="SET disabled_categories = :dc, updated_at = :ua",
        ExpressionAttributeValues={
            ":dc": disabled,
            ":ua": now,
        },
    )
    _invalidate_dashboard_cache()

    disabled_set = set(disabled)
    action = "enabled" if enabled else "disabled"
    return build_response(200, {
        "message": f"Category '{category_key}' {action} for location '{location_key}'",
        "company_key": company_key,
        "location_key": location_key,
        "disabled_categories": sorted(disabled_set),
        "inspection_categories": _list_inspection_categories(disabled_set),
        "updated_at": now,
    })


# ═══════════════════════════════════════════════
# API 10: DELETE /api/companies/{company_key}
# ═══════════════════════════════════════════════
def delete_company(event):
    """Deletes a company and all its locations and stations.
    OPTIMIZED: Uses targeted DynamoDB queries by PK instead of full table scan."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required"})

    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Company '{company_key}' not found"})

    # Query locations under this company (PK=COMPANY#ck, SK begins_with LOCATION#)
    loc_resp = dashboard_table.query(
        KeyConditionExpression=Key("PK").eq(f"COMPANY#{company_key}") & Key("SK").begins_with("LOCATION#"),
    )
    location_items = loc_resp.get("Items", [])

    deleted_locations = []
    deleted_stations = []

    for loc_item in location_items:
        lk = loc_item["SK"][9:]  # strip "LOCATION#"
        deleted_locations.append(lk)
        dashboard_table.delete_item(Key={"PK": loc_item["PK"], "SK": loc_item["SK"]})

        # Query stations under this location (PK=LOCATION#lk, SK begins_with STATION#)
        st_resp = dashboard_table.query(
            KeyConditionExpression=Key("PK").eq(f"LOCATION#{lk}") & Key("SK").begins_with("STATION#"),
        )
        for st_item in st_resp.get("Items", []):
            dashboard_table.delete_item(Key={"PK": st_item["PK"], "SK": st_item["SK"]})
            deleted_stations.append(st_item.get("station_id", ""))

    dashboard_table.delete_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"})

    # Remove reseller→company association if any
    assoc_resp = dashboard_table.scan(
        FilterExpression=Attr("SK").eq(f"COMPANY#{company_key}") & Attr("PK").begins_with("RESELLER#"),
        ProjectionExpression="PK, SK",
    )
    for assoc in assoc_resp.get("Items", []):
        dashboard_table.delete_item(Key={"PK": assoc["PK"], "SK": assoc["SK"]})

    _invalidate_dashboard_cache()

    return build_response(200, {
        "message": "Company deleted successfully",
        "company_key": company_key,
        "deleted_locations": deleted_locations,
        "deleted_stations": deleted_stations,
    })


# ═══════════════════════════════════════════════
# API 9: DELETE /api/companies/{ck}/locations/{lk}
# ═══════════════════════════════════════════════
def delete_location(event):
    """Deletes a location and all its stations.
    OPTIMIZED: Uses targeted DynamoDB query by PK instead of full table scan."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    location_key = str(path_params.get("location_key", "")).strip()

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key are required"})

    existing = dashboard_table.get_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}
    ).get("Item")
    if not existing:
        return build_response(404, {"error": f"Location '{location_key}' not found"})

    # Query all stations under this location (PK=LOCATION#lk)
    st_resp = dashboard_table.query(
        KeyConditionExpression=Key("PK").eq(f"LOCATION#{location_key}") & Key("SK").begins_with("STATION#"),
    )
    deleted_stations = []
    for st_item in st_resp.get("Items", []):
        dashboard_table.delete_item(Key={"PK": st_item["PK"], "SK": st_item["SK"]})
        deleted_stations.append(st_item.get("station_id", ""))

    dashboard_table.delete_item(Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"})
    _invalidate_dashboard_cache()

    return build_response(200, {
        "message": "Location deleted successfully",
        "location_key": location_key,
        "deleted_stations": deleted_stations,
    })


# ═══════════════════════════════════════════════
# API 10: DELETE /api/stations/{station_id}
# ═══════════════════════════════════════════════
def delete_station(event):
    """Deletes a single station.
    OPTIMIZED: Uses StationIdIndex GSI → parsed get_item → scan fallback."""
    path_params = event.get("pathParameters") or {}
    station_id = str(path_params.get("station_id", "")).strip()

    if not station_id:
        return build_response(400, {"error": "station_id is required"})

    station_item = _find_station_by_id(station_id)

    if not station_item:
        return build_response(404, {"error": f"Station '{station_id}' not found"})

    dashboard_table.delete_item(Key={"PK": station_item["PK"], "SK": station_item["SK"]})
    _invalidate_dashboard_cache()

    return build_response(200, {
        "message": "Station deleted successfully",
        "station_id": station_id,
    })


# ═══════════════════════════════════════════════
# Mapping: Dashboard typeKey → Inspection table label
# ═══════════════════════════════════════════════
TYPEKEY_TO_INSPECTION_LABEL = {
    "eyewash":       "Eyewash",
    "fire":          "Fire Extinguisher",
    "exitdoor":      "Exit Door",
    "racking":       "Monthly Racking",
    "hra":           "Quarterly HRA",
    "recordkeeping": "Recordkeeping",
}


# ═══════════════════════════════════════════════
# Mobile API: GET /api/mobile/inspection-status
# ═══════════════════════════════════════════════
def mobile_inspection_status(event):
    """
    Returns today's inspection workload for a specific location, grouped by
    category, with per-station completion status and a progress summary.

    Powers the mobile app's "inspection home screen."

    Query Parameters:
        company_key    (recommended) — Enables fast O(1) location lookup
        location_key   (required) — Which location (e.g. "austin-tx")
        category       (optional) — Filter to a single category typeKey
        auditor_name   (optional) — Filter progress to a specific inspector
    """

    try:
        t_start = _time.monotonic()
        params = event.get("queryStringParameters", {}) or {}
        company_key = str(params.get("company_key", "") or "").strip()
        location_key = str(params.get("location_key", "") or "").strip()
        filter_category = str(params.get("category", "") or "").strip()
        filter_auditor = str(params.get("auditor_name", "") or "").strip()

        if not location_key:
            return build_response(400, {"error": "location_key query parameter is required"})

        # Validate category if provided
        valid_type_keys = {st["key"] for st in STATION_TYPES}
        if filter_category and filter_category not in valid_type_keys:
            return build_response(400, {
                "error": f"Invalid category '{filter_category}'. Must be one of: {', '.join(sorted(valid_type_keys))}"
            })

        # ── Step 1: Resolve location (fast get_item when company_key provided) ──
        t_loc = _time.monotonic()
        location_item, company_key = _resolve_location_item(company_key, location_key)
        location_ms = int((_time.monotonic() - t_loc) * 1000)

        if not location_item:
            return build_response(404, {"error": f"Location '{location_key}' not found"})

        location_name = location_item.get("name", location_key)
        disabled_categories = _disabled_categories_from_item(location_item)

        if filter_category and filter_category in disabled_categories:
            return build_response(403, {
                "error": f"Category '{filter_category}' is disabled at this location",
            })

        # 1b. Query stations directly by PK — fast DynamoDB query (not a scan)
        t_st = _time.monotonic()
        station_resp = dashboard_table.query(
            KeyConditionExpression=Key("PK").eq(f"LOCATION#{location_key}"),
            ProjectionExpression="SK, station_id, #n, typeKey, nextDue, #s, lastInspected",
            ExpressionAttributeNames={"#n": "name", "#s": "status"},
        )
        station_items = convert_decimals(station_resp.get("Items", []))
        stations_ms = int((_time.monotonic() - t_st) * 1000)

        # Collect all stations under this location (skip disabled categories)
        location_stations = []
        for item in station_items:
            sk = item.get("SK", "")
            if not sk.startswith("STATION#"):
                continue
            type_key = item.get("typeKey", "")
            if type_key in disabled_categories:
                continue
            # Apply category filter if specified
            if filter_category and type_key != filter_category:
                continue
            location_stations.append({
                "station_id": item.get("station_id", sk.replace("STATION#", "")),
                "station_name": item.get("name", ""),
                "type_key": type_key,
                "next_due": item.get("nextDue", ""),
                "equipment_status": item.get("status", "ok"),
                "lastInspected": item.get("lastInspected", ""),
            })

        logger.info(f"[MOBILE] Location '{location_key}': {len(location_stations)} stations found")

        # ── Step 2: Scan inspections from all 6 tables in PARALLEL ──
        # Determine current month range for filtering (monthly scope per spec)
        today = datetime.now(timezone.utc)
        today_str = today.strftime("%Y-%m-%d")
        month_start_str = today.strftime("%Y-%m-01")
        # Calculate last day of current month
        if today.month == 12:
            _next_month = today.replace(year=today.year + 1, month=1, day=1)
        else:
            _next_month = today.replace(month=today.month + 1, day=1)
        month_end_str = (_next_month - timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(f"[MOBILE] Filtering inspections for month: {month_start_str} to {month_end_str}")

        all_inspections = []

        def _query_or_scan_inspection_table(type_label, ddb_table):
            """Use LocationDateIndex GSI query when available, otherwise scan with filter.
            GSI query reads only matching records — O(K) instead of O(N)."""
            try:
                items = []

                # FAST PATH: GSI query by facility_area + date range
                if location_name:
                    try:
                        kce = Key("facility_area").eq(location_name) & \
                              Key("date_of_audit").between(month_start_str, month_end_str)
                        query_kwargs = {
                            "IndexName": "LocationDateIndex",
                            "KeyConditionExpression": kce,
                        }
                        resp = ddb_table.query(**query_kwargs)
                        items.extend(resp.get("Items", []))
                        while "LastEvaluatedKey" in resp:
                            query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
                            resp = ddb_table.query(**query_kwargs)
                            items.extend(resp.get("Items", []))

                        if items:
                            return [(convert_decimals(item), type_label) for item in items]
                        else:
                            logger.info(
                                f"[MOBILE] GSI returned 0 for facility_area='{location_name}' "
                                f"on {type_label}, falling back to scan"
                            )
                    except Exception as gsi_err:
                        err_code = getattr(gsi_err, "response", {}).get("Error", {}).get("Code", "")
                        if err_code == "ValidationException" and "LocationDateIndex" in str(gsi_err):
                            logger.warning(f"[MOBILE] LocationDateIndex not ready on {type_label}, falling back to scan")
                        else:
                            raise

                # FALLBACK: scan with FilterExpression
                scan_kwargs = {
                    "FilterExpression": Attr("date_of_audit").between(
                        month_start_str, month_end_str
                    ) & (
                        Attr("facility_area").eq(location_name)
                        | Attr("location").eq(location_name)
                    ),
                }
                resp = ddb_table.scan(**scan_kwargs)
                items.extend(resp.get("Items", []))
                while "LastEvaluatedKey" in resp:
                    scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
                    resp = ddb_table.scan(**scan_kwargs)
                    items.extend(resp.get("Items", []))
                return [(convert_decimals(item), type_label) for item in items]
            except Exception as e:
                logger.error(f"[MOBILE] Error scanning {type_label}: {str(e)}")
                return []

        # Only query inspection tables for enabled categories
        enabled_type_keys = [
            st["key"] for st in STATION_TYPES if st["key"] not in disabled_categories
        ]
        tables_to_scan = {}
        if filter_category:
            if filter_category not in disabled_categories:
                label = TYPEKEY_TO_INSPECTION_LABEL.get(filter_category)
                if label and label in inspection_tables:
                    tables_to_scan[label] = inspection_tables[label]
        else:
            for tk in enabled_type_keys:
                label = TYPEKEY_TO_INSPECTION_LABEL.get(tk)
                if label and label in inspection_tables:
                    tables_to_scan[label] = inspection_tables[label]

        t_insp = _time.monotonic()
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(_query_or_scan_inspection_table, label, tbl): label
                for label, tbl in tables_to_scan.items()
            }
            for future in as_completed(futures):
                all_inspections.extend(future.result())

        inspections_ms = int((_time.monotonic() - t_insp) * 1000)
        logger.info(f"[MOBILE] Total inspections fetched (this month): {len(all_inspections)}")

        # ── Step 3: Build station_id → inspection mapping for this month ──
        # An inspection matches a station if:
        #   (a) station_id field matches (preferred, new flow), OR
        #   (b) station name matches (fallback, legacy flow)
        # AND the inspection date_of_audit is within the current month
        #     (already filtered in Step 2 by DynamoDB FilterExpression)
        # AND (if auditor_name filter) the auditor matches

        station_id_set = {s["station_id"] for s in location_stations}
        # OPTIMIZED: Pre-build name→id dict for O(1) lookup instead of O(N) inner loop
        station_name_to_id = {s["station_name"].lower(): s["station_id"] for s in location_stations}

        # Map: station_id → best matching inspection record
        station_inspection_map = {}  # station_id → {inspection_id, status, created_at, completed_at}

        for raw, type_label in all_inspections:
            try:
                date_of_audit = str(raw.get("date_of_audit") or "")
                # Safety check: ensure within current month (already filtered server-side)
                if date_of_audit < month_start_str or date_of_audit > month_end_str:
                    continue

                # Filter by auditor if specified
                if filter_auditor:
                    auditor = str(raw.get("auditor_name") or "").strip()
                    if filter_auditor.lower() != auditor.lower():
                        continue

                # Filter by location — match against location or facility_area
                insp_location = str(raw.get("location") or "").strip()
                insp_facility = str(raw.get("facility_area") or "").strip()
                location_match = (
                    location_name.lower() in (insp_location.lower(), insp_facility.lower())
                    if location_name else False
                )
                if not location_match:
                    continue

                # Try to match by station_id first, then by station name
                insp_station_id = str(raw.get("station_id") or "").strip()
                insp_station_name = str(raw.get("station") or "").strip()

                matched_station_id = None
                if insp_station_id and insp_station_id in station_id_set:
                    matched_station_id = insp_station_id
                elif insp_station_name:
                    # Fallback: O(1) dict lookup by station name (was O(N) loop)
                    matched_station_id = station_name_to_id.get(insp_station_name.lower())

                if not matched_station_id:
                    continue

                # Compute status
                categories = raw.get("categories") or []
                general_results = raw.get("general_results") or []
                stored_status = str(raw.get("status") or "").strip()

                if stored_status in ("paused", "in_progress"):
                    status = "started"
                else:
                    computed = compute_inspection_status(categories, general_results)
                    if computed == "completed":
                        status = "completed"
                    elif computed == "in_progress":
                        status = "started"
                    else:
                        status = "started"  # Record exists but no answers = started

                created_at = str(raw.get("created_at") or "")
                completed_at = str(raw.get("completed_at") or "") if raw.get("completed_at") else None

                # If status is completed but no completed_at, use created_at as fallback
                if status == "completed" and not completed_at:
                    completed_at = created_at

                inspection_id = str(raw.get("inspection_id") or "")
                session_id_val = str(raw.get("session_id") or "")

                # Keep the most recent inspection if multiple exist
                existing = station_inspection_map.get(matched_station_id)
                if not existing or created_at > existing.get("started_at", ""):
                    station_inspection_map[matched_station_id] = {
                        "inspection_id": inspection_id,
                        "session_id": session_id_val,
                        "status": status,
                        "started_at": created_at,
                        "completed_at": completed_at,
                    }
            except Exception as e:
                logger.error(f"[MOBILE] Error matching inspection: {str(e)}")
                continue

        # ── Step 4: Build grouped response ──
        # Group stations by typeKey (category)
        categories_map = {}  # typeKey → list of station dicts
        for station in location_stations:
            tk = station["type_key"]
            if tk not in categories_map:
                categories_map[tk] = []

            sid = station["station_id"]
            insp = station_inspection_map.get(sid)

            # Derive equipment_status: if inspection is completed this month,
            # override the stored dashboard status to "ok" + update lastInspected
            stored_eq_status = station.get("equipment_status", "ok")
            stored_last_inspected = station.get("lastInspected", "")
            next_due = station.get("next_due", "")

            if insp and insp["status"] == "completed":
                derived_eq_status = "ok"
                derived_last_inspected = insp.get("completed_at", today_str)[:10] if insp.get("completed_at") else today_str
            else:
                derived_eq_status = stored_eq_status
                derived_last_inspected = stored_last_inspected

            if insp:
                station_entry = {
                    "station_id": sid,
                    "station_name": station["station_name"],
                    "status": insp["status"],
                    "equipment_status": derived_eq_status,
                    "lastInspected": derived_last_inspected,
                    "nextDue": next_due,
                    "inspection_id": insp["inspection_id"],
                    "session_id": insp.get("session_id", ""),
                    "started_at": insp["started_at"],
                    "completed_at": insp["completed_at"],
                }
            else:
                # No inspection found this month — determine if pending or overdue
                # A station is "overdue" if its nextDue date has passed and no
                # inspection exists for the current month.
                if next_due and next_due < today_str:
                    no_insp_status = "overdue"
                else:
                    no_insp_status = "pending"

                station_entry = {
                    "station_id": sid,
                    "station_name": station["station_name"],
                    "status": no_insp_status,
                    "equipment_status": stored_eq_status,
                    "lastInspected": stored_last_inspected,
                    "nextDue": next_due,
                    "inspection_id": None,
                    "session_id": None,
                    "started_at": None,
                    "completed_at": None,
                }

            categories_map[tk].append(station_entry)

        # Build the categories array in the fixed STATION_TYPES order
        categories_response = []
        total_all = 0
        completed_all = 0
        started_all = 0
        pending_all = 0
        overdue_all = 0

        for st_type in STATION_TYPES:
            tk = st_type["key"]
            # Skip disabled categories for mobile
            if tk in disabled_categories:
                continue
            # Skip if category filter is active and this isn't the filtered category
            if filter_category and tk != filter_category:
                continue

            stations_list = categories_map.get(tk, [])
            cat_completed = sum(1 for s in stations_list if s["status"] == "completed")
            cat_started = sum(1 for s in stations_list if s["status"] == "started")
            cat_pending = sum(1 for s in stations_list if s["status"] == "pending")
            cat_overdue = sum(1 for s in stations_list if s["status"] == "overdue")
            cat_total = len(stations_list)

            total_all += cat_total
            completed_all += cat_completed
            started_all += cat_started
            pending_all += cat_pending
            overdue_all += cat_overdue

            categories_response.append({
                "category_key": tk,
                "category_name": st_type["label"],
                "counts": {
                    "total": cat_total,
                    "completed": cat_completed,
                    "started": cat_started,
                    "pending": cat_pending,
                    "overdue": cat_overdue,
                },
                "stations": stations_list,
            })

        # Build summary
        percent_complete = round((completed_all / total_all * 100)) if total_all > 0 else 0

        response = {
            "location_key": location_key,
            "location_name": location_name,
            "company_key": company_key,
            "date": today.strftime("%Y-%m-%d"),
            "summary": {
                "total": total_all,
                "completed": completed_all,
                "started": started_all,
                "pending": pending_all,
                "overdue": overdue_all,
                "percent_complete": percent_complete,
            },
            "categories": categories_response,
        }

        total_ms = int((_time.monotonic() - t_start) * 1000)
        logger.info(
            "[MOBILE] location=%dms stations=%dms inspections=%dms total=%dms enabled_categories=%d",
            location_ms, stations_ms, inspections_ms, total_ms, len(enabled_type_keys),
        )

        return build_response(200, response)

    except Exception as e:
        logger.exception(f"[MOBILE] mobile_inspection_status failed: {str(e)}")
        return build_response(500, {"error": f"Internal error: {str(e)}"})


# ═══════════════════════════════════════════════
# CENTRALIZED SESSION API
# Replaces per-Lambda pause/resume with unified auto-save + draft discovery
# ═══════════════════════════════════════════════

def update_session_status(event):
    """
    POST /api/sessions/status

    Updates the real-time status of a session (e.g. in_progress, completed).
    Replaces the old autosave, find-draft, and resume endpoints.

    Body: {
        "session_id": "uuid",
        "status": "in_progress" | "completed"
    }
    """
    body = parse_body(event)
    session_id = str(body.get("session_id", "")).strip()
    status = str(body.get("status", "")).strip()

    if not session_id:
        return build_response(400, {"error": "session_id is required"})
    if not status:
        return build_response(400, {"error": "status is required"})

    # Load session
    session_resp = session_table.get_item(Key={"session_id": session_id})
    session = session_resp.get("Item")
    if not session:
        return build_response(404, {"error": f"Session '{session_id}' not found"})

    inspection_id = str(session.get("inspection_id", "") or "").strip()
    if not inspection_id:
        return build_response(400, {"error": "No inspection linked to this session. Start an inspection first."})

    # Update session table
    session_table.update_item(
        Key={"session_id": session_id},
        UpdateExpression="SET #st = :st, updated_at = :u",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":st": status,
            ":u": now_iso(),
        },
    )

    # Also update the inspection table status
    inspection_type = _resolve_session_inspection_type(session, session_id, inspection_id)
    insp_table = _get_inspection_table(inspection_type)
    if insp_table:
        update_expr = "SET #st = :st, updated_at = :u"
        expr_vals = {":st": status, ":u": now_iso()}
        
        if status == "completed":
            update_expr += ", completed_at = :c"
            expr_vals[":c"] = now_iso()
            
        try:
            insp_table.update_item(
                Key={"inspection_id": inspection_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues=expr_vals,
            )
        except Exception as e:
            logger.warning(f"[SESSION] Failed to update inspection status: {e}")

    _invalidate_dashboard_cache()

    return build_response(200, {
        "updated": True,
        "session_id": session_id,
        "status": status
    })


def get_inspection_details(event):
    """
    GET /api/inspections/{inspection_id}/details

    Returns full inspection details by inspection_id.
    Searches across all 6 inspection tables to find the record.
    """
    path_params = event.get("pathParameters") or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})

    # Search all inspection tables in parallel
    found_inspection = None
    found_type = None

    def _lookup(label, table):
        try:
            resp = table.get_item(Key={"inspection_id": inspection_id})
            item = resp.get("Item")
            if item:
                return (convert_decimals(item), label)
        except Exception as e:
            logger.warning(f"[DETAILS] Error looking up {label}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_lookup, label, tbl): label for label, tbl in inspection_tables.items()}
        for future in as_completed(futures):
            result = future.result()
            if result:
                found_inspection, found_type = result

    if not found_inspection:
        return build_response(404, {"error": f"Inspection '{inspection_id}' not found"})

    # Find the inspection_type for progress computation
    type_to_insp = {v: k for k, v in INSPECTION_TYPE_TO_LABEL.items()}
    inspection_type = type_to_insp.get(found_type, "")
    type_to_category = {v: k for k, v in CATEGORY_TO_INSPECTION_TYPE.items()}
    category_key = type_to_category.get(inspection_type, "")

    progress = _session_progress(found_inspection, inspection_type)

    return build_response(200, {
        "inspection_id": inspection_id,
        "session_id": found_inspection.get("session_id", ""),
        "type": found_type,
        "category": category_key,
        "status": found_inspection.get("status", ""),
        "progress": progress,
        "auditor_name": found_inspection.get("auditor_name", ""),
        "facility_area": found_inspection.get("facility_area", ""),
        "station": found_inspection.get("station", ""),
        "station_id": found_inspection.get("station_id", ""),
        "date_of_audit": found_inspection.get("date_of_audit", ""),
        "categories": found_inspection.get("categories", []),
        "general_results": found_inspection.get("general_results", []),
        "notes": found_inspection.get("notes", ""),
        "created_at": found_inspection.get("created_at", ""),
        "updated_at": found_inspection.get("updated_at", ""),
        "completed_at": found_inspection.get("completed_at", ""),
    })


# ═══════════════════════════════════════════════
# Main Handler — Routes to correct function
# ═══════════════════════════════════════════════
def lambda_handler(event, context):
    """
    Main entry point. Routes based on HTTP method and path.
    """
    try:
        http_method = event.get("httpMethod", "")
        resource = event.get("resource", "")
        path = event.get("path", "")

        logger.info(f"Dashboard: {http_method} {resource} (path: {path})")

        # CORS preflight
        if http_method == "OPTIONS":
            return build_response(200, {"message": "CORS preflight OK"})

        # API Key validation
        auth_error = require_api_key(event)
        if auth_error:
            return auth_error

        # ── GET /api/resellers ──
        if http_method == "GET" and resource == "/api/resellers":
            return get_resellers(event)

        # ── POST /api/resellers ──
        elif http_method == "POST" and resource == "/api/resellers":
            return create_reseller(event)

        # ── PUT /api/resellers/{reseller_key} ──
        elif http_method == "PUT" and resource == "/api/resellers/{reseller_key}":
            return update_reseller(event)

        # ── DELETE /api/resellers/{reseller_key} ──
        elif http_method == "DELETE" and resource == "/api/resellers/{reseller_key}":
            return delete_reseller(event)


        # ── GET /api/companies ──
        elif http_method == "GET" and resource == "/api/companies":
            return get_companies(event)

        # ── POST /api/companies ──
        elif http_method == "POST" and resource == "/api/companies":
            return create_company(event)

        # ── POST /api/companies/{ck}/locations ──
        elif http_method == "POST" and resource == "/api/companies/{company_key}/locations":
            return create_location(event)

        # ── POST /api/companies/{ck}/locations/{lk}/stations ──
        elif http_method == "POST" and resource == "/api/companies/{company_key}/locations/{location_key}/stations":
            return create_station(event)

        # ── PUT /api/companies/{company_key} ──
        elif http_method == "PUT" and resource == "/api/companies/{company_key}":
            return update_company(event)

        # ── PUT /api/companies/{ck}/locations/{lk} ──
        elif http_method == "PUT" and resource == "/api/companies/{company_key}/locations/{location_key}":
            return update_location(event)

        # ── GET /api/companies/{ck}/locations/{lk}/inspection-categories ──
        elif http_method == "GET" and (
            resource == "/api/companies/{company_key}/locations/{location_key}/inspection-categories"
            or path.rstrip("/").endswith("/inspection-categories")
        ):
            return get_location_inspection_categories(event)

        # ── PUT /api/companies/{ck}/locations/{lk}/toggle-category ──
        elif http_method == "PUT" and (
            resource == "/api/companies/{company_key}/locations/{location_key}/toggle-category"
            or path.rstrip("/").endswith("/toggle-category")
        ):
            return toggle_location_category(event)

        # ── PUT /api/stations/{station_id} ──
        elif http_method == "PUT" and resource == "/api/stations/{station_id}":
            return update_station(event)

        # ── DELETE /api/companies/{company_key} ──
        elif http_method == "DELETE" and resource == "/api/companies/{company_key}":
            return delete_company(event)

        # ── DELETE /api/companies/{ck}/locations/{lk} ──
        elif http_method == "DELETE" and resource == "/api/companies/{company_key}/locations/{location_key}":
            return delete_location(event)

        # ── DELETE /api/stations/{station_id} ──
        elif http_method == "DELETE" and resource == "/api/stations/{station_id}":
            return delete_station(event)

        # ── GET /api/alerts ──
        elif http_method == "GET" and resource == "/api/alerts":
            return get_alerts(event)

        # ── GET /api/mobile/inspection-status ──
        elif http_method == "GET" and (
            resource == "/api/mobile/inspection-status"
            or path.rstrip("/").endswith("/api/mobile/inspection-status")
        ):
            logger.info("[MOBILE] Entering mobile_inspection_status")
            return mobile_inspection_status(event)

        # ── GET /api/inspections (was /admin/inspections) ──
        elif http_method == "GET" and (
            resource == "/api/inspections"
            or resource == "/admin/inspections"
            or path.rstrip("/").endswith("/api/inspections")
            or path.rstrip("/").endswith("/admin/inspections")
        ):
            logger.info("[ADMIN] Entering admin_list_inspections")
            return admin_list_inspections(event)

        # ═══════════════════════════════════════════════
        # Centralized Session Endpoints
        # ═══════════════════════════════════════════════

        # ── POST /api/sessions/status ──
        elif http_method == "POST" and (
            resource == "/api/sessions/status"
            or path.rstrip("/").endswith("/api/sessions/status")
        ):
            logger.info("[SESSION] Entering update_session_status")
            return update_session_status(event)

        # ── GET /api/inspections/{inspection_id}/details ──
        elif http_method == "GET" and (
            resource == "/api/inspections/{inspection_id}/details"
            or "/api/inspections/" in path and path.rstrip("/").endswith("/details")
        ):
            logger.info("[SESSION] Entering get_inspection_details")
            return get_inspection_details(event)

        else:
            return build_response(404, {"error": f"Route not found: {http_method} {resource} (path: {path})"})

    except Exception as e:
        logger.exception(f"FATAL handler crash: {str(e)}")
        return build_response(500, {"error": f"Handler crash: {str(e)}"})
