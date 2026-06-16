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
from datetime import datetime, timezone
from decimal import Decimal

import boto3

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


# ═══════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════

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


# ═══════════════════════════════════════════════
# RESELLER API 1: GET /api/resellers — Full Nested Tree
# ═══════════════════════════════════════════════
def get_resellers(event):
    """
    Returns the full Reseller → Company → Location → StationType → Station tree.
    This is the primary endpoint for the sidebar navigation.
    """
    all_items = convert_decimals(scan_full_table(dashboard_table))

    # Separate items by type
    resellers = {}         # reseller_key → reseller dict
    reseller_companies = {}  # reseller_key → [company_key, ...]
    companies = {}         # company_key → company dict
    locations = {}         # company_key → [location dicts]
    stations = {}          # location_key → [station dicts]

    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if sk == "METADATA" and pk.startswith("RESELLER#"):
            rk = pk.replace("RESELLER#", "")
            resellers[rk] = {
                "key": rk,
                "name": item.get("name", ""),
                "companies": [],
            }

        elif sk.startswith("COMPANY#") and pk.startswith("RESELLER#"):
            rk = pk.replace("RESELLER#", "")
            ck = sk.replace("COMPANY#", "")
            reseller_companies.setdefault(rk, []).append(ck)

        elif sk == "METADATA" and pk.startswith("COMPANY#"):
            ck = pk.replace("COMPANY#", "")
            companies[ck] = {
                "key": ck,
                "name": item.get("name", ""),
                "state": item.get("state", ""),
                "locations": [],
            }

        elif sk.startswith("LOCATION#") and pk.startswith("COMPANY#"):
            ck = pk.replace("COMPANY#", "")
            lk = sk.replace("LOCATION#", "")
            loc = {
                "key": lk,
                "name": item.get("name", ""),
                "state": item.get("state", ""),
                "address": item.get("address", ""),
                "city": item.get("city", ""),
                "zip": item.get("zip", ""),
                "phone": item.get("phone", ""),
                "_company_key": ck,
            }
            locations.setdefault(ck, []).append(loc)

        elif sk.startswith("STATION#") and pk.startswith("LOCATION#"):
            lk = pk.replace("LOCATION#", "")
            station = {
                "id": item.get("station_id", sk.replace("STATION#", "")),
                "name": item.get("name", ""),
                "route": item.get("route", ""),
                "status": item.get("status", "ok"),
                "lastInspected": item.get("lastInspected", ""),
                "nextDue": item.get("nextDue", ""),
                "notes": item.get("notes", ""),
                "typeKey": item.get("typeKey", ""),
                "_location_key": lk,
            }
            stations.setdefault(lk, []).append(station)

    # Helper: build location with nested station types
    def _build_location(loc):
        loc_key = loc["key"]
        loc_stations = stations.get(loc_key, [])
        station_types = []
        for st_type in STATION_TYPES:
            type_stations = [
                {k: v for k, v in s.items() if not k.startswith("_")}
                for s in loc_stations
                if s.get("typeKey") == st_type["key"]
            ]
            station_types.append({
                "key": st_type["key"],
                "label": st_type["label"],
                "icon": st_type["icon"],
                "stations": type_stations,
            })
        clean_loc = {k: v for k, v in loc.items() if not k.startswith("_")}
        clean_loc["stationTypes"] = station_types
        return clean_loc

    # Helper: build company with nested locations
    def _build_company(ck):
        company = companies.get(ck)
        if not company:
            return None
        company_copy = {
            "key": company["key"],
            "name": company["name"],
            "state": company["state"],
            "locations": [],
        }
        for loc in locations.get(ck, []):
            company_copy["locations"].append(_build_location(loc))
        return company_copy

    # Build the full reseller tree
    result = []
    for rk, reseller in resellers.items():
        company_keys = reseller_companies.get(rk, [])
        for ck in company_keys:
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

    return build_response(200, {"key": reseller_key, "name": new_name})


# ═══════════════════════════════════════════════
# RESELLER API 4: DELETE /api/resellers/{reseller_key} — Cascade Delete
# ═══════════════════════════════════════════════
def delete_reseller(event):
    """
    Deletes a reseller and CASCADE-DELETES all associated companies,
    their locations, and their stations.
    """
    path_params = event.get("pathParameters") or {}
    reseller_key = str(path_params.get("reseller_key", "")).strip()

    if not reseller_key:
        return build_response(400, {"error": "reseller_key is required"})

    existing = dashboard_table.get_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Reseller '{reseller_key}' not found"})

    all_items = scan_full_table(dashboard_table)

    # Find all company keys associated with this reseller
    company_keys = []
    for item in all_items:
        if item.get("PK") == f"RESELLER#{reseller_key}" and item.get("SK", "").startswith("COMPANY#"):
            ck = item["SK"].replace("COMPANY#", "")
            company_keys.append(ck)
            # Delete the association record
            dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})

    # Cascade: delete each company and its children
    deleted_companies = []
    deleted_locations = []
    deleted_stations = []

    for ck in company_keys:
        # Find locations for this company
        loc_keys = []
        for item in all_items:
            if item.get("PK") == f"COMPANY#{ck}" and item.get("SK", "").startswith("LOCATION#"):
                lk = item["SK"].replace("LOCATION#", "")
                loc_keys.append(lk)
                dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
                deleted_locations.append(lk)

        # Delete stations under those locations
        for item in all_items:
            if item.get("PK", "").startswith("LOCATION#") and item.get("SK", "").startswith("STATION#"):
                lk = item["PK"].replace("LOCATION#", "")
                if lk in loc_keys:
                    dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
                    deleted_stations.append(item.get("station_id", ""))

        # Delete the company itself
        dashboard_table.delete_item(Key={"PK": f"COMPANY#{ck}", "SK": "METADATA"})
        deleted_companies.append(ck)

    # Delete the reseller itself
    dashboard_table.delete_item(Key={"PK": f"RESELLER#{reseller_key}", "SK": "METADATA"})

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
    This is the primary endpoint for the sidebar navigation.
    """
    all_items = convert_decimals(scan_full_table(dashboard_table))

    # Separate items by type
    companies = {}   # key → company dict
    locations = {}   # company_key → [location dicts]
    stations = {}    # location_key → [station dicts]

    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if sk == "METADATA" and pk.startswith("COMPANY#"):
            company_key = pk.replace("COMPANY#", "")
            companies[company_key] = {
                "key": company_key,
                "name": item.get("name", ""),
                "state": item.get("state", ""),
                "locations": [],
            }

        elif sk.startswith("LOCATION#") and pk.startswith("COMPANY#"):
            company_key = pk.replace("COMPANY#", "")
            location_key = sk.replace("LOCATION#", "")
            loc = {
                "key": location_key,
                "name": item.get("name", ""),
                "state": item.get("state", ""),
                "address": item.get("address", ""),
                "city": item.get("city", ""),
                "zip": item.get("zip", ""),
                "phone": item.get("phone", ""),
                "_company_key": company_key,
            }
            locations.setdefault(company_key, []).append(loc)

        elif sk.startswith("STATION#") and pk.startswith("LOCATION#"):
            location_key = pk.replace("LOCATION#", "")
            station = {
                "id": item.get("station_id", sk.replace("STATION#", "")),
                "name": item.get("name", ""),
                "route": item.get("route", ""),
                "status": item.get("status", "ok"),
                "lastInspected": item.get("lastInspected", ""),
                "nextDue": item.get("nextDue", ""),
                "notes": item.get("notes", ""),
                "typeKey": item.get("typeKey", ""),
                "_location_key": location_key,
            }
            stations.setdefault(location_key, []).append(station)

    # Build the nested tree
    result = []
    for company_key, company in companies.items():
        company_locations = locations.get(company_key, [])

        for loc in company_locations:
            loc_key = loc["key"]
            loc_stations = stations.get(loc_key, [])

            # Group stations by typeKey into the 5 fixed categories
            station_types = []
            for st_type in STATION_TYPES:
                type_stations = [
                    {k: v for k, v in s.items() if not k.startswith("_")}
                    for s in loc_stations
                    if s.get("typeKey") == st_type["key"]
                ]
                station_types.append({
                    "key": st_type["key"],
                    "label": st_type["label"],
                    "icon": st_type["icon"],
                    "stations": type_stations,
                })

            # Remove internal keys
            clean_loc = {k: v for k, v in loc.items() if not k.startswith("_")}
            clean_loc["stationTypes"] = station_types
            company["locations"].append(clean_loc)

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
def update_station(event):
    """Updates a station's status, notes, dates, or name."""
    path_params = event.get("pathParameters") or {}
    station_id = path_params.get("station_id", "")
    body = parse_body(event)

    if not station_id:
        return build_response(400, {"error": "station_id is required in URL path"})

    # Find the station by scanning (since we need the PK to update)
    # In production, you'd use a GSI on station_id for O(1) lookup
    all_items = scan_full_table(dashboard_table)
    station_item = None
    for item in all_items:
        if item.get("SK") == f"STATION#{station_id}" and item.get("PK", "").startswith("LOCATION#"):
            station_item = item
            break

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

    # Return updated station
    updated = dashboard_table.get_item(Key={"PK": station_item["PK"], "SK": station_item["SK"]}).get("Item", {})
    updated = convert_decimals(updated)

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
    """Returns all stations where status is 'warn' or 'fail', enriched with reseller/company/location context."""
    all_items = convert_decimals(scan_full_table(dashboard_table))

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

    Query Parameters (all optional):
        location    — Filter by facility_area (case-insensitive)
        start_date  — Filter inspections on or after this date (YYYY-MM-DD)
        end_date    — Filter inspections on or before this date (YYYY-MM-DD)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        params = event.get("queryStringParameters", {}) or {}
        filter_location = str(params.get("location", "") or "").strip()
        filter_start = str(params.get("start_date", "") or "").strip()
        filter_end = str(params.get("end_date", "") or "").strip()

        all_inspections = []

        # Scan all 5 tables in PARALLEL to avoid timeout
        def _scan_table(type_label, ddb_table):
            try:
                items = convert_decimals(scan_full_table(ddb_table))
                logger.info(f"[ADMIN] Scanned {type_label}: {len(items)} items")
                return [(item, type_label) for item in items]
            except Exception as e:
                logger.error(f"Error scanning table for {type_label}: {str(e)}")
                return []

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(_scan_table, label, table): label
                for label, table in inspection_tables.items()
            }
            for future in as_completed(futures):
                all_inspections.extend(future.result())

        logger.info(f"[ADMIN] Total inspections scanned: {len(all_inspections)}")

        # Build response list with filters and computed fields
        result_list = []
        for raw, type_label in all_inspections:
            try:
                categories = raw.get("categories") or []
                general_results = raw.get("general_results") or []
                date_of_audit = str(raw.get("date_of_audit") or "")
                location = str(raw.get("location") or "")
                facility_area = str(raw.get("facility_area") or "")

                # Apply filters — match against location first, then facility_area
                if filter_location:
                    match_target = location if location else facility_area
                    if filter_location.lower() != match_target.lower():
                        continue
                if filter_start and date_of_audit < filter_start:
                    continue
                if filter_end and date_of_audit > filter_end:
                    continue

                # Honor stored status from pause/resume endpoints
                stored_status = str(raw.get("status") or "").strip()
                if stored_status in ("paused", "in_progress"):
                    status = stored_status
                else:
                    status = compute_inspection_status(categories, general_results)

                evidence = count_evidence(categories)

                # Compute progress for incomplete inspections
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

        # Sort newest first
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
# API 10: DELETE /api/companies/{company_key}
# ═══════════════════════════════════════════════
def delete_company(event):
    """Deletes a company and all its locations and stations."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required"})

    # Check company exists
    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"}).get("Item")
    if not existing:
        return build_response(404, {"error": f"Company '{company_key}' not found"})

    # Find all locations under this company
    all_items = scan_full_table(dashboard_table)
    deleted_locations = []
    deleted_stations = []

    # Collect location keys
    location_keys = []
    for item in all_items:
        if item.get("PK") == f"COMPANY#{company_key}" and item.get("SK", "").startswith("LOCATION#"):
            loc_key = item["SK"].replace("LOCATION#", "")
            location_keys.append(loc_key)
            dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
            deleted_locations.append(loc_key)

    # Delete all stations under those locations
    for item in all_items:
        if item.get("PK", "").startswith("LOCATION#") and item.get("SK", "").startswith("STATION#"):
            loc_key = item["PK"].replace("LOCATION#", "")
            if loc_key in location_keys:
                dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
                deleted_stations.append(item.get("station_id", ""))

    # Delete the company itself
    dashboard_table.delete_item(Key={"PK": f"COMPANY#{company_key}", "SK": "METADATA"})

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
    """Deletes a location and all its stations."""
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()
    location_key = str(path_params.get("location_key", "")).strip()

    if not company_key or not location_key:
        return build_response(400, {"error": "company_key and location_key are required"})

    # Check location exists
    existing = dashboard_table.get_item(
        Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"}
    ).get("Item")
    if not existing:
        return build_response(404, {"error": f"Location '{location_key}' not found"})

    # Delete all stations under this location
    all_items = scan_full_table(dashboard_table)
    deleted_stations = []
    for item in all_items:
        if item.get("PK") == f"LOCATION#{location_key}" and item.get("SK", "").startswith("STATION#"):
            dashboard_table.delete_item(Key={"PK": item["PK"], "SK": item["SK"]})
            deleted_stations.append(item.get("station_id", ""))

    # Delete the location
    dashboard_table.delete_item(Key={"PK": f"COMPANY#{company_key}", "SK": f"LOCATION#{location_key}"})

    return build_response(200, {
        "message": "Location deleted successfully",
        "location_key": location_key,
        "deleted_stations": deleted_stations,
    })


# ═══════════════════════════════════════════════
# API 10: DELETE /api/stations/{station_id}
# ═══════════════════════════════════════════════
def delete_station(event):
    """Deletes a single station."""
    path_params = event.get("pathParameters") or {}
    station_id = str(path_params.get("station_id", "")).strip()

    if not station_id:
        return build_response(400, {"error": "station_id is required"})

    # Find the station
    all_items = scan_full_table(dashboard_table)
    station_item = None
    for item in all_items:
        if item.get("SK") == f"STATION#{station_id}" and item.get("PK", "").startswith("LOCATION#"):
            station_item = item
            break

    if not station_item:
        return build_response(404, {"error": f"Station '{station_id}' not found"})

    dashboard_table.delete_item(Key={"PK": station_item["PK"], "SK": station_item["SK"]})

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
        location_key   (required) — Which location (e.g. "austin-tx")
        category       (optional) — Filter to a single category typeKey
        auditor_name   (optional) — Filter progress to a specific inspector
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        params = event.get("queryStringParameters", {}) or {}
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

        # ── Step 1: Get all stations under this location from dashboard table ──
        all_dashboard_items = scan_full_table(dashboard_table)
        all_dashboard_items = convert_decimals(all_dashboard_items)

        # Find the location name (from COMPANY#/LOCATION# record)
        location_name = ""
        location_found = False
        for item in all_dashboard_items:
            pk = item.get("PK", "")
            sk = item.get("SK", "")
            if sk == f"LOCATION#{location_key}" and pk.startswith("COMPANY#"):
                location_name = item.get("name", location_key)
                location_found = True
                break

        if not location_found:
            return build_response(404, {"error": f"Location '{location_key}' not found"})

        # Collect all stations under this location
        location_stations = []
        for item in all_dashboard_items:
            pk = item.get("PK", "")
            sk = item.get("SK", "")
            if pk == f"LOCATION#{location_key}" and sk.startswith("STATION#"):
                type_key = item.get("typeKey", "")
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
        # Determine today's date for filtering (daily scope per frontend requirement)
        today = datetime.now(timezone.utc)
        today_str = today.strftime("%Y-%m-%d")

        all_inspections = []

        def _scan_inspection_table(type_label, ddb_table):
            """Scan a single inspection table and return matching records."""
            try:
                items = convert_decimals(scan_full_table(ddb_table))
                return [(item, type_label) for item in items]
            except Exception as e:
                logger.error(f"[MOBILE] Error scanning {type_label}: {str(e)}")
                return []

        # Only scan tables for the categories we need
        tables_to_scan = {}
        if filter_category:
            label = TYPEKEY_TO_INSPECTION_LABEL.get(filter_category)
            if label and label in inspection_tables:
                tables_to_scan[label] = inspection_tables[label]
        else:
            tables_to_scan = inspection_tables

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(_scan_inspection_table, label, tbl): label
                for label, tbl in tables_to_scan.items()
            }
            for future in as_completed(futures):
                all_inspections.extend(future.result())

        logger.info(f"[MOBILE] Total inspections scanned: {len(all_inspections)}")

        # ── Step 3: Build station_id → inspection mapping for today ──
        # An inspection matches a station if:
        #   (a) station_id field matches (preferred, new flow), OR
        #   (b) station name matches (fallback, legacy flow)
        # AND the inspection date_of_audit is today
        # AND (if auditor_name filter) the auditor matches

        station_id_set = {s["station_id"] for s in location_stations}
        station_name_set = {s["station_name"].lower() for s in location_stations}

        # Map: station_id → best matching inspection record
        station_inspection_map = {}  # station_id → {inspection_id, status, created_at, completed_at}

        for raw, type_label in all_inspections:
            try:
                date_of_audit = str(raw.get("date_of_audit") or "")
                # Filter to today's date
                if date_of_audit != today_str:
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
                    # Fallback: match by station name against our station list
                    for s in location_stations:
                        if s["station_name"].lower() == insp_station_name.lower():
                            matched_station_id = s["station_id"]
                            break

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

                # Keep the most recent inspection if multiple exist
                existing = station_inspection_map.get(matched_station_id)
                if not existing or created_at > existing.get("started_at", ""):
                    station_inspection_map[matched_station_id] = {
                        "inspection_id": inspection_id,
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

            if insp:
                station_entry = {
                    "station_id": sid,
                    "station_name": station["station_name"],
                    "status": insp["status"],
                    "equipment_status": station.get("equipment_status", "ok"),
                    "lastInspected": station.get("lastInspected", ""),
                    "nextDue": station.get("next_due", ""),
                    "inspection_id": insp["inspection_id"],
                    "started_at": insp["started_at"],
                    "completed_at": insp["completed_at"],
                }
            else:
                station_entry = {
                    "station_id": sid,
                    "station_name": station["station_name"],
                    "status": "pending",
                    "equipment_status": station.get("equipment_status", "ok"),
                    "lastInspected": station.get("lastInspected", ""),
                    "nextDue": station.get("next_due", ""),
                    "inspection_id": None,
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

        for st_type in STATION_TYPES:
            tk = st_type["key"]
            # Skip if category filter is active and this isn't the filtered category
            if filter_category and tk != filter_category:
                continue

            stations_list = categories_map.get(tk, [])
            cat_completed = sum(1 for s in stations_list if s["status"] == "completed")
            cat_started = sum(1 for s in stations_list if s["status"] == "started")
            cat_pending = sum(1 for s in stations_list if s["status"] == "pending")
            cat_total = len(stations_list)

            total_all += cat_total
            completed_all += cat_completed
            started_all += cat_started
            pending_all += cat_pending

            categories_response.append({
                "category_key": tk,
                "category_name": st_type["label"],
                "counts": {
                    "total": cat_total,
                    "completed": cat_completed,
                    "started": cat_started,
                    "pending": cat_pending,
                },
                "stations": stations_list,
            })

        # Build summary
        percent_complete = round((completed_all / total_all * 100)) if total_all > 0 else 0

        response = {
            "location_key": location_key,
            "location_name": location_name,
            "date": today.strftime("%Y-%m-%d"),
            "summary": {
                "total": total_all,
                "completed": completed_all,
                "started": started_all,
                "pending": pending_all,
                "percent_complete": percent_complete,
            },
            "categories": categories_response,
        }

        return build_response(200, response)

    except Exception as e:
        logger.exception(f"[MOBILE] mobile_inspection_status failed: {str(e)}")
        return build_response(500, {"error": f"Internal error: {str(e)}"})


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

        else:
            return build_response(404, {"error": f"Route not found: {http_method} {resource} (path: {path})"})

    except Exception as e:
        logger.exception(f"FATAL handler crash: {str(e)}")
        return build_response(500, {"error": f"Handler crash: {str(e)}"})
