"""
OSHA Safety Hub — Dashboard Lambda
Manages the Company → Location → StationType → Station hierarchy
for the frontend dashboard sidebar.

Endpoints:
    GET    /api/companies                                    → Full nested tree
    POST   /api/companies                                    → Create company
    POST   /api/companies/{ck}/locations                     → Create location (auto-creates 5 station types)
    POST   /api/companies/{ck}/locations/{lk}/stations       → Create station
    PUT    /api/companies/{company_key}                       → Update company name/state
    PUT    /api/companies/{ck}/locations/{lk}                 → Update location details
    PUT    /api/stations/{station_id}                         → Update station status/notes
    DELETE /api/companies/{company_key}                       → Delete company + all locations & stations
    DELETE /api/companies/{ck}/locations/{lk}                 → Delete location + all stations
    DELETE /api/stations/{station_id}                         → Delete a single station
    GET    /api/alerts                                        → All stations with status != "ok"
    GET    /admin/inspections                                 → Unified inspection list (all 5 types)

DynamoDB Table: osha-dashboard (PK + SK single-table design)
    Company:  PK=COMPANY#{key}          SK=METADATA
    Location: PK=COMPANY#{ck}           SK=LOCATION#{lk}
    Station:  PK=LOCATION#{lk}          SK=STATION#{id}
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

# Inspection tables (for admin/inspections endpoint)
inspection_tables = {
    "Recordkeeping":      dynamodb.Table(os.getenv("RECORDKEEPING_TABLE", "osha-inspections")),
    "Eyewash":            dynamodb.Table(os.getenv("EYEWASH_TABLE", "osha-eyewash-inspections")),
    "Fire Extinguisher":  dynamodb.Table(os.getenv("FIRE_EXT_TABLE", "osha-fire-extinguisher-inspections")),
    "Monthly Racking":    dynamodb.Table(os.getenv("RACKING_TABLE", "osha-racking-inspections")),
    "Quarterly HRA":      dynamodb.Table(os.getenv("HRA_TABLE", "osha-hra-inspections")),
}

# ─────────────────────────────────────────────
# Fixed Station Type Categories
# ─────────────────────────────────────────────
STATION_TYPES = [
    {"key": "eyewash",       "label": "Eyewash",           "icon": "fa-eye"},
    {"key": "fire",          "label": "Fire Extinguisher",  "icon": "fa-fire-extinguisher"},
    {"key": "racking",       "label": "Monthly Racking",    "icon": "fa-th-large"},
    {"key": "hra",           "label": "Quarterly HRA",      "icon": "fa-clipboard-list"},
    {"key": "recordkeeping", "label": "Recordkeeping",      "icon": "fa-folder-open"},
]


# ═══════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════

def build_response(status_code, body):
    """Standard API Gateway response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Api-Key,x-api-key,X-Amz-Date",
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
    """Creates a new company."""
    body = parse_body(event)
    name = str(body.get("name", "")).strip()
    state = str(body.get("state", "")).strip()

    if not name:
        return build_response(400, {"error": "name is required"})

    key = slugify(name)

    # Check if already exists
    existing = dashboard_table.get_item(Key={"PK": f"COMPANY#{key}", "SK": "METADATA"}).get("Item")
    if existing:
        return build_response(409, {"error": f"Company '{name}' already exists with key '{key}'"})

    item = {
        "PK": f"COMPANY#{key}",
        "SK": "METADATA",
        "name": name,
        "state": state,
        "created_at": now_iso(),
    }
    dashboard_table.put_item(Item=item)

    return build_response(201, {
        "key": key,
        "name": name,
        "state": state,
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
    """Returns all stations where status is 'warn' or 'fail', enriched with company/location context."""
    all_items = convert_decimals(scan_full_table(dashboard_table))

    # Build lookup maps
    companies = {}   # key → {name, state}
    locations = {}   # location_key → {name, company_key, ...}
    alerts = []

    for item in all_items:
        pk = item.get("PK", "")
        sk = item.get("SK", "")

        if sk == "METADATA" and pk.startswith("COMPANY#"):
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

    # Enrich with company/location context
    enriched_alerts = []
    for alert in alerts:
        loc_key = alert.pop("_location_key", "")
        loc_info = locations.get(loc_key, {})
        company_key = loc_info.get("company_key", "")
        company_info = companies.get(company_key, {})

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
    Returns a unified list of ALL inspections from all 5 types,
    with computed status, evidence count, and aggregate stats.

    Query Parameters (all optional):
        location    — Filter by facility_area (case-insensitive)
        start_date  — Filter inspections on or after this date (YYYY-MM-DD)
        end_date    — Filter inspections on or before this date (YYYY-MM-DD)
    """
    params = event.get("queryStringParameters", {}) or {}
    filter_location = params.get("location", "").strip()
    filter_start = params.get("start_date", "").strip()
    filter_end = params.get("end_date", "").strip()

    all_inspections = []

    for type_label, ddb_table in inspection_tables.items():
        try:
            items = convert_decimals(scan_full_table(ddb_table))
            for item in items:
                all_inspections.append({"_raw": item, "type": type_label})
        except Exception as e:
            logger.error(f"Error scanning table for {type_label}: {str(e)}")

    # Build response list with filters and computed fields
    result_list = []
    for item_wrapper in all_inspections:
        raw = item_wrapper["_raw"]
        categories = raw.get("categories", [])
        general_results = raw.get("general_results", [])
        date_of_audit = raw.get("date_of_audit", "")
        facility_area = raw.get("facility_area", "")

        # Apply filters
        if filter_location and filter_location.lower() != facility_area.lower():
            continue
        if filter_start and date_of_audit < filter_start:
            continue
        if filter_end and date_of_audit > filter_end:
            continue

        status = compute_inspection_status(categories, general_results)
        evidence = count_evidence(categories)

        result_list.append({
            "inspection_id": raw.get("inspection_id"),
            "session_id": raw.get("session_id"),
            "company": raw.get("company", "Continental Battery"),
            "location": facility_area,
            "type": item_wrapper["type"],
            "date": date_of_audit,
            "inspector": raw.get("auditor_name", ""),
            "evidence_count": evidence,
            "status": status,
            "created_at": raw.get("created_at", ""),
        })

    # Sort newest first
    result_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    stats = {
        "total": len(result_list),
        "completed": sum(1 for i in result_list if i["status"] == "completed"),
        "pending": sum(1 for i in result_list if i["status"] == "pending"),
        "overdue": sum(1 for i in result_list if i["status"] == "overdue"),
    }

    return build_response(200, {"stats": stats, "inspections": result_list})


def compute_inspection_status(categories, general_results):
    """Calculate inspection status: completed, in_progress, pending, or overdue."""
    answered = 0
    total = 0
    for cat in categories:
        for item in cat.get("items", []):
            iid = item.get("id")
            if isinstance(iid, int) and iid in (11, 12):
                continue
            total += 1
            if item.get("answer", "").strip():
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
    for cat in categories:
        for item in cat.get("items", []):
            evidence = item.get("evidence", [])
            count += len(evidence) if isinstance(evidence, list) else 0
    return count


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
# Main Handler — Routes to correct function
# ═══════════════════════════════════════════════
def lambda_handler(event, context):
    """
    Main entry point. Routes based on HTTP method and path.
    """
    http_method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path = event.get("path", "")

    logger.info(f"Dashboard: {http_method} {resource} (path: {path})")

    # CORS preflight
    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # ── GET /api/companies ──
    if http_method == "GET" and resource == "/api/companies":
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

    # ── GET /admin/inspections ──
    elif http_method == "GET" and resource == "/admin/inspections":
        return admin_list_inspections(event)

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})
