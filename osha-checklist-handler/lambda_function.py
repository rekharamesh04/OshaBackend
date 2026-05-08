"""
OSHA Inspection Checklist - Lambda Handler
Single Lambda function handling all 8 API routes:
  POST /inspection-session          → Create a new inspection session (general info)
  POST /inspection                  → Submit checklist (linked to session)
  GET  /inspections                 → List all inspections (summary)
  GET  /inspection/{id}             → Get full inspection by ID
  GET  /inspection/checklist        → Get checklist template
  GET  /evidence/upload-url         → Generate pre-signed S3 URL for uploading evidence
  GET  /evidence/download-url       → Generate pre-signed S3 URL for downloading/viewing evidence
  GET  /admin/inspections           → Admin dashboard: unified list across all inspection types
"""

import json
import uuid
import os
import boto3
from datetime import datetime, timezone
from decimal import Decimal

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("osha-inspections")
sessions_table = dynamodb.Table("osha-inspection-sessions")

# Additional tables for admin dashboard (cross-inspection queries)
eyewash_table = dynamodb.Table("osha-eyewash-inspections")
fire_ext_table = dynamodb.Table("osha-fire-extinguisher-inspections")
racking_table = dynamodb.Table("osha-racking-inspections")
hra_table = dynamodb.Table("osha-hra-inspections")

# Initialize S3 client for evidence uploads
s3_client = boto3.client("s3")

# ─────────────────────────────────────────────
# Evidence Upload Configuration
# ─────────────────────────────────────────────
EVIDENCE_S3_BUCKET = os.environ.get("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")
UPLOAD_URL_EXPIRY = 900       # 15 minutes for uploads
DOWNLOAD_URL_EXPIRY = 3600    # 1 hour for downloads/viewing

ALLOWED_CONTENT_TYPES = [
    # Images
    "image/jpeg", "image/png", "image/gif", "image/webp", "image/heic", "image/heif",
    # Videos
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/webm", "video/3gpp",
]


# ─────────────────────────────────────────────
# Checklist Definition — Single Source of Truth
# ─────────────────────────────────────────────
OSHA_CHECKLIST = {
    "inspection_type": "OSHA Inspection Checklist",
    "available_answers": ["Yes", "No"],
    "categories": [
        {
            "id": 1,
            "name": "Administrative & Recordkeeping",
            "items": [
                {"id": 1, "title": "OSHA 300 Logs", "description": "Are logs for the current and previous 5 years on file?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 2, "title": "OSHA Poster", "description": "Is the \"Job Safety and Health: It's the Law\" poster displayed in a common area?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 3, "title": "Written Programs", "description": "Are written plans available for Hazard Communication, Lockout/Tagout, and Emergency Action?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 4, "title": "Training Records", "description": "Is there documented proof of training for Forklifts (PIT), HazCom, and PPE?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 5, "title": "Hazard Assessment", "description": "Is there a signed document certifying that a PPE hazard assessment was conducted for the facility?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 2,
            "name": "Forklift & Powered Industrial Trucks (29 CFR 1910.178)",
            "items": [
                {"id": 6, "title": "Daily Inspections", "description": "Are pre-operation checklists completed and filed for every shift?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 7, "title": "Operator Certification", "description": "Do all operators have valid certification (issued within the last 3 years)?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 11, "title": "Safety Equipment", "description": "Do trucks have functioning backup alarms, horns, and blue lights (if required by company policy)?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 12, "title": "Battery Handling", "description": "Are conveyors, overhead hoists, spreader bars used for battery changes in good repair?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
            "sub_sections": [
                {
                    "name": "Battery Charging Area",
                    "items": [
                        {"id": 8, "description": "Is \"No Smoking\" clearly posted?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                        {"id": 9, "description": "Is there adequate ventilation to  prevent hydrogen gas buildup?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                        {"id": 10, "description": "Are fire extinguishers  mounted within 75 feet of the charging station?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                    ],
                },
            ],
        },
        {
            "id": 3,
            "name": "Chemical & Corrosive Safety",
            "items": [
                {"id": 16, "title": "Spill Kits", "description": "Are acid-neutralizing spill kits(soda ash/absorbents) available and clearly marked?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 17, "title": "Safety Data Sheets(SDS)", "description": "Are Safety Data Sheets for all battery types and cleaning chemicals indexed and accessible 24/7 without a password/key?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 18, "title": "PPE Availability", "description": "Are acid-resistant gloves, face shields, aprons available for handling leaking units or servicing?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
            "sub_sections": [
                {
                    "name": "Eyewash Stations",
                    "items": [
                        {"id": 13, "description": "Reachable within 10 seconds (approx.55 feet)?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                        {"id": 14, "description": "Flow is tepid, continuous 15 minutes,and path is unobstructed?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                        {"id": 15, "description": "is there a log showing a weekly flush or activation test?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                    ],
                },
            ],
        },
        {
            "id": 4,
            "name": "Racking, Stacking & Pallets",
            "items": [
                {"id": 19, "title": "Rack Integrity", "description": "Are uprights free of dents or structural damage? (Look for yellow  or  red tags for damaged section).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 20, "title": "Load Capacities", "description": "Are maximum load weights clearly posted on the racking?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 21, "title": "Pallet Condition", "description": "Are heavy battery stacks on intact pallets(no missing boards or cracks)?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 22, "title": "Stability", "description": "Are pallets shrink-wrapped/banded if stacked high to prevent units from falling?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 23, "title": "Aisles", "description": "Are all main aisles at least 3 feet wide(or wide enough for safe PIT passage) and clear of clutter?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 5,
            "name": "Electrical & Fire Safety",
            "items": [
                {"id": 24, "title": "Electrical Panels", "description": "Is there a 36-inch clear working space in front of all panels?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 25, "title": "Fire Extinguishers", "description": "Are they inspected monthly, tagged annually, unobstructed?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 26, "title": "Exit Routes", "description": "Are all exits marked with illuminated signs and completely free of stored pallets or trash?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 27, "title": "Extension Cords", "description": "Are they used only temporary tasks(not as permanent wiring)? Are they free of splices/tape?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 6,
            "name": "Housekeeping & Hygiene",
            "items": [
                {"id": 28, "title": "Lead Dust/Sulfate", "description": "Are surfaces free of white acid bloom or dust accumulations?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 29, "title": "No Food/Drink", "description": "Are employees prohibited from eating or drinking in battery storage/service areas to prevent lead ingestion?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 30, "title": "Floor Condition", "description": "Are floors dry and free of slip/trip hazards like loose strapping or plastic wrap?", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
    ],
    "general_results": [
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
    ],
    "notes": "",
}


# ─────────────────────────────────────────────
# Helper: Build HTTP Response with CORS headers
# ─────────────────────────────────────────────
def build_response(status_code, body):
    """Builds a standardized API Gateway response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body, default=str),
    }


# ─────────────────────────────────────────────
# Helper: Build item description lookup from checklist template
# ─────────────────────────────────────────────
def build_description_lookup():
    """Creates a dict mapping item_id → {description, title} from the checklist template.
    Handles both top-level items and sub_section items."""
    lookup = {}
    for category in OSHA_CHECKLIST.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = {"description": item.get("description", ""), "title": item.get("title", "")}
        for sub_section in category.get("sub_sections", []):
            for item in sub_section.get("items", []):
                lookup[item["id"]] = {"description": item.get("description", ""), "title": item.get("title", "")}
    return lookup


# ─────────────────────────────────────────────
# Helper: Convert Decimal types from DynamoDB
# ─────────────────────────────────────────────
def convert_decimals(obj):
    """DynamoDB returns numbers as Decimal. Convert them to int/float for JSON."""
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


# ─────────────────────────────────────────────
# Helper: Compute inspection status from categories
# ─────────────────────────────────────────────
def compute_status(categories, general_results):
    """
    Determines the status of an inspection based on its answers and findings.
    Returns: "pending", "overdue", or "completed"

    Logic:
      1. If ANY item has answer == "" → "pending"
      2. If ALL items answered AND any finding has due_date before today → "overdue"
      3. Otherwise → "completed"
    """
    # Check all items across categories (and sub_sections for OSHA)
    for cat in (categories or []):
        for item in cat.get("items", []):
            if item.get("answer", "") == "":
                return "pending"
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                if item.get("answer", "") == "":
                    return "pending"

    # All answered — check for overdue findings
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Check item-level findings
    for cat in (categories or []):
        for item in cat.get("items", []):
            if item.get("finding") and item.get("due_date") and item["due_date"] < today:
                return "overdue"
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                if item.get("finding") and item.get("due_date") and item["due_date"] < today:
                    return "overdue"

    # Check general_results findings
    for result in (general_results or []):
        if not isinstance(result, dict):
            continue
        if result.get("finding") and result.get("due_date") and result["due_date"] < today:
            return "overdue"

    return "completed"


# ─────────────────────────────────────────────
# Helper: Count evidence files across all items
# ─────────────────────────────────────────────
def count_evidence(categories):
    """Counts total evidence files across all items in all categories (including sub_sections)."""
    count = 0
    for cat in (categories or []):
        for item in cat.get("items", []):
            count += len(item.get("evidence", []))
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                count += len(item.get("evidence", []))
    return count


# ─────────────────────────────────────────────
# Helper: Scan a DynamoDB table with full pagination
# ─────────────────────────────────────────────
def scan_full_table(ddb_table):
    """Scans a DynamoDB table and handles pagination to return all items."""
    result = ddb_table.scan()
    items = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = ddb_table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))
    return items


# ─────────────────────────────────────────────
# API 1: GET /inspection/checklist — Get Checklist Template
# ─────────────────────────────────────────────
def get_checklist(event):
    """
    Returns the full OSHA checklist template.
    Frontend uses this to render the inspection form dynamically.
    """
    return build_response(200, OSHA_CHECKLIST)


# ─────────────────────────────────────────────
# API 2: POST /inspection-session — Create Inspection Session
# ─────────────────────────────────────────────
def create_session(event):
    """
    Creates a new inspection session with the auditor's general information.
    This is Step 1 of the flow — user enters their details first.

    Expects JSON body:
    {
        "auditor_name": "string",
        "facility_area": "string",
        "date_of_audit": "YYYY-MM-DD"
    }
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    # Validate required fields
    auditor_name = body.get("auditor_name", "").strip()
    facility_area = body.get("facility_area", "").strip()
    date_of_audit = body.get("date_of_audit", "").strip()

    if not auditor_name:
        return build_response(400, {"error": "auditor_name is required"})
    if not facility_area:
        return build_response(400, {"error": "facility_area is required"})
    if not date_of_audit:
        return build_response(400, {"error": "date_of_audit is required"})

    # Generate unique session ID and timestamp
    session_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    # Save session to DynamoDB
    session_item = {
        "session_id": session_id,
        "auditor_name": auditor_name,
        "facility_area": facility_area,
        "date_of_audit": date_of_audit,
        "created_at": created_at,
    }

    sessions_table.put_item(Item=session_item)

    return build_response(201, {
        "session_id": session_id,
        "auditor_name": auditor_name,
        "facility_area": facility_area,
        "date_of_audit": date_of_audit,
        "created_at": created_at,
    })


# ─────────────────────────────────────────────
# API 3: POST /inspection — Submit Checklist (linked to session)
# ─────────────────────────────────────────────
def create_inspection(event):
    """
    Creates a new OSHA inspection record linked to an existing session.
    This is Step 2 — user submits checklist answers.

    Expects JSON body:
    {
        "session_id": "string",
        "categories": [
            {
                "id": 1,
                "name": "Administrative & Recordkeeping",
                "items": [
                    { "id": 1, "answer": "Yes" },
                    { "id": 2, "answer": "No" }
                ]
            }
        ],
        "general_results": [
            { "finding": "", "action_item": "", "responsible": "", "due_date": "" }
        ],
        "notes": "string"                          // optional (max 5000 chars)
    }
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    # Validate required fields
    session_id = body.get("session_id", "").strip()
    general_results = body.get("general_results", [])
    notes = body.get("notes", "").strip() if isinstance(body.get("notes", ""), str) else ""
    categories = body.get("categories", [])

    if not session_id:
        return build_response(400, {"error": "session_id is required"})
    if len(notes) > 5000:
        return build_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return build_response(400, {"error": "general_results must be a list"})
    if not categories or not isinstance(categories, list):
        return build_response(400, {"error": "categories must be a non-empty list"})

    # Look up the session to get auditor details
    session_result = sessions_table.get_item(Key={"session_id": session_id})
    session = session_result.get("Item")

    if not session:
        return build_response(404, {"error": "Session not found. Create a session first via POST /inspection-session"})

    # Validate each category and its items
    for i, cat in enumerate(categories):
        if not isinstance(cat, dict):
            return build_response(400, {"error": f"Category at index {i} must be an object"})
        if "id" not in cat:
            return build_response(400, {"error": f"Category at index {i} is missing id"})
        if "name" not in cat:
            return build_response(400, {"error": f"Category at index {i} is missing name"})
        items = cat.get("items", [])
        if not isinstance(items, list):
            return build_response(400, {"error": f"Category '{cat.get('name')}' items must be a list"})
        for j, item in enumerate(items):
            if not isinstance(item, dict):
                return build_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' must be an object"})
            if "id" not in item:
                return build_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' is missing id"})
            if "answer" not in item:
                return build_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' is missing answer"})

        # Validate sub_sections if present
        sub_sections = cat.get("sub_sections", [])
        if not isinstance(sub_sections, list):
            return build_response(400, {"error": f"Category '{cat.get('name')}' sub_sections must be a list"})
        for s, sub in enumerate(sub_sections):
            if not isinstance(sub, dict):
                return build_response(400, {"error": f"Sub-section at index {s} in category '{cat.get('name')}' must be an object"})
            if "name" not in sub:
                return build_response(400, {"error": f"Sub-section at index {s} in category '{cat.get('name')}' is missing name"})
            sub_items = sub.get("items", [])
            if not isinstance(sub_items, list):
                return build_response(400, {"error": f"Sub-section '{sub.get('name')}' items must be a list"})
            for k, sub_item in enumerate(sub_items):
                if not isinstance(sub_item, dict):
                    return build_response(400, {"error": f"Item at index {k} in sub-section '{sub.get('name')}' must be an object"})
                if "id" not in sub_item:
                    return build_response(400, {"error": f"Item at index {k} in sub-section '{sub.get('name')}' is missing id"})
                if "answer" not in sub_item:
                    return build_response(400, {"error": f"Item at index {k} in sub-section '{sub.get('name')}' is missing answer"})

    # Generate unique ID and timestamp
    inspection_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    # Build the item to save (merge session info + checklist data)
    item = {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "auditor_name": session.get("auditor_name", ""),
        "facility_area": session.get("facility_area", ""),
        "date_of_audit": session.get("date_of_audit", ""),
        "categories": categories,
        "general_results": general_results if general_results else [],
        "notes": notes,
        "created_at": created_at,
    }

    # Save to DynamoDB
    table.put_item(Item=item)

    # Return the generated ID
    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "created_at": created_at,
    })


# ─────────────────────────────────────────────
# API 4: GET /inspections — List All Inspections
# ─────────────────────────────────────────────
def list_inspections(event):
    """
    Returns a summary list of all inspections.
    Does NOT include the full categories array (keeps it lightweight).
    Sorted by created_at (newest first).
    """
    result = table.scan()
    items = result.get("Items", [])

    # Handle pagination if table has more than 1MB of data
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    # Convert Decimal types
    items = convert_decimals(items)

    # Return summary only (remove categories to keep payload small)
    summary_list = []
    for item in items:
        summary_list.append({
            "inspection_id": item.get("inspection_id"),
            "session_id": item.get("session_id"),
            "auditor_name": item.get("auditor_name"),
            "facility_area": item.get("facility_area"),
            "date_of_audit": item.get("date_of_audit"),
            "created_at": item.get("created_at"),
        })

    # Sort by created_at (newest first)
    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return build_response(200, summary_list)


# ─────────────────────────────────────────────
# API 5: GET /inspection/{id} — Get Full Inspection
# ─────────────────────────────────────────────
def get_inspection(event):
    """
    Returns the full inspection object including all categories and responses.
    Extracts inspection_id from the URL path parameters.
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    # Fetch from DynamoDB
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    # Convert Decimal types
    item = convert_decimals(item)

    # Enrich items with descriptions from checklist template
    description_lookup = build_description_lookup()
    categories = item.get("categories", [])
    for category in categories:
        for checklist_item in category.get("items", []):
            item_id = checklist_item.get("id")
            if item_id in description_lookup:
                info = description_lookup[item_id]
                checklist_item["description"] = info["description"]
                if info["title"]:
                    checklist_item["title"] = info["title"]
        for sub_section in category.get("sub_sections", []):
            for checklist_item in sub_section.get("items", []):
                item_id = checklist_item.get("id")
                if item_id in description_lookup:
                    info = description_lookup[item_id]
                    checklist_item["description"] = info["description"]
                    if info["title"]:
                        checklist_item["title"] = info["title"]

    # Build ordered response so JSON keys are in a logical order
    ordered_item = {
        "inspection_id": item.get("inspection_id"),
        "session_id": item.get("session_id"),
        "auditor_name": item.get("auditor_name"),
        "facility_area": item.get("facility_area"),
        "date_of_audit": item.get("date_of_audit"),
        "categories": categories,
        "general_results": item.get("general_results", []),
        "notes": item.get("notes", ""),
        "created_at": item.get("created_at"),
    }

    return build_response(200, ordered_item)


# ─────────────────────────────────────────────
# API 6: GET /evidence/upload-url — Generate Pre-Signed Upload URL
# ─────────────────────────────────────────────
def generate_upload_url(event):
    """
    Generates a pre-signed S3 URL that the mobile app can use to upload
    an image or video directly to S3.

    Query Parameters:
        filename       (required) — Original file name (e.g. "photo_001.jpg")
        contentType    (required) — MIME type (e.g. "image/jpeg", "video/mp4")
        inspectionType (optional) — For folder organization
                                    (e.g. "fire-extinguisher", "eyewash", "racking", "hra", "osha")

    Returns:
        {
            "upload_url": "https://s3.amazonaws.com/...",
            "file_url": "https://s3.amazonaws.com/...",
            "file_key": "evidence/fire-extinguisher/...",
            "expires_in": 900
        }
    """
    params = event.get("queryStringParameters", {}) or {}
    filename = params.get("filename", "").strip()
    content_type = params.get("contentType", "").strip()
    inspection_type = params.get("inspectionType", "general").strip()

    if not filename:
        return build_response(400, {"error": "filename query parameter is required"})
    if not content_type:
        return build_response(400, {"error": "contentType query parameter is required"})
    if content_type not in ALLOWED_CONTENT_TYPES:
        return build_response(400, {
            "error": f"contentType '{content_type}' is not allowed. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"
        })

    # Generate unique S3 key: evidence/<inspection_type>/<date>/<uuid>_<filename>
    date_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    unique_id = str(uuid.uuid4())[:8]
    safe_filename = filename.replace(" ", "_")
    s3_key = f"evidence/{inspection_type}/{date_prefix}/{unique_id}_{safe_filename}"

    try:
        upload_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": EVIDENCE_S3_BUCKET,
                "Key": s3_key,
                "ContentType": content_type,
            },
            ExpiresIn=UPLOAD_URL_EXPIRY,
        )
    except Exception as e:
        print(f"Error generating pre-signed URL: {str(e)}")
        return build_response(500, {"error": "Failed to generate upload URL"})

    file_url = f"https://{EVIDENCE_S3_BUCKET}.s3.amazonaws.com/{s3_key}"

    return build_response(200, {
        "upload_url": upload_url,
        "file_url": file_url,
        "file_key": s3_key,
        "expires_in": UPLOAD_URL_EXPIRY,
    })


# ─────────────────────────────────────────────
# API 7: GET /evidence/download-url — Generate Pre-Signed Download URL
# ─────────────────────────────────────────────
def generate_download_url(event):
    """
    Generates a pre-signed S3 URL for downloading/viewing an evidence file.

    Query Parameters:
        fileKey  (required) — The S3 object key (returned as file_key from upload-url)

    Returns:
        {
            "download_url": "https://s3.amazonaws.com/...",
            "expires_in": 3600
        }
    """
    params = event.get("queryStringParameters", {}) or {}
    file_key = params.get("fileKey", "").strip()

    if not file_key:
        return build_response(400, {"error": "fileKey query parameter is required"})

    # Verify the file exists in S3
    try:
        s3_client.head_object(Bucket=EVIDENCE_S3_BUCKET, Key=file_key)
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "404":
            return build_response(404, {"error": "File not found in S3"})
        print(f"Error checking S3 object: {str(e)}")
        return build_response(500, {"error": "Failed to verify file existence"})

    try:
        download_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": EVIDENCE_S3_BUCKET,
                "Key": file_key,
            },
            ExpiresIn=DOWNLOAD_URL_EXPIRY,
        )
    except Exception as e:
        print(f"Error generating download URL: {str(e)}")
        return build_response(500, {"error": "Failed to generate download URL"})

    return build_response(200, {
        "download_url": download_url,
        "expires_in": DOWNLOAD_URL_EXPIRY,
    })


# ─────────────────────────────────────────────
# API 8: PATCH /inspection/{id} — Update an Existing Inspection
# ─────────────────────────────────────────────
def update_inspection(event):
    """
    Updates an existing OSHA inspection record (partial update / upsert of fields).
    Allows the mobile app to save progress, correct answers, add findings,
    attach evidence, or update notes after the initial submission.

    URL param:
        inspection_id  — UUID of the inspection to update

    Accepts any subset of the original POST /inspection body:
    {
        "categories":       [ ... ]    // optional — merged with existing data
        "general_results": [ ... ]    // optional — replaces existing list if provided
        "notes":            "string"  // optional — replaces existing notes if provided
    }
    """
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    # Load the existing inspection
    result = table.get_item(Key={"inspection_id": inspection_id})
    existing = result.get("Item")
    if not existing:
        return build_response(404, {"error": "Inspection not found"})

    try:
        body = json.loads(event.get("body", "{}") or "{}")
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    # Merge categories if provided
    incoming_cats = body.get("categories")
    if incoming_cats is not None:
        if not isinstance(incoming_cats, list):
            return build_response(400, {"error": "categories must be a list"})
        # Merge: update only items present in incoming list, keep rest unchanged
        existing_cats = existing.get("categories", [])
        existing_by_id = {str(c.get("id")): c for c in existing_cats if isinstance(c, dict)}
        for inc_cat in incoming_cats:
            if not isinstance(inc_cat, dict):
                continue
            cat_id = str(inc_cat.get("id", ""))
            exist_cat = existing_by_id.get(cat_id)
            if exist_cat is None:
                existing_cats.append(inc_cat)
                continue
            # Merge items
            exist_items_by_id = {str(i.get("id")): i for i in exist_cat.get("items", []) if isinstance(i, dict)}
            for inc_item in inc_cat.get("items", []):
                if not isinstance(inc_item, dict):
                    continue
                item_id_key = str(inc_item.get("id", ""))
                if item_id_key in exist_items_by_id:
                    exist_items_by_id[item_id_key].update(
                        {k: v for k, v in inc_item.items() if v != "" or k == "answer"}
                    )
                else:
                    exist_cat["items"].append(inc_item)
            # Merge sub_sections if present
            for inc_sub in inc_cat.get("sub_sections", []):
                for exist_sub in exist_cat.get("sub_sections", []):
                    if exist_sub.get("name") == inc_sub.get("name"):
                        exist_sub_items_by_id = {str(i.get("id")): i for i in exist_sub.get("items", []) if isinstance(i, dict)}
                        for inc_item in inc_sub.get("items", []):
                            if not isinstance(inc_item, dict):
                                continue
                            item_id_key = str(inc_item.get("id", ""))
                            if item_id_key in exist_sub_items_by_id:
                                exist_sub_items_by_id[item_id_key].update(
                                    {k: v for k, v in inc_item.items() if v != "" or k == "answer"}
                                )
                            else:
                                exist_sub.get("items", []).append(inc_item)
        existing["categories"] = existing_cats

    # Replace general_results if provided
    incoming_results = body.get("general_results")
    if incoming_results is not None:
        if not isinstance(incoming_results, list):
            return build_response(400, {"error": "general_results must be a list"})
        existing["general_results"] = incoming_results

    # Replace notes if provided
    if "notes" in body:
        notes = body.get("notes", "")
        if not isinstance(notes, str):
            notes = ""
        if len(notes) > 5000:
            return build_response(400, {"error": "notes must be under 5000 characters"})
        existing["notes"] = notes.strip()

    # Recompute status based on updated data
    categories     = existing.get("categories", [])
    general_results = existing.get("general_results", [])
    existing["status"]     = compute_status(categories, general_results)
    existing["updated_at"] = datetime.now(timezone.utc).isoformat()

    table.put_item(Item=existing)

    return build_response(200, {
        "inspection_id": inspection_id,
        "status": existing["status"],
        "updated_at": existing["updated_at"],
        "message": "Inspection updated successfully",
    })


# ─────────────────────────────────────────────
# API 9: DELETE /inspection/{id} — Delete Inspection
# ─────────────────────────────────────────────
def delete_inspection(event):
    """
    Deletes an OSHA inspection record by inspection_id.
    Optionally also deletes the linked session record.

    URL param:
        inspection_id  — UUID of the inspection to delete

    Query params:
        delete_session  (optional, default "false") — pass "true" to also remove the session
    """
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    # Verify the record exists before deleting
    result = table.get_item(Key={"inspection_id": inspection_id})
    existing = result.get("Item")
    if not existing:
        return build_response(404, {"error": "Inspection not found"})

    session_id = existing.get("session_id", "")

    # Delete the inspection
    table.delete_item(Key={"inspection_id": inspection_id})

    # Always delete the linked session unless explicitly told to keep it
    params = event.get("queryStringParameters", {}) or {}
    keep_session = params.get("keep_session", "false").strip().lower()
    session_deleted = False
    
    if keep_session not in {"true", "1", "yes"} and session_id:
        try:
            sessions_table.delete_item(Key={"session_id": session_id})
            session_deleted = True
        except Exception as e:
            print(f"Warning: could not delete session {session_id}: {str(e)}")

    return build_response(200, {
        "message": "Inspection deleted successfully",
        "inspection_id": inspection_id,
        "session_id": session_id,
        "session_deleted": session_deleted,
    })


# ─────────────────────────────────────────────
# API 10: GET /admin/inspections — Admin Dashboard
# ─────────────────────────────────────────────
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

    # Define which tables to scan and their type labels
    table_config = [
        (table, "Recordkeeping"),
        (eyewash_table, "Eyewash"),
        (fire_ext_table, "Fire Extinguisher"),
        (racking_table, "Racking"),
        (hra_table, "Quarterly HRA"),
    ]

    all_inspections = []

    for ddb_table, type_label in table_config:
        try:
            items = scan_full_table(ddb_table)
            items = convert_decimals(items)
            for item in items:
                all_inspections.append({
                    "_raw": item,  # Keep full item for status computation
                    "type": type_label,
                })
        except Exception as e:
            print(f"Error scanning table for {type_label}: {str(e)}")
            # Continue with other tables even if one fails

    # Sort by created_at (oldest first) for consistent ordering
    all_inspections.sort(key=lambda x: x["_raw"].get("created_at", ""))

    # Build the response list with computed fields
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

        status = compute_status(categories, general_results)
        evidence = count_evidence(categories)

        result_list.append({
            "inspection_id": raw.get("inspection_id"),
            "session_id": raw.get("session_id"),
            "company": "Continental Battery",
            "location": facility_area,
            "type": item_wrapper["type"],
            "date": date_of_audit,
            "inspector": raw.get("auditor_name", ""),
            "evidence_count": evidence,
            "status": status,
            "created_at": raw.get("created_at", ""),
        })

    # Sort by created_at (newest first) for display
    result_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    # Compute stats from the filtered list
    stats = {
        "total": len(result_list),
        "completed": sum(1 for i in result_list if i["status"] == "completed"),
        "pending": sum(1 for i in result_list if i["status"] == "pending"),
        "overdue": sum(1 for i in result_list if i["status"] == "overdue"),
    }

    return build_response(200, {
        "stats": stats,
        "inspections": result_list,
    })


# ─────────────────────────────────────────────
# Main Handler — Routes to correct function
# ─────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Main entry point. Routes the request based on HTTP method and path.

    Routes:
        GET  /inspection/checklist              → get_checklist
        POST /inspection-session                → create_session
        POST /inspection                        → create_inspection
        GET  /inspections                       → list_inspections
        GET  /inspection/{inspection_id}        → get_inspection
        GET  /evidence/upload-url               → generate_upload_url
        GET  /evidence/download-url             → generate_download_url
        GET  /admin/inspections                 → admin_list_inspections
        OPTIONS (any)                           → CORS preflight
    """
    http_method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path = event.get("path", "")

    print(f"Received: {http_method} {resource} (path: {path})")  # CloudWatch logging

    # CORS preflight
    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # Route to the correct handler
    if http_method == "GET" and resource == "/inspection/checklist":
        return get_checklist(event)

    elif http_method == "POST" and resource == "/inspection-session":
        return create_session(event)

    elif http_method == "POST" and resource == "/inspection":
        return create_inspection(event)

    elif http_method == "GET" and resource == "/inspections":
        return list_inspections(event)

    elif http_method == "GET" and resource == "/inspection/{inspection_id}":
        return get_inspection(event)

    elif http_method == "GET" and resource == "/evidence/upload-url":
        return generate_upload_url(event)

    elif http_method == "GET" and resource == "/evidence/download-url":
        return generate_download_url(event)

    elif http_method == "GET" and resource == "/admin/inspections":
        return admin_list_inspections(event)

    elif http_method == "PATCH" and resource == "/inspection/{inspection_id}":
        return update_inspection(event)

    elif http_method == "DELETE" and resource == "/inspection/{inspection_id}":
        return delete_inspection(event)

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})
