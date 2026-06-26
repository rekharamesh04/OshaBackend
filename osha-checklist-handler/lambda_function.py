"""
OSHA Inspection Checklist - Lambda Handler
Single Lambda function handling API routes:

  --- CRUD (Inspections) ---
  POST /inspection-session          → Create a new inspection session (general info)
  POST /inspection                  → Submit checklist (linked to session)
  GET  /inspections                 → List all inspections (summary)
  GET  /inspection/{id}             → Get full inspection by ID
  GET  /inspection/checklist        → Get checklist template
  GET  /evidence/upload-url         → Generate pre-signed S3 URL for uploading evidence
  GET  /evidence/download-url       → Generate pre-signed S3 URL for downloading/viewing evidence
  DELETE /inspection/{id}           → Delete an inspection by ID

  --- Checklist Template Management (CRUD) ---
  POST   /checklist-template                    → Create a checklist template
  GET    /checklist-templates                   → List all templates (filter by tenant_id)
  GET    /checklist-template/{checklist_type}    → Get a specific template (with tenant fallback)
  PUT    /checklist-template/{checklist_type}    → Update an existing template
  DELETE /checklist-template/{checklist_type}    → Delete a company-specific template
"""

import copy
import json
import os
import uuid

import boto3
from datetime import datetime, timezone
from decimal import Decimal

try:
    from checklist_loader import (
        load_checklist, clear_cache, filter_disabled_items,
        get_company_config, save_company_config, VALID_BLOCKED_LABELS,
    )
except ImportError:
    load_checklist = None
    clear_cache = None
    filter_disabled_items = None
    get_company_config = None
    save_company_config = None
    VALID_BLOCKED_LABELS = {"need_review", "fail"}

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("osha-inspections")
sessions_table = dynamodb.Table("osha-inspection-sessions")
templates_table = dynamodb.Table(os.getenv("CHECKLIST_TEMPLATE_TABLE", "osha-checklist-templates"))

# API Key Authentication
EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()


# Initialize S3 client for evidence uploads
s3_client = boto3.client("s3")

# ─────────────────────────────────────────────
# Evidence Upload Configuration
# ─────────────────────────────────────────────
EVIDENCE_S3_BUCKET = os.environ.get("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")
UPLOAD_URL_EXPIRY = 900       # 15 minutes for uploads
DOWNLOAD_URL_EXPIRY = 3600    # 1 hour for downloads/viewing
MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB

ALLOWED_CONTENT_TYPES = [
    # Images
    "image/jpeg", "image/png", "image/gif", "image/webp", "image/heic", "image/heif",
    # Videos
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/webm", "video/3gpp",
    # Documents
    "application/pdf",
    "application/msword",                                                          # .doc
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",      # .docx
    "application/vnd.ms-excel",                                                    # .xls
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",            # .xlsx
    "application/vnd.ms-powerpoint",                                               # .ppt
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",    # .pptx
    "text/plain",                                                                  # .txt
    "text/csv",                                                                    # .csv
    "application/zip",                                                             # .zip
    "application/x-rar-compressed",                                                # .rar
    "application/x-7z-compressed",                                                 # .7z
]


# ─────────────────────────────────────────────
# Checklist Definition — Fallback (used when DynamoDB is unreachable)
# ─────────────────────────────────────────────
_FALLBACK_CHECKLIST = {
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
    """Builds a standardized API Gateway response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        },
        "body": json.dumps(body, default=str),
    }


# ─────────────────────────────────────────────
# Helper: Get checklist template (DynamoDB → fallback)
# ─────────────────────────────────────────────
def get_checklist_template(company_key="default"):
    """Load checklist from DynamoDB with company overlay fallback. Falls back to hardcoded."""
    if load_checklist is not None:
        template = load_checklist("recordkeeping", company_key)
        if template is not None:
            return template
    return copy.deepcopy(_FALLBACK_CHECKLIST)


# ─────────────────────────────────────────────
# Helper: Build item description lookup from checklist template
# ─────────────────────────────────────────────
def build_description_lookup(company_key="default"):
    """Creates a dict mapping item_id → {description, title} from the checklist template.
    Handles both top-level items and sub_section items."""
    checklist = get_checklist_template(company_key)
    lookup = {}
    for category in checklist.get("categories", []):
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
# API 1: GET /inspection/checklist — Get Checklist Template
# ─────────────────────────────────────────────
def get_checklist(event):
    """
    Returns the OSHA checklist template for mobile inspection.
    Disabled items are filtered out so the mobile app only sees enabled questions.
    Supports optional ?company_key= query parameter for company-specific checklists.
    """
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key)
    # Filter out disabled items for mobile — only return enabled questions
    if filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return build_response(200, template)


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
        "date_of_audit": "YYYY-MM-DD",
        "location": "string",          // optional
        "station": "string",            // optional — station display name
        "station_id": "string"          // optional — dashboard station ID for progress tracking
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
    location = body.get("location", "").strip()
    station = body.get("station", "").strip()
    station_id = body.get("station_id", "").strip()

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
        "location": location,
        "station": station,
        "station_id": station_id,
        "created_at": created_at,
    }

    sessions_table.put_item(Item=session_item)

    return build_response(201, {
        "session_id": session_id,
        "auditor_name": auditor_name,
        "facility_area": facility_area,
        "date_of_audit": date_of_audit,
        "location": location,
        "station": station,
        "station_id": station_id,
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
        "location": session.get("location", ""),
        "station": session.get("station", ""),
        "station_id": session.get("station_id", ""),
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
            "location": item.get("location"),
            "station": item.get("station"),
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
        "location": item.get("location"),
        "station": item.get("station"),
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
    an image, video, or document directly to S3.

    Query Parameters:
        filename       (required) — Original file name (e.g. "photo_001.jpg", "report.pdf")
        contentType    (required) — MIME type (e.g. "image/jpeg", "application/pdf")
        fileSize       (optional) — File size in bytes for server-side validation (max 25 MB)
        inspectionType (optional) — For folder organization
                                    (e.g. "fire-extinguisher", "eyewash", "racking", "hra", "osha")

    Returns:
        {
            "upload_url": "https://s3.amazonaws.com/...",
            "file_url": "https://s3.amazonaws.com/...",
            "file_key": "evidence/fire-extinguisher/...",
            "content_type": "application/pdf",
            "filename": "report.pdf",
            "max_file_size_bytes": 26214400,
            "expires_in": 900
        }
    """
    params = event.get("queryStringParameters", {}) or {}
    filename = params.get("filename", "").strip()
    content_type = params.get("contentType", "").strip()
    file_size_str = params.get("fileSize", "").strip()
    inspection_type = params.get("inspectionType", "general").strip()

    if not filename:
        return build_response(400, {"error": "filename query parameter is required"})
    if not content_type:
        return build_response(400, {"error": "contentType query parameter is required"})
    if content_type not in ALLOWED_CONTENT_TYPES:
        return build_response(400, {
            "error": f"contentType '{content_type}' is not allowed. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"
        })

    # Validate file size if provided
    if file_size_str:
        try:
            file_size = int(file_size_str)
            if file_size > MAX_FILE_SIZE_BYTES:
                max_mb = MAX_FILE_SIZE_BYTES / (1024 * 1024)
                return build_response(400, {
                    "error": f"File size ({file_size} bytes) exceeds the maximum allowed size of {max_mb:.0f} MB"
                })
            if file_size <= 0:
                return build_response(400, {"error": "fileSize must be a positive integer"})
        except ValueError:
            return build_response(400, {"error": "fileSize must be a valid integer (bytes)"})

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
        "content_type": content_type,
        "filename": safe_filename,
        "max_file_size_bytes": MAX_FILE_SIZE_BYTES,
        "expires_in": UPLOAD_URL_EXPIRY,
    })


# ─────────────────────────────────────────────
# API 7: GET /evidence/download-url — Generate Pre-Signed Download URL
# ─────────────────────────────────────────────
def generate_download_url(event):
    """
    Generates a pre-signed S3 URL for downloading/viewing an evidence file.
    Supports inline viewing (e.g. PDFs in browser) and forced file downloads.

    Query Parameters:
        fileKey   (required) — The S3 object key (returned as file_key from upload-url)
        download  (optional) — Set to "true" to force browser download instead of inline view

    Returns:
        {
            "download_url": "https://s3.amazonaws.com/...",
            "filename": "report.pdf",
            "content_type": "application/pdf",
            "file_size": 1048576,
            "expires_in": 3600
        }
    """
    params = event.get("queryStringParameters", {}) or {}
    file_key = params.get("fileKey", "").strip()
    force_download = params.get("download", "false").strip().lower() in ("1", "true", "yes")

    if not file_key:
        return build_response(400, {"error": "fileKey query parameter is required"})

    # Verify the file exists in S3 and get metadata
    try:
        head = s3_client.head_object(Bucket=EVIDENCE_S3_BUCKET, Key=file_key)
        content_type = head.get("ContentType", "application/octet-stream")
        file_size = head.get("ContentLength", 0)
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "404":
            return build_response(404, {"error": "File not found in S3"})
        print(f"Error checking S3 object: {str(e)}")
        return build_response(500, {"error": "Failed to verify file existence"})

    # Extract original filename from S3 key (strip the UUID prefix)
    raw_filename = file_key.rsplit("/", 1)[-1]
    if "_" in raw_filename:
        filename = raw_filename.split("_", 1)[1]
    else:
        filename = raw_filename

    # Build pre-signed URL with Content-Disposition for proper browser handling
    disposition = "attachment" if force_download else "inline"
    try:
        download_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": EVIDENCE_S3_BUCKET,
                "Key": file_key,
                "ResponseContentDisposition": f'{disposition}; filename="{filename}"',
            },
            ExpiresIn=DOWNLOAD_URL_EXPIRY,
        )
    except Exception as e:
        print(f"Error generating download URL: {str(e)}")
        return build_response(500, {"error": "Failed to generate download URL"})

    return build_response(200, {
        "download_url": download_url,
        "filename": filename,
        "content_type": content_type,
        "file_size": file_size,
        "expires_in": DOWNLOAD_URL_EXPIRY,
    })


# ─────────────────────────────────────────────
# API 8: DELETE /inspection/{id} — Delete Inspection
# ─────────────────────────────────────────────
def delete_inspection(event):
    """
    Deletes an inspection record by inspection_id.
    Also deletes the associated session record from the sessions table.

    Path parameter:
        inspection_id (required) — The inspection ID to delete

    Query parameter:
        delete_session (optional, default true) — Also delete the linked session
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    # Fetch the inspection to verify it exists and get session_id
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    session_id = str(item.get("session_id", "")).strip()

    # Delete the inspection
    try:
        table.delete_item(Key={"inspection_id": inspection_id})
    except Exception as e:
        print(f"Error deleting inspection: {str(e)}")
        return build_response(500, {"error": f"Failed to delete inspection: {str(e)}"})

    # Optionally delete the linked session
    delete_session_flag = str(
        (event.get("queryStringParameters") or {}).get("delete_session", "true")
    ).strip().lower()
    deleted_session = False
    if delete_session_flag in {"1", "true", "yes", "y"} and session_id:
        try:
            sessions_table.delete_item(Key={"session_id": session_id})
            deleted_session = True
        except Exception as e:
            print(f"Warning: Failed to delete linked session {session_id}: {str(e)}")

    return build_response(200, {
        "message": "Inspection deleted successfully",
        "inspection_id": inspection_id,
        "session_id": session_id,
        "session_deleted": deleted_session,
    })


# ═══════════════════════════════════════════════════════════════
# CHECKLIST TEMPLATE MANAGEMENT (Overlay Model)
# ═══════════════════════════════════════════════════════════════
# Companies CANNOT edit default questions. They can only:
#   1. Toggle items on/off (disabled_items)
#   2. Add custom questions (custom_items)
# ═══════════════════════════════════════════════════════════════

VALID_CHECKLIST_TYPES = {
    "fire-extinguisher", "eyewash", "exit-door",
    "racking", "hra", "recordkeeping",
}


def _convert_floats_to_decimal(obj):
    """Convert float values to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_floats_to_decimal(i) for i in obj]
    return obj


def _get_or_create_overlay(company_key, checklist_type):
    """Get existing overlay or return empty structure."""
    try:
        resp = templates_table.get_item(Key={
            "tenant_id": company_key,
            "checklist_type": checklist_type,
        })
        item = resp.get("Item")
        if item:
            return convert_decimals(item)
    except Exception:
        pass
    return {
        "tenant_id": company_key,
        "checklist_type": checklist_type,
        "disabled_items": [],
        "custom_items": [],
    }


# ─────────────────────────────────────────────
# GET /checklist-templates — List All Templates
# ─────────────────────────────────────────────
def list_checklist_templates(event):
    """Lists all templates and overlays. Optionally filter by company_key."""
    params = event.get("queryStringParameters") or {}
    company_key_filter = params.get("company_key", params.get("tenant_id", "")).strip()

    try:
        if company_key_filter:
            resp = templates_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key("tenant_id").eq(company_key_filter)
            )
            items = resp.get("Items", [])
        else:
            resp = templates_table.scan()
            items = resp.get("Items", [])
            while "LastEvaluatedKey" in resp:
                resp = templates_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
                items.extend(resp.get("Items", []))
    except Exception as e:
        return build_response(500, {"error": f"Failed to list templates: {str(e)}"})

    items = convert_decimals(items)
    summary = []
    for item in items:
        entry = {
            "company_key": item.get("tenant_id"),
            "checklist_type": item.get("checklist_type"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }
        if "categories" in item:
            cat_count = 0
            item_count = 0
            for cat in item.get("categories", []):
                cat_count += 1
                item_count += len(cat.get("items", []))
                for sub in cat.get("sub_sections", []):
                    item_count += len(sub.get("items", []))
            entry["type"] = "master"
            entry["inspection_type"] = item.get("inspection_type")
            entry["category_count"] = cat_count
            entry["item_count"] = item_count
        else:
            entry["type"] = "overlay"
            entry["disabled_count"] = len(item.get("disabled_items", []))
            entry["custom_count"] = len(item.get("custom_items", []))

        summary.append(entry)

    summary.sort(key=lambda x: (x.get("company_key", ""), x.get("checklist_type", "")))
    return build_response(200, {"templates": summary, "count": len(summary)})


# ─────────────────────────────────────────────
# GET /checklist-template/{checklist_type} — Get Merged Checklist (Admin view)
# ─────────────────────────────────────────────
def get_checklist_template_by_type(event):
    """
    Returns the full checklist with overlay applied (all items with is_enabled flag).
    Admin UI uses this to show the complete list with toggles.
    """
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    if not checklist_type:
        return build_response(400, {"error": "checklist_type is required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"

    try:
        if load_checklist is not None:
            # Admin view always reads fresh from DynamoDB — bypasses Lambda in-memory cache
            # so mutations (toggle-item, add-custom-item) are immediately visible even if
            # a different Lambda container handled the write.
            template = load_checklist(checklist_type, company_key, force_refresh=True)
            if template:
                template["_source"] = "default" if company_key == "default" else "default + overlay"
                template["_company_key"] = company_key
                return build_response(200, template)

        return build_response(404, {"error": f"Template not found for '{checklist_type}'"})

    except Exception as e:
        return build_response(500, {"error": f"Failed to get template: {str(e)}"})


# ─────────────────────────────────────────────
# GET /checklist-template/{checklist_type}/config — Get Raw Overlay
# ─────────────────────────────────────────────
def get_tenant_config(event):
    """Returns the raw company overlay (disabled_items + custom_items) for admin UI."""
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    if not checklist_type:
        return build_response(400, {"error": "checklist_type is required"})

    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "")).strip()
    if not company_key:
        return build_response(400, {"error": "company_key query parameter is required"})
    if company_key == "default":
        return build_response(400, {"error": "Config endpoint is for company overlays only, not 'default'"})

    try:
        from checklist_loader import load_company_overlay
        overlay = load_company_overlay(checklist_type, company_key)
        if overlay:
            return build_response(200, convert_decimals(overlay))
        return build_response(200, {
            "company_key": company_key,
            "checklist_type": checklist_type,
            "disabled_items": [],
            "custom_items": [],
            "message": "No customization - using full default checklist",
        })
    except Exception as e:
        return build_response(500, {"error": f"Failed to get config: {str(e)}"})

# ─────────────────────────────────────────────
# PUT /checklist-template/{checklist_type}/toggle-item — Enable/Disable Item
# ─────────────────────────────────────────────
def toggle_checklist_item(event):
    """
    Toggle a default checklist item on/off for a company.

    Body: {"company_key": "cigroupusa", "item_id": 3, "enabled": false}
    """
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    if not checklist_type:
        return build_response(400, {"error": "checklist_type is required"})
    if checklist_type not in VALID_CHECKLIST_TYPES:
        return build_response(400, {"error": f"Invalid checklist_type. Must be one of: {sorted(VALID_CHECKLIST_TYPES)}"})

    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON"})

    company_key = str(body.get("company_key", body.get("tenant_id", ""))).strip()
    item_id = body.get("item_id")
    enabled = body.get("enabled")

    if not company_key:
        return build_response(400, {"error": "company_key is required"})
    if company_key == "default":
        return build_response(400, {"error": "Cannot modify default template. Only company overlays can be modified."})
    if item_id is None:
        return build_response(400, {"error": "item_id is required"})
    if enabled is None or not isinstance(enabled, bool):
        return build_response(400, {"error": "enabled must be true or false"})

    # Validate item_id exists in default template
    try:
        from checklist_loader import get_default_item_ids
        valid_ids = get_default_item_ids(checklist_type)
        normalized_id = int(item_id) if isinstance(item_id, (int, float)) else item_id
        if valid_ids and normalized_id not in valid_ids:
            return build_response(400, {"error": f"item_id {item_id} does not exist in the default '{checklist_type}' checklist"})
    except Exception:
        pass  # If validation fails, proceed anyway

    overlay = _get_or_create_overlay(company_key, checklist_type)
    disabled = [int(x) if isinstance(x, (int, float, Decimal)) else x for x in overlay.get("disabled_items", [])]
    normalized_item = int(item_id) if isinstance(item_id, (int, float)) else item_id

    if enabled:
        # Remove from disabled list
        disabled = [x for x in disabled if x != normalized_item]
    else:
        # Add to disabled list
        if normalized_item not in disabled:
            disabled.append(normalized_item)

    now = datetime.now(timezone.utc).isoformat()

    try:
        templates_table.update_item(
            Key={"tenant_id": company_key, "checklist_type": checklist_type},
            UpdateExpression="SET #di = :di, #ua = :ua",
            ExpressionAttributeNames={"#di": "disabled_items", "#ua": "updated_at"},
            ExpressionAttributeValues={
                ":di": _convert_floats_to_decimal(disabled),
                ":ua": now,
            },
        )
        # Ensure custom_items exists
        templates_table.update_item(
            Key={"tenant_id": company_key, "checklist_type": checklist_type},
            UpdateExpression="SET #ci = if_not_exists(#ci, :empty), #ca = if_not_exists(#ca, :now)",
            ExpressionAttributeNames={"#ci": "custom_items", "#ca": "created_at"},
            ExpressionAttributeValues={":empty": [], ":now": now},
        )
    except Exception as e:
        return build_response(500, {"error": f"Failed to toggle item: {str(e)}"})

    if clear_cache:
        clear_cache()

    action = "enabled" if enabled else "disabled"
    return build_response(200, {
        "message": f"Item {item_id} {action} for company '{company_key}' in '{checklist_type}'",
        "disabled_items": disabled,
        "updated_at": now,
    })


# ─────────────────────────────────────────────
# POST /checklist-template/{checklist_type}/custom-item — Add Custom Question
# ─────────────────────────────────────────────
def add_custom_item(event):
    """
    Add a custom question for a company.

    Body: {"company_key": "cigroupusa", "category_id": 1, "description": "..."}
    """
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    if not checklist_type:
        return build_response(400, {"error": "checklist_type is required"})
    if checklist_type not in VALID_CHECKLIST_TYPES:
        return build_response(400, {"error": f"Invalid checklist_type"})

    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON"})

    company_key = str(body.get("company_key", body.get("tenant_id", ""))).strip()
    category_id = body.get("category_id")
    description = str(body.get("description", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required"})
    if company_key == "default":
        return build_response(400, {"error": "Cannot add custom items to default template"})
    if category_id is None:
        return build_response(400, {"error": "category_id is required (which category to add the question to)"})
    if not description:
        return build_response(400, {"error": "description is required"})

    import time
    custom_id = f"custom_{int(time.time())}"

    custom_item = {
        "id": custom_id,
        "title": "Custom Field",
        "description": description,
        "category_id": int(category_id) if isinstance(category_id, (int, float)) else category_id,
        "is_custom": True,
        "answer": "",
        "finding": "",
        "action_item": "",
        "responsible": "",
        "due_date": "",
        "evidence": [],
    }

    now = datetime.now(timezone.utc).isoformat()

    try:
        templates_table.update_item(
            Key={"tenant_id": company_key, "checklist_type": checklist_type},
            UpdateExpression="SET #ci = list_append(if_not_exists(#ci, :empty), :new_item), #ua = :ua, #di = if_not_exists(#di, :empty), #ca = if_not_exists(#ca, :now)",
            ExpressionAttributeNames={
                "#ci": "custom_items", "#ua": "updated_at",
                "#di": "disabled_items", "#ca": "created_at",
            },
            ExpressionAttributeValues={
                ":new_item": _convert_floats_to_decimal([custom_item]),
                ":ua": now, ":empty": [], ":now": now,
            },
        )
    except Exception as e:
        return build_response(500, {"error": f"Failed to add custom item: {str(e)}"})

    if clear_cache:
        clear_cache()

    return build_response(201, {
        "message": "Custom item added successfully",
        "custom_item": custom_item,
        "updated_at": now,
    })


# ─────────────────────────────────────────────
# DELETE /checklist-template/{checklist_type}/custom-item/{item_id} — Remove Custom Question
# ─────────────────────────────────────────────
def delete_custom_item(event):
    """Remove a custom question from a company's overlay."""
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    item_id = str(path_params.get("item_id", "")).strip()

    if not checklist_type or not item_id:
        return build_response(400, {"error": "checklist_type and item_id are required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "")).strip()
    if not company_key:
        return build_response(400, {"error": "company_key query parameter is required"})
    if company_key == "default":
        return build_response(400, {"error": "Cannot modify default template"})

    overlay = _get_or_create_overlay(company_key, checklist_type)
    custom_items = overlay.get("custom_items", [])

    new_custom = [ci for ci in custom_items if str(ci.get("id", "")) != item_id]
    if len(new_custom) == len(custom_items):
        return build_response(404, {"error": f"Custom item '{item_id}' not found in overlay"})

    now = datetime.now(timezone.utc).isoformat()

    try:
        templates_table.update_item(
            Key={"tenant_id": company_key, "checklist_type": checklist_type},
            UpdateExpression="SET #ci = :ci, #ua = :ua",
            ExpressionAttributeNames={"#ci": "custom_items", "#ua": "updated_at"},
            ExpressionAttributeValues={
                ":ci": _convert_floats_to_decimal(new_custom),
                ":ua": now,
            },
        )
    except Exception as e:
        return build_response(500, {"error": f"Failed to delete custom item: {str(e)}"})

    if clear_cache:
        clear_cache()

    return build_response(200, {
        "message": f"Custom item '{item_id}' removed from '{checklist_type}' for company '{company_key}'",
        "updated_at": now,
    })


# ─────────────────────────────────────────────
# DELETE /checklist-template/{checklist_type} — Delete Tenant Overlay (Revert to Default)
# ─────────────────────────────────────────────
def delete_checklist_template(event):
    """Deletes a company's overlay, reverting them to the full default checklist."""
    path_params = event.get("pathParameters", {}) or {}
    checklist_type = str(path_params.get("checklist_type", "")).strip()
    if not checklist_type:
        return build_response(400, {"error": "checklist_type is required"})

    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "")).strip()
    if not company_key:
        return build_response(400, {"error": "company_key query parameter is required"})
    if company_key == "default":
        return build_response(403, {"error": "Cannot delete default templates."})

    try:
        existing = templates_table.get_item(Key={"tenant_id": company_key, "checklist_type": checklist_type})
        if not existing.get("Item"):
            return build_response(404, {"error": f"No overlay found for company '{company_key}', type '{checklist_type}'"})
    except Exception as e:
        return build_response(500, {"error": f"Failed to verify: {str(e)}"})

    try:
        templates_table.delete_item(Key={"tenant_id": company_key, "checklist_type": checklist_type})
    except Exception as e:
        return build_response(500, {"error": f"Failed to delete: {str(e)}"})

    if clear_cache:
        clear_cache()

    return build_response(200, {
        "message": f"Overlay deleted. '{company_key}' will now use the default '{checklist_type}' checklist.",
        "company_key": company_key,
        "checklist_type": checklist_type,
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
        DELETE /inspection/{inspection_id}      → delete_inspection
        GET  /evidence/upload-url               → generate_upload_url
        GET  /evidence/download-url             → generate_download_url
        OPTIONS (any)                           → CORS preflight
    """
    http_method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path = event.get("path", "")

    print(f"Received: {http_method} {resource} (path: {path})")  # CloudWatch logging

    # CORS preflight
    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # API Key validation
    auth_error = require_api_key(event)
    if auth_error:
        return auth_error

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

    elif http_method == "DELETE" and resource == "/inspection/{inspection_id}":
        return delete_inspection(event)

    elif http_method == "GET" and resource == "/evidence/upload-url":
        return generate_upload_url(event)

    elif http_method == "GET" and resource == "/evidence/download-url":
        return generate_download_url(event)

    # ── Checklist Template Management (Overlay Model) ──
    elif http_method == "GET" and resource == "/checklist-templates":
        return list_checklist_templates(event)

    elif http_method == "GET" and resource == "/checklist-template/{checklist_type}":
        return get_checklist_template_by_type(event)

    elif http_method == "GET" and resource == "/checklist-template/{checklist_type}/config":
        return get_tenant_config(event)

    elif http_method == "PUT" and resource == "/checklist-template/{checklist_type}/toggle-item":
        return toggle_checklist_item(event)

    elif http_method == "POST" and resource == "/checklist-template/{checklist_type}/custom-item":
        return add_custom_item(event)

    elif http_method == "DELETE" and resource == "/checklist-template/{checklist_type}/custom-item/{item_id}":
        return delete_custom_item(event)

    elif http_method == "DELETE" and resource == "/checklist-template/{checklist_type}":
        return delete_checklist_template(event)

    # ── Company Config (Blocked Verdict Label) ──
    elif http_method == "GET" and resource == "/company-config/{company_key}":
        return handle_get_company_config(event)

    elif http_method == "PUT" and resource == "/company-config/{company_key}":
        return handle_put_company_config(event)

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})


# ─────────────────────────────────────────────
# Company Config: GET /company-config/{company_key}
# ─────────────────────────────────────────────
def handle_get_company_config(event):
    """
    Returns the company-level configuration.
    Currently includes: blocked_verdict_label ("need_review" or "fail").
    """
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required"})

    if get_company_config is None:
        return build_response(500, {"error": "checklist_loader not available"})

    config = get_company_config(company_key)
    return build_response(200, {
        "company_key": company_key,
        "blocked_verdict_label": config.get("blocked_verdict_label", "need_review"),
    })


# ─────────────────────────────────────────────
# Company Config: PUT /company-config/{company_key}
# ─────────────────────────────────────────────
def handle_put_company_config(event):
    """
    Updates the company-level configuration.
    Body: { "blocked_verdict_label": "fail" | "need_review" }
    """
    path_params = event.get("pathParameters") or {}
    company_key = str(path_params.get("company_key", "")).strip()

    if not company_key:
        return build_response(400, {"error": "company_key is required"})

    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON"})

    label = str(body.get("blocked_verdict_label", "")).strip().lower()

    if label not in VALID_BLOCKED_LABELS:
        return build_response(400, {
            "error": f"blocked_verdict_label must be one of: {sorted(VALID_BLOCKED_LABELS)}",
        })

    if save_company_config is None:
        return build_response(500, {"error": "checklist_loader not available"})

    result = save_company_config(company_key, label)
    return build_response(200, {
        "message": f"Company config updated for '{company_key}'",
        "company_key": company_key,
        "blocked_verdict_label": result["blocked_verdict_label"],
        "updated_at": result["updated_at"],
    })

