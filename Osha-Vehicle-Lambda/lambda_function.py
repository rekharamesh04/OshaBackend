"""
Vehicle Inspection (Pre-trip / DVIR) - Lambda Handler

AWS setup (Dev) — same pattern as HRA / Fire Extinguisher / Exit Door:
  1. Create DynamoDB table osha-vehicle-inspections (PK: inspection_id, String)
  2. Deploy this folder as a Lambda; set API_KEY, INSPECTION_TABLE_NAME,
     SESSION_TABLE_NAME (osha-inspection-sessions), CHECKLIST_TEMPLATE_TABLE
  3. API Gateway routes (below) → this Lambda (same auth style as peers)
  4. Optional: seed osha-checklist-templates with tenant_id=default,
     checklist_type=vehicle-inspection

Routes:
  GET    /vehicle-inspection/checklist              → Get checklist template
  POST   /vehicle-inspection                        → Create inspection (linked to session)
  GET    /vehicle-inspections                       → List inspections (summary)
  GET    /vehicle-inspection/{inspection_id}        → Get full inspection
  PUT    /vehicle-inspection/{inspection_id}        → Update header fields + answers
  DELETE /vehicle-inspection/{inspection_id}        → Delete inspection
"""

import copy
import json
import logging
import os
import uuid

import boto3
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger(__name__)

try:
    from checklist_loader import (
        load_checklist,
        clear_cache,
        filter_disabled_items,
        get_company_config,
        sync_inspection_with_template,
        build_mobile_checklist_response,
    )
except ImportError:
    load_checklist = None
    clear_cache = None
    filter_disabled_items = None
    get_company_config = None
    sync_inspection_with_template = None
    build_mobile_checklist_response = None

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.getenv("INSPECTION_TABLE_NAME", "osha-vehicle-inspections"))
sessions_table = dynamodb.Table(os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions"))

EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()

CHECKLIST_TYPE = "vehicle-inspection"
VALID_ANSWERS = {"OK", "Minor Defect", "Major Defect", "N/A"}

USA_INSPECTION_ITEMS = [
    (1, "Air Brake System"),
    (2, "Cab / Sleeper"),
    (3, "Cargo Securement"),
    (4, "Coupling Devices"),
    (5, "Dangerous Goods"),
    (6, "Driver Controls"),
    (7, "Driver Seat"),
    (8, "Electric Brake System"),
    (9, "Emergency Equipment and Safety Devices"),
    (10, "Exhaust System"),
    (11, "Frame and Cargo Body"),
    (12, "Fuel System"),
    (13, "Glass and Mirrors"),
    (14, "Horn"),
    (15, "Lamps and Reflectors"),
    (16, "Steering / Defroster"),
    (17, "Suspension System"),
    (18, "Hydraulic Brake System"),
    (19, "Wheels, Hubs and Fasteners"),
    (20, "Tires"),
    (21, "Windshield Wiper / Washer"),
    (22, "Other"),
]

INSPECTION_INSTANCE_ITEMS = [
    (23, "Fleet Complete DVIR"),
    (24, "Emergency Safety Kit"),
]


def _blank_item(item_id, description):
    return {
        "id": item_id,
        "description": description,
        "answer": "",
        "finding": "",
        "action_item": "",
        "responsible": "",
        "due_date": "",
        "evidence": [],
    }


_FALLBACK_CHECKLIST = {
    "inspection_type": "Vehicle Inspection",
    "general_information": {
        "account": "",
        "date": "",
        "motor_carrier": "",
        "plate_number": "",
        "odometer": "",
        "location": "",
        "start_date": "",
        "checklist": "Vehicle Inspection Report — Pre-trip Inspection",
        "leader": "",
        "team": [],
    },
    "available_answers": ["OK", "Minor Defect", "Major Defect", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "USA Inspection Items",
            "items": [_blank_item(i, d) for i, d in USA_INSPECTION_ITEMS],
        },
        {
            "id": 2,
            "name": "Inspection Instances",
            "items": [_blank_item(i, d) for i, d in INSPECTION_INSTANCE_ITEMS],
        },
    ],
    "general_results": [
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
    ],
    "notes": "",
}


def _normalized_headers(event):
    headers = event.get("headers") or {}
    return {
        str(k).strip().lower(): ("" if v is None else str(v).strip())
        for k, v in headers.items()
    }


def require_api_key(event):
    if not EXPECTED_API_KEY:
        return build_response(500, {"error": "Server API_KEY env var is not configured"})
    headers = _normalized_headers(event)
    provided = (
        headers.get("x-api-key")
        or headers.get("x_api_key")
        or headers.get("apikey")
        or ""
    ).strip()
    if not provided:
        qsp = event.get("queryStringParameters") or {}
        provided = (
            qsp.get("x-api-key") or qsp.get("api_key") or qsp.get("apikey") or ""
        ).strip()
    if not provided or provided != EXPECTED_API_KEY:
        return build_response(403, {"error": "Forbidden", "message": "Invalid or missing API key"})
    return None


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        },
        "body": json.dumps(body, default=str),
    }


def get_checklist_template(company_key="default", force_refresh=False):
    if load_checklist is not None:
        template = load_checklist(CHECKLIST_TYPE, company_key, force_refresh=force_refresh)
        if template is not None:
            return template
    return copy.deepcopy(_FALLBACK_CHECKLIST)


def build_description_lookup(company_key="default"):
    checklist = get_checklist_template(company_key)
    lookup = {}
    for category in checklist.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = item["description"]
    return lookup


def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


def _compute_status(categories):
    for cat in categories or []:
        if not isinstance(cat, dict):
            continue
        for it in cat.get("items", []):
            if not isinstance(it, dict):
                continue
            if str(it.get("answer", "")).strip() == "":
                return "in_progress"
    return "completed" if categories else "in_progress"


def _validate_categories(categories, allow_empty_answer=False):
    if not categories or not isinstance(categories, list):
        return "categories must be a non-empty list"
    for i, cat in enumerate(categories):
        if not isinstance(cat, dict):
            return f"Category at index {i} must be an object"
        if "id" not in cat:
            return f"Category at index {i} is missing id"
        if "name" not in cat:
            return f"Category at index {i} is missing name"
        items = cat.get("items", [])
        if not isinstance(items, list):
            return f"Category '{cat.get('name')}' items must be a list"
        for j, item in enumerate(items):
            if not isinstance(item, dict):
                return f"Item at index {j} in category '{cat.get('name')}' must be an object"
            if "id" not in item:
                return f"Item at index {j} in category '{cat.get('name')}' is missing id"
            if "answer" not in item:
                return f"Item at index {j} in category '{cat.get('name')}' is missing answer"
            answer = str(item.get("answer", "")).strip()
            if answer == "":
                if allow_empty_answer:
                    continue
                return (
                    f"Item {item.get('id')} in category '{cat.get('name')}' "
                    f"has empty answer; use one of {sorted(VALID_ANSWERS)} or leave blank only on update"
                )
            if answer not in VALID_ANSWERS:
                return (
                    f"Item {item.get('id')} has invalid answer '{answer}'. "
                    f"Must be one of: {sorted(VALID_ANSWERS)}"
                )
    return None


def _extract_header_fields(body):
    return {
        "account": str(body.get("account", "")).strip(),
        "date": str(body.get("date", "")).strip(),
        "motor_carrier": str(body.get("motor_carrier", "")).strip(),
        "plate_number": str(body.get("plate_number", "")).strip(),
        "odometer": str(body.get("odometer", "")).strip(),
    }


# ─────────────────────────────────────────────
# API 1: GET /vehicle-inspection/checklist
# ─────────────────────────────────────────────
def get_checklist(event):
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key, force_refresh=True)
    if build_mobile_checklist_response is not None and template is not None:
        template = build_mobile_checklist_response(template, CHECKLIST_TYPE, company_key)
    elif filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return build_response(200, template)


# ─────────────────────────────────────────────
# API 2: POST /vehicle-inspection
# ─────────────────────────────────────────────
def create_inspection(event):
    """
    Expects JSON body:
    {
        "session_id": "string",
        "account": "", "date": "", "motor_carrier": "", "plate_number": "", "odometer": "",
        "team": ["string"],
        "categories": [
            {
                "id": 1,
                "name": "USA Inspection Items",
                "items": [
                    { "id": 1, "answer": "OK", "finding": "", "action_item": "", "responsible": "", "due_date": "" }
                ]
            }
        ],
        "general_results": [],
        "notes": ""
    }
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    session_id = body.get("session_id", "").strip()
    team = body.get("team", [])
    general_results = body.get("general_results", [])
    notes = body.get("notes", "").strip() if isinstance(body.get("notes", ""), str) else ""
    categories = body.get("categories", [])
    header = _extract_header_fields(body)

    if not session_id:
        return build_response(400, {"error": "session_id is required"})
    if len(notes) > 5000:
        return build_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return build_response(400, {"error": "general_results must be a list"})

    # Allow creating with empty answers (in progress) or valid PDF answers
    err = _validate_categories(categories, allow_empty_answer=True)
    if err:
        return build_response(400, {"error": err})

    session_result = sessions_table.get_item(Key={"session_id": session_id})
    session = session_result.get("Item")
    if not session:
        return build_response(
            404,
            {"error": "Session not found. Create a session first via POST /inspection-session"},
        )

    inspection_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    derived_status = _compute_status(categories)

    item = {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "company_key": str(body.get("company_key", "")).strip(),
        "auditor_name": session.get("auditor_name", ""),
        "location": session.get("location", ""),
        "facility_area": session.get("facility_area", ""),
        "station": session.get("station", ""),
        "station_id": session.get("station_id", ""),
        "date_of_audit": session.get("date_of_audit", ""),
        "account": header["account"],
        "date": header["date"] or session.get("date_of_audit", ""),
        "motor_carrier": header["motor_carrier"],
        "plate_number": header["plate_number"],
        "odometer": header["odometer"],
        "team": team if team else [],
        "categories": categories,
        "general_results": general_results if general_results else [],
        "notes": notes,
        "status": derived_status,
        "created_at": created_at,
        "updated_at": created_at,
    }

    if derived_status == "completed":
        item["completed_at"] = created_at
        logger.info(
            "[SUBMIT] Vehicle inspection %s marked completed at %s",
            inspection_id,
            created_at,
        )

    table.put_item(Item=item)

    try:
        sessions_table.update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET inspection_id = :iid, inspection_type = :itype, updated_at = :u",
            ExpressionAttributeValues={
                ":iid": inspection_id,
                ":itype": CHECKLIST_TYPE,
                ":u": created_at,
            },
        )
    except Exception:
        logger.exception(
            "Failed to link session %s to inspection %s (%s)",
            session_id,
            inspection_id,
            CHECKLIST_TYPE,
        )

    return build_response(
        201,
        {
            "inspection_id": inspection_id,
            "session_id": session_id,
            "created_at": created_at,
            "status": derived_status,
            "message": "Vehicle inspection created.",
        },
    )


# ─────────────────────────────────────────────
# API 3: GET /vehicle-inspections
# ─────────────────────────────────────────────
def list_inspections(event):
    result = table.scan()
    items = result.get("Items", [])

    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    items = convert_decimals(items)

    summary_list = []
    for item in items:
        summary_list.append(
            {
                "inspection_id": item.get("inspection_id"),
                "session_id": item.get("session_id"),
                "auditor_name": item.get("auditor_name"),
                "location": item.get("location"),
                "facility_area": item.get("facility_area"),
                "station": item.get("station"),
                "date_of_audit": item.get("date_of_audit"),
                "account": item.get("account", ""),
                "date": item.get("date", ""),
                "motor_carrier": item.get("motor_carrier", ""),
                "plate_number": item.get("plate_number", ""),
                "odometer": item.get("odometer", ""),
                "status": item.get("status", ""),
                "team": item.get("team", []),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
            }
        )

    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return build_response(200, summary_list)


# ─────────────────────────────────────────────
# API 4: GET /vehicle-inspection/{id}
# ─────────────────────────────────────────────
def get_inspection(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()

    result = table.get_item(Key={"inspection_id": inspection_id}, ConsistentRead=True)
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    item = convert_decimals(item)

    if company_key and company_key != "default" and sync_inspection_with_template is not None:
        synced = sync_inspection_with_template(item, CHECKLIST_TYPE, company_key)
        if synced.get("categories") != item.get("categories"):
            item = synced
            item["updated_at"] = datetime.now(timezone.utc).isoformat()
            table.put_item(Item=item)

    description_lookup = build_description_lookup(company_key or "default")
    categories = item.get("categories", [])
    for category in categories:
        for checklist_item in category.get("items", []):
            item_id = checklist_item.get("id")
            if item_id in description_lookup:
                checklist_item["description"] = description_lookup[item_id]

    ordered_item = {
        "inspection_id": item.get("inspection_id"),
        "session_id": item.get("session_id"),
        "auditor_name": item.get("auditor_name"),
        "location": item.get("location"),
        "facility_area": item.get("facility_area"),
        "station": item.get("station"),
        "date_of_audit": item.get("date_of_audit"),
        "account": item.get("account", ""),
        "date": item.get("date", ""),
        "motor_carrier": item.get("motor_carrier", ""),
        "plate_number": item.get("plate_number", ""),
        "odometer": item.get("odometer", ""),
        "team": item.get("team", []),
        "categories": categories,
        "general_results": item.get("general_results", []),
        "notes": item.get("notes", ""),
        "status": item.get("status", ""),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "completed_at": item.get("completed_at"),
    }

    return build_response(200, ordered_item)


# ─────────────────────────────────────────────
# API 5: PUT /vehicle-inspection/{id}
# ─────────────────────────────────────────────
def update_inspection(event):
    """
    Update PDF header fields and/or checklist answers.

    Body (all optional except at least one meaningful field):
    {
        "account": "", "date": "", "motor_carrier": "", "plate_number": "", "odometer": "",
        "team": [],
        "categories": [...],
        "general_results": [],
        "notes": ""
    }
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()
    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    if not isinstance(body, dict) or not body:
        return build_response(400, {"error": "Request body is required"})

    result = table.get_item(Key={"inspection_id": inspection_id}, ConsistentRead=True)
    item = result.get("Item")
    if not item:
        return build_response(404, {"error": "Inspection not found"})

    updated_at = datetime.now(timezone.utc).isoformat()

    for field in ("account", "date", "motor_carrier", "plate_number", "odometer"):
        if field in body:
            item[field] = str(body.get(field, "")).strip()

    if "team" in body:
        if not isinstance(body.get("team"), list):
            return build_response(400, {"error": "team must be a list"})
        item["team"] = body["team"]

    if "notes" in body:
        notes = body.get("notes", "")
        if not isinstance(notes, str):
            return build_response(400, {"error": "notes must be a string"})
        if len(notes) > 5000:
            return build_response(400, {"error": "notes must be under 5000 characters"})
        item["notes"] = notes.strip()

    if "general_results" in body:
        if not isinstance(body.get("general_results"), list):
            return build_response(400, {"error": "general_results must be a list"})
        item["general_results"] = body["general_results"]

    if "categories" in body:
        categories = body.get("categories")
        err = _validate_categories(categories, allow_empty_answer=True)
        if err:
            return build_response(400, {"error": err})
        item["categories"] = categories
        derived_status = _compute_status(categories)
        item["status"] = derived_status
        if derived_status == "completed" and not item.get("completed_at"):
            item["completed_at"] = updated_at
        elif derived_status != "completed":
            item.pop("completed_at", None)

    if "company_key" in body and str(body.get("company_key", "")).strip():
        item["company_key"] = str(body.get("company_key", "")).strip()

    item["updated_at"] = updated_at
    table.put_item(Item=item)

    return build_response(
        200,
        {
            "inspection_id": inspection_id,
            "status": item.get("status", ""),
            "updated_at": updated_at,
            "message": "Vehicle inspection updated.",
        },
    )


# ─────────────────────────────────────────────
# API 6: DELETE /vehicle-inspection/{id}
# ─────────────────────────────────────────────
def delete_inspection(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    session_id = str(item.get("session_id", "")).strip()

    try:
        table.delete_item(Key={"inspection_id": inspection_id})
    except Exception as e:
        print(f"Error deleting inspection: {str(e)}")
        return build_response(500, {"error": f"Failed to delete inspection: {str(e)}"})

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

    return build_response(
        200,
        {
            "message": "Inspection deleted successfully",
            "inspection_id": inspection_id,
            "session_id": session_id,
            "session_deleted": deleted_session,
        },
    )


# ─────────────────────────────────────────────
# Main Handler
# ─────────────────────────────────────────────
def lambda_handler(event, context):
    _claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    if _claims:
        _secure_company = _claims.get("custom:company_key", "")
        _groups = _claims.get("cognito:groups", "")
        if isinstance(_groups, str):
            _groups = [g.strip() for g in _groups.split(",")]
        elif not _groups:
            _groups = []

        if _secure_company and "SuperAdmin" not in _groups:
            if event.get("queryStringParameters") is None:
                event["queryStringParameters"] = {}
            event["queryStringParameters"]["company_key"] = _secure_company
            event["queryStringParameters"]["tenant_id"] = _secure_company

            import json as _json

            _raw_body = event.get("body") or "{}"
            try:
                _body_obj = _json.loads(_raw_body) if isinstance(_raw_body, str) else _raw_body
                if isinstance(_body_obj, dict):
                    _body_obj["company_key"] = _secure_company
                    event["body"] = _json.dumps(_body_obj)
            except Exception:
                pass

    http_method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method", "")
    resource = event.get("resource") or event.get("routeKey", "")
    path = event.get("path") or event.get("rawPath", "")

    print(f"Received: {http_method} {resource} (path: {path})")

    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    auth_error = require_api_key(event)
    if auth_error:
        return auth_error

    if http_method == "GET" and resource == "/vehicle-inspection/checklist":
        return get_checklist(event)

    if http_method == "POST" and resource == "/vehicle-inspection":
        return create_inspection(event)

    if http_method == "GET" and resource == "/vehicle-inspections":
        return list_inspections(event)

    if http_method == "GET" and resource == "/vehicle-inspection/{inspection_id}":
        return get_inspection(event)

    if http_method == "PUT" and resource == "/vehicle-inspection/{inspection_id}":
        return update_inspection(event)

    if http_method == "DELETE" and resource == "/vehicle-inspection/{inspection_id}":
        return delete_inspection(event)

    return build_response(404, {"error": f"Route not found: {http_method} {resource}"})
