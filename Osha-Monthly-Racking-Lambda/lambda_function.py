"""
Monthly Racking Inspection - Lambda Handler
Single Lambda function handling all API routes:

  --- CRUD (5) ---
  POST   /racking-inspection                          → Create new inspection
  GET    /racking-inspections                         → List all inspections
  GET    /racking-inspection/{id}                     → Get full inspection by ID
  GET    /racking-inspection/checklist                → Get checklist template
  DELETE /racking-inspection/{id}                     → Delete inspection by ID

    --- Mobile + AI Endpoints ---
  PATCH /racking/session/{id}/items/{item_id}       → Update single checklist item
  PATCH /racking/session/{id}/items/{item_id}/note  → Add note to a checklist item
  GET   /racking/session/{id}/report                → Slimmed inspection report
    GET   /racking/ai-enablement                      → AI feasibility matrix from the workbook
  POST  /racking/analyze                            → AI image analysis (Claude via Bedrock)
  POST  /racking/voice                              → Voice command parser

    --- Pause / Resume ---
  POST  /racking/session/{id}/pause                 → Pause an in-progress session
  GET   /racking/session/{id}/resume                → Resume a paused session
"""

import base64
import concurrent.futures
import copy
import io
import json
import logging
import os
import mimetypes
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

try:
    from PIL import Image
except Exception:
    Image = None

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────
# AWS Clients
# ─────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(os.getenv("INSPECTION_TABLE_NAME", "osha-racking-inspections"))
sessions_table = dynamodb.Table(os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions"))
s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
SONNET_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
HAIKU_MODEL_ID = os.getenv("BEDROCK_VOICE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
EVIDENCE_BUCKET = os.getenv("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")
BATCH_WORKER_COUNT = int(os.getenv("BATCH_WORKER_COUNT", "4"))

MAX_IMAGE_WIDTH = 800
MAX_IMAGE_QUALITY = 85
IMAGE_CONFIDENCE_BLOCK_THRESHOLD = 0.35
SUPPORTED_BEDROCK_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
VALID_OBJECTS = {
    "upright", "brace", "beam", "anchor", "connector",
    "guard", "load_label", "pallet"
}

# API Key Authentication
EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()


# ─────────────────────────────────────────────
# Checklist Definition — Single Source of Truth
# ─────────────────────────────────────────────
RACKING_CHECKLIST = {
    "inspection_type": "Racking Inspection",
    "general_information": {
        "location": "",
        "start_date": "",
        "checklist": "Racking Inspection",
        "leader": "",
        "team": [],
    },
    "available_answers": ["In compliance", "Needs maintenance", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Rack Frame Uprights",
            "items": [
                {"id": 1, "description": "Uprights are free of damage and deflection. No greater than 1/2 inch.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 2, "description": "Braces are in good condition.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 3, "description": "Uprights free of any visible rust, corrosion or twisting. No greater than 1/2 inch.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 2,
            "name": "Horizontal Cross Beams",
            "items": [
                {"id": 4, "description": "Beams are in good condition. Deflection no greater than 0.55% of the total horizontal length of the beam.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 5, "description": "All retaining clips (locking pins) in place.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 6, "description": "Horizontal beams and retaining clips free of any visible rust, corrosion or twisting. No greater than 1/2 inch.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 7, "description": "Cross beams are free of any pinching.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 3,
            "name": "Anchor Points",
            "items": [
                {"id": 8, "description": "All legs anchored to the floor. 1 anchor in the front foot plate and 1 in the back foot plate.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 9, "description": "Nuts on anchors are inspected for tightness. Any loose nuts are retightened.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 10, "description": "Anchor bolts in good condition.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 11, "description": "Back to back rack connectors in place.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 12, "description": "If building wall is used as anchor point, anchors in good condition.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 13, "description": "Footplate welds free of any visible rust, corrosion or twisting. No greater than 1/2 inch.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 4,
            "name": "Alignment",
            "items": [
                {"id": 14, "description": "Racks in proper alignment.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 15, "description": "Racks in vertical plumb. Not exceeding 1/2 inch per 10 feet of height.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 5,
            "name": "Guard Rails",
            "items": [
                {"id": 16, "description": "All end racks guarded in high traffic areas.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 17, "description": "All guards in good condition.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 6,
            "name": "Load Ratings",
            "items": [
                {"id": 18, "description": "Visible and legible from the powered industrial truck (P.I.T.) operator's point of view.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 19, "description": "Load capacity labels are contrasting to background AND other labeling.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 7,
            "name": "Pallets",
            "items": [
                {"id": 20, "description": "Pallets are in good condition & show no signs of stress or being compromised.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 21, "description": "Pallet support mechanisms (wire decking, wood supports, metal bracing) are in place and in good condition.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 22, "description": "All pallets above shoulder height are wrapped or banded.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
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
            "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        },
        "body": json.dumps(body, default=str),
    }


# ─────────────────────────────────────────────
# Helper: Build item description lookup from checklist template
# ─────────────────────────────────────────────
def build_description_lookup():
    """Creates a dict mapping item_id → description from the checklist template."""
    lookup = {}
    for category in RACKING_CHECKLIST.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = item["description"]
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


def safe_json_parse(text: str) -> dict:
    """Safely parse JSON from Claude response, handling code blocks and partial JSON."""
    if not text:
        return {"error": "empty"}
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return {"error": "parse_failed", "raw": text}
    return {"error": "parse_failed", "raw": text}


def _looks_like_heic(image_bytes):
    if not image_bytes or len(image_bytes) < 16:
        return False
    header = image_bytes[:32].lower()
    return b"ftypheic" in header or b"ftypheif" in header or b"ftypheix" in header or b"ftyphevc" in header


def _log_racking_fail_diag(item_id, image_source, content_type, reason, image_bytes=None):
    diag = {
        "item": str(item_id),
        "source": image_source,
        "content_type": content_type or "",
        "image_bytes": len(image_bytes or b""),
        "reason": reason,
    }
    logger.error("[RACKING_FAIL_DIAG] item=%s %s", item_id, json.dumps(diag, default=str))


def prepare_image_bytes(image_bytes, content_type="image/jpeg"):
    if Image is None or not image_bytes:
        return image_bytes, content_type or "image/jpeg"

    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")
        if img.width > MAX_IMAGE_WIDTH:
            ratio = MAX_IMAGE_WIDTH / float(img.width)
            new_size = (MAX_IMAGE_WIDTH, max(1, int(img.height * ratio)))
            img = img.resize(new_size, Image.LANCZOS)

        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=MAX_IMAGE_QUALITY, optimize=True)
        return buffer.getvalue(), "image/jpeg"
    except Exception:
        # Preserve bytes for downstream diagnostics, but do not pretend unsupported formats are valid.
        return image_bytes, content_type or "image/jpeg"


def _extract_image(body: dict) -> Tuple[Optional[bytes], str]:
    image_base64 = str(body.get("image_base64", "") or body.get("imageBase64", "")).strip()
    file_key = str(body.get("file_key", "") or body.get("fileKey", "")).strip()

    if image_base64:
        if "," in image_base64 and image_base64.startswith("data:"):
            image_base64 = image_base64.split(",", 1)[1]
        return base64.b64decode(image_base64), "image/jpeg"

    if file_key:
        obj = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=file_key)
        data = obj["Body"].read()
        ct = obj.get("ContentType", "") or ""
        if not ct or ct == "application/octet-stream":
            guessed, _ = mimetypes.guess_type(file_key)
            ct = guessed or "image/jpeg"
        return data, ct

    return None, "image/jpeg"


MANUAL_ONLY_ITEMS = set()
PARTIAL_AI_ITEMS = {"4", "9", "15"}

ITEM_CONFIDENCE_LEVEL = {
    "1": "high", "2": "high", "3": "high", "4": "medium", "5": "high", "6": "high",
    "7": "medium", "8": "high", "9": "low", "10": "high", "11": "high", "12": "high",
    "13": "high", "14": "medium", "15": "medium", "16": "high", "17": "high", "18": "high",
    "19": "high", "20": "high", "21": "high", "22": "high",
}

ITEM_MEDIA_REQUIREMENT = {
    "4": "scale_reference_photo",
    "9": "tight_hardware_photo",
    "15": "full_height_alignment_photo",
}

MANUAL_REVIEW_REASON = {
    "4": "AI can detect visible beam sag or bowing, but precise deflection measurement needs a scale reference and manual confirmation.",
    "9": "AI can flag loose or missing nuts, but physical tightness requires a torque check and manual confirmation.",
    "15": "AI can detect an obvious lean, but exact plumb measurement needs a reference and manual confirmation.",
}

MANUAL_ONLY_REASON = {}


CHECKLIST_VISUAL_RULES = {
    "1": """
ITEM: Uprights are free of damage and deflection.
LOOK FOR: Vertical upright columns, visible dents, bends, crushing, twisting, and missing sections.
PASS if: Upright appears straight, continuous, and structurally intact with no obvious deformation.
FAIL if: Upright is bent, dented, crushed, twisted, or visibly deflected from vertical.
NOTE: Evaluate only visible evidence. Do not guess hidden deformation from a weak image.
""",
    "2": """
ITEM: Braces are in good condition.
LOOK FOR: Diagonal and horizontal braces, weld points, joints, and attachment hardware.
PASS if: Braces are present, straight, and show no visible cracking, bending, or missing hardware.
FAIL if: Brace is missing, bent, broken, cracked, or visibly loose.
NOTE: Check both brace geometry and connection points. Partial visibility is not enough to pass.
""",
    "3": """
ITEM: Uprights are free of visible rust, corrosion, or twisting.
LOOK FOR: Surface finish, orange/brown rust, pitting, flaking paint, and geometric twisting.
PASS if: Upright surfaces look clean and straight with no obvious corrosion or twist.
FAIL if: Rust, corrosion, twisting, or distortion is visible anywhere on the upright.
NOTE: Dark paint can hide surface issues; if the surface is not clear, fail it.
""",
    "4": """
ITEM: Beams are in good condition with acceptable deflection.
LOOK FOR: Beam span, sagging, bowing, impact damage, and any visible distortion.
PASS if: Beam looks straight and supported with no obvious sag or damage.
FAIL if: Beam visibly bows, sags, bends, or shows impact damage.
NOTE: If a scale reference is not visible, treat precise deflection as unverified.
""",
    "5": """
ITEM: All retaining clips or locking pins are in place.
LOOK FOR: Beam connection points, locking hardware, and seated clip/pin ends.
PASS if: Each visible connection has a retaining clip or locking pin installed.
FAIL if: Any visible connection is missing its pin or the pin is visibly loose/absent.
NOTE: A clip that cannot be clearly seen is not confirmed. Inconclusive = fail.
""",
    "6": """
ITEM: Horizontal beams and retaining clips are free of rust, corrosion, or twisting.
LOOK FOR: Beam faces, clips, seams, and connection points.
PASS if: Surfaces appear clean and straight with no obvious corrosion or twist.
FAIL if: Rust, corrosion, twisting, or surface degradation is visible.
NOTE: Check both beam body and clip hardware. Surface uncertainty is a fail.
""",
    "7": """
ITEM: Cross beams are free of pinching.
LOOK FOR: Crushing marks, compression dents, forklift contact points, and abnormal deformation.
PASS if: Beam surfaces look uniform with no pinching or crushing marks.
FAIL if: Beam shows visible pinch marks, crushing, or local deformation.
NOTE: Look at the beam face and edges. Do not assume hidden damage is absent.
""",
    "8": """
ITEM: All legs are anchored to the floor.
LOOK FOR: Front and back foot plates and floor anchor bolts at each leg.
PASS if: Both anchor points are visible and appear installed.
FAIL if: Any foot plate appears unanchored, missing a bolt, or incomplete.
NOTE: Each leg needs visible anchoring evidence. Missing one side is a fail.
""",
    "9": """
ITEM: Nuts on anchors are tight and retightened if loose.
LOOK FOR: Anchor nuts, washers, seated threads, and visible looseness.
PASS if: Nuts are visible and seated correctly with no obvious looseness.
FAIL if: Nut is missing, visibly loose, cross-threaded, or not seated.
NOTE: Actual torque cannot be measured from a photo; treat visible looseness as fail.
""",
    "10": """
ITEM: Anchor bolts are in good condition.
LOOK FOR: Bolt heads, threads, washers, and surrounding concrete/floor condition.
PASS if: Bolt appears intact, straight, and free of visible corrosion or damage.
FAIL if: Bolt is bent, corroded, sheared, missing, or visibly damaged.
NOTE: Inspect the full visible bolt assembly. Hidden fasteners cannot be assumed good.
""",
    "11": """
ITEM: Back-to-back rack connectors are in place.
LOOK FOR: Connectors between adjacent rack rows and their fasteners.
PASS if: Connector hardware is visible and appears installed.
FAIL if: Connector hardware is missing, broken, or not visible where expected.
NOTE: If the connector area is blocked or out of frame, fail it rather than guessing.
""",
    "12": """
ITEM: Wall anchors are in good condition when a wall anchor is used.
LOOK FOR: Wall anchor points, attachment hardware, and surrounding wall interface.
PASS if: Visible wall anchors appear intact and secured.
FAIL if: Wall anchor is damaged, missing, corroded, or loose.
NOTE: Evaluate only when a wall anchor is actually present. If none is visible, do not infer.
""",
    "13": """
ITEM: Footplate welds are free of rust, corrosion, or twisting.
LOOK FOR: Weld beads, base plates, and visible stress marks at the foot plate.
PASS if: Welds appear continuous and intact with no visible rust or twist.
FAIL if: Weld shows corrosion, cracking, separation, or deformation.
NOTE: Weld areas should be visible enough to judge. If the weld is hidden, fail it.
""",
    "14": """
ITEM: Racks are in proper alignment.
LOOK FOR: Rack row spacing, vertical lines, aisle perspective, and row uniformity.
PASS if: The rack row appears straight and consistently aligned.
FAIL if: Columns or beams visibly offset, staggered, or skewed.
NOTE: Use an aisle-wide photo. Do not infer hidden alignment from a close-up view.
""",
    "15": """
ITEM: Racks are in vertical plumb.
LOOK FOR: Upright verticality from base to top across the full rack height.
PASS if: Upright looks visibly plumb with no obvious lean.
FAIL if: Upright visibly leans, tilts, or bows out of vertical.
NOTE: If the image lacks a reference line, exact measurement is unverified.
""",
    "16": """
ITEM: End racks are guarded in high traffic areas.
LOOK FOR: End-of-aisle guards, barriers, rack protectors, or corner impact protection.
PASS if: Guarding is visibly installed at the rack end in the traffic area.
FAIL if: No guard is visible where impact protection is expected.
NOTE: Confirm the protection is at the traffic-facing end, not elsewhere in the aisle.
""",
    "17": """
ITEM: Guards are in good condition.
LOOK FOR: Bends, impact damage, missing sections, loose mounting, and deformation.
PASS if: Guard appears intact and firmly mounted.
FAIL if: Guard is bent, broken, missing, or visibly loose.
NOTE: A guard that looks slightly off but unclear still fails because the condition is not verified.
""",
    "18": """
ITEM: Load ratings are visible and legible from the operator point of view.
LOOK FOR: Load rating placard from forklift eye level and readable text contrast.
PASS if: Label is present and readable from the operator perspective.
FAIL if: Label is missing, blocked, too far away, upside down, or unreadable.
NOTE: If operator-view legibility cannot be confirmed, treat it as a fail.
""",
    "19": """
ITEM: Load capacity labels contrast with background and other labeling.
LOOK FOR: Label color contrast, background color, and competing signage around the placard.
PASS if: Load label stands out clearly from surrounding labels and surface.
FAIL if: Label blends into background or is visually overwhelmed by other labeling.
NOTE: Contrast must be obvious in the image. Ambiguous contrast is a fail.
""",
    "20": """
ITEM: Pallets are in good condition and not compromised.
LOOK FOR: Broken boards, cracks, missing members, bowing, collapse, and stress damage.
PASS if: Pallet appears intact and structurally sound.
FAIL if: Any pallet damage, stress, or compromise is visible.
NOTE: Evaluate the pallet body, not just the outer edge. One damaged board is enough to fail.
""",
    "21": """
ITEM: Pallet support mechanisms are present and in good condition.
LOOK FOR: Wire decking, wood supports, metal bracing, or other support systems under the pallet.
PASS if: Support mechanism is visible, installed, and appears intact.
FAIL if: Support mechanism is missing, damaged, or visibly failing.
NOTE: If the support system cannot be clearly seen, do not assume it is present.
""",
    "22": """
ITEM: All pallets above shoulder height are wrapped or banded.
LOOK FOR: Elevated pallet loads and visible wrap or banding on the load.
PASS if: Upper pallets show visible wrap or banding securing the load.
FAIL if: Elevated pallet is unsecured, unwrapped, or not banded.
NOTE: Elevated loads must be visibly secured. If wrapping cannot be confirmed, fail it.
""",
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_body(event):
    body = event.get("body", "")
    if not body:
        return {}
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        return json.loads(body)
    return {}


def sanitize_for_dynamodb(value):
    if isinstance(value, dict):
        return {key: sanitize_for_dynamodb(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_for_dynamodb(item) for item in value]
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def save_inspection(inspection):
    table.put_item(Item=sanitize_for_dynamodb(inspection))


def load_inspection(inspection_id):
    inspection_id = str(inspection_id or "").strip()
    if not inspection_id:
        return None
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")
    if item:
        return convert_decimals(item)
    result = table.scan()
    items = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))
    for item in items:
        if str(item.get("session_id", "")).strip() == inspection_id:
            return convert_decimals(item)
    return None


def load_inspection_by_session(session_id):
    session_id = str(session_id or "").strip()
    if not session_id:
        return None
    result = table.scan()
    items = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))
    for item in items:
        if str(item.get("session_id", "")).strip() == session_id:
            return convert_decimals(item)
    return None


def find_item(inspection, item_id):
    item_id = str(item_id or "").strip()
    for cat_idx, category in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(category.get("items", [])):
            if str(item.get("id", "")).strip() == item_id:
                return item, cat_idx, item_idx
    return None, None, None


def get_linear_checklist_items(inspection):
    flattened = []
    for cat_idx, category in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(category.get("items", [])):
            flattened.append({"category_index": cat_idx, "item_index": item_idx, "item": item})
    return flattened


def clamp_index(index, total):
    if total <= 0:
        return 0
    return max(0, min(index, total - 1))


def get_current_voice_item(inspection):
    items = get_linear_checklist_items(inspection)
    if not items:
        return None, None
    try:
        current_index = int(inspection.get("current_item_index", 0) or 0)
    except Exception:
        current_index = 0
    current_index = clamp_index(current_index, len(items))
    return items[current_index], current_index


def next_unanswered_index(inspection):
    items = get_linear_checklist_items(inspection)
    for idx, entry in enumerate(items):
        if not str(entry["item"].get("answer", "")).strip():
            return idx, entry
    return None


def normalize_label(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def object_matches_expected(detected, expected):
    detected_norm = normalize_label(detected)
    expected_norm = normalize_label(expected)
    if not detected_norm or detected_norm in {"unclear", "other"}:
        return False
    return detected_norm == expected_norm or detected_norm in expected_norm or expected_norm in detected_norm


def get_item_ai_policy(item_id):
    sid = str(item_id or "").strip()
    if sid in MANUAL_ONLY_ITEMS:
        return {
            "mode": "manual",
            "manual_review_required": True,
            "confidence_level": ITEM_CONFIDENCE_LEVEL.get(sid, "n/a"),
            "media_requirement": "manual_action",
            "reason": MANUAL_ONLY_REASON.get(sid, "Manual inspection required."),
        }
    if sid in PARTIAL_AI_ITEMS:
        return {
            "mode": "partial",
            "manual_review_required": True,
            "confidence_level": ITEM_CONFIDENCE_LEVEL.get(sid, "medium"),
            "media_requirement": ITEM_MEDIA_REQUIREMENT.get(sid, "photo"),
            "reason": MANUAL_REVIEW_REASON.get(sid, "AI assists but manual confirmation is required."),
        }
    return {
        "section": "Racking Inspection",
        "ai_feasible": "Yes",
        "confidence_level": ITEM_CONFIDENCE_LEVEL.get(sid, "high"),
        "media_requirement": "photo",
        "mode": "ai",
        "manual_review_required": False,
        "reason": "Full visual AI inspection is feasible with a clear image.",
    }


def get_item_visual_rule(item_id, item_description=""):
    sid = str(item_id or "").strip()
    rule = CHECKLIST_VISUAL_RULES.get(sid)
    if rule:
        return rule.strip()
    return "\n".join([
        f"ITEM: {item_description or 'Racking checklist item'}",
        "LOOK FOR: The visible rack component and any obvious damage, deformation, missing hardware, or obstruction.",
        "PASS if: The item is clearly visible and appears compliant.",
        "FAIL if: The item is missing, blocked, unclear, damaged, or cannot be verified from the image.",
        "NOTE: Inconclusive evidence is a fail.",
    ])


def get_ai_enablement_matrix(event):
    rows = []
    for category in RACKING_CHECKLIST.get("categories", []):
        for item in category.get("items", []):
            policy = get_item_ai_policy(item.get("id"))
            rows.append({
                "item_id": item.get("id"),
                "section": category.get("name", ""),
                "description": item.get("description", ""),
                "ai_feasible": policy.get("ai_feasible", "Yes"),
                "ai_mode": policy.get("mode", "ai"),
                "manual_review_required": policy.get("manual_review_required", False),
                "confidence_level": policy.get("confidence_level", "High"),
                "media_requirement": policy.get("media_requirement", "photo"),
                "reason": policy.get("reason", ""),
            })
    return build_response(200, {"ai_enablement_matrix": rows})


def build_item_prompt(checklist_item, rule, policy):
    item_id = checklist_item.get("id", "?")
    item_desc = checklist_item.get("description", "")
    policy = policy or get_item_ai_policy(str(item_id))

    parts = [
        f"You are evaluating checklist item #{item_id} in an OSHA racking inspection.",
        "",
        "CHECKLIST ITEM DETAILS",
        f"Item ID    : {item_id}",
        f"Description: {item_desc}",
        "",
        "STRICT VISUAL RULE FOR THIS ITEM",
        rule.strip(),
        "",
        "YOUR EVALUATION TASK",
        "STEP 1: Is the correct rack component CLEARLY VISIBLE as the main subject?",
        "        If not, set object_detected=other/unclear, pass=false, confidence < 0.4.",
        "STEP 2: Does the image CLEARLY SATISFY every requirement in the STRICT VISUAL RULE?",
        "        Apply HARD RULES from system instructions if relevant to this item.",
        "STEP 3: Score confidence (0.0-1.0) based on image clarity. If confidence < 0.5, pass must be false.",
        "",
        "REMINDERS: INCONCLUSIVE = FAIL. Do NOT assume what is behind an obstruction. Report ONLY what you directly observe. Return JSON only.",
    ]

    if policy.get("mode") == "partial":
        parts.extend([
            "",
            "PARTIAL AI MODE",
            "This item requires human confirmation even after AI analysis.",
            f"Required evidence type: {policy.get('media_requirement', 'photo')}",
            f"Manual checkpoint: {policy.get('reason', '')}",
            "Evaluate only what is visually verifiable in the media.",
        ])

    return "\n".join(parts)


def invoke_claude_json(system_prompt, prompt, image_bytes=None, media_type="image/jpeg", max_tokens=200):
    content = []
    # Image MUST come before text for Bedrock vision models
    if image_bytes is not None:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type or "image/jpeg",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
            },
        })
    content.append({"type": "text", "text": prompt})

    response = bedrock.invoke_model(
        modelId=SONNET_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": content}],
        }),
    )
    payload = json.loads(response["body"].read())
    text = ""
    for part in payload.get("content", []):
        if part.get("type") == "text":
            text += part.get("text", "")
    return safe_json_parse(text)


IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a CERTIFIED OSHA RACKING SAFETY INSPECTOR with field inspection experience.
Your job is to evaluate one or more photographs (frames from video or multiple images) against one specific racking checklist item.
Each image is analyzed independently; the system aggregates results using the best (highest confidence, most favorable) outcome.

PRIME DIRECTIVE — ZERO TOLERANCE FOR AMBIGUITY:
• INCONCLUSIVE = FAIL. Always.
• PARTIALLY VISIBLE = FAIL. Always.
• CANNOT READ LABEL/TAG/LOAD RATING = FAIL. Always.
• OBSTRUCTED VIEW = FAIL. Always.
• If you are forming the thought "it probably is fine" → that is a FAIL.
• pass=true requires CLEAR, UNAMBIGUOUS, DIRECT visual confirmation.

STEP 1 — SUBJECT PRESENCE CHECK:
Identify what rack component is present:
    • upright         → vertical rack column / upright post
    • brace           → diagonal or horizontal brace
    • beam            → horizontal cross beam / pallet beam
    • anchor          → foot plate, anchor bolt, nut, or concrete attachment point
    • connector       → back-to-back rack connector / coupling hardware
    • guard           → rack end guard / barrier / protector
    • load_label      → load capacity placard / rating label
    • pallet          → pallet or pallet load
    • other           → not a rack component relevant to the item
    • unclear         → cannot determine
If the item to evaluate is NOT clearly the primary subject → object_detected="other"/"unclear", pass=false, stop.

STEP 2 — ITEM-SPECIFIC CONDITION CHECK:
Only proceed if Step 1 confirmed the correct rack component.
Apply ONLY the strict_visual_rule provided. Do not evaluate outside its scope.

HARD RULES (override everything):
    R1. Bent, crushed, twisted, or obviously deflected upright/beam/brace → ALWAYS fail items 1, 2, 3, 4, 6, 7, 13, 14, 15
    R2. Missing retaining clip, locking pin, anchor bolt, nut, or connector hardware → ALWAYS fail items 5, 8, 9, 10, 11, 12
    R3. Visible guard missing or visibly damaged in a traffic area → ALWAYS fail items 16 and 17
    R4. Load label missing, blocked, unreadable, or low contrast → ALWAYS fail items 18 and 19
    R5. Broken boards, cracks, collapse, or compromised pallet structure → ALWAYS fail item 20
    R6. Missing or damaged pallet support mechanism → ALWAYS fail item 21
    R7. Unsecured elevated pallet / no visible wrap or banding → ALWAYS fail item 22

CONFIDENCE CALIBRATION:
    1.0 = Crystal clear image, condition unambiguously confirmed or denied
    0.8 = Clear image, minor uncertainty about one small detail
    0.6 = Adequate image, some details slightly unclear
    0.4 = Image blurry, partial, or condition only partially visible
    0.2 = Very poor image quality or subject barely visible
    If confidence < 0.5, you MUST set pass=false regardless of what you think you see.

Return JSON ONLY. No markdown. No extra text.
{
    "object_detected": "upright|brace|beam|anchor|connector|guard|load_label|pallet|other|unclear",
    "condition_checked": "<10-word max summary of what was evaluated>",
    "pass": true|false,
    "confidence": <float 0.0-1.0>,
    "reason": "<2-3 sentences: exactly what you see and the visual evidence for pass or fail>",
    "worker_message": "<actionable instruction under 12 words>",
    "suggested_action": "<specific corrective action, or null if passed>"
}
"""


def analyze_item_image(event):
    body = parse_body(event)
    inspection_id = str(
        body.get("inspection_id")
        or body.get("inspectionId")
        or body.get("session_id")
        or body.get("sessionId")
        or ""
    ).strip()
    item_id = str(body.get("item_id") or body.get("itemId") or "").strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id or session_id is required"})
    if not item_id:
        return build_response(400, {"error": "item_id is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": "Checklist item not found"})

    policy = get_item_ai_policy(item_id)
    if policy.get("mode") == "manual":
        checklist_item.setdefault("evidence", []).append({
            "analyzed_at": now_iso(),
            "ai_mode": "manual",
            "manual_review_required": True,
            "reason": policy.get("notes", "Manual inspection required."),
        })
        checklist_item["answer"] = ""
        checklist_item["finding"] = policy.get("notes", "Manual inspection required.")
        checklist_item["action_item"] = "Complete manual verification and submit the final answer."
        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["status"] = "in_progress"
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)
        return build_response(200, {
            "inspection_id": inspection.get("inspection_id", inspection_id),
            "item_id": item_id,
            "blocked": False,
            "move_next": False,
            "pass": False,
            "ai_mode": "manual",
            "manual_review_required": True,
            "confidence_level": policy.get("confidence_level", "Low"),
            "media_requirement": policy.get("media_requirement", "photo"),
            "message": "Manual inspection required for this checklist item.",
            "reason": policy.get("notes", "Manual inspection required."),
            "updated_item": checklist_item,
            "inspection_status": inspection.get("status", "in_progress"),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        })

    image_sources = []
    if isinstance(body.get("file_keys"), list) and body.get("file_keys"):
        for fk in body.get("file_keys"):
            if fk:
                image_sources.append(("file_key", str(fk).strip()))
    elif isinstance(body.get("image_base64s"), list) and body.get("image_base64s"):
        for b64 in body.get("image_base64s"):
            if b64:
                image_sources.append(("image_base64", str(b64).strip()))
    else:
        image_sources.append(("single_body", body))

    if not image_sources:
        return build_response(400, {"error": "Provide file_key(s) or image_base64(s)"})

    per_image_results = []
    rule = get_item_visual_rule(item_id, checklist_item.get("description", ""))
    prompt = build_item_prompt(checklist_item, rule, policy)

    for index, source in enumerate(image_sources):
        try:
            if source[0] == "file_key":
                obj = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=source[1])
                image_bytes = obj["Body"].read()
                content_type = obj.get("ContentType", "") or ""
                if not content_type or content_type == "application/octet-stream":
                    guessed, _ = mimetypes.guess_type(source[1])
                    content_type = guessed or "image/jpeg"
                file_key_for_record = source[1]
            elif source[0] == "image_base64":
                payload = source[1]
                if "," in payload and payload.startswith("data:"):
                    header = payload.split(",", 1)[0]
                    if ";" in header:
                        content_type = header.split(";", 1)[0].replace("data:", "") or "image/jpeg"
                    else:
                        content_type = "image/jpeg"
                    payload = payload.split(",", 1)[1]
                else:
                    content_type = "image/jpeg"
                if str(payload).startswith(("http://", "https://", "s3://")):
                    return build_response(400, {"error": "image_base64 must contain image bytes or a data URL, not a URI"})
                image_bytes = base64.b64decode(payload, validate=True)
                file_key_for_record = f"inline_{index}"
            else:
                image_bytes, content_type = _extract_image(source[1])
                file_key_for_record = str(source[1].get("file_key") or source[1].get("fileKey") or f"inline_{index}")
        except Exception as exc:
            return build_response(400, {"error": f"Invalid image input: {str(exc)}"})

        if not image_bytes:
            continue

        image_bytes, content_type = prepare_image_bytes(image_bytes, content_type)
        if not image_bytes:
            return build_response(400, {"error": "Could not process image bytes"})
        if content_type not in SUPPORTED_BEDROCK_IMAGE_TYPES:
            if content_type in {"image/heic", "image/heif"} or _looks_like_heic(image_bytes):
                _log_racking_fail_diag(item_id, source[1], content_type, "unsupported_heic_for_bedrock", image_bytes)
                return build_response(422, {
                    "error": "Unsupported image format for AI analysis",
                    "message": "Convert HEIC/HEIF images to JPEG or PNG before uploading for /racking/analyze.",
                    "content_type": content_type,
                })
            _log_racking_fail_diag(item_id, source[1], content_type, "unsupported_image_type_for_bedrock", image_bytes)
            return build_response(422, {
                "error": "Unsupported image format for AI analysis",
                "message": "Bedrock vision accepts JPEG, PNG, WebP, or GIF. Please re-upload the image in a supported format.",
                "content_type": content_type,
            })

        try:
            logger.info("[RACKING_ANALYZE] item=%s file_key=%s", item_id, file_key_for_record)
            analysis = invoke_claude_json(IMAGE_ANALYSIS_SYSTEM_PROMPT, prompt, image_bytes=image_bytes, media_type=content_type, max_tokens=200)
        except Exception as exc:
            error_text = str(exc)
            logger.exception("[BEDROCK_EXCEPTION] item=%s error_text=%s", item_id, error_text[:500])
            
            if "ValidationException" in error_text and "Could not process image" in error_text:
                _log_racking_fail_diag(item_id, source[1], content_type, error_text, image_bytes)
                return build_response(422, {
                    "error": "Bedrock could not process the image",
                    "message": "The image bytes reached Bedrock, but the model rejected them. Re-upload as a standard JPEG/PNG/WebP image.",
                    "content_type": content_type,
                })
            
            if "ThrottlingException" in error_text:
                logger.warning("[BEDROCK_THROTTLED] item=%s", item_id)
                return build_response(502, {
                    "error": "Bedrock service throttled",
                    "item_id": item_id,
                    "retry_after": 30,
                })
            
            if "InputLengthException" in error_text or "payload" in error_text.lower():
                logger.warning("[BEDROCK_PAYLOAD_TOO_LARGE] item=%s prompt_len=%s image_size=%s", item_id, len(prompt), len(image_bytes))
                return build_response(413, {
                    "error": "Request payload too large for Bedrock",
                    "item_id": item_id,
                    "prompt_len": len(prompt),
                    "image_size": len(image_bytes),
                })
            
            return build_response(502, {"error": f"Bedrock failed: {error_text}"})

        object_detected = str(analysis.get("object_detected", "unclear")).lower().strip()
        confidence = float(analysis.get("confidence", 0.0) or 0.0)
        passed = bool(analysis.get("pass", False))
        wrong_image = object_detected not in VALID_OBJECTS
        low_confidence = confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD
        blocked = wrong_image or low_confidence

        per_image_results.append({
            "file_key": file_key_for_record,
            "analyzed_at": now_iso(),
            "ai_mode": policy.get("mode", "ai"),
            "manual_review_required": bool(policy.get("manual_review_required", False)),
            "confidence_level": policy.get("confidence_level", "High"),
            "media_requirement": policy.get("media_requirement", "photo"),
            "object_detected": object_detected,
            "condition_checked": str(analysis.get("condition_checked", checklist_item.get("description", ""))).strip(),
            "pass": passed,
            "is_compliant": passed and not blocked,
            "confidence": confidence,
            "reason": str(analysis.get("reason", "")).strip(),
            "worker_message": str(analysis.get("worker_message", "")).strip(),
            "suggested_action": str(analysis.get("suggested_action", "")).strip(),
            "blocked": blocked,
        })

    if not per_image_results:
        return build_response(400, {"error": "No valid images were provided or extracted."})

    non_block_pass = [result for result in per_image_results if result.get("pass") and not result.get("blocked")]
    best = max(non_block_pass, key=lambda item: item.get("confidence", 0.0)) if non_block_pass else max(per_image_results, key=lambda item: item.get("confidence", 0.0))

    checklist_item.setdefault("evidence", [])
    checklist_item["evidence"].extend(per_image_results)

    blocked_overall = all(result.get("blocked", False) for result in per_image_results)
    if blocked_overall:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"] = ""
        checklist_item["finding"] = best.get("reason") or "Image(s) not sufficient."
        checklist_item["action_item"] = best.get("suggested_action") or "Retake clear images and try again."
        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)
        return build_response(200, {
            "inspection_id": inspection.get("inspection_id", inspection_id),
            "item_id": item_id,
            "blocked": True,
            "move_next": False,
            "pass": False,
            "object_detected": best.get("object_detected"),
            "condition_checked": best.get("condition_checked"),
            "confidence": best.get("confidence"),
            "message": best.get("worker_message") or "Checklist item is not clearly visible.",
            "reason": best.get("reason"),
            "suggested_action": best.get("suggested_action"),
            "updated_item": checklist_item,
            "inspection_status": inspection.get("status", "in_progress"),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        })

    passed_overall = bool(best.get("pass", False)) and not best.get("blocked", False)
    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"] = best.get("reason") or ""
    checklist_item["action_item"] = best.get("suggested_action") or ("" if passed_overall else "Correct the issue and retake.")

    if policy.get("manual_review_required"):
        checklist_item["ai_suggested_answer"] = "In compliance" if best.get("pass") else "Needs maintenance"
        checklist_item["answer"] = ""
        checklist_item["finding"] = f"AI suggestion: {'Pass' if best.get('pass') else 'Fail'}. {best.get('reason') or ''}".strip()
        checklist_item["action_item"] = best.get("suggested_action") or policy.get("notes", "Human confirmation required.")
    else:
        checklist_item["answer"] = "In compliance" if passed_overall else "Needs maintenance"

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    next_pos = next_unanswered_index(inspection)
    inspection["current_item_index"] = next_pos[0] if next_pos else 0
    inspection["status"] = "completed" if not next_pos else "in_progress"
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "inspection_id": inspection.get("inspection_id", inspection_id),
        "item_id": item_id,
        "blocked": False,
        "move_next": not bool(policy.get("manual_review_required")),
        "pass": passed_overall,
        "ai_mode": policy.get("mode", "ai"),
        "manual_review_required": bool(policy.get("manual_review_required", False)),
        "confidence_level": policy.get("confidence_level", "High"),
        "media_requirement": policy.get("media_requirement", "photo"),
        "object_detected": best.get("object_detected"),
        "condition_checked": best.get("condition_checked"),
        "confidence": best.get("confidence"),
        "message": "AI suggestion ready. Manual confirmation required before finalizing." if policy.get("manual_review_required") else (best.get("worker_message") or "Item analyzed."),
        "reason": best.get("reason"),
        "suggested_action": best.get("suggested_action"),
        "updated_item": checklist_item,
        "inspection_status": inspection["status"],
        "current_item_index": inspection.get("current_item_index", 0),
        "inspection": inspection,
        "categories": inspection.get("categories", []),
    })


def batch_analyze_items(event):
    """Batch analyze multiple racking item images in parallel.

    POST /racking/analyze-batch
    Expects JSON body:
    {
        "inspection_id": "...",
        "images": [
            { "item_id": "1", "file_key": "uploads/session-xyz/item-1.jpg" },
            { "item_id": "2", "file_keys": ["img-a.jpg", "img-b.jpg"] },
            { "item_id": "3", "image_base64": "<base64>" }
        ]
    }
    Returns: { "inspection_id": "...", "results": { "1": {...}, "2": {...} } }
    """
    body = parse_body(event)
    inspection_id = str(
        body.get("inspection_id")
        or body.get("inspectionId")
        or body.get("session_id")
        or body.get("sessionId")
        or ""
    ).strip()
    images = body.get("images", [])

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    if not isinstance(images, list) or not images:
        return build_response(400, {"error": "images must be a non-empty list of {item_id, file_key|image_base64}"})

    # Build one synthetic event per item (reuses the single-item analyze path)
    child_events = []
    for img in images:
        iid = str(img.get("item_id", "") or img.get("itemId", "")).strip()
        if not iid:
            continue
        child_body = {"inspection_id": inspection_id, "item_id": iid}
        # Support multiple images per item (file_keys list or single file_key)
        if "file_keys" in img and isinstance(img.get("file_keys"), list):
            child_body["file_keys"] = img["file_keys"]
        elif "file_key" in img:
            child_body["file_key"] = img["file_key"]
        # Support multiple base64 images per item
        if "image_base64s" in img and isinstance(img.get("image_base64s"), list):
            child_body["image_base64s"] = img["image_base64s"]
        elif "image_base64" in img:
            child_body["image_base64"] = img["image_base64"]
        child_events.append({"body": child_body})

    if not child_events:
        return build_response(400, {"error": "No valid items found in images list (each entry needs item_id)"})

    logger.info("[RACKING_BATCH] inspection_id=%s items=%d workers=%d", inspection_id, len(child_events), BATCH_WORKER_COUNT)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_WORKER_COUNT) as ex:
        futures = {ex.submit(analyze_item_image, ev): ev for ev in child_events}
        for fut in concurrent.futures.as_completed(futures):
            ev = futures[fut]
            iid = ev.get("body", {}).get("item_id", "unknown")
            try:
                res = fut.result()
            except Exception as exc:
                logger.exception("[RACKING_BATCH_ERROR] item=%s error=%s", iid, str(exc))
                results[iid] = {"error": str(exc)}
                continue
            # analyze_item_image returns a build_response dict; unpack the body
            body_raw = res.get("body") if isinstance(res, dict) else None
            try:
                parsed = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})
            except Exception:
                parsed = body_raw
            results[iid] = parsed

    return build_response(200, {
        "inspection_id": inspection_id,
        "total": len(child_events),
        "results": results,
    })


def update_checklist_item(event):
    path_params = event.get("pathParameters") or {}
    inspection_id = str(path_params.get("inspection_id") or path_params.get("session_id") or path_params.get("id") or "").strip()
    item_id = str(path_params.get("item_id") or "").strip()

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})

    body = parse_body(event)
    answer = str(body.get("answer", "")).strip()
    finding = str(body.get("finding", "")).strip()
    action_item = str(body.get("action_item", "")).strip()
    responsible = str(body.get("responsible", "")).strip()
    due_date = str(body.get("due_date", "")).strip()
    evidence = body.get("evidence", [])
    clear_block = bool(body.get("clear_block", False))

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": "Checklist item not found"})

    if answer and answer not in RACKING_CHECKLIST.get("available_answers", []):
        return build_response(400, {"error": f"answer must be one of {RACKING_CHECKLIST.get('available_answers', [])}"})

    if answer:
        checklist_item["answer"] = answer
    if finding:
        checklist_item["finding"] = finding
    if action_item:
        checklist_item["action_item"] = action_item
    if responsible:
        checklist_item["responsible"] = responsible
    if due_date:
        checklist_item["due_date"] = due_date
    if isinstance(evidence, list) and evidence:
        checklist_item.setdefault("evidence", [])
        checklist_item["evidence"].extend(evidence)
    if clear_block:
        checklist_item["blocked_by_wrong_image"] = False

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    next_pos = next_unanswered_index(inspection)
    inspection["current_item_index"] = next_pos[0] if next_pos else 0
    inspection["status"] = "completed" if not next_pos else "in_progress"
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "inspection_id": inspection_id,
        "item_id": item_id,
        "updated_item": checklist_item,
        "inspection": inspection,
        "categories": inspection.get("categories", []),
        "status": inspection.get("status", "in_progress"),
        "updated_at": inspection.get("updated_at", ""),
    })


def add_note_to_item(event):
    path_params = event.get("pathParameters") or {}
    inspection_id = str(path_params.get("inspection_id") or path_params.get("session_id") or path_params.get("id") or "").strip()
    item_id = str(path_params.get("item_id") or "").strip()

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})

    body = parse_body(event)
    note = str(body.get("note", "")).strip()
    if not note:
        return build_response(400, {"error": "note is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": "Checklist item not found"})

    existing = checklist_item.get("finding", "")
    checklist_item["finding"] = (existing + " | " if existing else "") + f"Worker note: {note}"
    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "inspection_id": inspection_id,
        "item_id": item_id,
        "finding": checklist_item["finding"],
        "message": "Note saved successfully.",
    })


def delete_inspection(event):
    path_params = event.get("pathParameters") or {}
    inspection_id = str(path_params.get("inspection_id") or path_params.get("id") or "").strip()
    session_id = str(path_params.get("session_id") or "").strip()

    if not inspection_id and not session_id:
        return build_response(400, {"error": "inspection_id or session_id is required"})

    deleted_ids = []
    if inspection_id:
        inspection = load_inspection(inspection_id)
        if not inspection:
            return build_response(404, {"error": "Inspection not found"})
        table.delete_item(Key={"inspection_id": inspection.get("inspection_id", inspection_id)})
        deleted_ids.append(inspection.get("inspection_id", inspection_id))
        return build_response(200, {"message": "Inspection deleted successfully", "inspection_id": deleted_ids[0]})

    result = table.scan()
    items = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    for item in items:
        if str(item.get("session_id", "")).strip() == session_id:
            iid = str(item.get("inspection_id", "")).strip()
            if iid:
                table.delete_item(Key={"inspection_id": iid})
                deleted_ids.append(iid)

    if not deleted_ids:
        return build_response(404, {"error": "No inspections found for the session"})

    try:
        sessions_table.delete_item(Key={"session_id": session_id})
    except Exception:
        pass

    return build_response(200, {
        "message": "Session deleted successfully",
        "session_id": session_id,
        "deleted_inspection_count": len(deleted_ids),
        "deleted_inspection_ids": deleted_ids,
    })


def parse_voice_intent_locally(text):
    t = (text or "").strip().lower()
    if not t:
        return None
    if any(p in t for p in ["repeat", "say again", "read again", "read item"]):
        return {"intent": "repeat_item", "confidence": 0.95, "message": "Repeating current item.", "move_to_item": False, "note_text": None}
    if any(p in t for p in ["help", "what can i say", "show commands", "list commands"]):
        return {"intent": "help", "confidence": 0.95, "message": "Say next, back, capture, retake, add note, or repeat.", "move_to_item": False, "note_text": None}
    if any(p in t for p in ["next", "skip", "continue"]):
        return {"intent": "next_item", "confidence": 0.95, "message": "Moving to the next item.", "move_to_item": True, "note_text": None}
    if any(p in t for p in ["back", "previous", "go back"]):
        return {"intent": "previous_item", "confidence": 0.95, "message": "Moving to the previous item.", "move_to_item": True, "note_text": None}
    if any(p in t for p in ["retake", "retry", "reshoot", "again"]):
        return {"intent": "retake_photo", "confidence": 0.95, "message": "Retake the photo for this item.", "move_to_item": False, "note_text": None}
    if any(p in t for p in ["capture", "take photo", "photo", "scan"]):
        return {"intent": "capture_photo", "confidence": 0.95, "message": "Capture the image for this item.", "move_to_item": False, "note_text": None}
    if t.startswith("note ") or t.startswith("add note"):
        note_text = re.sub(r"^(add note|note)\s+", "", t, count=1).strip()
        return {"intent": "add_note", "confidence": 0.95, "message": "Adding your note.", "move_to_item": False, "note_text": note_text or None}
    return {"intent": "unknown", "confidence": 0.0, "message": "I did not understand that command.", "move_to_item": False, "note_text": None}


def racking_voice_hint(item_id, item=None):
    hints = {
        "1": "Capture a wide shot of the upright and surrounding area.",
        "2": "Capture a close-up of the braces and connection points.",
        "3": "Show the upright surface for rust, corrosion, or twisting.",
        "4": "Capture the beam span with a scale reference.",
        "5": "Zoom in on the locking pin or retaining clip.",
        "6": "Show the beams and clips for rust or twisting.",
        "7": "Show the beam faces where pinching or crushing would appear.",
        "8": "Show both the front and back foot plates with anchor bolts.",
        "9": "Show the anchor nuts and any looseness.",
        "10": "Zoom in on the anchor bolt condition.",
        "11": "Show the back to back rack connector area.",
        "12": "Show the wall anchor only if one is present.",
        "13": "Show the footplate welds and any corrosion.",
        "14": "Capture the rack row from the aisle for alignment.",
        "15": "Capture the full height of the upright for plumb check.",
        "16": "Show the end rack guard in the traffic area.",
        "17": "Show the guard for bends, missing sections, or damage.",
        "18": "Capture the load label from forklift operator eye level.",
        "19": "Show the load label contrast against the background.",
        "20": "Show the pallet face and board condition.",
        "21": "Show the pallet support mechanism under the pallet.",
        "22": "Show elevated pallets for wrapping or banding.",
    }
    if str(item_id) in hints:
        return hints[str(item_id)]
    if item and item.get("description"):
        return item["description"]
    return "Capture a clear image of the current checklist item."


def handle_voice_command(event):
    body = parse_body(event)
    transcript = str(body.get("voice_text", "") or body.get("text", "") or body.get("transcript", "") or body.get("speech_text", "")).strip()
    inspection_id = str(body.get("inspection_id", "") or body.get("session_id", "") or body.get("inspectionId", "") or body.get("sessionId", "") or "").strip()
    item_id = str(body.get("item_id", "") or body.get("itemId", "") or "").strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id or session_id is required"})
    if not transcript:
        return build_response(400, {"error": "voice_text, text, or transcript is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    items = get_linear_checklist_items(inspection)
    if not items:
        return build_response(400, {"error": "Checklist items are not available"})

    parsed = parse_voice_intent_locally(transcript)
    current_ctx, current_index = get_current_voice_item(inspection)
    if current_index is None:
        current_index = 0

    target_index = current_index
    target_ctx = current_ctx
    if item_id:
        for index, entry in enumerate(items):
            if str(entry["item"].get("id")) == item_id:
                target_index = index
                target_ctx = entry
                break

    intent = parsed.get("intent", "unknown")
    move_to_item = bool(parsed.get("move_to_item", False))
    note_text = parsed.get("note_text")

    if intent == "next_item":
        target_index = clamp_index(target_index + 1, len(items))
        move_to_item = True
    elif intent == "previous_item":
        target_index = clamp_index(target_index - 1, len(items))
        move_to_item = True

    if move_to_item:
        target_ctx = items[target_index]
        inspection["current_item_index"] = target_index
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

    if target_ctx is None:
        target_ctx = items[target_index]

    item = target_ctx["item"]
    item_id_resolved = str(item.get("id", ""))
    hint = racking_voice_hint(item_id_resolved, item)

    return build_response(200, {
        "inspection_id": inspection.get("inspection_id", inspection_id),
        "session_id": inspection.get("session_id", ""),
        "intent": intent,
        "confidence": parsed.get("confidence", 0.0),
        "message": parsed.get("message", ""),
        "move_to_item": move_to_item,
        "voice_text": transcript,
        "note_text": note_text,
        "current_item_index": inspection.get("current_item_index", current_index),
        "item_id": item_id_resolved,
        "category_index": target_ctx["category_index"],
        "item": item,
        "capture_hint": hint,
        "inspection": inspection,
    })


# ─────────────────────────────────────────────
# API 1: GET /racking-inspection/checklist — Get Checklist Template
# ─────────────────────────────────────────────
def get_checklist(event):
    """
    Returns the full racking checklist template.
    Frontend uses this to render the inspection form dynamically.
    """
    return build_response(200, RACKING_CHECKLIST)


# ─────────────────────────────────────────────
# API 2: POST /racking-inspection — Create New Inspection
# ─────────────────────────────────────────────
def create_inspection(event):
    """
    Creates a new racking inspection record linked to an existing session.
    Session is created via POST /inspection-session (in osha-checklist Lambda).

    Expects JSON body:
    {
        "session_id": "string",
        "team": ["string"],                            // optional
        "categories": [
            {
                "id": 1,
                "name": "Rack Frame Uprights",
                "items": [
                    { "id": 1, "answer": "In compliance", "finding": "", "action_item": "", "responsible": "", "due_date": "" }
                ]
            }
        ],
        "general_results": [
            { "finding": "", "action_item": "", "responsible": "", "due_date": "" }
        ],
        "notes": "string"                              // optional (max 5000 chars)
    }
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    # Validate required fields
    session_id = body.get("session_id", "").strip()
    team = body.get("team", [])
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
        "team": team if team else [],
        "categories": categories,
        "general_results": general_results if general_results else [],
        "notes": notes,
        "created_at": created_at,
    }

    item["status"] = "in_progress"
    item["current_item_index"] = 0
    item["updated_at"] = created_at

    # Save to DynamoDB
    save_inspection(item)

    # Return the generated ID
    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "created_at": created_at,
        "status": "in_progress",
    })


# ─────────────────────────────────────────────
# API 3: GET /racking-inspections — List All Inspections
# ─────────────────────────────────────────────
def list_inspections(event):
    """
    Returns a summary list of all racking inspections.
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
            "team": item.get("team", []),
            "created_at": item.get("created_at"),
            "status": item.get("status", "in_progress"),
            "updated_at": item.get("updated_at", item.get("created_at")),
        })

    # Sort by created_at (newest first)
    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return build_response(200, summary_list)


# ─────────────────────────────────────────────
# API 4: GET /racking-inspection/{id} — Get Full Inspection
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
                checklist_item["description"] = description_lookup[item_id]

    # Build ordered response so JSON keys are in a logical order
    ordered_item = {
        "inspection_id": item.get("inspection_id"),
        "session_id": item.get("session_id"),
        "auditor_name": item.get("auditor_name"),
        "facility_area": item.get("facility_area"),
        "date_of_audit": item.get("date_of_audit"),
        "location": item.get("location"),
        "station": item.get("station"),
        "team": item.get("team", []),
        "categories": categories,
        "general_results": item.get("general_results", []),
        "notes": item.get("notes", ""),
        "created_at": item.get("created_at"),
        "status": item.get("status", "in_progress"),
        "current_item_index": item.get("current_item_index", 0),
        "updated_at": item.get("updated_at", item.get("created_at")),
    }

    return build_response(200, ordered_item)


# ─────────────────────────────────────────────
# Pause / Resume helpers
# ─────────────────────────────────────────────
def merge_categories(existing_cats, incoming_cats):
    """Merge incoming category answers into existing categories, preserving previous data."""
    if not existing_cats:
        return incoming_cats
    if not incoming_cats:
        return copy.deepcopy(existing_cats)

    merged = copy.deepcopy(existing_cats)
    # Build lookup of incoming items by id
    incoming_lookup = {}
    for cat in incoming_cats:
        for item in cat.get("items", []):
            iid = item.get("id")
            if iid is not None:
                incoming_lookup[iid] = item

    # Merge into existing
    for cat in merged:
        for i, item in enumerate(cat.get("items", [])):
            iid = item.get("id")
            if iid in incoming_lookup:
                inc = incoming_lookup[iid]
                # Only overwrite if incoming has non-empty values
                for key in ["answer", "finding", "action_item", "responsible", "due_date"]:
                    val = str(inc.get(key, "")).strip()
                    if val:
                        item[key] = val
                # Merge evidence lists
                if isinstance(inc.get("evidence"), list) and inc["evidence"]:
                    existing_evidence = item.get("evidence", [])
                    existing_keys = {e.get("file_key") for e in existing_evidence if isinstance(e, dict) and e.get("file_key")}
                    for ev in inc["evidence"]:
                        if isinstance(ev, dict) and ev.get("file_key") not in existing_keys:
                            existing_evidence.append(ev)
                    item["evidence"] = existing_evidence
    return merged


def compute_status_from_categories(inspection):
    """Calculate inspection status: in_progress, passed, or failed."""
    items = get_linear_checklist_items(inspection)
    all_items = [entry["item"] for entry in items]
    if any(item.get("answer", "") == "" for item in all_items):
        return "in_progress"
    if any(item.get("answer") == "Needs maintenance" for item in all_items):
        return "failed"
    return "passed"


def compute_progress(inspection):
    """Compute progress stats: total, answered, and percentage."""
    items = get_linear_checklist_items(inspection)
    total = len(items)
    answered = sum(1 for entry in items if str(entry["item"].get("answer", "")).strip())
    percentage = round((answered / total * 100)) if total > 0 else 0
    return {"total": total, "answered": answered, "percentage": percentage}


def find_next_unanswered(inspection):
    """Find the next unanswered checklist item ID."""
    items = get_linear_checklist_items(inspection)
    for entry in items:
        if not str(entry["item"].get("answer", "")).strip():
            return entry["item"].get("id")
    return None


def pause_session(event):
    """POST handler — pause an in-progress inspection session."""
    path_params = event.get("pathParameters") or {}
    session_id = path_params.get("session_id", "").strip()
    if not session_id:
        return build_response(400, {"error": "session_id is required"})

    inspection = load_inspection(session_id)
    if not inspection:
        return build_response(404, {"error": f"Inspection not found for session {session_id}"})

    # Merge incoming data if provided
    try:
        body = parse_body(event)
    except Exception:
        body = {}

    if body.get("categories"):
        inspection["categories"] = merge_categories(
            inspection.get("categories", []),
            body["categories"],
        )

    if body.get("general_results"):
        inspection["general_results"] = body["general_results"]

    if "notes" in body:
        inspection["notes"] = body["notes"]

    progress = compute_progress(inspection)

    inspection["status"] = "paused"
    inspection["last_paused_at"] = now_iso()
    inspection["updated_at"] = now_iso()

    save_inspection(inspection)

    # Update session table
    try:
        sessions_table.update_item(
            Key={"session_id": inspection.get("session_id", session_id)},
            UpdateExpression="SET #status = :status, progress = :progress, inspection_type = :itype, updated_at = :updated_at, last_paused_at = :paused_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "paused",
                ":progress": sanitize_for_dynamodb(progress),
                ":itype": "racking",
                ":updated_at": now_iso(),
                ":paused_at": now_iso(),
            },
        )
    except Exception as e:
        print(f"Failed to update session table: {e}")

    return build_response(200, {
        "session_id": inspection.get("session_id", session_id),
        "inspection_id": inspection.get("inspection_id"),
        "status": "paused",
        "progress": progress,
        "last_paused_at": inspection["last_paused_at"],
    })


def resume_session(event):
    """GET handler — resume a paused inspection session."""
    path_params = event.get("pathParameters") or {}
    session_id = path_params.get("session_id", "").strip()
    if not session_id:
        return build_response(400, {"error": "session_id is required"})

    inspection = load_inspection(session_id)
    if not inspection:
        return build_response(404, {"error": f"Inspection not found for session {session_id}"})

    progress = compute_progress(inspection)
    next_item = find_next_unanswered(inspection)

    inspection["status"] = "in_progress"
    inspection["resumed_at"] = now_iso()
    inspection["updated_at"] = now_iso()

    save_inspection(inspection)

    # Update session table
    try:
        sessions_table.update_item(
            Key={"session_id": inspection.get("session_id", session_id)},
            UpdateExpression="SET #status = :status, progress = :progress, inspection_type = :itype, updated_at = :updated_at, resumed_at = :resumed_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "in_progress",
                ":progress": sanitize_for_dynamodb(progress),
                ":itype": "racking",
                ":updated_at": now_iso(),
                ":resumed_at": now_iso(),
            },
        )
    except Exception as e:
        print(f"Failed to update session table: {e}")

    inspection_data = convert_decimals(inspection)
    inspection_data["progress"] = progress
    inspection_data["next_unanswered_item_id"] = next_item

    return build_response(200, inspection_data)


# ─────────────────────────────────────────────
# Main Handler — Routes to correct function
# ─────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Main entry point. Routes the request based on HTTP method and path.

    Routes:
        GET  /racking-inspection/checklist          → get_checklist
        GET  /racking/ai-enablement                 → get_ai_enablement_matrix
        POST /racking-inspection                    → create_inspection
        GET  /racking-inspections                   → list_inspections
        GET  /racking-inspection/{inspection_id}    → get_inspection
        PATCH /racking/session/{id}/items/{item_id} → update_checklist_item
        PATCH /racking/session/{id}/items/{item_id}/note → add_note_to_item
        POST /racking/analyze                       → analyze_item_image
        POST /racking/voice                         → handle_voice_command
        DELETE /racking/session/{id}               → delete_inspection
        POST /racking/session/{id}/pause            → pause_session
        GET  /racking/session/{id}/resume           → resume_session
        OPTIONS (any)                               → CORS preflight
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
    if http_method == "GET" and resource == "/racking-inspection/checklist":
        return get_checklist(event)

    if http_method == "GET" and (resource == "/racking/ai-enablement" or re.search(r"/racking(?:-inspection)?/ai-enablement$", path)):
        return get_ai_enablement_matrix(event)

    elif http_method == "POST" and resource == "/racking-inspection":
        return create_inspection(event)

    elif http_method == "GET" and resource == "/racking-inspections":
        return list_inspections(event)

    elif http_method == "GET" and resource == "/racking-inspection/{inspection_id}":
        return get_inspection(event)

    # ── Delete inspection by ID (CRUD) ────────────────────────────────────
    elif http_method == "DELETE" and resource == "/racking-inspection/{inspection_id}":
        return delete_inspection(event)

    elif http_method == "PATCH" and (resource == "/racking/session/{id}/items/{item_id}" or re.search(r"/racking/session/[^/]+/items/[^/]+$", path)):
        parts = path.rstrip("/").split("/")
        if len(parts) >= 5:
            event["pathParameters"] = {"session_id": parts[-3], "inspection_id": parts[-3], "item_id": parts[-1]}
        return update_checklist_item(event)

    elif http_method == "PATCH" and (resource == "/racking/session/{id}/items/{item_id}/note" or re.search(r"/racking/session/[^/]+/items/[^/]+/note$", path)):
        parts = path.rstrip("/").split("/")
        if len(parts) >= 6:
            event["pathParameters"] = {"session_id": parts[-4], "inspection_id": parts[-4], "item_id": parts[-2]}
        return add_note_to_item(event)

    elif http_method == "POST" and (resource == "/racking/analyze-batch" or re.search(r"/racking/analyze-batch$", path)):
        return batch_analyze_items(event)

    elif http_method == "POST" and (resource == "/racking/analyze" or re.search(r"/racking/analyze$", path)):
        return analyze_item_image(event)

    elif http_method == "POST" and (resource == "/racking/voice" or re.search(r"/racking/voice$", path)):
        return handle_voice_command(event)

    elif http_method == "DELETE" and (resource == "/racking/session/{id}" or re.search(r"/racking/session/[^/]+$", path)):
        parts = path.rstrip("/").split("/")
        if len(parts) >= 4:
            event["pathParameters"] = {"session_id": parts[-1], "inspection_id": parts[-1], "id": parts[-1]}
        return delete_inspection(event)

    # ── Pause session ─────────────────────────────────────────────────────
    elif http_method == "POST" and re.search(r"/racking/session/[^/]+/pause$", path):
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            session_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = session_id
            return pause_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid pause path"})

    # ── Resume session ────────────────────────────────────────────────────
    elif http_method == "GET" and re.search(r"/racking/session/[^/]+/resume$", path):
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            session_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = session_id
            return resume_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid resume path"})

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})