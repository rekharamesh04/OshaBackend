"""
Fire Extinguisher Monthly Inspection - Lambda Handler (Phase 1)
Single Lambda function handling 12 API routes:

  --- Original CRUD (4) ---
  POST /fire-extinguisher-inspection                          → Create new inspection
  GET  /fire-extinguisher-inspections                         → List all inspections
  GET  /fire-extinguisher-inspection/{id}                     → Get full inspection by ID
  GET  /fire-extinguisher-inspection/checklist                → Get checklist template

  --- New Mobile + AI Endpoints (8) ---
  PATCH /fire-extinguisher/session/{id}/items/{item_id}       → Update single checklist item
  PATCH /fire-extinguisher/session/{id}/items/{item_id}/note  → Add note to a checklist item
  GET   /fire-extinguisher/evidence/upload-url                → S3 presigned PUT URL
  GET   /fire-extinguisher/evidence/download-url              → S3 presigned GET URL
  GET   /fire-extinguisher/session/{id}/report                → Slimmed inspection report
  DELETE /fire-extinguisher/session/{session_id}              → Delete session + inspection
  POST  /fire-extinguisher/voice                              → Voice command parser
  POST  /fire-extinguisher/analyze                            → AI image analysis (Claude 3.5 Sonnet)
"""

import base64
import concurrent.futures
import copy
import io
import json
import logging
import os
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
table = dynamodb.Table(os.getenv("INSPECTION_TABLE_NAME", "osha-fire-extinguisher-inspections"))
sessions_table = dynamodb.Table(os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions"))
s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
sagemaker_runtime = boto3.client("sagemaker-runtime", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# Module-level thread pool — reused across warm Lambda invocations.
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
SONNET_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "")
HAIKU_MODEL_ID = os.getenv("BEDROCK_VOICE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
EVIDENCE_BUCKET = os.getenv("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")
TRAINING_BUCKET = os.getenv("TRAINING_S3_BUCKET", "osha-training-data")
PRESIGNED_URL_EXPIRY = int(os.getenv("PRESIGNED_URL_EXPIRY", "3600"))
UPLOAD_URL_EXPIRY = int(os.getenv("UPLOAD_URL_EXPIRY", "900"))
DOWNLOAD_URL_EXPIRY = int(os.getenv("DOWNLOAD_URL_EXPIRY", "3600"))
YOLO_ENDPOINT_NAME = os.getenv("YOLO_ENDPOINT_NAME", "").strip()
YOLO_DETECTION_CONFIDENCE_THRESHOLD = float(os.getenv("YOLO_DETECTION_CONFIDENCE_THRESHOLD", "0.10"))
MAX_IMAGE_WIDTH = 800
MAX_IMAGE_QUALITY = 85
YOLO_IMAGE_SIZE = 800
IMAGE_CONFIDENCE_BLOCK_THRESHOLD = 0.35
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}


# ─────────────────────────────────────────────
# Checklist Definition — Single Source of Truth (12 items)
# ─────────────────────────────────────────────
FIRE_EXTINGUISHER_CHECKLIST = {
    "inspection_type": "Fire Extinguisher Monthly Inspection",
    "general_information": {
        "location": "", "start_date": "",
        "checklist": "Fire Extinguisher Monthly Inspection",
        "leader": "", "team": []
    },
    "available_answers": ["Yes", "No", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Fire Extinguisher Inspection",
            "items": [
                {"id": 1, "description": "Extinguishers are at minimum within 50 feet of areas of risk.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 2, "description": "Extinguishers are mounted in height that is accessible from a seated position.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 3, "description": "Extinguishers are not obstructed and are easily accessible.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 4, "description": "Extinguishers are marked with proper signage (above the unit and viewable from 180 degrees).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 5, "description": "Extinguishers' pins and seals are in place.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 6, "description": "Extinguishers are in good, clean condition. No visible damage to units. Units are wiped down & clean.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 7, "description": "Extinguishers' nozzles are free of blockage.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 8, "description": "Extinguishers are fully charged. Pressure gauges show adequate pressure (within green zone) and the gauge glass is intact, clean, and readable.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 9, "description": "Extinguishers' instructions face outward for visibility and are clean, readable, and not blurry, dusty, folded, peeled, or damaged.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 10, "description": "Extinguisher tags are initialed and dated certifying monthly visual inspection took place. Tags must be attached, legible, clean, and not torn, dusty, dirty, blurry, or missing date/initials. Any extinguisher(s) that did not pass, need to be noted in this inspection and brought to compliance through corrective actions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 11, "description": "Number of extinguishers inspected:", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 12, "description": "Number of extinguishers compliant:", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
            ]
        }
    ],
    "general_results": [
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
    ],
    "notes": ""
}

# ─────────────────────────────────────────────
# Explicit Visual Rules per Checklist Item (AI)
# ─────────────────────────────────────────────
CHECKLIST_VISUAL_RULES = {
    "1": (
        "RULE — PRESENCE WITHIN 50 FEET OF RISK AREA:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A physical fire extinguisher is clearly and unambiguously visible.\n"
        "  2. The extinguisher is mounted on a wall, bracket, or stand — NOT on the floor unsecured.\n"
        "  3. No obvious risk area is visible at a distance that looks clearly beyond one normal room length.\n"
        "FAIL if:\n"
        "  - The bracket or mount is empty.\n"
        "  - No extinguisher is visible at all.\n"
        "  - The extinguisher is lying on the floor with no bracket.\n"
        "NOTE: You cannot measure 50 feet from a photo. If presence is confirmed, mark PASS "
        "and note in reason: 'Distance to risk area could not be verified from image — field validation required.'"
    ),
    "2": (
        "RULE — MOUNTED HEIGHT ACCESSIBLE FROM SEATED POSITION:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. The extinguisher body or its handle is at roughly waist-to-shoulder height.\n"
        "  3. There is no evidence the extinguisher is mounted so high that a seated person could not reach it.\n"
        "FAIL if:\n"
        "  - The extinguisher handle appears higher than roughly 5 feet from the floor.\n"
        "  - The extinguisher appears mounted near the ceiling.\n"
        "If height cannot be judged, set pass=false, condition_checked='height_not_verifiable'."
    ),
    "3": (
        "RULE — NOT OBSTRUCTED, EASILY ACCESSIBLE:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. No boxes, pallets, equipment, or objects are blocking access.\n"
        "  3. The path to the extinguisher appears clear.\n"
        "FAIL if:\n"
        "  - Any object is blocking direct access.\n"
        "  - Only the top or small part is visible because items block it.\n"
        "If obstruction status is unclear, set pass=false and request a retake."
    ),
    "4": (
        "RULE — PROPER SIGNAGE VISIBLE FROM 180 DEGREES:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. A sign or marker is visibly mounted ABOVE or near the extinguisher.\n"
        "  3. The sign appears oriented to be readable from multiple angles.\n"
        "FAIL if:\n"
        "  - No signage is visible above or near the extinguisher.\n"
        "  - The sign is present but facing away, or is too small/faded to read."
    ),
    "5": (
        "RULE — SAFETY PIN AND TAMPER SEAL IN PLACE:\n\n"
        "IMPORTANT — THIS IS A WIDE/FAR SHOT:\n"
        "  You are looking for the PRESENCE or ABSENCE of the pin and seal as visible shapes.\n\n"
        "WHAT TO LOOK FOR:\n"
        "  THE PIN: A small metal ring, loop, or straight pin through the trigger handle.\n"
        "  THE TAMPER SEAL: A plastic tag, colored string, zip-tie near the handle.\n\n"
        "DISTANCE-AWARE EVALUATION:\n"
        "  WIDE/FAR SHOT: PASS if ring/loop shape OR colored tag visible near handle.\n"
        "    INCONCLUSIVE → PASS and note close-up verification recommended.\n"
        "  CLOSE SHOT: PASS if pin ring visible AND tamper seal present.\n\n"
        "FAIL CONDITIONS (ALL must be true):\n"
        "  1. Handle/trigger area is CLEARLY visible, AND\n"
        "  2. The handle hole appears COMPLETELY EMPTY, AND\n"
        "  3. No colored tag/string/plastic visible near handle.\n\n"
        "CRITICAL: From wide shot — when in doubt, PASS. Only fail if handle is clearly bare."
    ),
    "6": (
        "RULE — GOOD CLEAN CONDITION, NO VISIBLE DAMAGE:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. No visible dents, major scratches, rust, corrosion, or deformation.\n"
        "  3. The extinguisher appears clean — no heavy dust, grease, or grime.\n"
        "  4. The hose (if present) appears intact.\n"
        "FAIL if:\n"
        "  - Visible rust, corrosion, or pitting.\n"
        "  - Dents or physical deformation.\n"
        "  - Heavy dust, grime, or grease coating.\n"
        "  - Hose visibly cracked, detached, or missing."
    ),
    "7": (
        "RULE — NOZZLE/HOSE FREE OF BLOCKAGE AND ATTACHED TO EXTINGUISHER:\n\n"
        "CRITICAL PREREQUISITE — EXTINGUISHER MUST BE PRESENT:\n"
        "  FAIL immediately if no fire extinguisher body is visible, or hose is detached.\n\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher body is clearly visible.\n"
        "  2. The hose is visibly connected to the extinguisher body.\n"
        "  3. The nozzle tip or hose end is visible from any angle.\n"
        "  4. No material is covering the OUTSIDE of the nozzle tip.\n"
        "  5. The hose is not kinked, crushed, or tied shut.\n"
        "  6. The nozzle/horn is physically intact.\n\n"
        "FAIL if:\n"
        "  - No fire extinguisher body visible.\n"
        "  - Hose detached or shown in isolation.\n"
        "  - Nozzle tip NOT visible anywhere in the image.\n"
        "  - Tape, cap, cloth covering the nozzle tip.\n\n"
        "DO NOT FAIL because:\n"
        "  - You cannot see INTO the nozzle opening (not required).\n"
        "  - Shadow inside the nozzle opening (shadow ≠ blockage)."
    ),
    "8": (
        "ITEM 8 — PRESSURE GAUGE / NEEDLE IN GREEN ZONE:\n\n"
        "PASS only if ALL are true:\n"
        "1. A pressure gauge is clearly visible.\n"
        "2. The TRUE center-attached moving needle is clearly pointing inside the GREEN zone.\n"
        "3. The needle is NOT in the left red recharge zone.\n"
        "4. The needle is NOT in the right red overcharge zone.\n\n"
        "FAIL if ANY are true:\n"
        "- The true needle is in a red zone.\n"
        "- The needle position cannot be confidently verified.\n"
        "- The gauge is broken, cracked, or missing.\n\n"
        "Important:\n"
        "- A red-dominant gauge face is normal and is NOT a fail by itself.\n"
        "- ANTI-HALLUCINATION: The printed white '195' mark and outer printed scale lines are NOT the needle."
    ),
    "9": (
        "RULE — INSTRUCTION LABEL READABLE AND FACING OUTWARD:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. The instruction label is clearly visible.\n"
        "  2. The label is FACING OUTWARD — text is readable from the front.\n"
        "  3. The label text is READABLE — not blurry, dusty, or faded.\n"
        "  4. The label is INTACT — not peeling, torn, or folded.\n"
        "  5. The label is CLEAN — no heavy dust, grease, or grime.\n"
        "  6. The label is properly ATTACHED to the extinguisher body.\n"
        "FAIL if:\n"
        "  - Label is rotated so text faces away.\n"
        "  - Text is blurry, faded, or unreadable.\n"
        "  - Label is torn, peeled, or folded.\n"
        "  - No label is visible on the extinguisher body."
    ),
    "10": (
        "ITEM 10 — INSPECTION TAG:\n\n"
        "DEFAULT ASSUMPTION: Tags with a visible grid ARE valid inspection records.\n"
        "A pre-printed year grid with ANY physical mark, hole, or darkening in a recent year cell = PASS.\n\n"
        "PASS if ALL are true:\n"
        "1. A tag is physically attached and visible.\n"
        "2. ANY mark, hole, or darkening is visible in ANY year cell from 2025 onward.\n\n"
        "FAIL ONLY IF:\n"
        "- No tag is visible at all, OR\n"
        "- The tag is completely destroyed/unreadable, OR\n"
        "- ALL year cells from 2025 onward are clearly blank.\n\n"
        "DO NOT FAIL because:\n"
        "- You cannot see a clean circular hole.\n"
        "- A year was skipped.\n"
        "- Future years are blank.\n"
        "- You cannot read initials or a specific month."
    ),
}

VALIDATION_KEYWORDS = {
    "1": ["extinguisher", "present", "mounted", "bracket", "visible", "fire"],
    "2": ["height", "mounted", "accessible", "reach", "high", "low", "wall", "bracket"],
    "3": ["blocked", "clear", "accessible", "unobstructed", "path", "obstruction", "box", "pallet"],
    "4": ["sign", "signage", "visible", "above", "marker", "label", "red sign", "pictogram"],
    "5": ["pin", "ring", "loop", "bar", "metal", "handle", "seal", "tamper", "tag", "string",
          "tie", "plastic", "colored", "visible", "absent", "missing", "bare", "empty"],
    "6": ["damage", "rust", "dirty", "clean", "wear", "dent", "corrosion", "scratch", "condition", "intact"],
    "7": ["nozzle", "hose", "blocked", "blockage", "obstructed", "free", "clear",
           "visible", "intact", "opening", "attached", "connected", "extinguisher"],
    "8": ["gauge", "pressure", "needle", "green"],
    "9": ["label", "instruction", "instructions", "facing outward", "front-facing",
           "readable", "visible", "clear", "clean", "blurry", "dust", "damaged", "peeled", "attached"],
    "10": ["tag", "date", "dated", "initial", "initialed", "torn", "dusty", "dirty",
            "missing", "attached", "legible", "blurry", "clean", "intact", "visible", "inspection"],
}

# ─────────────────────────────────────────────
# AI System Prompts
# ─────────────────────────────────────────────
IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a STRICT fire extinguisher safety inspector verifying images for OSHA compliance.
Your decisions affect worker safety. When in doubt, FAIL — never guess a pass.

GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a condition is met, set pass=false.

CORE EVIDENCE POLICY:
- Judge only what is directly visible in the image.
- Do not infer compliance or failure from dominant background colors.
- For gauges, judge only whether the needle is pointing into the green zone.
- If the required evidence is not clearly visible, fail.

STEP 1 — PRESENCE CHECK:
Confirm whether a physical fire extinguisher is clearly visible.
A real fire extinguisher: cylindrical pressure vessel, nozzle/hose, pressure gauge, handle, safety pin, instruction label.
If you are not 100% certain → object_detected = "other", pass = false.

STEP 2 — CONDITION CHECK:
Only if Step 1 confirms a fire extinguisher, evaluate the specific checklist item.

ABSOLUTE RULES:
1. pass=true is ONLY valid when object_detected="fire_extinguisher" AND the condition is clearly met.
2. If object_detected is "other" or "unclear" → pass MUST be false.
3. Do NOT guess. Do NOT infer from context clues not visible in the image.
4. reason MUST describe specifically what you see and why it passes or fails.
5. worker_message must be actionable.

Return JSON ONLY. No markdown. No extra text.

Schema:
{
  "object_detected": "fire_extinguisher|other|unclear",
  "condition_checked": "<short string>",
  "pass": true|false,
  "confidence": <float 0.0–1.0>,
  "reason": "<detailed sentence>",
  "worker_message": "<under 15 words>",
  "suggested_action": "<corrective action or null>"
}
"""

COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a STRICT fire extinguisher component inspector for OSHA compliance.
You are analyzing CLOSE-UP images of specific components (pressure gauge, label, tag).

GOLDEN RULE: INCONCLUSIVE = FAIL

IMPORTANT CONTEXT:
  - For items 8–10, the full extinguisher body does NOT need to be visible.
  - You are evaluating a SPECIFIC COMPONENT at close range.

STEP 1 — TARGET COMPONENT VISIBILITY:
  Confirm whether the requested target component is clearly visible.
STEP 2 — CONDITION CHECK:
  Follow the strict visual rule for this checklist item EXACTLY.
STEP 3 — CHECKS OBJECT:
  Return the required `checks` fields per the contract.
  Every field must be present with an explicit enum value.
  If a field state is unclear → use "unclear" AND set pass=false.

CORE EVIDENCE POLICY:
- Judge only what is directly visible in the image.
- For gauges, judge only whether the needle is pointing into the green zone.
- Do not infer compliance from dominant background colors.

ITEM 8 SPECIAL RULE — PRESSURE GAUGE:
  Identify the true needle attached to the center pivot.
  CRITICAL: Do NOT mistake printed white outer scale lines for the moving needle.
  needle_zone = green → PASS (if glass is intact and readable)
  needle_zone = red_recharge or red_overcharge → FAIL
  needle_zone = unclear → FAIL
  Fogged, cracked, or dusty glass → FAIL

ITEM 10 SPECIAL RULE — INSPECTION TAG:
  If a tag is visible and you can confirm a PUNCH HOLE or MARK on ANY RECENT YEAR (2025+), PASS it.
  Do NOT fail if a year is skipped or future years are blank.

Return JSON ONLY. No markdown. No extra text.
Schema:
{
  "object_detected": "target_component|other|unclear",
  "condition_checked": "<short string>",
  "pass": true|false,
  "confidence": <float 0.0–1.0>,
  "reason": "<detailed sentence>",
  "worker_message": "<under 15 words>",
  "suggested_action": "<corrective action or null>",
  "checks": { "<item_specific_fields>": "<enum_value>" }
}
"""

VOICE_SYSTEM_PROMPT = """
You are a voice assistant for a fire extinguisher inspection app. Workers use voice commands hands-free.

Return JSON only. No markdown. No extra text.

Schema:
{
  "intent": "next_item|previous_item|capture_photo|retake_photo|add_note|repeat_item|help|clarify|unknown",
  "confidence": 0.0,
  "message": "short reply under 10 words",
  "move_to_item": true|false,
  "note_text": "extracted note text or null"
}

Rules:
- This app is ONLY for fire extinguisher inspection.
- move_to_item = true only for next_item and previous_item.
- If worker says something like "the extinguisher was behind a box" treat it as add_note.
- If ambiguous use clarify.
- If the current checklist item is 7-10, give short item-specific guidance.
"""


# ═══════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════

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


def now_iso():
    """Returns current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def parse_body(event):
    """Safely parse JSON body from API Gateway event."""
    body = event.get("body", "{}")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def build_description_lookup():
    lookup = {}
    for category in FIRE_EXTINGUISHER_CHECKLIST.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = item["description"]
    return lookup


def safe_json_parse(text: str) -> dict:
    """Parse JSON robustly — strips markdown fences and handles edge cases."""
    if not text or not text.strip():
        return {}
    t = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t)
    if m:
        t = m.group(1).strip()
    if t.startswith("{"):
        try:
            return json.loads(t)
        except json.JSONDecodeError:
            pass
    brace = t.find("{")
    if brace != -1:
        candidate = t[brace:]
        depth, end = 0, 0
        for i, ch in enumerate(candidate):
            if ch == "{": depth += 1
            elif ch == "}": depth -= 1
            if depth == 0:
                end = i + 1
                break
        if end:
            try:
                return json.loads(candidate[:end])
            except json.JSONDecodeError:
                pass
    return {}


def extract_text_from_claude_response(body: dict) -> str:
    for block in body.get("content", []):
        if block.get("type") == "text":
            return block.get("text", "").strip()
    return ""


def get_route(event):
    method = event.get("httpMethod", event.get("requestContext", {}).get("http", {}).get("method", "")).upper()
    path = event.get("path", event.get("rawPath", "")).rstrip("/")
    resource = event.get("resource", "")
    return method, path, resource


def path_endswith(path: str, suffix: str) -> bool:
    return path.rstrip("/").endswith(suffix.rstrip("/"))


def get_query(event, key, default=""):
    return (event.get("queryStringParameters") or {}).get(key, default)


def prepare_image_bytes(raw_bytes: bytes, max_width: int = MAX_IMAGE_WIDTH, quality: int = MAX_IMAGE_QUALITY) -> bytes:
    """Resize/optimize image using PIL. Falls back to raw bytes if PIL unavailable."""
    if Image is None:
        return raw_bytes
    try:
        img = Image.open(io.BytesIO(raw_bytes))
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        if img.width > max_width:
            ratio = max_width / img.width
            img = img.resize((max_width, int(img.height * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        return buf.getvalue()
    except Exception:
        return raw_bytes


def convert_floats_to_decimal(obj):
    """Convert floats to Decimal for DynamoDB writes."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    return obj


def sanitize_for_dynamodb(obj):
    return convert_floats_to_decimal(obj)


def deep_copy_checklist():
    return copy.deepcopy(FIRE_EXTINGUISHER_CHECKLIST)


def get_all_items(inspection):
    items = []
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            items.append(item)
    return items


def next_unanswered_index(inspection):
    for cat_idx, cat in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(cat.get("items", [])):
            iid = int(item.get("id", 0))
            if iid in (11, 12):
                continue
            if not item.get("answer", "").strip() or item.get("blocked_by_wrong_image"):
                return cat_idx, item_idx
    return None


def load_inspection_by_session_id(session_id: str):
    try:
        resp = table.scan()
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))
        for it in items:
            if str(it.get("session_id", "")).strip() == session_id:
                return convert_decimals(it)
    except Exception:
        logger.exception("load_inspection_by_session_id failed")
    return None


def load_inspection_by_any_id(id_value: str):
    """Try loading by inspection_id first, then by session_id."""
    insp = load_inspection(id_value)
    if insp:
        return insp
    return load_inspection_by_session_id(id_value)


def merge_item_records(existing, incoming):
    merged = dict(existing)
    for key, val in incoming.items():
        if key == "evidence":
            old_ev = merged.get("evidence", [])
            new_ev = val if isinstance(val, list) else []
            seen = {str(e.get("file_key", "")) for e in old_ev if isinstance(e, dict) and e.get("file_key")}
            for ev in new_ev:
                fk = str(ev.get("file_key", "")) if isinstance(ev, dict) else str(ev)
                if fk and fk not in seen:
                    old_ev.append(ev)
                    seen.add(fk)
            merged["evidence"] = old_ev
        elif val not in (None, "", []):
            merged[key] = val
    return merged


def merge_categories(existing_cats, incoming_cats):
    lookup = {}
    for cat in existing_cats:
        for item in cat.get("items", []):
            lookup[int(item.get("id", 0))] = item
    for cat in incoming_cats:
        for item in cat.get("items", []):
            iid = int(item.get("id", 0))
            if iid in lookup:
                lookup[iid] = merge_item_records(lookup[iid], item)
            else:
                lookup[iid] = item
    for cat in existing_cats:
        new_items = []
        for item in cat.get("items", []):
            iid = int(item.get("id", 0))
            new_items.append(lookup.get(iid, item))
        cat["items"] = new_items
    return existing_cats


# ── Checklist rule lookups ─────────────────────
def checklist_rule_for_item(item_id) -> str:
    return CHECKLIST_VISUAL_RULES.get(str(item_id), "")


def expected_keywords_for_item(item_id) -> list:
    return VALIDATION_KEYWORDS.get(str(item_id), [])


def required_yolo_class_for_item(item_id) -> Optional[str]:
    """Returns None for all items — YOLO is dormant."""
    return None


def item_zoom_hint(item_id) -> str:
    hints = {
        "7": "Move camera close to the nozzle/hose area.",
        "8": "Move camera close to the pressure gauge dial.",
        "9": "Move camera close to the instruction label.",
        "10": "Move camera close to the inspection tag.",
    }
    return hints.get(str(item_id), "")


def default_action_for_item(item_id) -> str:
    actions = {
        "7": "Retake showing hose attached to extinguisher and nozzle tip visible.",
        "8": "Retake with gauge dial filling most of the frame.",
        "9": "Retake with instruction label clearly readable.",
        "10": "Retake with inspection tag clearly visible.",
    }
    return actions.get(str(item_id), "Point the camera directly at the fire extinguisher and retake.")


def voice_item_hint(item_id) -> str:
    hints = {
        "7": "Take a close-up of the nozzle and hose.",
        "8": "Take a close-up of the pressure gauge.",
        "9": "Take a close-up of the instruction label.",
        "10": "Take a close-up of the inspection tag.",
    }
    return hints.get(str(item_id), "")


def build_finding_and_action(item_id, passed, blocked, reason, suggested_action, condition_checked):
    if blocked:
        finding = reason or "Could not verify — image unclear or wrong."
        action = suggested_action or default_action_for_item(item_id)
    elif passed:
        finding = reason or "Condition verified — compliant."
        action = ""
    else:
        finding = reason or "Condition not met — non-compliant."
        action = suggested_action or "Address the issue and retake."
    return finding, action


def component_check_contract(item_id: str) -> Optional[dict]:
    contracts = {
        "8": {
            "target": "pressure_gauge",
            "checks_schema": {
                "gauge_visible": "true|false",
                "needle_zone": "green|red_recharge|red_overcharge|unclear",
                "gauge_glass_condition": "intact|cracked|fogged|unclear",
                "gauge_readable": "true|false"
            }
        },
        "9": {
            "target": "instruction_label",
            "checks_schema": {
                "label_visible": "true|false",
                "label_facing_outward": "true|false",
                "label_readable": "true|false",
                "label_clean": "true|false",
                "label_intact": "true|false",
                "label_attached": "true|false"
            }
        },
        "10": {
            "target": "inspection_tag",
            "checks_schema": {
                "tag_visible": "true|false",
                "tag_attached": "true|false",
                "tag_legible": "true|false",
                "tag_clean": "true|false",
                "tag_intact": "true|false",
                "recent_date_or_mark_present": "true|false"
            }
        }
    }
    return contracts.get(str(item_id))


def enforce_component_checks(item_id, analysis, passed, condition_checked, reason, worker_message, suggested_action):
    """Post-Claude enforcement for component items 8-10. Catches hallucinated passes."""
    checks = analysis.get("checks", {})
    if not isinstance(checks, dict):
        return passed, condition_checked, reason, worker_message, suggested_action

    sid = str(item_id)

    def _bool(key):
        v = checks.get(key)
        if isinstance(v, bool): return v
        t = str(v).strip().lower()
        if t in ("true", "yes"): return True
        if t in ("false", "no"): return False
        return None

    if sid == "8":
        if _bool("gauge_visible") is not True:
            return False, "gauge_not_visible", "Gauge not visible.", "Move closer to the gauge.", "Retake showing gauge dial."
        nz = str(checks.get("needle_zone", "unclear")).strip().lower()
        if nz != "green":
            return False, f"needle_{nz}", f"Needle in {nz} zone.", f"Gauge shows {nz}.", suggested_action
        gc = str(checks.get("gauge_glass_condition", "unclear")).strip().lower()
        if gc not in ("intact", ""):
            return False, f"glass_{gc}", f"Gauge glass is {gc}.", f"Glass is {gc}.", suggested_action

    elif sid == "9":
        if _bool("label_visible") is not True:
            return False, "label_not_visible", "Label not visible.", "Show the instruction label.", "Retake showing label."
        if _bool("label_facing_outward") is not True:
            return False, "label_not_outward", "Label not facing outward.", "Rotate extinguisher so label faces camera.", suggested_action
        if _bool("label_readable") is not True:
            return False, "label_not_readable", "Label not readable.", "Move closer to read label.", suggested_action
        if _bool("label_clean") is not True:
            return False, "label_dirty", "Label is dirty/dusty.", "Clean the label and retake.", suggested_action
        if _bool("label_intact") is not True:
            return False, "label_damaged", "Label is damaged.", "Label needs replacement.", suggested_action

    elif sid == "10":
        if _bool("tag_visible") is not True:
            return False, "tag_not_visible", "Tag not visible.", "Show the inspection tag.", "Retake showing tag."
        if _bool("tag_attached") is not True:
            return False, "tag_detached", "Tag not attached.", "Reattach tag.", suggested_action
        if _bool("tag_legible") is not True:
            return False, "tag_illegible", "Tag not legible.", "Replace tag.", suggested_action
        if _bool("recent_date_or_mark_present") is not True:
            return False, "no_recent_date", "No recent date/mark.", "Update tag with current date.", suggested_action

    return passed, condition_checked, reason, worker_message, suggested_action


def best_detection_confidence(detections, target_class):
    best = 0.0
    for d in (detections or []):
        if str(d.get("class", "")).lower() == target_class.lower():
            best = max(best, float(d.get("confidence", 0)))
    return best


# ── YOLO Integration (Dormant) ─────────────────
def _normalize_yolo_detections(raw) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return raw.get("detections", raw.get("predictions", []))
    return []


def invoke_yolo_endpoint(image_bytes: bytes) -> list:
    if not YOLO_ENDPOINT_NAME:
        return []
    try:
        resp = sagemaker_runtime.invoke_endpoint(
            EndpointName=YOLO_ENDPOINT_NAME,
            ContentType="image/jpeg",
            Body=image_bytes,
        )
        raw = json.loads(resp["Body"].read())
        return _normalize_yolo_detections(raw)
    except Exception:
        logger.exception("YOLO endpoint invocation failed")
        return []


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


def load_inspection(inspection_id):
    """Load an inspection record from DynamoDB and convert decimals."""
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")
    if item:
        return convert_decimals(item)
    return None


def save_inspection(inspection):
    """Save an inspection record back to DynamoDB."""
    table.put_item(Item=inspection)


def find_item(inspection, item_id):
    """Find a checklist item by its ID. Returns (item, category_index, item_index) or (None, -1, -1)."""
    target_id = int(item_id)
    for cat_idx, category in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(category.get("items", [])):
            if int(item.get("id", -1)) == target_id:
                return item, cat_idx, item_idx
    return None, -1, -1


def compute_status(inspection):
    """Calculate inspection status: completed, in_progress, or not_started."""
    answered = 0
    total = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            if int(item.get("id", 0)) in [11, 12]:
                continue  # skip summary items
            total += 1
            if item.get("answer", "").strip():
                answered += 1
    if answered == 0:
        return "not_started"
    elif answered >= total:
        return "completed"
    else:
        return "in_progress"


def update_summary_items(inspection):
    """Auto-calculate items 11 (total inspected) and 12 (total compliant)."""
    total_inspected = 0
    total_compliant = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = int(item.get("id", 0))
            if iid in [11, 12]:
                continue
            ans = item.get("answer", "").strip()
            if ans:
                total_inspected += 1
            if ans == "Yes":
                total_compliant += 1

    # Update item 11
    item11, c11, i11 = find_item(inspection, "11")
    if item11:
        item11["answer"] = str(total_inspected)
        inspection["categories"][c11]["items"][i11] = item11

    # Update item 12
    item12, c12, i12 = find_item(inspection, "12")
    if item12:
        item12["answer"] = str(total_compliant)
        inspection["categories"][c12]["items"][i12] = item12


# ═══════════════════════════════════════════════
# BEDROCK / CLAUDE INTEGRATION
# ═══════════════════════════════════════════════

def invoke_claude_json(system_prompt, user_text, image_bytes=None, media_type="image/jpeg", model_id=None, max_tokens=220):
    """Call Claude via Bedrock. Returns parsed JSON dict."""
    if model_id is None:
        model_id = SONNET_MODEL_ID

    content_blocks = []
    if image_bytes is not None:
        content_blocks.append({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type,
                        "data": base64.b64encode(image_bytes).decode("utf-8")},
        })
    content_blocks.append({"type": "text", "text": user_text})

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": content_blocks}],
    }

    response = bedrock.invoke_model(
        modelId=model_id, contentType="application/json",
        accept="application/json", body=json.dumps(payload),
    )
    body = json.loads(response["body"].read())
    return safe_json_parse(extract_text_from_claude_response(body))


# ═══════════════════════════════════════════════
# TRAINING DATA COLLECTION (Phase 1 → Phase 3)
# ═══════════════════════════════════════════════

def save_training_data(image_bytes, media_type, item_id, checklist_description, analysis, inspection):
    """
    Passively saves the analyzed frame and its AI label to the training S3 bucket.
    This data accumulates over months and will be used to fine-tune Claude 3 Haiku in Phase 3.
    """
    try:
        date_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        unique_id = uuid.uuid4().hex[:8]
        base_key = f"fire_extinguisher/{date_prefix}/item_{item_id}_{unique_id}"

        # Determine file extension
        ext = "jpg"
        if "png" in media_type:
            ext = "png"
        elif "webp" in media_type:
            ext = "webp"

        img_key = f"{base_key}.{ext}"
        json_key = f"{base_key}.json"

        # Save the image
        s3.put_object(
            Bucket=TRAINING_BUCKET,
            Key=img_key,
            Body=image_bytes,
            ContentType=media_type,
        )

        # Save the JSON label
        label_data = {
            "frame_s3_key": f"s3://{TRAINING_BUCKET}/{img_key}",
            "item_id": str(item_id),
            "checklist_question": checklist_description,
            "llm_response": analysis,
            "human_override": None,
            "timestamp": now_iso(),
            "facility": inspection.get("facility_area", "unknown"),
            "auditor": inspection.get("auditor_name", "unknown"),
        }
        s3.put_object(
            Bucket=TRAINING_BUCKET,
            Key=json_key,
            Body=json.dumps(label_data, default=str),
            ContentType="application/json",
        )

        logger.info(f"Training data saved: {img_key}")
    except Exception as e:
        # Non-fatal - don't break the inspection flow
        logger.warning(f"Training data collection failed: {str(e)}")


# ═══════════════════════════════════════════════
# ASYNC JOB SYSTEM
# ═══════════════════════════════════════════════

def save_async_job(job_id, status="pending", result=None, error=None):
    item = sanitize_for_dynamodb({
        "session_id": job_id, "job_status": status,
        "created_at": now_iso(), "updated_at": now_iso(),
    })
    if result is not None:
        item["result"] = sanitize_for_dynamodb(result)
    if error is not None:
        item["error"] = str(error)[:2000]
    sessions_table.put_item(Item=item)


def load_async_job(job_id):
    resp = sessions_table.get_item(Key={"session_id": job_id})
    item = resp.get("Item")
    return convert_decimals(item) if item else None


def start_async_analyze_job(event):
    job_id = f"analyze-{uuid.uuid4().hex[:12]}"
    save_async_job(job_id, status="pending")
    body = parse_body(event)
    async_event = {"async_worker": True, "job_id": job_id, "original_body": body}
    try:
        func_name = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "")
        lambda_client.invoke(FunctionName=func_name, InvocationType="Event",
                             Payload=json.dumps(async_event, default=str))
    except Exception:
        logger.exception("Failed to invoke async worker")
        save_async_job(job_id, status="failed", error="Failed to start async worker")
    return build_response(202, {"job_id": job_id, "job_status": "pending",
                                "message": "Analysis started. Poll GET /fire-extinguisher/analyze/status/{job_id}"})


def process_async_analyze_worker(event):
    job_id = event.get("job_id", "")
    body = event.get("original_body", {})
    try:
        save_async_job(job_id, status="processing")
        fake_event = {"body": json.dumps(body, default=str)}
        result = analyze_item_image(fake_event, _is_async=True)
        result_body = json.loads(result.get("body", "{}"))
        save_async_job(job_id, status="completed", result=result_body)
    except Exception as exc:
        logger.exception("Async worker failed")
        save_async_job(job_id, status="failed", error=str(exc))
    return {"statusCode": 200}


def get_analyze_job_status(event):
    path_params = event.get("pathParameters") or {}
    job_id = path_params.get("job_id") or ""
    if not job_id:
        return build_response(400, {"error": "job_id is required"})
    job = load_async_job(job_id)
    if not job:
        return build_response(404, {"error": "Job not found"})
    result = job.get("result")
    if job.get("job_status") == "completed" and isinstance(result, dict):
        inspection_id = str(result.get("inspection_id", "")).strip()
        if inspection_id:
            fresh = load_inspection(inspection_id)
            if fresh:
                result = dict(result)
                result["inspection"] = fresh
                result["categories"] = fresh.get("categories", [])
                result["inspection_status"] = fresh.get("status", "in_progress")
    return build_response(200, {"job_id": job.get("session_id", job_id), "job_status": job.get("job_status", "unknown"),
                                "created_at": job.get("created_at"), "updated_at": job.get("updated_at"),
                                "error": job.get("error"), "result": result})


# ═══════════════════════════════════════════════
# API 1: GET /fire-extinguisher-inspection/checklist
# ═══════════════════════════════════════════════
def get_checklist(event):
    """Returns the full fire extinguisher checklist template."""
    return build_response(200, FIRE_EXTINGUISHER_CHECKLIST)


# ═══════════════════════════════════════════════
# API 2: POST /fire-extinguisher-inspection
# ═══════════════════════════════════════════════
def create_inspection(event):
    """
    Creates a new fire extinguisher inspection record linked to an existing session.
    Session is created via POST /inspection-session (in osha-checklist Lambda).

    Expects JSON body:
    {
        "session_id": "string",
        "team": ["string"],
        "categories": [
            {
                "id": 1,
                "name": "Fire Extinguisher Inspection",
                "items": [
                    { "id": 1, "answer": "Yes", "finding": "", "action_item": "", "responsible": "", "due_date": "" }
                ]
            }
        ],
        "general_results": [
            { "finding": "", "action_item": "", "responsible": "", "due_date": "" }
        ],
        "notes": "string"
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
    created_at = now_iso()

    # Build the item to save
    item = {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "auditor_name": session.get("auditor_name", ""),
        "facility_area": session.get("facility_area", ""),
        "date_of_audit": session.get("date_of_audit", ""),
        "team": team if team else [],
        "categories": categories,
        "general_results": general_results if general_results else [],
        "notes": notes,
        "status": "not_started",
        "current_item_index": 0,
        "created_at": created_at,
        "updated_at": created_at,
    }

    # Save to DynamoDB
    table.put_item(Item=item)

    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "created_at": created_at,
    })


# ═══════════════════════════════════════════════
# API 3: GET /fire-extinguisher-inspections
# ═══════════════════════════════════════════════
def list_inspections(event):
    """
    Returns a summary list of all fire extinguisher inspections.
    Does NOT include the full categories array (keeps it lightweight).
    """
    result = table.scan()
    items = result.get("Items", [])

    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    items = convert_decimals(items)

    summary_list = []
    for item in items:
        summary_list.append({
            "inspection_id": item.get("inspection_id"),
            "session_id": item.get("session_id"),
            "auditor_name": item.get("auditor_name"),
            "facility_area": item.get("facility_area"),
            "date_of_audit": item.get("date_of_audit"),
            "team": item.get("team", []),
            "status": item.get("status", "unknown"),
            "created_at": item.get("created_at"),
        })

    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return build_response(200, summary_list)


# ═══════════════════════════════════════════════
# API 4: GET /fire-extinguisher-inspection/{id}
# ═══════════════════════════════════════════════
def get_inspection(event):
    """Returns the full inspection object including all categories and responses."""
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    item = convert_decimals(item)

    # Enrich items with descriptions from checklist template
    description_lookup = build_description_lookup()
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
        "facility_area": item.get("facility_area"),
        "date_of_audit": item.get("date_of_audit"),
        "team": item.get("team", []),
        "categories": categories,
        "general_results": item.get("general_results", []),
        "notes": item.get("notes", ""),
        "status": item.get("status", "unknown"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at", ""),
    }

    return build_response(200, ordered_item)


# ═══════════════════════════════════════════════
# API 5: PATCH /fire-extinguisher/session/{id}/items/{item_id}
# ═══════════════════════════════════════════════
def update_checklist_item(event):
    """
    Updates a single checklist item's answer, finding, and evidence.
    The mobile app saves items one at a time rather than the whole form.
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id = path_params.get("item_id", "")
    body = parse_body(event)

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Checklist item {item_id} not found"})

    # Update fields if provided
    if "answer" in body:
        checklist_item["answer"] = body["answer"]
    if "finding" in body:
        checklist_item["finding"] = body["finding"]
    if "action_item" in body:
        checklist_item["action_item"] = body["action_item"]
    if "responsible" in body:
        checklist_item["responsible"] = body["responsible"]
    if "due_date" in body:
        checklist_item["due_date"] = body["due_date"]
    if "evidence" in body:
        checklist_item.setdefault("evidence", [])
        if isinstance(body["evidence"], list):
            checklist_item["evidence"].extend(body["evidence"])
        else:
            checklist_item["evidence"].append(body["evidence"])

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item

    # Auto-update summary items
    update_summary_items(inspection)
    inspection["status"] = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(sanitize_for_dynamodb(inspection))

    return build_response(200, {
        "message": f"Item {item_id} updated",
        "updated_item": checklist_item,
        "inspection_status": inspection["status"],
    })


# ═══════════════════════════════════════════════
# API 6: PATCH /fire-extinguisher/session/{id}/items/{item_id}/note
# ═══════════════════════════════════════════════
def add_note_to_item(event):
    """Appends a text note to a checklist item's finding field."""
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id = path_params.get("item_id", "")
    body = parse_body(event)
    note_text = body.get("note", "").strip()

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})
    if not note_text:
        return build_response(400, {"error": "note text is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Checklist item {item_id} not found"})

    # Append note with timestamp
    timestamp = now_iso()
    existing_finding = checklist_item.get("finding", "").strip()
    new_note = f"[{timestamp}] {note_text}"

    if existing_finding:
        checklist_item["finding"] = f"{existing_finding}\n{new_note}"
    else:
        checklist_item["finding"] = new_note

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["updated_at"] = now_iso()
    save_inspection(sanitize_for_dynamodb(inspection))

    return build_response(200, {
        "message": f"Note added to item {item_id}",
        "updated_item": checklist_item,
    })


# ═══════════════════════════════════════════════
# API 7: GET /fire-extinguisher/evidence/upload-url
# ═══════════════════════════════════════════════
def generate_upload_url(event):
    """
    Generates an S3 presigned PUT URL so the mobile app can upload
    inspection photos directly to S3 without going through the Lambda.
    """
    qsp = event.get("queryStringParameters", {}) or {}
    inspection_id = qsp.get("inspection_id", "").strip()
    item_id = qsp.get("item_id", "").strip()
    file_ext = qsp.get("ext", "jpg").strip().lower()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id query param is required"})

    # Build a unique S3 key
    unique_id = uuid.uuid4().hex[:8]
    s3_key = f"inspections/{inspection_id}/item_{item_id}_{unique_id}.{file_ext}"

    content_type_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}
    content_type = content_type_map.get(file_ext, "image/jpeg")

    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": EVIDENCE_BUCKET, "Key": s3_key, "ContentType": content_type},
        ExpiresIn=PRESIGNED_URL_EXPIRY,
    )

    return build_response(200, {
        "upload_url": url,
        "file_key": s3_key,
        "bucket": EVIDENCE_BUCKET,
        "expires_in": PRESIGNED_URL_EXPIRY,
    })


# ═══════════════════════════════════════════════
# API 8: GET /fire-extinguisher/evidence/download-url
# ═══════════════════════════════════════════════
def generate_download_url(event):
    """Generates an S3 presigned GET URL to download/view an evidence photo."""
    qsp = event.get("queryStringParameters", {}) or {}
    file_key = qsp.get("file_key", "").strip()

    if not file_key:
        return build_response(400, {"error": "file_key query param is required"})

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": EVIDENCE_BUCKET, "Key": file_key},
        ExpiresIn=PRESIGNED_URL_EXPIRY,
    )

    return build_response(200, {
        "download_url": url,
        "file_key": file_key,
        "expires_in": PRESIGNED_URL_EXPIRY,
    })


# ═══════════════════════════════════════════════
# API 9: GET /fire-extinguisher/session/{id}/report
# ═══════════════════════════════════════════════
def get_inspection_report(event):
    """
    Returns a slimmed-down version of the inspection, stripping large
    evidence arrays to prevent hitting API Gateway's 6MB payload limit.
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    # Strip evidence details but keep count
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            evidence = item.get("evidence", [])
            item["evidence_count"] = len(evidence)
            item["evidence"] = []  # remove the heavy data

    return build_response(200, inspection)


# ═══════════════════════════════════════════════
# API 10: DELETE /fire-extinguisher/session/{session_id}
# ═══════════════════════════════════════════════
def delete_session(event):
    """Deletes a session and its linked inspection records."""
    path_params = event.get("pathParameters", {}) or {}
    session_id = str(path_params.get("session_id", "")).strip()

    if not session_id:
        return build_response(400, {"error": "session_id is required"})

    delete_inspections_flag = str(get_query(event, "delete_inspections", "true")).strip().lower()
    delete_inspections = delete_inspections_flag in {"1", "true", "yes", "y"}

    # Check session exists
    existing = sessions_table.get_item(Key={"session_id": session_id}).get("Item")
    if not existing:
        return build_response(404, {"error": "Session not found"})

    deleted_inspection_ids = []
    if delete_inspections:
        try:
            scan_res = table.scan()
            items = scan_res.get("Items", [])
            while "LastEvaluatedKey" in scan_res:
                scan_res = table.scan(ExclusiveStartKey=scan_res["LastEvaluatedKey"])
                items.extend(scan_res.get("Items", []))

            for it in items:
                if str(it.get("session_id", "")).strip() == session_id:
                    inspection_id = str(it.get("inspection_id", "")).strip()
                    if inspection_id:
                        table.delete_item(Key={"inspection_id": inspection_id})
                        deleted_inspection_ids.append(inspection_id)
        except Exception as e:
            logger.exception("Failed deleting linked inspections")
            return build_response(500, {"error": f"Failed deleting linked inspections: {str(e)}"})

    try:
        sessions_table.delete_item(Key={"session_id": session_id})
    except Exception as e:
        logger.exception("Failed deleting session")
        return build_response(500, {"error": f"Failed deleting session: {str(e)}"})

    return build_response(200, {
        "message": "Session deleted successfully",
        "session_id": session_id,
        "deleted_linked_inspections": delete_inspections,
        "deleted_inspection_count": len(deleted_inspection_ids),
        "deleted_inspection_ids": deleted_inspection_ids,
    })


# ═══════════════════════════════════════════════
# API 11: POST /fire-extinguisher/voice
# ═══════════════════════════════════════════════
def voice_command(event):
    """
    Parses a voice command from the worker. Tries fast local regex first,
    falls back to Claude 3 Haiku for complex/ambiguous commands.

    Supported intents: next_item, previous_item, capture_photo, add_note,
                       mark_yes, mark_no, mark_na, go_to_item
    """
    body = parse_body(event)
    text = body.get("text", "").strip().lower()

    if not text:
        return build_response(400, {"error": "text is required"})

    # Try local regex first (fast, free, no API call)
    intent = _parse_voice_local(text)
    if intent:
        return build_response(200, intent)

    # Fallback to Haiku for complex commands
    try:
        haiku_prompt = f"""Parse this voice command from a warehouse safety inspector into a structured intent.

Voice command: "{text}"

Valid intents: next_item, previous_item, capture_photo, add_note, mark_yes, mark_no, mark_na, go_to_item

Return JSON only:
{{"intent": "one_of_the_valid_intents", "parameters": {{"note_text": "if add_note", "item_number": null_or_int}}, "confidence": 0.0_to_1.0}}"""

        result = invoke_claude_json(
            system_prompt="You are a voice command parser for an OSHA inspection app. Return JSON only.",
            user_text=haiku_prompt,
            model_id=HAIKU_MODEL_ID,
            max_tokens=150,
        )
        return build_response(200, result)
    except Exception as e:
        logger.warning(f"Voice Haiku fallback failed: {str(e)}")
        return build_response(200, {
            "intent": "unknown",
            "parameters": {},
            "confidence": 0.0,
            "raw_text": text,
        })


def _parse_voice_local(text):
    """Fast local regex parser for common voice commands."""
    text = text.strip().lower()

    if re.search(r"\b(next|forward|move on|continue)\b", text):
        return {"intent": "next_item", "parameters": {}, "confidence": 0.95}
    if re.search(r"\b(back|previous|go back)\b", text):
        return {"intent": "previous_item", "parameters": {}, "confidence": 0.95}
    if re.search(r"\b(capture|take|photo|picture|snap|shoot)\b", text):
        return {"intent": "capture_photo", "parameters": {}, "confidence": 0.95}
    if re.search(r"\b(yes|pass|compliant|good|ok|okay|approve)\b", text):
        return {"intent": "mark_yes", "parameters": {}, "confidence": 0.90}
    if re.search(r"\b(no|fail|non.?compliant|bad|reject)\b", text):
        return {"intent": "mark_no", "parameters": {}, "confidence": 0.90}
    if re.search(r"\b(n/?a|not applicable|skip)\b", text):
        return {"intent": "mark_na", "parameters": {}, "confidence": 0.90}

    note_match = re.search(r"\b(?:note|add note|comment|remark)[:\s]+(.+)", text)
    if note_match:
        return {"intent": "add_note", "parameters": {"note_text": note_match.group(1).strip()}, "confidence": 0.90}

    goto_match = re.search(r"\b(?:go to|jump to|item|number)\s*(\d+)\b", text)
    if goto_match:
        return {"intent": "go_to_item", "parameters": {"item_number": int(goto_match.group(1))}, "confidence": 0.90}

    return None  # let Haiku handle it


# ═══════════════════════════════════════════════
# API 12: POST /fire-extinguisher/analyze
# 🔥 Phase 1 AI — Zero-Shot Claude 3.5 Sonnet
# ═══════════════════════════════════════════════
def analyze_item_image(event, _is_async=False):
    """Enhanced AI endpoint with async support, parallel prep, and component enforcement."""
    if not _is_async and not event.get("async_worker"):
        return start_async_analyze_job(event)

    body = parse_body(event)
    inspection_id = str(body.get("inspection_id", "")).strip()
    item_id = str(body.get("item_id", "")).strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    if not item_id:
        return build_response(400, {"error": "item_id is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Item {item_id} not found"})

    if item_id in ("11", "12"):
        update_summary_items(inspection)
        inspection["status"] = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(sanitize_for_dynamodb(inspection))
        i11, _, _ = find_item(inspection, "11")
        i12, _, _ = find_item(inspection, "12")
        return build_response(200, {
            "inspection_id": inspection_id,
            "item_id": item_id,
            "blocked": False,
            "move_next": True,
            "message": "Summary auto-calculated from inspection evidence.",
            "updated_item_11": i11,
            "updated_item_12": i12,
            "inspection_status": inspection["status"],
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        })

    # ── Extract image ──
    raw_bytes = None
    media_type = "image/jpeg"
    file_key = body.get("file_key", "")

    if body.get("image_base64"):
        raw_b64 = body["image_base64"]
        if "," in raw_b64:
            header, raw_b64 = raw_b64.split(",", 1)
            if "png" in header: media_type = "image/png"
            elif "webp" in header: media_type = "image/webp"
        raw_bytes = base64.b64decode(raw_b64)
    elif file_key:
        try:
            bucket = EVIDENCE_BUCKET
            fk = file_key
            if fk.startswith("s3://"):
                parts = fk.replace("s3://", "").split("/", 1)
                bucket, fk = parts[0], parts[1]
            s3_obj = s3.get_object(Bucket=bucket, Key=fk)
            raw_bytes = s3_obj["Body"].read()
            media_type = s3_obj.get("ContentType", "image/jpeg")
        except Exception:
            logger.exception(f"Failed to fetch {file_key} from S3")
            return build_response(400, {"error": "Failed to load image from S3"})

    if raw_bytes is None:
        return build_response(400, {"error": "Provide image_base64 or file_key"})

    # ── Parallel prep: resize + YOLO (dormant) ──
    opt_bytes = None
    yolo_dets = []
    try:
        f_img = _THREAD_POOL.submit(prepare_image_bytes, raw_bytes)
        f_yolo = _THREAD_POOL.submit(invoke_yolo_endpoint, raw_bytes) if YOLO_ENDPOINT_NAME else None
        opt_bytes = f_img.result()
        if f_yolo: yolo_dets = f_yolo.result()
    except Exception:
        opt_bytes = prepare_image_bytes(raw_bytes)

    # ── YOLO gate (dormant) ──
    req_class = required_yolo_class_for_item(item_id)
    yolo_blocked = False
    yolo_conf = 0.0
    if req_class and YOLO_ENDPOINT_NAME:
        yolo_conf = best_detection_confidence(yolo_dets, req_class)
        if yolo_conf < YOLO_DETECTION_CONFIDENCE_THRESHOLD:
            yolo_blocked = True

    # ── Build prompt ──
    contract = component_check_contract(item_id)
    is_component = contract is not None
    visual_rule = checklist_rule_for_item(item_id)
    kw = ", ".join(expected_keywords_for_item(item_id))

    if is_component:
        prompt = (f"INSPECTION CONTEXT:\n- Checklist Item: {checklist_item.get('description')}\n"
                  f"- Target Component: {contract['target']}\n- STRICT VISUAL RULE: {visual_rule}\n"
                  f"- Expected Keywords: {kw}\n\nYOUR TASK:\n1. VERIFY PRESENCE of '{contract['target']}'.\n"
                  f"2. EVALUATE RULE strictly.\n3. EXTRACT CHECKS.\nIf unclear or rule not met, fail. JSON only.")
        sys_prompt = COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT
    else:
        prompt = (f"INSPECTION CONTEXT:\n- Checklist Item: {checklist_item.get('description')}\n"
                  f"- STRICT VISUAL RULE: {visual_rule}\n- Expected Keywords: {kw}\n\n"
                  f"YOUR TASK:\n1. VERIFY fire extinguisher presence.\n2. EVALUATE RULE strictly.\n"
                  f"If unclear or rule not met, fail. JSON only.")
        sys_prompt = IMAGE_ANALYSIS_SYSTEM_PROMPT

    # ── Bedrock call ──
    inference_time = 0.0
    analysis = {}
    claude_error = None

    if not yolo_blocked:
        try:
            t0 = time.time()
            analysis = invoke_claude_json(sys_prompt, prompt, opt_bytes, media_type)
            inference_time = round(time.time() - t0, 2)
            logger.info(f"Sonnet inference {item_id}: {inference_time}s")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                time.sleep(2)
                try:
                    analysis = invoke_claude_json(sys_prompt, prompt, opt_bytes, media_type)
                except Exception as e2:
                    claude_error = str(e2)
            else:
                claude_error = str(e)
        except Exception as e:
            claude_error = str(e)

    if claude_error:
        logger.exception("Bedrock analysis failed")
        return build_response(502, {"error": f"AI analysis failed: {claude_error}"})

    # ── Extract results ──
    passed = bool(analysis.get("pass", False))
    confidence = float(analysis.get("confidence", 0.0) or 0.0)
    reason = str(analysis.get("reason", "")).strip()
    worker_message = str(analysis.get("worker_message", "")).strip()
    sugg_action = str(analysis.get("suggested_action", "")).strip()
    condition_checked = str(analysis.get("condition_checked", "")).strip()
    object_detected = str(analysis.get("object_detected", "unclear")).lower().strip()

    # ── Overrides ──
    if yolo_blocked:
        passed, blocked = False, True
        reason = f"Fast-gate: {req_class} not detected (conf={yolo_conf:.2f})"
        worker_message = f"Point camera at the {req_class}."
    else:
        blocked = confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD
        if not is_component and passed and object_detected not in ("fire_extinguisher", "extinguisher"):
            passed, reason = False, "Primary object not detected as fire extinguisher."
        if is_component and analysis.get("checks") and passed and not blocked:
            passed, condition_checked, reason, worker_message, sugg_action = enforce_component_checks(
                item_id, analysis, passed, condition_checked, reason, worker_message, sugg_action)

    # ── Build finding/action text ──
    finding_text, action_text = build_finding_and_action(item_id, passed, blocked, reason, sugg_action, condition_checked)
    zoom_hint = item_zoom_hint(item_id)
    blocked_suggested_action = sugg_action or (
        f"{zoom_hint} Remove obstruction and retake." if zoom_hint
        else "Point the camera directly at the fire extinguisher and retake."
    )

    # ── Item 1 special case: extinguisher genuinely missing ──
    if blocked and str(item_id) == "1":
        missing_kw = ["missing", "empty", "no extinguisher", "not present", "absent", "bracket"]
        if any(kw in (reason or "").lower() for kw in missing_kw):
            logger.info("Item 1: extinguisher confirmed missing. Recording as fail.")
            checklist_item["answer"] = "No"
            checklist_item["blocked_by_wrong_image"] = False
            checklist_item["finding"] = reason or "Fire extinguisher not found at this location."
            checklist_item["action_item"] = sugg_action or "Replace missing fire extinguisher immediately."
            blocked = False
            passed = False

    # ── Evidence record ──
    ev_record = {
        "file_key": file_key,
        "analyzed_at": now_iso(),
        "object_detected": object_detected,
        "condition_checked": condition_checked,
        "is_extinguisher": (object_detected == "fire_extinguisher") or is_component,
        "pass": passed,
        "is_compliant": passed and not blocked,
        "confidence": confidence,
        "reason": reason,
        "worker_message": worker_message,
        "suggested_action": sugg_action or "",
        "blocked": blocked,
    }
    checklist_item.setdefault("evidence", []).append(ev_record)

    # ── BLOCKED response ──
    if blocked:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"] = ""
        checklist_item["finding"] = finding_text
        checklist_item["action_item"] = action_text
        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(sanitize_for_dynamodb(inspection))

        try:
            _THREAD_POOL.submit(save_training_data, opt_bytes, media_type, item_id, checklist_item.get("description"), analysis, inspection)
        except Exception:
            pass

        return build_response(200, {
            "inspection_id": inspection_id,
            "item_id": item_id,
            "blocked": True,
            "move_next": False,
            "pass": False,
            "object_detected": object_detected,
            "condition_checked": condition_checked,
            "confidence": confidence,
            "message": worker_message or ("Target component not clear. Move closer and retake." if is_component else "No fire extinguisher detected. Point camera directly at the extinguisher."),
            "reason": finding_text,
            "suggested_action": blocked_suggested_action or action_text,
            "updated_item": checklist_item,
            "inspection_status": inspection.get("status", "in_progress"),
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        })

    # ── SCORED (pass or fail) response ──
    checklist_item["answer"] = "Yes" if passed else "No"
    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"] = finding_text
    checklist_item["action_item"] = action_text
    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item

    # Advance to next unanswered item
    next_pos = next_unanswered_index(inspection)
    if next_pos is not None:
        inspection["current_item_index"] = next_pos[1]
    else:
        inspection["current_item_index"] = item_idx

    update_summary_items(inspection)
    inspection["status"] = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(sanitize_for_dynamodb(inspection))

    item11, _, _ = find_item(inspection, "11")
    item12, _, _ = find_item(inspection, "12")

    try:
        _THREAD_POOL.submit(save_training_data, opt_bytes, media_type, item_id, checklist_item.get("description"), analysis, inspection)
    except Exception:
        pass

    return build_response(200, {
        "inspection_id": inspection_id,
        "item_id": item_id,
        "blocked": False,
        "move_next": True,
        "pass": passed,
        "object_detected": object_detected,
        "condition_checked": condition_checked,
        "confidence": confidence,
        "message": worker_message or "Item recorded.",
        "reason": finding_text,
        "suggested_action": action_text,
        "updated_item": checklist_item,
        "summary_item_11": item11,
        "summary_item_12": item12,
        "inspection_status": inspection["status"],
        "current_item_index": inspection.get("current_item_index", 0),
        "next_item_index": inspection.get("current_item_index", 0),
        "inspection": inspection,
        "categories": inspection.get("categories", []),
    })


# ═══════════════════════════════════════════════
# Main Handler — Routes to correct function
# ═══════════════════════════════════════════════
def lambda_handler(event, context):
    """
    Main entry point. Routes the request based on HTTP method and path.
    """
    # Async self-invoke: if Lambda called itself for background AI work
    if event.get("async_worker"):
        return process_async_analyze_worker(event)

    http_method = event.get("httpMethod", "")
    resource = event.get("resource", "")
    path = event.get("path", "")

    logger.info(f"Received: {http_method} {resource} (path: {path})")

    # CORS preflight
    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # ── Original 4 CRUD routes ──
    if http_method == "GET" and resource == "/fire-extinguisher-inspection/checklist":
        return get_checklist(event)

    elif http_method == "POST" and resource == "/fire-extinguisher-inspection":
        return create_inspection(event)

    elif http_method == "GET" and resource == "/fire-extinguisher-inspections":
        return list_inspections(event)

    elif http_method == "GET" and resource == "/fire-extinguisher-inspection/{inspection_id}":
        return get_inspection(event)

    # ── PATCH item ──
    elif http_method == "PATCH" and "/items/" in path and "/note" not in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            items_idx = parts.index("items")
            inspection_id = parts[session_idx + 1]
            item_id = parts[items_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            event["pathParameters"]["item_id"] = item_id
            return update_checklist_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH item path"})

    # ── PATCH note ──
    elif http_method == "PATCH" and "/note" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            items_idx = parts.index("items")
            inspection_id = parts[session_idx + 1]
            item_id = parts[items_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            event["pathParameters"]["item_id"] = item_id
            return add_note_to_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH note path"})

    # ── S3 presigned URLs ──
    elif http_method == "GET" and "upload-url" in path:
        return generate_upload_url(event)

    elif http_method == "GET" and "download-url" in path:
        return generate_download_url(event)

    # ── Slimmed report ──
    elif http_method == "GET" and "/report" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            inspection_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            return get_inspection_report(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid report path"})

    # ── Delete session ──
    elif http_method == "DELETE" and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            session_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = session_id
            return delete_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid delete path"})

    # ── Voice command ──
    elif http_method == "POST" and "voice" in path:
        return voice_command(event)

    # ── AI Analyze (async — returns job_id) ──
    elif http_method == "POST" and "analyze" in path:
        return analyze_item_image(event)

    # ── AI Analyze status polling ──
    elif http_method == "GET" and "analyze/status" in path:
        parts = path.rstrip("/").split("/")
        try:
            status_idx = parts.index("status")
            job_id = parts[status_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["job_id"] = job_id
            return get_analyze_job_status(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid analyze status path"})

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})

