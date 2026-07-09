"""
Exit Door Monthly Inspection - Lambda Handler
Single Lambda function handling all API routes:

  --- CRUD (5) ---
  POST   /exit-door-inspection                          → Create new inspection
  GET    /exit-door-inspections                         → List all inspections
  GET    /exit-door-inspection/{id}                     → Get full inspection by ID
  GET    /exit-door-inspection/checklist                → Get checklist template
  DELETE /exit-door-inspection/{id}                     → Delete inspection by ID

  --- Mobile + AI Endpoints ---
  PATCH  /exit-door/session/{id}/items/{item_id}        → Update single checklist item
  PATCH  /exit-door/session/{id}/items/{item_id}/note   → Add note to a checklist item
  GET    /exit-door/session/{id}/report                 → Slimmed inspection report
  DELETE /exit-door/session/{session_id}                → Delete session + inspection
  POST   /exit-door/session/{id}/pause                  → Pause an inspection session
  GET    /exit-door/session/{id}/resume                 → Resume a paused session
  POST   /exit-door/voice                               → Voice command parser
  POST   /exit-door/analyze                             → AI image analysis (Claude via Bedrock)
    POST   /exit-door/analyze-batch                       → Batch AI image analysis
  GET    /exit-door/analyze/status/{job_id}             → Poll async analysis job

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHECKLIST STRUCTURE  (18 items across 4 sections)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Section 1 — Door Operation & Hardware  (items  1–5)
Section 2 — Clearance & Dimensions     (items  6–8)
Section 3 — Signage & Visibility       (items  9–13)
Section 4 — Maintenance & Environment  (items 14–17)
Summary                                (item   18 = total doors inspected,
                                        item   19 = total doors compliant)

AI-ANALYZABLE ITEMS (camera photo → Claude decision):
  1  No Keys/Tools Required
  3  Outward Swing
  6  Minimum Width (28 in)
  7  Ceiling Height (7 ft 6 in)
  8  Zero Obstructions
  9  EXIT Sign Present
 10  Sign Letter Specs (6-inch letters)
 11  Proper Illumination
 12  Directional Indicators
 13  NOT AN EXIT Labels
 14  Direct Discharge (door leads outside)
 15  No High-Hazard Travel (visible hazards along exit path)
 17  Outside Safety (barriers near traffic)

NON-AI ITEMS (manual inspector answer only):
  2  Side-Hinged Design       — requires physical push/pull test
  4  Panic Hardware Force     — requires force measurement
  5  Fail-Safe Reliability    — requires wiring/alarm assessment
 16  Self-Closing Fire Doors  — requires certification check
 18  Summary: total inspected (auto-calculated)
 19  Summary: total compliant (auto-calculated)
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

try:
    from checklist_loader import (
        load_checklist, clear_cache, filter_disabled_items, get_company_config,
        sync_inspection_with_template, resolve_verdict_fields, enrich_analyze_response,
        build_mobile_checklist_response,
    )
except ImportError:
    load_checklist = None
    clear_cache = None
    filter_disabled_items = None
    get_company_config = None
    sync_inspection_with_template = None
    resolve_verdict_fields = None
    enrich_analyze_response = None
    build_mobile_checklist_response = None

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
table = dynamodb.Table(os.getenv("INSPECTION_TABLE_NAME", "osha-exit-door-inspections"))
# Use the shared sessions table by default so multiple checklists can share sessions
sessions_table = dynamodb.Table(os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions"))
s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# Module-level thread pool — reused across warm Lambda invocations.
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=6)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
SONNET_MODEL_ID  = os.getenv("BEDROCK_MODEL_ID",       "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
HAIKU_MODEL_ID   = os.getenv("BEDROCK_VOICE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
EVIDENCE_BUCKET  = os.getenv("EVIDENCE_S3_BUCKET",     "osha-inspection-evidence-media")

MAX_IMAGE_WIDTH            = 800
MAX_IMAGE_WIDTH_COMPONENT  = 1200
MAX_IMAGE_QUALITY          = 85
MAX_IMAGE_QUALITY_COMPONENT = 92

IMAGE_CONFIDENCE_BLOCK_THRESHOLD     = 0.35
COMPONENT_CONFIDENCE_BLOCK_THRESHOLD = float(os.getenv("COMPONENT_CONFIDENCE_BLOCK_THRESHOLD", "0.10"))
BATCH_WORKER_COUNT = int(os.getenv("BATCH_WORKER_COUNT", "4"))

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AI-ANALYZABLE vs NON-AI ITEM CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AI_ANALYZABLE_ITEMS = {1, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17}
NON_AI_ITEMS        = {2, 4, 5, 16}        # require physical test / cert check
SUMMARY_ITEMS = set()              # auto-calculated

# ─────────────────────────────────────────────
# Checklist Definition — Single Source of Truth (19 items)
# ─────────────────────────────────────────────
def _item(id_, desc, ai_flag):
    return {
        "id": id_,
        "description": desc,
        "ai_analyzable": ai_flag,
        "answer": "",
        "finding": "",
        "action_item": "",
        "responsible": "",
        "due_date": "",
        "evidence": [],
        "blocked_by_wrong_image": False,
    }


EXIT_DOOR_CHECKLIST = {
    "inspection_type": "Exit Door Monthly Inspection",
    "general_information": {
        "location": "", "start_date": "",
        "checklist": "Exit Door Monthly Inspection",
        "leader": "", "team": []
    },
    "available_answers": ["Yes", "No", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Door Operation & Hardware",
            "items": [
                _item(1,  "No Keys or Tools Required: Can employees open the exit door from the inside at all times without keys, tools, or special knowledge? (Devices like panic bars that lock only from the outside are permitted).",
                      True),
                _item(2,  "Side-Hinged Design: Are all required exit doors side-hinged? (Sliding, revolving, or overhead doors generally cannot serve as designated exit doors).",
                      False),
                _item(3,  "Outward Swing: Does the door swing outward in the direction of exit travel? (Mandatory if the room serves more than 50 people or a high-hazard area).",
                      True),
                _item(4,  "Panic Hardware Force: If panic hardware is installed, does the door unlatch and open by applying 15 pounds of force or less?",
                      False),
                _item(5,  "Fail-Safe Reliability: Are the doors completely free of any device or alarm that could lock or restrict emergency use if the device fails?",
                      False),
            ]
        },
        {
            "id": 2,
            "name": "Clearance & Dimensions",
            "items": [
                _item(6,  "Minimum Width: Is the exit door and access route at least 28 inches wide at all points?",
                      True),
                _item(7,  "Ceiling Height: Is the ceiling of the exit route at least 7 feet, 6 inches high? (Any projections like pipes or light fixtures must not drop below 6 feet, 8 inches).",
                      True),
                _item(8,  "Zero Obstructions: Is the path leading to and away from the door 100% clear? No inventory, trash cans, or temporary equipment should ever block the way.",
                      True),
            ]
        },
        {
            "id": 3,
            "name": "Signage & Visibility",
            "items": [
                _item(9,  "The EXIT Sign: Is every designated exit door marked with a clearly visible EXIT sign?",
                      True),
                _item(10, "Sign Letter Specs: Are the letters on the EXIT sign at least 6 inches high with a principal stroke width of at least 3/4 inch?",
                      True),
                _item(11, "Proper Illumination: Are the EXIT signs adequately lit at all times (either internally illuminated or by a reliable external light source)?",
                      True),
                _item(12, "Directional Indicators: If the direct line of sight to the exit door isn't immediately obvious, are there signs posted along the hallway pointing toward it?",
                      True),
                _item(13, "NOT AN EXIT Labels: Are any doors or passages that could easily be mistaken for an exit clearly marked 'Not an Exit' or labeled for their actual use (e.g., 'Closet' or 'Breakroom')?",
                      True),
            ]
        },
        {
            "id": 4,
            "name": "Maintenance & Environment",
            "items": [
                _item(14, "Direct Discharge: Does the exit door lead directly outside, or to a street, walkway, or open public refuge area?",
                      True),
                _item(15, "No High-Hazard Travel: Is the exit path arranged so employees do not have to walk toward high-hazard areas (like chemical storage or furnace rooms) to escape?",
                      True),
                _item(16, "Self-Closing Fire Doors: If the door is a fire door connecting multiple stories, is it certified and self-closing?",
                      False),
                _item(17, "Outside Safety: If the door opens directly onto an alley or driveway where vehicles operate, are there barriers or warnings to keep workers from stepping directly into traffic?",
                      True),
            ]
        },
        {
            "id": 5,
            "name": "Summary",
            "items": [
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
    # ── Section 1 ──────────────────────────────────────────────────────────
    "1": (
        "RULE — NO KEYS OR TOOLS REQUIRED (INTERIOR ACCESS):\n\n"
        "PASS if ALL of the following are true:\n"
        "  1. An exit door is clearly visible in the image.\n"
        "  2. The door handle, push-bar, or latch mechanism is clearly visible.\n"
        "  3. There is NO visible lock, padlock, chain, keyed deadbolt, or 'Key Required' sign\n"
        "     on the interior side of the door.\n"
        "  4. The door appears operable from the inside WITHOUT any key, tool, or special knowledge.\n\n"
        "FAIL if:\n"
        "  - A padlock, chain, keyed cylinder, or keyed deadbolt is visible on the inside.\n"
        "  - A sign says 'Key Required' or 'Use Key to Exit'.\n"
        "  - A slide bolt or barrel bolt is visible that is not a simple push/pull device.\n\n"
        "NEED REVIEW only if the door mechanism is completely hidden or too blurry to assess."
    ),
    "3": (
        "RULE — OUTWARD SWING (DIRECTION OF EXIT TRAVEL):\n\n"
        "PASS if:\n"
        "  1. An exit door is clearly visible.\n"
        "  2. The door hinges are on the inside and the door clearly swings OUTWARD (away from\n"
        "     the occupants, toward the outside/egress side).\n"
        "     Visual cues: hinges visible on the push side, door panel opens away from camera.\n\n"
        "FAIL if:\n"
        "  - The door clearly swings INWARD (toward the room, away from exit direction).\n"
        "  - Hinges are on the exterior side indicating inward swing from an exit path.\n\n"
        "NEED REVIEW if swing direction cannot be determined from the image angle."
    ),
    # ── Section 2 ──────────────────────────────────────────────────────────
    "6": (
        "RULE — MINIMUM WIDTH (28 INCHES):\n\n"
        "PASS if:\n"
        "  1. An exit door is visible.\n"
        "  2. The door appears at least 28 inches wide. For reference, a standard interior door\n"
        "     is 28–36 inches; a door that appears clearly narrower than a standard door is FAIL.\n"
        "  3. If a tape measure, reference marker, or ruler is visible confirming >= 28 in = PASS.\n\n"
        "FAIL if:\n"
        "  - The door is visibly very narrow (appears less than 28 inches, e.g. a closet door).\n"
        "  - A visible measurement shows less than 28 inches.\n\n"
        "NEED REVIEW if door width cannot be estimated from the image."
    ),
    "7": (
        "RULE — CEILING HEIGHT (7 FT 6 IN MINIMUM; NO PROJECTION BELOW 6 FT 8 IN):\n\n"
        "PASS if:\n"
        "  1. Exit corridor/route is visible.\n"
        "  2. Ceiling appears at least 7 ft 6 in high (visually taller than a standard 7-ft door).\n"
        "  3. No pipes, fixtures, or projections are hanging below ~6 ft 8 in from the floor.\n\n"
        "FAIL if:\n"
        "  - Ceiling is visibly very low (shorter than a standard door height).\n"
        "  - A pipe, duct, beam, or fixture hangs clearly at head-height or lower.\n"
        "  - A visible measurement confirms height below the required minimums.\n\n"
        "NEED REVIEW if ceiling height cannot be reasonably estimated."
    ),
    "8": (
        "RULE — ZERO OBSTRUCTIONS IN EXIT PATH:\n\n"
        "PASS if ALL of the following are true:\n"
        "  1. The exit door and the path leading to/from it are clearly visible.\n"
        "  2. NO boxes, pallets, trash cans, furniture, equipment, signage stands, or any\n"
        "     other object is placed in the exit path or directly in front of the door.\n"
        "  3. The floor path is clear all the way to the door.\n\n"
        "FAIL if:\n"
        "  - Any object of any kind is placed in front of or blocking the door.\n"
        "  - The path to the door is narrowed or blocked by items stored alongside it.\n"
        "  - The door cannot be fully opened due to an item placed against it.\n\n"
        "If obstruction status is unclear, FAIL and request a wider-angle retake."
    ),
    # ── Section 3 ──────────────────────────────────────────────────────────
    "9": (
        "RULE — EXIT SIGN PRESENT AND VISIBLE:\n\n"
        "PASS if ALL of the following are true:\n"
        "  1. An exit door is visible in the image.\n"
        "  2. An 'EXIT' sign (or local equivalent) is clearly mounted above or adjacent to the door.\n"
        "  3. The sign is visible from the current camera angle.\n\n"
        "FAIL if:\n"
        "  - No EXIT sign is visible above or adjacent to the door.\n"
        "  - A sign is present but is completely unlit/unreadable (dark, fallen, face-down).\n\n"
        "NEED REVIEW only if the entire door/doorframe area is not visible."
    ),
    "10": (
        "RULE — EXIT SIGN LETTER SIZE (6-INCH LETTERS, 3/4-INCH STROKE):\n\n"
        "PASS if:\n"
        "  1. An EXIT sign is clearly visible.\n"
        "  2. The letters appear large — at least 6 inches tall. Reference: on a standard 12×6 inch\n"
        "     EXIT sign, the letters nearly fill the sign height. If letters are tiny relative to the\n"
        "     sign housing, FAIL.\n"
        "  3. The letter strokes appear bold (not thin/outline-only).\n\n"
        "FAIL if:\n"
        "  - Letters are clearly small (e.g. a tiny EXIT label, not a full-size sign).\n"
        "  - Letters appear as thin outlines without bold strokes.\n\n"
        "NEED REVIEW if the sign is too far away or blurry to assess letter size."
    ),
    "11": (
        "RULE — EXIT SIGN PROPERLY ILLUMINATED:\n\n"
        "PASS if:\n"
        "  1. An EXIT sign is visible.\n"
        "  2. The sign is visibly illuminated — internally lit (glowing letters or backlit panel)\n"
        "     OR clearly externally lit by a dedicated light source directed at it.\n\n"
        "FAIL if:\n"
        "  - The sign is dark, unlit, or clearly has a burned-out lamp.\n"
        "  - The sign is present but shows no light output whatsoever.\n\n"
        "NEED REVIEW if lighting conditions in the photo make it impossible to assess."
    ),
    "12": (
        "RULE — DIRECTIONAL INDICATOR SIGNS (IF EXIT NOT DIRECTLY VISIBLE):\n\n"
        "IMPORTANT: This item is N/A if the exit door IS directly visible from this point.\n\n"
        "PASS if:\n"
        "  1. The exit route requires navigating a turn/corridor (exit door not in direct line of sight).\n"
        "  2. An arrow or directional EXIT sign is clearly posted pointing toward the exit.\n\n"
        "FAIL if:\n"
        "  - Exit is not directly visible from this corridor/hallway point AND no directional\n"
        "    EXIT arrow/sign is mounted anywhere along the route visible in this image.\n\n"
        "N/A if the exit door itself is already clearly visible in the image (no direction sign needed)."
    ),
    "13": (
        "RULE — 'NOT AN EXIT' LABELS ON NON-EXIT DOORS:\n\n"
        "CLASSIFICATION LOGIC — Determine the door type first:\n"
        "  • If the door has an 'EMERGENCY EXIT', 'EXIT', or 'FIRE EXIT' label/sign → it IS a fire exit door.\n"
        "    This check does NOT apply to fire exit doors. Mark as N/A.\n"
        "  • If the door does NOT have any 'EMERGENCY EXIT', 'EXIT', or 'FIRE EXIT' label/sign\n"
        "    → it is a NON-EXIT door (e.g., washroom, office, water closet, closet, breakroom).\n"
        "    Proceed to evaluate below.\n\n"
        "PASS if ALL of the following are true:\n"
        "  1. A door is visible in the image that does NOT have an 'Emergency Exit' or 'EXIT' label.\n"
        "  2. That non-exit door IS clearly labeled with one of the following:\n"
        "     - 'NOT AN EXIT' or 'NOT EXIT'\n"
        "     - A descriptive label indicating its actual use (e.g., 'CLOSET', 'STOREROOM',\n"
        "       'BREAKROOM', 'WASHROOM', 'RESTROOM', 'OFFICE', 'WATER CLOSET', 'STORAGE',\n"
        "       'MECHANICAL ROOM', 'ELECTRICAL ROOM', 'JANITOR', or similar).\n"
        "  3. The label is clearly readable in the image.\n\n"
        "FAIL if:\n"
        "  - A door is visible that does NOT have an 'Emergency Exit' / 'EXIT' label\n"
        "    AND also does NOT have a 'Not an Exit' label or any descriptive label identifying\n"
        "    its actual use.\n"
        "  - In other words: a non-exit door exists that could easily be mistaken for an exit\n"
        "    because it has NO labeling at all.\n\n"
        "N/A if:\n"
        "  - The door in the image IS clearly labeled 'EMERGENCY EXIT', 'EXIT', or 'FIRE EXIT'\n"
        "    (it is a fire exit, so this 'Not an Exit' check does not apply).\n"
        "  - No doors are visible in the image.\n\n"
        "SUMMARY: Read the label on the door. If it says Emergency Exit / EXIT → N/A.\n"
        "If it has no Emergency Exit label but has a 'Not an Exit' or descriptive label → PASS.\n"
        "If it has no Emergency Exit label AND no 'Not an Exit' or descriptive label → FAIL."
    ),
    # ── Section 4 ──────────────────────────────────────────────────────────
    "14": (
        "RULE — DIRECT DISCHARGE TO OUTSIDE:\n\n"
        "PASS if:\n"
        "  1. The exit door is visible.\n"
        "  2. Through the door (if open) or from door's exterior side, a clear outdoor area, street,\n"
        "     walkway, parking lot, or open space is visible — confirming direct outdoor discharge.\n"
        "  3. OR the door is clearly an exterior door (exterior construction materials visible).\n\n"
        "FAIL if:\n"
        "  - The door opens into another interior room, storage area, or dead-end corridor.\n"
        "  - There is clearly no outdoor discharge path.\n\n"
        "NEED REVIEW if the discharge destination cannot be determined from the image."
    ),
    "15": (
        "RULE — NO HIGH-HAZARD TRAVEL ALONG EXIT PATH:\n\n"
        "IMPORTANT: This item is N/A if the exit path is clearly a clean corridor with no visible\n"
        "hazardous materials or equipment of any kind.\n\n"
        "PASS if ALL of the following are true:\n"
        "  1. The exit path / corridor visible in the image is clear of high-hazard indicators.\n"
        "  2. NO chemical drums, gas cylinders, flammable storage cabinets (red/yellow cabinets),\n"
        "     or hazardous material containers are visible along the exit route.\n"
        "  3. NO heavy machinery, industrial equipment, or furnace/boiler installations are present\n"
        "     in or directly adjacent to the visible exit path.\n"
        "  4. NO HAZMAT warning signs (diamond placards, FLAMMABLE, DANGER, CHEMICAL STORAGE,\n"
        "     COMPRESSED GAS labels) are visible on walls, doors, or equipment along the route.\n\n"
        "FAIL if:\n"
        "  - Flammable storage cabinets (typically red or yellow) are visible in the exit path.\n"
        "  - Chemical drums, barrels, or containers with HAZMAT labels appear alongside the route.\n"
        "  - Gas cylinders (compressed gas) are stored or chained in the exit corridor.\n"
        "  - Heavy industrial machinery or furnace/boiler equipment occupies the exit path.\n"
        "  - Warning signs such as DANGER, FLAMMABLE, HAZARDOUS MATERIAL, or CHEMICAL STORAGE\n"
        "    are posted on or immediately adjacent to the exit route.\n\n"
        "N/A if the corridor shown is clearly an ordinary hallway, office, or retail space with\n"
        "no industrial equipment or hazardous materials visible anywhere in the frame."
    ),
    "17": (
        "RULE — OUTSIDE SAFETY (BARRIERS NEAR VEHICLE TRAFFIC):\n\n"
        "IMPORTANT: This item is N/A if the door does NOT open onto a vehicle traffic area.\n\n"
        "PASS if:\n"
        "  1. The door opens onto an area where vehicles operate (alley, driveway, loading area).\n"
        "  2. A physical barrier (bollard, guardrail, painted stop line, warning sign) is clearly\n"
        "     visible protecting workers from stepping directly into vehicle traffic.\n\n"
        "FAIL if:\n"
        "  - The door opens directly onto a vehicle-operated area with NO barrier or warning.\n\n"
        "N/A if the door opens onto a pedestrian-only area, walkway, or open field with no vehicles."
    ),
}

# ─────────────────────────────────────────────
# Keyword Validation per Checklist Item
# ─────────────────────────────────────────────
VALIDATION_KEYWORDS = {
    "1":  ["door", "exit", "lock", "padlock", "chain", "key", "handle", "bar", "latch", "open"],
    "3":  ["door", "swing", "hinge", "outward", "inward", "direction", "exit"],
    "6":  ["door", "width", "wide", "narrow", "28", "inches", "clearance"],
    "7":  ["ceiling", "height", "projection", "pipe", "duct", "fixture", "feet", "inches"],
    "8":  ["clear", "blocked", "obstruction", "path", "door", "box", "pallet", "trash"],
    "9":  ["exit", "sign", "visible", "mounted", "door", "above"],
    "10": ["exit", "sign", "letter", "size", "large", "small", "inch", "stroke"],
    "11": ["exit", "sign", "light", "lit", "illuminat", "glow", "dark"],
    "12": ["exit", "sign", "arrow", "direction", "corridor", "hallway", "pointing"],
    "13": ["not an exit", "not exit", "closet", "storeroom", "breakroom", "washroom", "restroom", "office", "water closet", "storage", "mechanical", "electrical", "janitor", "emergency exit", "fire exit", "exit", "door", "label", "sign", "no label", "unlabeled"],
    "14": ["outside", "exterior", "door", "discharge", "outdoor", "street", "walkway"],
    "15": ["hazard", "chemical", "flammable", "gas", "cylinder", "drum", "machinery", "danger", "warning", "storage", "cabinet", "furnace", "boiler"],
    "17": ["barrier", "bollard", "guardrail", "vehicle", "traffic", "alley", "driveway"],
}

# ─────────────────────────────────────────────
# AI System Prompts
# ─────────────────────────────────────────────
IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a STRICT exit door safety inspector verifying images for OSHA compliance.
Your decisions affect worker safety. When in doubt, FAIL — never guess a pass.

═══════════════════════════════════════════════════════════════
GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a condition is met, set pass=false.
Never assume compliance from an unclear image.
═══════════════════════════════════════════════════════════════

CORE EVIDENCE POLICY:
- Judge only what is directly visible in the image.
- Do not infer compliance from dominant background colors or general tidiness.
- If the required evidence is not clearly visible, fail.
- Never guess on safety-critical checks.

STEP 1 — PRESENCE CHECK (non-negotiable):
Confirm whether a physical exit door or exit route element is clearly and unambiguously visible.

A real exit door / exit route:
  ✓ A door with hinges, frame, handle or push bar
  ✓ A corridor or hallway leading to an exit
  ✓ EXIT sign mounted above or beside a door
  ✓ An exit route with floor markings or signage

NOT an exit door element:
  ✗ Random walls, ceilings, unrelated equipment
  ✗ Office furniture, shelves, pallets with no door visible
  ✗ Anything you are not 100% certain is an exit door / exit route element

If you are not 100% certain → object_detected = "other", pass = false.

STEP 2 — CONDITION CHECK:
Only if Step 1 confirms an exit door/route element, evaluate the specific checklist item
using the strict visual rule provided. Read the rule carefully — each item has
specific PASS and FAIL criteria. Follow them exactly.

ABSOLUTE RULES:
1. pass=true is ONLY valid when object_detected="exit_door" AND the condition is clearly met.
2. If object_detected is "other" or "unclear" → pass MUST be false.
3. Do NOT guess. Do NOT infer from context clues not visible in the image.
4. If the required condition is not clearly visible → pass=false.
5. If the image is blurry, dark, or the item is partially hidden → pass=false.
6. reason MUST describe specifically what you see and why it passes or fails.
7. condition_checked must state exactly what visual condition you verified.
8. worker_message must be actionable — tell the worker what to do next.

Return JSON ONLY. No markdown. No extra text.

Schema:
{
  "object_detected": "exit_door|other|unclear",
  "condition_checked": "<short string naming the condition verified, or 'not_visible'>",
  "pass": true|false,
  "confidence": <float 0.0–1.0>,
  "reason": "<one detailed sentence describing exactly what you see and why it passes or fails>",
  "worker_message": "<short actionable instruction for the worker, under 15 words>",
  "suggested_action": "<corrective action string, or null if passed>"
}
"""

VOICE_SYSTEM_PROMPT = """
You are a voice assistant for an exit door inspection app. Inspectors use voice commands hands-free.

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
- This app is ONLY for exit door inspection.
- move_to_item = true only for next_item and previous_item.
- If inspector says something observational (e.g. "the sign is missing") treat as add_note.
- If ambiguous use clarify.
- Never mention fire extinguishers or unrelated topics.
"""


# ═══════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-api-key,X-Amz-Security-Token,Accept,Origin",
        },
        "body": json.dumps(body, default=str),
    }


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_body(event):
    body = event.get("body", "{}")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def get_checklist_template(company_key="default", force_refresh=False):
    """Load checklist from DynamoDB with company overlay fallback. Falls back to hardcoded."""
    if load_checklist is not None:
        template = load_checklist("exit-door", company_key, force_refresh=force_refresh)
        if template is not None:
            return template
    return copy.deepcopy(EXIT_DOOR_CHECKLIST)


def build_description_lookup(company_key="default"):
    checklist = get_checklist_template(company_key)
    lookup = {}
    for category in checklist.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = item["description"]
    return lookup


def safe_json_parse(text: str) -> dict:
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
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
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


def get_query(event, key, default=""):
    return (event.get("queryStringParameters") or {}).get(key, default)


def prepare_image_bytes(image_bytes: bytes, content_type: str = "image/jpeg") -> Tuple[bytes, str]:
    if Image is None:
        return image_bytes, content_type
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")
        if img.width > MAX_IMAGE_WIDTH:
            ratio = MAX_IMAGE_WIDTH / float(img.width)
            new_size = (MAX_IMAGE_WIDTH, max(1, int(img.height * ratio)))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=MAX_IMAGE_QUALITY, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning(f"Image preparation failed: {str(e)}")
        return image_bytes, content_type


def convert_floats_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    return obj


def sanitize_for_dynamodb(obj):
    return convert_floats_to_decimal(obj)


def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj


def deep_copy_checklist(tenant_id="default"):
    return get_checklist_template(tenant_id)


# ═══════════════════════════════════════════════════════════════
# DYNAMODB HELPERS
# ═══════════════════════════════════════════════════════════════

def load_inspection(inspection_id):
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")
    return convert_decimals(item) if item else None


def save_inspection(inspection):
    table.put_item(Item=sanitize_for_dynamodb(inspection))


def load_inspection_by_session_id(session_id: str):
    session_id = str(session_id or "").strip()
    if not session_id:
        return None
    try:
        session_resp = sessions_table.get_item(Key={"session_id": session_id})
        session = session_resp.get("Item")
        if session:
            inspection_id = str(session.get("inspection_id", "")).strip()
            if inspection_id:
                return load_inspection(inspection_id)
    except Exception:
        logger.exception("load_inspection_by_session_id fast-path failed")
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
        logger.exception("load_inspection_by_session_id scan fallback failed")
    return None


def load_inspection_by_any_id(id_value: str):
    id_value = str(id_value or "").strip()
    if not id_value:
        return None
    insp = load_inspection(id_value)
    if insp:
        return insp
    return load_inspection_by_session_id(id_value)


def _numeric_item_id(item_or_id):
    """Return int ID for default checklist items, or None for custom string IDs."""
    raw = item_or_id.get("id", 0) if isinstance(item_or_id, dict) else item_or_id
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def _link_session_to_inspection(session_id, inspection_id, inspection_type="exit-door"):
    """Stamp inspection_id + inspection_type on the shared session row for autosave/resume."""
    session_id = str(session_id or "").strip()
    inspection_id = str(inspection_id or "").strip()
    if not session_id or not inspection_id:
        return
    try:
        sessions_table.update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET inspection_id = :iid, inspection_type = :itype, updated_at = :u",
            ExpressionAttributeValues={
                ":iid": inspection_id,
                ":itype": inspection_type,
                ":u": now_iso(),
            },
        )
    except Exception:
        logger.exception(
            "Failed to link session %s to inspection %s (%s)",
            session_id, inspection_id, inspection_type,
        )


def find_item(inspection, item_id):
    for cat_idx, category in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(category.get("items", [])):
            stored_id = item.get("id")
            if stored_id == item_id:
                return item, cat_idx, item_idx
            try:
                if int(stored_id) == int(item_id):
                    return item, cat_idx, item_idx
            except (ValueError, TypeError):
                if str(stored_id) == str(item_id):
                    return item, cat_idx, item_idx
    return None, -1, -1


def get_all_items(inspection):
    items = []
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            items.append(item)
    return items


def next_unanswered_index(inspection):
    for cat_idx, cat in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(cat.get("items", [])):
            iid = _numeric_item_id(item)
            if iid in SUMMARY_ITEMS:
                continue
            if not item.get("answer", "").strip() or item.get("blocked_by_wrong_image"):
                return cat_idx, item_idx
    return None


def compute_status(inspection):
    items = get_all_items(inspection)
    if any(item.get("answer", "") == "" for item in items):
        return "in_progress"
    if any(item.get("answer") == "No" for item in items):
        return "failed"
    return "passed"


def update_summary_items(inspection):
    total_inspected = 0
    total_compliant = 0
    for item in get_all_items(inspection):
        iid = _numeric_item_id(item)
        if iid in SUMMARY_ITEMS:
            continue
        for ev in item.get("evidence", []):
            if isinstance(ev, dict) and ev.get("is_exit_door"):
                total_inspected += 1
                if ev.get("is_compliant"):
                    total_compliant += 1


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
            lookup[str(item.get("id"))] = item
    for cat in incoming_cats:
        for item in cat.get("items", []):
            iid = str(item.get("id"))
            if iid in lookup:
                lookup[iid] = merge_item_records(lookup[iid], item)
            else:
                lookup[iid] = item
    for cat in existing_cats:
        new_items = []
        for item in cat.get("items", []):
            iid = str(item.get("id"))
            new_items.append(lookup.get(iid, item))
        cat["items"] = new_items
    return existing_cats


# ═══════════════════════════════════════════════════════════════
# AI HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def checklist_rule_for_item(item_id: str, checklist_item: dict) -> str:
    return CHECKLIST_VISUAL_RULES.get(str(item_id), checklist_item.get("description", ""))


def expected_keywords_for_item(item_id: str) -> List[str]:
    return VALIDATION_KEYWORDS.get(str(item_id), [])


def item_zoom_hint(item_id: str) -> str:
    hints = {
        "1":  "Show the full door handle/lock area from inside. Keep mechanism clearly visible.",
        "3":  "Step back to show full door and hinges. Direction of swing must be visible.",
        "6":  "Step back to show the full door width in frame. Include the door frame edges.",
        "7":  "Step back to show door and ceiling. Keep full height from floor to ceiling visible.",
        "8":  "Show the full exit path from door to camera position. Include floor and sides.",
        "9":  "Show the exit door and the sign above/beside it. Keep sign readable.",
        "10": "Zoom in on the EXIT sign letters. Fill most of the frame with the sign face.",
        "11": "Show the EXIT sign clearly in the photo. Ensure lighting condition is apparent.",
        "12": "Show the corridor/hallway pointing toward the exit. Include any directional signs.",
        "13": "Show the full door and any labels/signs on it. If the door has an Emergency Exit label, capture it. If it has a 'Not an Exit' or other label (e.g., Closet, Washroom), make sure the label is readable.",
        "14": "Show the exit door open or from outside to confirm it leads outdoors.",
        "15": "Photograph the full length of the exit corridor from floor to ceiling. Capture walls, floor, and any equipment or storage visible along the route.",
        "17": "Show the exterior door area including any barriers or the driveway/alley nearby.",
    }
    return hints.get(str(item_id), "")


def default_action_for_item(item_id: str) -> str:
    actions = {
        "1":  "Remove any lock/chain on exit door interior and ensure door opens freely from inside.",
        "2":  "Replace sliding/revolving/overhead door with compliant side-hinged exit door.",
        "3":  "Rehang door to swing outward in direction of exit travel.",
        "4":  "Service or replace panic hardware so door opens with 15 lbs force or less.",
        "5":  "Remove or rewire any fail-unsafe locking device from exit door.",
        "6":  "Widen exit door/corridor to meet 28-inch minimum width requirement.",
        "7":  "Remove or raise low-hanging projections to meet 6 ft 8 in clearance minimum.",
        "8":  "Immediately remove all obstructions from the exit path and keep it clear.",
        "9":  "Install a compliant EXIT sign above or adjacent to the exit door.",
        "10": "Replace EXIT sign with one having letters at least 6 inches high.",
        "11": "Replace bulb or repair power to ensure EXIT sign is properly illuminated.",
        "12": "Install directional EXIT arrow signs along the route pointing to the exit.",
        "13": "Label misleading doors with 'NOT AN EXIT' or their actual use.",
        "14": "Reroute exit to discharge directly outside or to a safe refuge area.",
        "15": "Redesign exit path to avoid routing employees through high-hazard areas.",
        "16": "Certify fire door and ensure it is self-closing per applicable code.",
        "17": "Install bollards, guardrails, or warning signage to protect workers from traffic.",
    }
    return actions.get(str(item_id), "Retake clear image and correct non-compliance.")


def build_finding_and_action(
    item_id: str,
    passed: bool,
    blocked: bool,
    reason: str,
    suggested_action: Optional[str],
    condition_checked: str,
) -> Tuple[str, str]:
    clean_reason  = str(reason or "").strip()
    clean_action  = str(suggested_action or "").strip()

    if blocked:
        finding    = clean_reason or f"Image is not sufficient to verify checklist item {item_id}."
        action_item = clean_action or (item_zoom_hint(item_id) or default_action_for_item(item_id))
        return finding, action_item

    if passed:
        finding    = clean_reason or f"Checklist item {item_id} passed. Condition verified: {condition_checked or 'verified'}."
        action_item = clean_action or "No corrective action required."
        return finding, action_item

    finding    = clean_reason or f"Checklist item {item_id} failed. Condition not compliant: {condition_checked or 'not_verified'}."
    action_item = clean_action or default_action_for_item(item_id)
    return finding, action_item


# ═══════════════════════════════════════════════════════════════
# BEDROCK / CLAUDE INTEGRATION
# ═══════════════════════════════════════════════════════════════

def invoke_claude_json(system_prompt, user_text, image_bytes=None, media_type="image/jpeg",
                       model_id=None, max_tokens=220):
    if model_id is None:
        model_id = SONNET_MODEL_ID

    content_blocks = []
    if image_bytes is not None:
        content_blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(image_bytes).decode("utf-8"),
            },
        })
    content_blocks.append({"type": "text", "text": user_text})

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": content_blocks}],
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    body = json.loads(response["body"].read())
    return safe_json_parse(extract_text_from_claude_response(body))


# ═══════════════════════════════════════════════════════════════
# ASYNC JOB SYSTEM
# ═══════════════════════════════════════════════════════════════

def save_async_job(job_id, status="pending", result=None, error=None):
    item = sanitize_for_dynamodb({
        "session_id": job_id,
        "job_status": status,
        "created_at": now_iso(),
        "updated_at": now_iso(),
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

    func_name = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "").strip()
    if not func_name:
        save_async_job(job_id, status="failed", error="AWS_LAMBDA_FUNCTION_NAME is not configured")
        return build_response(500, {
            "error": "Async worker is not configured. AWS_LAMBDA_FUNCTION_NAME env var is missing.",
            "job_id": job_id,
            "job_status": "failed",
        })

    async_event = {"async_worker": True, "job_id": job_id, "original_body": body}
    try:
        lambda_client.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps(async_event, default=str),
        )
    except Exception:
        logger.exception("Failed to invoke async worker")
        save_async_job(job_id, status="failed", error="Failed to start async worker")
        return build_response(503, {
            "error": "Failed to queue async analysis job. Please retry.",
            "job_id": job_id,
            "job_status": "failed",
        })

    return build_response(202, {
        "job_id": job_id,
        "job_status": "pending",
        "message": "Analysis started. Poll GET /exit-door/analyze/status/{job_id}",
    })


def process_async_analyze_worker(event):
    job_id = event.get("job_id", "")
    body   = event.get("original_body", {})
    try:
        save_async_job(job_id, status="processing")
        fake_event  = {"body": json.dumps(body, default=str)}
        result      = analyze_item_image(fake_event, _is_async=True)
        result_body = json.loads(result.get("body", "{}"))
        company_key = str(body.get("company_key", "")).strip()
        if enrich_analyze_response is not None:
            result_body = enrich_analyze_response(result_body, company_key)
        save_async_job(job_id, status="completed", result=result_body)
    except Exception as exc:
        logger.exception("Async worker failed")
        save_async_job(job_id, status="failed", error=str(exc))
    return {"statusCode": 200}


def get_analyze_job_status(event):
    path_params = event.get("pathParameters") or {}
    job_id      = path_params.get("job_id") or ""
    if not job_id:
        return build_response(400, {"error": "job_id is required"})
    job = load_async_job(job_id)
    if not job:
        return build_response(404, {"error": "Job not found"})
    result = job.get("result")
    if job.get("job_status") == "completed" and isinstance(result, dict):
        params = event.get("queryStringParameters") or {}
        company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()
        if enrich_analyze_response is not None:
            result = enrich_analyze_response(result, company_key)
        inspection_id = str(result.get("inspection_id", "")).strip()
        if inspection_id:
            fresh = load_inspection(inspection_id)
            if fresh:
                result = dict(result)
                result["inspection"]        = fresh
                result["categories"]        = fresh.get("categories", [])
                result["inspection_status"] = fresh.get("status", "in_progress")
    return build_response(200, {
        "job_id":     job.get("session_id", job_id),
        "job_status": job.get("job_status", "unknown"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "error":      job.get("error"),
        "result":     result,
    })


# ═══════════════════════════════════════════════════════════════
# IMAGE EXTRACTION HELPER
# ═══════════════════════════════════════════════════════════════

def _extract_image_from_request(body: dict) -> Tuple[Optional[bytes], str]:
    image_base64 = str(body.get("image_base64", "")).strip()
    file_key     = str(body.get("file_key", "")).strip() or str(body.get("fileKey", "")).strip()

    if image_base64:
        if "," in image_base64 and image_base64.startswith("data:"):
            image_base64 = image_base64.split(",", 1)[1]
        return base64.b64decode(image_base64), "image/jpeg"

    if file_key:
        try:
            bucket = EVIDENCE_BUCKET
            key    = file_key
            if key.startswith("s3://"):
                parts  = key.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key    = parts[1] if len(parts) > 1 else ""
            obj          = s3.get_object(Bucket=bucket, Key=key)
            image_bytes  = obj["Body"].read()
            content_type = obj.get("ContentType", "image/jpeg")
            return image_bytes, content_type
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(f"S3 key not found: {file_key}")
            raise

    return None, "image/jpeg"


# ═══════════════════════════════════════════════════════════════
# API 1: GET /exit-door-inspection/checklist
# ═══════════════════════════════════════════════════════════════
def get_checklist(event):
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key, force_refresh=True)
    if build_mobile_checklist_response is not None and template is not None:
        template = build_mobile_checklist_response(template, "exit-door", company_key)
    elif filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return build_response(200, template)


# ═══════════════════════════════════════════════════════════════
# API 2: POST /exit-door-inspection
# ═══════════════════════════════════════════════════════════════
def create_inspection(event):
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    session_id      = body.get("session_id", "").strip()
    inspection_id   = body.get("inspection_id", "").strip()
    team            = body.get("team", [])
    general_results = body.get("general_results", [])
    notes           = body.get("notes", "").strip() if isinstance(body.get("notes", ""), str) else ""
    categories      = body.get("categories", [])

    if not session_id and not inspection_id:
        return build_response(400, {"error": "session_id or inspection_id is required"})
    if len(notes) > 5000:
        return build_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return build_response(400, {"error": "general_results must be a list"})
    if not categories or not isinstance(categories, list):
        return build_response(400, {"error": "categories must be a non-empty list"})

    # Resolve session metadata in a background thread while validating body
    def _load_session():
        if session_id:
            try:
                r = sessions_table.get_item(Key={"session_id": session_id})
                return r.get("Item")
            except Exception:
                return None
        return None

    session_future = _THREAD_POOL.submit(_load_session)

    # Validate categories
    for i, cat in enumerate(categories):
        if not isinstance(cat, dict):
            return build_response(400, {"error": f"Category at index {i} must be an object"})
        for j, item in enumerate(cat.get("items", [])):
            if not isinstance(item, dict) or "id" not in item or "answer" not in item:
                return build_response(400, {"error": f"Invalid item at category {i}, item {j}"})

    session = session_future.result(timeout=5)

    auditor_name  = ""
    facility_area = ""
    date_of_audit = ""

    if session:
        auditor_name  = str(session.get("auditor_name",  body.get("auditor_name",  ""))).strip()
        location      = str(session.get("location",      body.get("location",      ""))).strip()
        facility_area = str(session.get("facility_area", body.get("facility_area", ""))).strip()
        station       = str(session.get("station",       body.get("station",       ""))).strip()
        station_id    = str(session.get("station_id",    body.get("station_id",    ""))).strip()
        date_of_audit = str(session.get("date_of_audit", body.get("date_of_audit", ""))).strip()
        if not inspection_id:
            inspection_id = str(session.get("inspection_id", "")).strip()
    else:
        auditor_name  = str(body.get("auditor_name",  "")).strip()
        location      = str(body.get("location",      "")).strip()
        facility_area = str(body.get("facility_area", "")).strip()
        station       = str(body.get("station",       "")).strip()
        station_id    = str(body.get("station_id",    "")).strip()
        date_of_audit = str(body.get("date_of_audit", "")).strip()

    existing = load_inspection(inspection_id) if inspection_id else None

    if existing:
        merged_cats = merge_categories(copy.deepcopy(existing.get("categories", [])), categories)
        existing_gr = existing.get("general_results", [])
        merged_gr   = []
        for idx, ri in enumerate(general_results if general_results else []):
            if not isinstance(ri, dict):
                continue
            base = existing_gr[idx] if idx < len(existing_gr) and isinstance(existing_gr[idx], dict) else {}
            merged_r = dict(base)
            merged_r.update({k: v for k, v in ri.items() if v})
            merged_gr.append(merged_r)
        if not merged_gr:
            merged_gr = copy.deepcopy(existing_gr)

        record = dict(existing)
        record.update({
            "session_id":      session_id or existing.get("session_id", ""),
            "auditor_name":    auditor_name  or existing.get("auditor_name",  ""),
            "location":        location      or existing.get("location",      ""),
            "facility_area":   facility_area or existing.get("facility_area", ""),
            "station":         station       or existing.get("station",       ""),
            "date_of_audit":   date_of_audit or existing.get("date_of_audit", ""),
            "team":            team if team else existing.get("team", []),
            "categories":      merged_cats,
            "general_results": merged_gr,
            "notes":           notes if notes else str(existing.get("notes", "")).strip(),
            "status":          compute_status({"categories": merged_cats}),
            "updated_at":      now_iso(),
        })
        save_inspection(record)
        linked_session_id = session_id or existing.get("session_id", "")
        _link_session_to_inspection(linked_session_id, inspection_id)
        return build_response(200, {
            "inspection_id": inspection_id,
            "session_id":    linked_session_id,
            "created_at":    existing.get("created_at", now_iso()),
            "updated_at":    record["updated_at"],
            "status":        record["status"],
            "message":       "Inspection updated and merged successfully.",
        })

    if not inspection_id:
        inspection_id = str(uuid.uuid4())

    created_at = now_iso()
    record = {
        "inspection_id":      inspection_id,
        "session_id":         session_id,
        "auditor_name":       auditor_name,
        "location":           location,
        "facility_area":      facility_area,
        "station":            station,
        "station_id":         station_id,
        "date_of_audit":      date_of_audit,
        "team":               team if team else [],
        "categories":         categories,
        "general_results":    general_results if general_results else [],
        "notes":              notes,
        "status":             "not_started",
        "current_item_index": 0,
        "created_at":         created_at,
        "updated_at":         created_at,
    }
    save_inspection(record)
    _link_session_to_inspection(session_id, inspection_id)
    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id":    session_id,
        "created_at":    created_at,
        "status":        "not_started",
    })


# ═══════════════════════════════════════════════════════════════
# API 3: GET /exit-door-inspections
# ═══════════════════════════════════════════════════════════════
def list_inspections(event):
    result = table.scan()
    items  = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))
    items = convert_decimals(items)
    summary_list = [{
        "inspection_id":      item.get("inspection_id"),
        "session_id":         item.get("session_id"),
        "auditor_name":       item.get("auditor_name"),
        "location":           item.get("location"),
        "facility_area":      item.get("facility_area"),
        "station":            item.get("station"),
        "date_of_audit":      item.get("date_of_audit"),
        "team":               item.get("team", []),
        "status":             item.get("status", "unknown"),
        "created_at":         item.get("created_at"),
        "current_item_index": item.get("current_item_index", 0),
        "notes":              item.get("notes", ""),
    } for item in items]
    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return build_response(200, summary_list)


# ═══════════════════════════════════════════════════════════════
# API 4: GET /exit-door-inspection/{id}
# ═══════════════════════════════════════════════════════════════
def get_inspection(event):
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()

    item = load_inspection_by_any_id(inspection_id)
    if not item:
        return build_response(404, {"error": "Inspection not found"})

    if company_key and company_key != "default" and sync_inspection_with_template is not None:
        synced = sync_inspection_with_template(item, "exit-door", company_key)
        if synced.get("categories") != item.get("categories"):
            item = synced
            item["updated_at"] = now_iso()
            save_inspection(item)

    description_lookup = build_description_lookup(company_key or "default")
    for cat in item.get("categories", []):
        for ci in cat.get("items", []):
            if ci.get("id") in description_lookup:
                ci["description"] = description_lookup[ci["id"]]
    return build_response(200, {
        "inspection_id":   item.get("inspection_id"),
        "session_id":      item.get("session_id"),
        "auditor_name":    item.get("auditor_name"),
        "location":        item.get("location"),
        "facility_area":   item.get("facility_area"),
        "station":         item.get("station"),
        "date_of_audit":   item.get("date_of_audit"),
        "team":            item.get("team", []),
        "categories":      item.get("categories", []),
        "general_results": item.get("general_results", []),
        "notes":           item.get("notes", ""),
        "status":          item.get("status", "unknown"),
        "created_at":      item.get("created_at"),
        "updated_at":      item.get("updated_at", ""),
    })


# ═══════════════════════════════════════════════════════════════
# API 5: DELETE /exit-door-inspection/{inspection_id}
# ═══════════════════════════════════════════════════════════════
def delete_inspection(event):
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()
    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    result = table.get_item(Key={"inspection_id": inspection_id})
    item   = result.get("Item")
    if not item:
        return build_response(404, {"error": "Inspection not found"})
    session_id = str(item.get("session_id", "")).strip()
    table.delete_item(Key={"inspection_id": inspection_id})

    deleted_session      = False
    delete_session_flag  = str((event.get("queryStringParameters") or {}).get("delete_session", "true")).strip().lower()
    if delete_session_flag in {"1", "true", "yes", "y"} and session_id:
        try:
            sessions_table.delete_item(Key={"session_id": session_id})
            deleted_session = True
        except Exception as e:
            logger.warning(f"Failed to delete linked session {session_id}: {e}")

    return build_response(200, {
        "message":        "Inspection deleted successfully",
        "inspection_id":  inspection_id,
        "session_id":     session_id,
        "session_deleted": deleted_session,
    })


# ═══════════════════════════════════════════════════════════════
# API 6: PATCH /exit-door/session/{id}/items/{item_id}
# ═══════════════════════════════════════════════════════════════
def update_checklist_item(event):
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id       = path_params.get("item_id", "")
    body          = parse_body(event)

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Checklist item {item_id} not found"})

    for field in ("answer", "finding", "action_item", "responsible", "due_date"):
        if field in body:
            checklist_item[field] = body[field]
    if "evidence" in body:
        checklist_item.setdefault("evidence", [])
        if isinstance(body["evidence"], list):
            checklist_item["evidence"].extend(body["evidence"])
        else:
            checklist_item["evidence"].append(body["evidence"])

    if body.get("blocked_by_wrong_image") is not None:
        checklist_item["blocked_by_wrong_image"] = bool(body["blocked_by_wrong_image"])
    if bool(body.get("clear_block", False)):
        checklist_item["blocked_by_wrong_image"] = False

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    update_summary_items(inspection)
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "message":           f"Item {item_id} updated",
        "updated_item":      checklist_item,
        "inspection_status": inspection["status"],
        "categories":        inspection.get("categories", []),
        "inspection":        inspection,
    })


# ═══════════════════════════════════════════════════════════════
# API 7: PATCH /exit-door/session/{id}/items/{item_id}/note
# ═══════════════════════════════════════════════════════════════
def add_note_to_item(event):
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id       = path_params.get("item_id", "")
    body          = parse_body(event)
    note_text     = body.get("note", "").strip()

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})
    if not note_text:
        return build_response(400, {"error": "note text is required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Checklist item {item_id} not found"})

    timestamp       = now_iso()
    existing        = checklist_item.get("finding", "").strip()
    new_note        = f"[{timestamp}] {note_text}"
    checklist_item["finding"] = f"{existing}\n{new_note}" if existing else new_note

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {"message": f"Note added to item {item_id}", "updated_item": checklist_item})


# ═══════════════════════════════════════════════════════════════
# API 8: GET /exit-door/session/{id}/report
# ═══════════════════════════════════════════════════════════════
def get_inspection_report(event):
    path_params   = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            cleaned = []
            for ev in item.get("evidence", []):
                if isinstance(ev, dict):
                    cleaned.append({
                        "file_key":          str(ev.get("file_key", ""))[:300],
                        "analyzed_at":       ev.get("analyzed_at", ""),
                        "object_detected":   ev.get("object_detected", ""),
                        "condition_checked": ev.get("condition_checked", ""),
                        "pass":              ev.get("pass", False),
                        "is_compliant":      ev.get("is_compliant", False),
                        "confidence":        ev.get("confidence", 0.0),
                        "blocked":           ev.get("blocked", False),
                    })
                elif isinstance(ev, str) and ev.strip():
                    cleaned.append({"file_key": str(ev)[:300]})
            item["evidence"] = cleaned[-1:] if cleaned else []

    body_str = json.dumps(inspection, default=str)
    size_kb  = len(body_str.encode("utf-8")) / 1024
    if size_kb > 4000:
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = [{"file_key": str(ev.get("file_key", "") if isinstance(ev, dict) else ev)[:300]}
                                    for ev in item.get("evidence", [])]
    return build_response(200, inspection)


# ═══════════════════════════════════════════════════════════════
# API 9: DELETE /exit-door/session/{session_id}
# ═══════════════════════════════════════════════════════════════
def delete_session(event):
    path_params = event.get("pathParameters", {}) or {}
    session_id  = str(path_params.get("session_id", "")).strip()
    if not session_id:
        return build_response(400, {"error": "session_id is required"})

    existing = sessions_table.get_item(Key={"session_id": session_id}).get("Item")
    if not existing:
        return build_response(404, {"error": "Session not found"})

    delete_inspections_flag = str(get_query(event, "delete_inspections", "true")).strip().lower()
    delete_inspections      = delete_inspections_flag in {"1", "true", "yes", "y"}
    deleted_ids = []

    if delete_inspections:
        scan_res = table.scan()
        items    = scan_res.get("Items", [])
        while "LastEvaluatedKey" in scan_res:
            scan_res = table.scan(ExclusiveStartKey=scan_res["LastEvaluatedKey"])
            items.extend(scan_res.get("Items", []))
        for it in items:
            if str(it.get("session_id", "")).strip() == session_id:
                iid = str(it.get("inspection_id", "")).strip()
                if iid:
                    table.delete_item(Key={"inspection_id": iid})
                    deleted_ids.append(iid)

    sessions_table.delete_item(Key={"session_id": session_id})
    return build_response(200, {
        "message":                    "Session deleted successfully",
        "session_id":                 session_id,
        "deleted_linked_inspections": delete_inspections,
        "deleted_inspection_count":   len(deleted_ids),
        "deleted_inspection_ids":     deleted_ids,
    })


# ═══════════════════════════════════════════════════════════════
# PAUSE / RESUME SESSION
# ═══════════════════════════════════════════════════════════════

def compute_progress(inspection):
    total    = 0
    answered = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = _numeric_item_id(item)
            if iid in SUMMARY_ITEMS:
                continue
            total += 1
            if item.get("answer", "").strip():
                answered += 1
    percentage = round((answered / total * 100), 1) if total else 0
    return {"total": total, "answered": answered, "percentage": percentage}


def find_next_unanswered(inspection):
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = _numeric_item_id(item)
            if iid in SUMMARY_ITEMS:
                continue
            if not item.get("answer", "").strip():
                return item.get("id")
    return None


def pause_session(event):
    try:
        session_id = (event.get("pathParameters") or {}).get("session_id", "")
        if not session_id:
            return build_response(400, {"error": "Missing session_id"})
        inspection = load_inspection_by_any_id(session_id)
        if not inspection:
            return build_response(404, {"error": f"Inspection not found for session {session_id}"})

        body = {}
        raw  = event.get("body", "")
        if raw:
            try:
                body = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                body = {}

        if body.get("categories"):
            inspection["categories"] = merge_categories(inspection.get("categories", []), body["categories"])
        if "general_results" in body:
            inspection["general_results"] = body["general_results"]
        if "notes" in body:
            inspection["notes"] = body["notes"]

        progress = compute_progress(inspection)
        ts = now_iso()
        inspection["status"]         = "paused"
        inspection["last_paused_at"] = ts
        inspection["updated_at"]     = ts
        save_inspection(inspection)

        try:
            sessions_table.update_item(
                Key={"session_id": inspection.get("session_id", session_id)},
                UpdateExpression="SET #status = :status, progress = :progress, inspection_type = :itype, updated_at = :ua, last_paused_at = :pa",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": "paused",
                    ":progress": sanitize_for_dynamodb(progress),
                    ":itype": "exit-door",
                    ":ua": now_iso(),
                    ":pa": now_iso(),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to update session table: {e}")

        return build_response(200, {
            "session_id":    inspection.get("session_id", session_id),
            "inspection_id": inspection.get("inspection_id"),
            "status":        "paused",
            "progress":      progress,
            "last_paused_at": ts,
        })
    except Exception:
        logger.exception("pause_session failed")
        return build_response(500, {"error": "Internal error while pausing session"})


def resume_session(event):
    try:
        session_id = (event.get("pathParameters") or {}).get("session_id", "")
        if not session_id:
            return build_response(400, {"error": "Missing session_id"})
        inspection = load_inspection_by_any_id(session_id)
        if not inspection:
            return build_response(404, {"error": f"Inspection not found for session {session_id}"})

        progress        = compute_progress(inspection)
        next_item_id    = find_next_unanswered(inspection)
        ts              = now_iso()
        inspection["status"]      = "in_progress"
        inspection["resumed_at"]  = ts
        inspection["updated_at"]  = ts
        save_inspection(inspection)

        try:
            sessions_table.update_item(
                Key={"session_id": inspection.get("session_id", session_id)},
                UpdateExpression="SET #status = :status, updated_at = :ua, resumed_at = :ra",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": "in_progress", ":ua": now_iso(), ":ra": now_iso()},
            )
        except Exception as e:
            logger.warning(f"Failed to update session table: {e}")

        result = dict(inspection)
        result["progress"]                = progress
        result["next_unanswered_item_id"] = next_item_id
        return build_response(200, result)
    except Exception:
        logger.exception("resume_session failed")
        return build_response(500, {"error": "Internal error while resuming session"})


# ═══════════════════════════════════════════════════════════════
# API: POST /exit-door/voice
# ═══════════════════════════════════════════════════════════════
def voice_command(event):
    body            = parse_body(event)
    text            = body.get("text", "").strip()
    current_item_id = str(body.get("current_item_id", "")).strip()
    inspection_id   = str(body.get("inspection_id", "")).strip()

    if not text:
        return build_response(400, {"error": "text is required"})

    def _load():
        if not (inspection_id and current_item_id):
            return None, None
        insp = load_inspection_by_any_id(inspection_id)
        if not insp:
            return None, None
        item, _, _ = find_item(insp, current_item_id)
        return insp, item

    dynamo_future = _THREAD_POOL.submit(_load)
    local         = _parse_voice_local(text.lower())

    try:
        inspection, current_item = dynamo_future.result(timeout=5)
    except Exception:
        inspection, current_item = None, None

    if local:
        if local.get("intent") == "next_item" and current_item and current_item.get("blocked_by_wrong_image"):
            hint = item_zoom_hint(current_item_id)
            return build_response(200, {
                "intent": "clarify", "confidence": 0.99,
                "message": f"{hint} Fix and retake first." if hint else "Please retake the photo first.",
                "move_to_item": False, "note_text": None,
            })
        hint = item_zoom_hint(current_item_id)
        if hint and local.get("intent") in ["capture_photo", "retake_photo", "repeat_item", "help"]:
            local["message"] = hint
        return build_response(200, local)

    item_context = ""
    if current_item:
        item_context = f"Current checklist item: {current_item.get('description', '')}"
        hint = item_zoom_hint(current_item_id)
        if hint:
            item_context += f"\nPhoto guidance: {hint}"

    prompt = f"{item_context}\nWorker said: {text}\nReturn JSON only."
    try:
        result = invoke_claude_json(
            system_prompt=VOICE_SYSTEM_PROMPT,
            user_text=prompt,
            model_id=HAIKU_MODEL_ID,
            max_tokens=150,
        )
        if "intent" not in result:
            raise ValueError("No intent in result")
        return build_response(200, result)
    except Exception as e:
        logger.warning(f"Voice Haiku fallback failed: {str(e)}")
        return build_response(200, {
            "intent": "clarify", "confidence": 0.2,
            "message": "Please repeat the command.", "move_to_item": False, "note_text": None,
        })


def _parse_voice_local(text):
    text = text.strip().lower()
    if re.search(r"\b(next item|move next|go forward|continue)\b", text):
        return {"intent": "next_item",     "confidence": 0.99, "message": "Moving to next item.", "move_to_item": True,  "note_text": None}
    if re.search(r"\bnext\b", text) and len(text.split()) <= 2:
        return {"intent": "next_item",     "confidence": 0.95, "message": "Moving to next item.", "move_to_item": True,  "note_text": None}
    if re.search(r"\b(previous item|go back|back to previous)\b", text):
        return {"intent": "previous_item", "confidence": 0.99, "message": "Going back one item.", "move_to_item": True,  "note_text": None}
    if re.search(r"\bback\b", text) and len(text.split()) <= 2:
        return {"intent": "previous_item", "confidence": 0.95, "message": "Going back one item.", "move_to_item": True,  "note_text": None}
    if re.search(r"\b(capture photo|take photo|take picture|capture image|scan)\b", text):
        return {"intent": "capture_photo", "confidence": 0.99, "message": "Capture the photo now.", "move_to_item": False, "note_text": None}
    if re.search(r"\b(retake photo|retake|try again|take again|redo)\b", text):
        return {"intent": "retake_photo",  "confidence": 0.99, "message": "Retaking. Point camera at the exit door.", "move_to_item": False, "note_text": None}
    if re.search(r"\b(add note|add comment|note this|write note)\b", text):
        return {"intent": "add_note",      "confidence": 0.95, "message": "Go ahead, add your note.", "move_to_item": False, "note_text": None}
    if re.search(r"\b(repeat|say again|read again|read item|what is this item)\b", text):
        return {"intent": "repeat_item",   "confidence": 0.95, "message": "Repeating current item.", "move_to_item": False, "note_text": None}
    if re.search(r"\b(help|what can i say|show commands)\b", text):
        return {"intent": "help",          "confidence": 0.95, "message": "Say next, back, capture, retake, add note, or repeat.", "move_to_item": False, "note_text": None}
    note_match = re.search(r"\b(?:note|add note|comment)[:\s]+(.+)", text)
    if note_match:
        return {"intent": "add_note",      "confidence": 0.90, "message": "Note saved.", "move_to_item": False, "note_text": note_match.group(1).strip()}
    return None


# ═══════════════════════════════════════════════════════════════
# MULTITHREADED AI ANALYSIS — helper used for batch analysis
# ═══════════════════════════════════════════════════════════════

def _analyze_single_item_bedrock(
    item_id: str,
    checklist_item: dict,
    image_bytes: bytes,
    content_type: str,
) -> dict:
    """
    Run Bedrock analysis for one AI-analyzable checklist item.
    Returns the raw analysis dict from Claude.
    Used by _THREAD_POOL for parallel multi-item analysis.
    """
    bedrock_img, bedrock_type = prepare_image_bytes(image_bytes, content_type)
    rule    = checklist_rule_for_item(item_id, checklist_item)
    ai_flag = checklist_item.get("ai_analyzable", False)

    # Use .get() with safe fallbacks so a missing/None field never crashes here
    item_db_id   = checklist_item.get("id", item_id)
    item_desc    = checklist_item.get("description") or checklist_item.get("desc") or f"Checklist item {item_id}"

    # Item 13 is special: inspector photographs NON-exit doors to verify labeling
    if str(item_id) == "13":
        prompt = (
            f"Checklist item to inspect:\n"
            f"- item_id: {item_db_id}\n"
            f"- item_description: {item_desc}\n"
            f"- ai_analyzable: {ai_flag}\n"
            f"- strict_visual_rule: {rule}\n\n"
            f"STEP 1: Is a door clearly visible in the image?\n"
            f"STEP 2: Read ANY labels/signs on the door. Classify the door:\n"
            f"  - If the door has 'EMERGENCY EXIT', 'EXIT', or 'FIRE EXIT' label → object_detected='exit_door'\n"
            f"  - If the door does NOT have an exit label → object_detected='non_exit_door'\n"
            f"  - If no door is visible → object_detected='other'\n"
            f"STEP 3: Based on the classification, apply the strict_visual_rule above.\n"
            f"  - For exit doors: this check is N/A (pass=true, condition_checked='n/a_fire_exit_door').\n"
            f"  - For non-exit doors: check if a 'Not an Exit' or descriptive label is present.\n"
            f"Do not guess. If the label is not clearly visible, fail.\n"
            f"Return JSON only."
        )
    else:
        prompt = (
            f"Checklist item to inspect:\n"
            f"- item_id: {item_db_id}\n"
            f"- item_description: {item_desc}\n"
            f"- ai_analyzable: {ai_flag}\n"
            f"- strict_visual_rule: {rule}\n\n"
            f"STEP 1: Is a physical exit door or exit route element clearly and unambiguously visible?\n"
            f"STEP 2: If yes, does the image clearly satisfy the strict_visual_rule above?\n"
            f"Do not approve just because a door is present.\n"
            f"Do not guess. If the required condition is not clearly visible, fail.\n"
            f"Return JSON only."
        )
    return invoke_claude_json(
        system_prompt=IMAGE_ANALYSIS_SYSTEM_PROMPT,
        user_text=prompt,
        image_bytes=bedrock_img,
        media_type=bedrock_type,
        max_tokens=220,
    )


# ═══════════════════════════════════════════════════════════════
# API: POST /exit-door/analyze
# AI Image Analysis with multithreaded batch support
# ═══════════════════════════════════════════════════════════════

def analyze_item_image(event, _is_async=False):
    if not _is_async and not event.get("async_worker"):
        return start_async_analyze_job(event)

    t0   = time.time()
    body = parse_body(event)
    logger.info(
        "[REQUEST] analyze called: inspection_id=%s item_id=%s has_file_key=%s",
        body.get("inspection_id"), body.get("item_id"),
        bool(body.get("file_key") or body.get("fileKey")),
    )

    inspection_id     = str(body.get("inspection_id", "")).strip()
    item_id           = str(body.get("item_id", "")).strip()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    if not item_id:
        return build_response(400, {"error": "item_id is required"})

    # ── Parallel: load inspection + extract image concurrently ────────────
    def _load():
        return load_inspection_by_any_id(inspection_id)

    def _extract_img():
        return _extract_image_from_request(body)

    load_future   = _THREAD_POOL.submit(_load)
    img_future    = _THREAD_POOL.submit(_extract_img)

    try:
        inspection = load_future.result(timeout=10)
    except Exception as e:
        return build_response(500, {"error": f"Failed to load inspection: {str(e)}"})

    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": "Checklist item not found"})

    numeric_item_id = _numeric_item_id(item_id)

    # Summary items: auto-calculate only
    if numeric_item_id in SUMMARY_ITEMS:
        update_summary_items(inspection)
        inspection["status"]     = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)
        return build_response(200, {
            "inspection_id":     inspection_id,
            "item_id":           item_id,
            "blocked":           False,
            "move_next":         True,
            "message":           "Summary auto-calculated from inspection evidence.",
            "inspection_status": inspection["status"],
            "inspection":        inspection,
            "categories":        inspection.get("categories", []),
        })

    # Non-AI items: no image analysis, return guidance
    if numeric_item_id in NON_AI_ITEMS:
        logger.info(f"[NON-AI] item_id={item_id} — requires manual inspection, skipping AI")
        return build_response(200, {
            "inspection_id":  inspection_id,
            "item_id":        item_id,
            "ai_analyzable":  False,
            "blocked":        False,
            "move_next":      False,
            "message":        (
                f"Item {item_id} requires a physical test or certification check and "
                "cannot be verified by AI image analysis. Please record your manual observation."
            ),
            "manual_guidance": _manual_guidance(item_id),
            "inspection":     inspection,
            "categories":     inspection.get("categories", []),
        })

    # Extract image for AI-analyzable items
    try:
        image_bytes, content_type = img_future.result(timeout=10)
    except FileNotFoundError as e:
        return build_response(400, {"error": f"Invalid image input: {str(e)}"})
    except Exception as e:
        return build_response(400, {"error": f"Image extraction failed: {str(e)}"})

    if image_bytes is None:
        return build_response(400, {"error": "Provide image_base64 or file_key"})

    # Run AI analysis via Bedrock
    try:
        analysis = _analyze_single_item_bedrock(item_id, checklist_item, image_bytes, content_type)
        logger.info(f"[TIMING] Bedrock done for item {item_id}: {time.time() - t0:.2f}s")
    except Exception as e:
        logger.exception("Bedrock analysis failed")
        return build_response(502, {"error": f"Bedrock failed: {str(e)}"})

    object_detected   = str(analysis.get("object_detected", "unclear")).lower().strip()
    condition_checked = str(analysis.get("condition_checked", "not_visible")).strip()
    passed            = bool(analysis.get("pass", False))
    confidence        = float(analysis.get("confidence", 0.0) or 0.0)
    reason            = str(analysis.get("reason", "")).strip()
    worker_message    = str(analysis.get("worker_message", "")).strip()
    suggested_action  = analysis.get("suggested_action", None)
    if suggested_action is not None:
        suggested_action = str(suggested_action).strip() or None

    # Hard safety guard: pass requires exit_door detection
    # EXCEPTION: Item 13 ("NOT AN EXIT Labels") — the inspector photographs NON-exit doors
    # (washroom, office, closet, etc.) to verify they have proper "Not an Exit" labeling.
    # For item 13, any door detection (exit_door, door, non_exit_door) is valid.
    _is_item_13 = str(item_id) == "13"
    if passed and object_detected != "exit_door" and not _is_item_13:
        expected_kw  = expected_keywords_for_item(item_id)
        lower_reason = reason.lower()
        if not (expected_kw and any(kw in lower_reason for kw in expected_kw)):
            logger.warning(f"AI pass=true but object_detected={object_detected} — overriding. item_id={item_id}")
            passed           = False
            object_detected  = "other"
            confidence       = 0.0
            condition_checked = "not_visible"
            worker_message   = "No exit door detected. Point camera at the exit door."
            reason           = "Object in image is not an exit door."
            suggested_action = "Point camera directly at the exit door and retake."

    # For item 13, a non-exit door IS the expected subject — don't flag as wrong_image
    if _is_item_13:
        wrong_image = object_detected not in ("exit_door", "door", "non_exit_door")
    else:
        wrong_image = object_detected != "exit_door"

    if object_detected == "exit_door" and confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD:
        object_detected  = "unclear"
        wrong_image      = True
        worker_message   = "Exit door detected but image is not clear enough. Move closer and retake."
        condition_checked = "not_visible"

    blocked          = wrong_image
    zoom_hint        = item_zoom_hint(item_id)
    finding_text, action_text = build_finding_and_action(
        item_id=item_id, passed=passed, blocked=blocked,
        reason=reason, suggested_action=suggested_action, condition_checked=condition_checked,
    )

    evidence_record = {
        "file_key":          body.get("file_key") or body.get("fileKey") or "",
        "analyzed_at":       now_iso(),
        "object_detected":   object_detected,
        "condition_checked": condition_checked,
        "is_exit_door":      object_detected == "exit_door",
        "pass":              passed,
        "is_compliant":      passed and not blocked,
        "confidence":        confidence,
        "reason":            reason,
        "worker_message":    worker_message,
        "suggested_action":  suggested_action or "",
        "blocked":           blocked,
    }
    checklist_item.setdefault("evidence", [])
    checklist_item["evidence"].append(evidence_record)

    if blocked:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"]      = ""
        checklist_item["finding"]     = finding_text
        checklist_item["action_item"] = action_text
        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        # Resolve company-level blocked verdict label
        _company_key = str(body.get("company_key", "")).strip()
        _verdict_label, _verdict_display = ("need_review", "Need Verification")
        if resolve_verdict_fields is not None:
            _verdict_label, _verdict_display = resolve_verdict_fields(_company_key, False, True)

        blocked_body = {
            "inspection_id":      inspection_id,
            "item_id":            item_id,
            "ai_analyzable":      True,
            "blocked":            True,
            "move_next":          False,
            "pass":               False,
            "object_detected":    object_detected,
            "condition_checked":  condition_checked,
            "confidence":         confidence,
            "blocked_verdict_label": _verdict_label,
            "verdict_display":    _verdict_display,
            "message":            worker_message or "Exit door not detected. Point camera at exit door.",
            "reason":             finding_text,
            "suggested_action":   zoom_hint or action_text,
            "updated_item":       checklist_item,
            "inspection_status":  inspection.get("status", "in_progress"),
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection":         inspection,
            "categories":         inspection.get("categories", []),
        }
        if _company_key:
            blocked_body["company_key"] = _company_key
        return build_response(200, blocked_body)

    checklist_item["answer"]               = "Yes" if passed else "No"
    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"]              = finding_text
    checklist_item["action_item"]          = action_text

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    next_pos = next_unanswered_index(inspection)
    inspection["current_item_index"] = next_pos[1] if next_pos else item_idx

    update_summary_items(inspection)
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    # Resolve company-level verdict label for non-pass results
    _verdict_label_final = None
    _verdict_display = "Pass" if passed else "Fail"
    if not passed and resolve_verdict_fields is not None:
        _ck = str(body.get("company_key", "")).strip()
        _verdict_label_final, _verdict_display = resolve_verdict_fields(_ck, False, False)

    resp_body = {
        "inspection_id":      inspection_id,
        "item_id":            item_id,
        "ai_analyzable":      True,
        "blocked":            False,
        "move_next":          True,
        "pass":               passed,
        "object_detected":    object_detected,
        "condition_checked":  condition_checked,
        "confidence":         confidence,
        "message":            worker_message or "Item recorded.",
        "reason":             finding_text,
        "suggested_action":   action_text,
        "updated_item":       checklist_item,
        "inspection_status":  inspection["status"],
        "current_item_index": inspection.get("current_item_index", 0),
        "next_item_index":    inspection.get("current_item_index", 0),
        "inspection":         inspection,
        "categories":         inspection.get("categories", []),
        "verdict_display":    _verdict_display,
    }
    if _verdict_label_final is not None:
        resp_body["blocked_verdict_label"] = _verdict_label_final
    _ck = str(body.get("company_key", "")).strip()
    if _ck:
        resp_body["company_key"] = _ck

    return build_response(200, resp_body)


def batch_analyze_items(event):
    body = parse_body(event)
    inspection_id = str(body.get("inspection_id", "")).strip()
    images = body.get("images", [])

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    if not isinstance(images, list) or not images:
        return build_response(400, {"error": "images must be a non-empty list of {item_id,file_key|image_base64}"})

    child_events = []
    for img in images:
        if not isinstance(img, dict):
            continue
        item_id = str(img.get("item_id", "")).strip()
        if not item_id:
            continue

        child_body = {"inspection_id": inspection_id, "item_id": item_id}
        if isinstance(img.get("file_keys"), list) and img.get("file_keys"):
            child_body["file_keys"] = img["file_keys"]
        elif img.get("file_key"):
            child_body["file_key"] = img["file_key"]

        if isinstance(img.get("image_base64s"), list) and img.get("image_base64s"):
            child_body["image_base64s"] = img["image_base64s"]
        elif img.get("image_base64"):
            child_body["image_base64"] = img["image_base64"]

        if not any(k in child_body for k in ("file_keys", "file_key", "image_base64s", "image_base64")):
            continue

        child_events.append({"body": json.dumps(child_body, default=str)})

    if not child_events:
        return build_response(400, {"error": "No valid image requests were provided"})

    max_workers = max(1, min(BATCH_WORKER_COUNT, len(child_events)))
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as batch_pool:
        futures = {
            batch_pool.submit(analyze_item_image, child_event, True): child_event
            for child_event in child_events
        }
        for future in concurrent.futures.as_completed(futures):
            child_event = futures[future]
            item_id = str((parse_body(child_event).get("item_id", ""))).strip()
            try:
                response = future.result()
                payload = response.get("body") if isinstance(response, dict) else None
                parsed = json.loads(payload) if isinstance(payload, str) else (payload or {})
            except Exception as exc:
                results[item_id or "unknown"] = {"error": str(exc)}
                continue
            results[item_id or str(len(results))] = parsed

    return build_response(200, {
        "inspection_id": inspection_id,
        "results": results,
    })


def _manual_guidance(item_id: str) -> str:
    guides = {
        "2":  "Physically verify the door is side-hinged (has vertical hinge pins on one side). Sliding, revolving, and overhead doors fail this check.",
        "4":  "Apply a push-force meter to the panic bar. The door must unlatch and begin to open at 15 lbs or less. Record the measured force.",
        "5":  "Check the door alarm/lock wiring diagram. Verify the fail-safe mode releases the lock on power or signal failure. Do NOT rely on visual inspection alone.",
        "15": "Walk the full exit route and note if the path passes through any room storing flammables, chemicals, compressed gases, or heavy machinery. Any such route fails.",
        "16": "Check the fire door label/certificate on the door edge. Verify it is UL-listed, test that the door self-closes and latches from the open position.",
    }
    return guides.get(str(item_id), "Record your manual observation and enter Yes/No/N/A.")


# ═══════════════════════════════════════════════════════════════
# LAMBDA HANDLER — Main Router
# ═══════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    if event.get("async_worker"):
        return process_async_analyze_worker(event)

    http_method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method", "")
    resource = event.get("resource") or event.get("routeKey", "")
    path = event.get("path") or event.get("rawPath", "")

    logger.info(f"Received: {http_method} {resource} (path: {path})")

    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # ── CRUD routes ───────────────────────────────────────────────────────
    if http_method == "GET"    and resource == "/exit-door-inspection/checklist":
        return get_checklist(event)
    if http_method == "POST"   and resource == "/exit-door-inspection":
        return create_inspection(event)
    if http_method == "GET"    and resource == "/exit-door-inspections":
        return list_inspections(event)
    if http_method == "GET" and re.search(r"/exit-door-inspection/[^/]+$", path) and "checklist" not in path:
        inspection_id = path.rstrip("/").split("/")[-1]
        event.setdefault("pathParameters", {})
        event["pathParameters"]["inspection_id"] = inspection_id
        return get_inspection(event)
    if http_method == "DELETE" and re.search(r"/exit-door-inspection/[^/]+$", path):
        inspection_id = path.rstrip("/").split("/")[-1]
        event.setdefault("pathParameters", {})
        event["pathParameters"]["inspection_id"] = inspection_id
        return delete_inspection(event)

    # ── PATCH item ────────────────────────────────────────────────────────
    if http_method == "PATCH" and "/items/" in path and "/note" not in path:
        parts = path.rstrip("/").split("/")
        try:
            si  = parts.index("session")
            ii  = parts.index("items")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = parts[si + 1]
            event["pathParameters"]["item_id"]       = parts[ii + 1]
            return update_checklist_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH item path"})

    # ── PATCH note ────────────────────────────────────────────────────────
    if http_method == "PATCH" and "/note" in path:
        parts = path.rstrip("/").split("/")
        try:
            si  = parts.index("session")
            ii  = parts.index("items")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = parts[si + 1]
            event["pathParameters"]["item_id"]       = parts[ii + 1]
            return add_note_to_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH note path"})

    # ── Report ────────────────────────────────────────────────────────────
    if http_method == "GET" and "/report" in path:
        parts = path.rstrip("/").split("/")
        try:
            si = parts.index("session")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = parts[si + 1]
            return get_inspection_report(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid report path"})

    # ── Delete session ────────────────────────────────────────────────────
    if http_method == "DELETE" and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            si = parts.index("session")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = parts[si + 1]
            return delete_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid delete path"})

    # ── Pause ─────────────────────────────────────────────────────────────
    if http_method == "POST" and "/pause" in path and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            si = parts.index("session")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = parts[si + 1]
            return pause_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid pause path"})

    # ── Resume ────────────────────────────────────────────────────────────
    if http_method == "GET" and "/resume" in path and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            si = parts.index("session")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = parts[si + 1]
            return resume_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid resume path"})

    # ── Voice ─────────────────────────────────────────────────────────────
    if http_method == "POST" and "voice" in path:
        return voice_command(event)

    # ── AI Analyze batch ─────────────────────────────────────────────────
    if http_method == "POST" and path.rstrip("/").endswith("/exit-door/analyze-batch"):
        return batch_analyze_items(event)

    # ── AI Analyze (async) ────────────────────────────────────────────────
    if http_method == "POST" and "analyze" in path and "status" not in path:
        return analyze_item_image(event)

    # ── AI Analyze status polling ─────────────────────────────────────────
    if http_method == "GET" and "analyze/status" in path:
        parts = path.rstrip("/").split("/")
        try:
            si = parts.index("status")
            event.setdefault("pathParameters", {})
            event["pathParameters"]["job_id"] = parts[si + 1]
            return get_analyze_job_status(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid analyze status path"})

    return build_response(404, {"error": f"Route not found: {http_method} {resource}"})