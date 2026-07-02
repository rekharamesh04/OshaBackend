"""
Fire Extinguisher Monthly Inspection - Lambda Handler
Single Lambda function handling all API routes:

  --- CRUD (5) ---
  POST   /fire-extinguisher-inspection                          → Create new inspection
  GET    /fire-extinguisher-inspections                         → List all inspections
  GET    /fire-extinguisher-inspection/{id}                     → Get full inspection by ID
  GET    /fire-extinguisher-inspection/checklist                → Get checklist template
  DELETE /fire-extinguisher-inspection/{id}                     → Delete inspection by ID

  --- Mobile + AI Endpoints ---
  PATCH /fire-extinguisher/session/{id}/items/{item_id}       → Update single checklist item
  PATCH /fire-extinguisher/session/{id}/items/{item_id}/note  → Add note to a checklist item
  GET   /fire-extinguisher/session/{id}/report                → Slimmed inspection report
  DELETE /fire-extinguisher/session/{session_id}              → Delete session + inspection
  POST  /fire-extinguisher/session/{id}/pause                 → Pause an inspection session
  GET   /fire-extinguisher/session/{id}/resume                → Resume a paused session
  POST  /fire-extinguisher/voice                              → Voice command parser
  POST  /fire-extinguisher/analyze                            → AI image analysis (Claude via Bedrock)
  GET   /fire-extinguisher/analyze/status/{job_id}            → Poll async analysis job
  GET|POST /fire-extinguisher/qr/generate                     → Generate height QR code
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
    import qrcode
    from qrcode.constants import ERROR_CORRECT_H
except Exception:
    qrcode = None
    ERROR_CORRECT_H = None

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
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# Module-level thread pool — reused across warm Lambda invocations.
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
SONNET_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
HAIKU_MODEL_ID = os.getenv("BEDROCK_VOICE_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")
EVIDENCE_BUCKET = os.getenv("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")

MAX_IMAGE_WIDTH = 800
MAX_IMAGE_WIDTH_COMPONENT = 1200
MAX_IMAGE_QUALITY = 85
MAX_IMAGE_QUALITY_COMPONENT = 92

# QR Code height reference for checklist item 2
QR_CODE_TYPE_IDENTIFIER = "OSHA_FE_HEIGHT"
MAX_ACCESSIBLE_HEIGHT_CM = float(os.getenv("MAX_ACCESSIBLE_HEIGHT_CM", "122"))
MIN_MOUNTING_HEIGHT_CM = float(os.getenv("MIN_MOUNTING_HEIGHT_CM", "10"))

IMAGE_CONFIDENCE_BLOCK_THRESHOLD = 0.35
COMPONENT_CONFIDENCE_BLOCK_THRESHOLD = float(os.getenv("COMPONENT_CONFIDENCE_BLOCK_THRESHOLD", "0.10"))

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}

try:
    from checklist_loader import load_checklist, clear_cache, filter_disabled_items, get_company_config, sync_inspection_with_template
except ImportError:
    load_checklist = None
    clear_cache = None
    filter_disabled_items = None
    get_company_config = None
    sync_inspection_with_template = None


# ─────────────────────────────────────────────
# Checklist Definition — Fallback (used when DynamoDB is unreachable)
# ─────────────────────────────────────────────
_FALLBACK_CHECKLIST = {
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
                {"id": 1,  "description": "Extinguishers are at minimum within 50 feet of areas of risk.",                                                                                                                                                                                                                                                                                                                                    "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 2,  "description": "Extinguishers are mounted in height that is accessible from a seated position.",                                                                                                                                                                                                                                                                                                                   "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 3,  "description": "Extinguishers are not obstructed and are easily accessible.",                                                                                                                                                                                                                                                                                                                                     "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 4,  "description": "Extinguishers are marked with proper signage (above the unit and viewable from 180 degrees).",                                                                                                                                                                                                                                                                                                    "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 5,  "description": "Extinguishers' pins and seals are in place.",                                                                                                                                                                                                                                                                                                                                                     "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 6,  "description": "Extinguishers are in good, clean condition. No visible damage to units. Units are wiped down & clean.",                                                                                                                                                                                                                                                                                           "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 7,  "description": "Extinguishers' nozzles are free of blockage.",                                                                                                                                                                                                                                                                                                                                                    "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 8,  "description": "Extinguishers are fully charged. Pressure gauges show adequate pressure (within green zone) and the gauge glass is intact, clean, and readable.",                                                                                                                                                                                                                                                  "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 9,  "description": "Extinguishers' instructions face outward for visibility and are clean, readable, and not blurry, dusty, folded, peeled, or damaged.",                                                                                                                                                                                                                                                             "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 10, "description": "Extinguisher tags are initialed and dated certifying monthly visual inspection took place. Tags must be attached, legible, clean, and not torn, dusty, dirty, blurry, or missing date/initials. Any extinguisher(s) that did not pass, need to be noted in this inspection and brought to compliance through corrective actions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 11, "description": "Number of extinguishers inspected:",                                                                                                                                                                                                                                                                                                                                                              "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
                {"id": 12, "description": "Number of extinguishers compliant:",                                                                                                                                                                                                                                                                                                                                                              "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": [], "blocked_by_wrong_image": False},
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
        "RULE — PLACEMENT RELATIVE TO RISK AREAS (WITHIN 50 FT):\n"
        "\n"
        "⚠⚠⚠ YELLOW TAPE PERIMETER DETECTION & SIGNAGE ⚠⚠⚠\n"
        "This rule uses a YELLOW TAPE boundary to verify the 50ft placement requirement.\n"
        "The yellow tape marks a perimeter around the risk area.\n"
        "The physical fire extinguisher MUST be INSIDE the yellow tape boundary, AND there MUST be signage at the top of it.\n"
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "STEP 1: Is a physical FIRE EXTINGUISHER clearly visible?\n"
        "  NO → VERDICT: NEED REVIEW (cannot assess placement without seeing the unit)\n"
        "  YES → Continue to Step 2\n"
        "\n"
        "STEP 2: Is a YELLOW TAPE perimeter visible in the image?\n"
        "  The yellow tape typically:\n"
        "    - Forms a rectangular or polygonal boundary on the floor/ground\n"
        "    - Is bright yellow color (standard caution tape)\n"
        "    - May have black stripes or text (standard caution tape pattern)\n"
        "    - Marks an area/perimeter clearly visible in the photo\n"
        "\n"
        "  YES, yellow tape boundary is visible → Continue to Step 3\n"
        "  NO, no yellow tape visible → VERDICT: NEED REVIEW\n"
        "       (Cannot verify placement without marked perimeter)\n"
        "\n"
        "STEP 3: Is the FIRE EXTINGUISHER positioned INSIDE the yellow tape boundary?\n"
        "  'Inside the boundary' means:\n"
        "    - The extinguisher body is fully or mostly within the taped area\n"
        "    - The extinguisher is on the same side of the yellow line as the protected area\n"
        "    - The extinguisher is not outside/beyond the yellow tape perimeter\n"
        "\n"
        "  YES, extinguisher is inside the yellow tape boundary → Continue to Step 4\n"
        "  NO, extinguisher is outside the yellow tape boundary → VERDICT: FAIL\n"
        "  UNCLEAR, extinguisher position relative to tape is ambiguous → Continue to Step 4\n"
        "           (Default to pass when tape is present but position unclear)\n"
        "\n"
        "STEP 4: Is there SIGNAGE visible at the top of the fire extinguisher?\n"
        "  YES, signage is visible at the top → VERDICT: PASS\n"
        "  NO, no signage visible at the top → VERDICT: FAIL\n"
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "INTERPRETATION GUIDE:\n"
        "\n"
        "YELLOW TAPE MEANINGS:\n"
        "  - Marks a 50ft perimeter around a risk area (kitchen, storage, etc)\n"
        "  - Extinguisher inside tape = within 50ft of risk area = PASS\n"
        "  - Extinguisher outside tape = beyond 50ft of risk area = FAIL\n"
        "\n"
        "WHAT COUNTS AS 'INSIDE' THE BOUNDARY:\n"
        "  ✓ Extinguisher cylinder is clearly within the taped perimeter\n"
        "  ✓ Extinguisher is on the protected side of the yellow line\n"
        "  ✓ Extinguisher position is ambiguous but tape is present = PASS (give benefit)\n"
        "\n"
        "WHAT COUNTS AS 'OUTSIDE' THE BOUNDARY:\n"
        "  ✗ Extinguisher is clearly beyond the yellow tape line\n"
        "  ✗ Extinguisher is positioned outside/away from the marked perimeter\n"
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "EXAMPLES:\n"
        "\n"
        "  Example 1: Yellow tape forms a rectangle on the floor.\n"
        "             Red extinguisher is standing inside the taped area.\n"
        "             Signage is visible at the top.\n"
        "    Step 1: YES, extinguisher is visible\n"
        "    Step 2: YES, yellow tape boundary is visible\n"
        "    Step 3: YES, extinguisher is inside the boundary\n"
        "    Step 4: YES, signage is visible at the top\n"
        "    VERDICT: PASS ✓\n"
        "    FINDING: 'Physical fire extinguisher is positioned within the 50ft perimeter marked by yellow tape with signage at the top.'\n"
        "    ACTION: 'No corrective action required.'\n"
        "\n"
        "  Example 2: Yellow tape marks a perimeter.\n"
        "             Fire extinguisher is positioned outside/beyond the yellow tape line.\n"
        "    Step 1: YES, extinguisher is visible\n"
        "    Step 2: YES, yellow tape boundary is visible\n"
        "    Step 3: NO, extinguisher is outside the boundary\n"
        "    VERDICT: FAIL ✗\n"
        "    FINDING: 'Fire extinguisher is positioned outside the 50ft perimeter marked by yellow tape.'\n"
        "    ACTION: 'Relocate the extinguisher to be within the yellow tape boundary (within 50ft of the risk area).'\n"
        "\n"
        "  Example 3: Extinguisher inside yellow tape but missing signage at the top.\n"
        "    Step 1: YES, extinguisher is visible\n"
        "    Step 2: YES, yellow tape boundary is visible\n"
        "    Step 3: YES, extinguisher is inside the boundary\n"
        "    Step 4: NO, no signage visible at the top\n"
        "    VERDICT: FAIL ✗\n"
        "    FINDING: 'Fire extinguisher is within the 50ft perimeter marked by yellow tape but lacks signage at the top.'\n"
        "    ACTION: 'Install required signage at the top of the fire extinguisher.'\n"
        "\n"
        "  Example 4: No yellow tape visible in image\n"
        "    Step 1: YES, extinguisher is visible\n"
        "    Step 2: NO, no yellow tape boundary visible\n"
        "    VERDICT: NEED REVIEW ⚠\n"
        "    FINDING: 'No yellow tape perimeter is visible in the image.'\n"
        "    ACTION: 'Please retake the image showing the yellow tape boundary that marks the 50ft perimeter.'\n"
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "\n"
        "CRITICAL SAFETY CHECKS:\n"
        "  ⚠ Physical fire extinguisher MUST be visible.\n"
        "  ⚠ Signage MUST be present at the top of the fire extinguisher.\n"
        "  ⚠ Yellow tape is present = you have a reference marker, make PASS/FAIL decision.\n"
        "  ⚠ No yellow tape visible = NEED REVIEW (cannot verify without the marked boundary).\n"
        "  ⚠ Extinguisher clearly inside tape and has signage = PASS.\n"
        "  ⚠ Extinguisher clearly outside tape = FAIL.\n"
        "  ⚠ Position ambiguous + tape present + signage present = PASS (give benefit of doubt).\n"
        "  ⚠ Do NOT use NEED REVIEW when yellow tape is visible and position/signage is determinable.\n"
    ),
    "2": (
        "RULE — MOUNTED HEIGHT ACCESSIBLE FROM SEATED POSITION (QR CODE VERIFICATION):\n"
        "This item is verified using a QR code reference marker placed beside the extinguisher.\n"
        "The QR code encodes the measured handle height from the floor in centimeters.\n"
        "The system decodes the QR code programmatically — you do NOT need to read it visually.\n"
        "PASS if the QR code is present in the image alongside a fire extinguisher and the \n"
        "encoded height is within the accessible range (handle ≤ 122 cm from floor).\n"
        "FAIL if no QR code is visible, or if the encoded height exceeds the accessible range.\n"
        "Do NOT attempt to estimate height visually — rely solely on QR code data."
    ),
    "3": (
        "RULE — NOT OBSTRUCTED, EASILY ACCESSIBLE:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. No boxes, pallets, equipment, furniture, or other objects are within arm's reach in front of "
        "     or directly beside the extinguisher that would require moving something to grab it.\n"
        "  3. The path to the extinguisher appears clear and unobstructed.\n"
        "FAIL if:\n"
        "  - Any object is blocking direct access to the extinguisher.\n"
        "  - The extinguisher is surrounded by shelving, boxes, or clutter.\n"
        "  - Only the top or a small part of the extinguisher is visible because items block it.\n"
        "If obstruction status is unclear, set pass=false and request a retake from further back."
    ),
   "4": (
    "RULE — PROPER SIGNAGE VISIBLE:\n"
    "\n"
    "⚠⚠⚠ BINARY VERDICT ONLY ⚠⚠⚠\n"
    "When the extinguisher is visible:\n"
    "  PASS or FAIL (not NEED REVIEW)\n"
    "NEED REVIEW only if the extinguisher itself is not visible.\n"
    "\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "STEP 1: Is the fire extinguisher visible in the image?\n"
    "  NO → VERDICT: NEED REVIEW (cannot assess signage without seeing the unit)\n"
    "  YES → Continue to Step 2\n"
    "\n"
    "STEP 2: Is there ANY sign or marker visible above, near, or adjacent to the extinguisher?\n"
    "  The sign can be:\n"
    "    - A red rectangular sign with 'FIRE EXTINGUISHER' text (typical)\n"
    "    - A pictogram/symbol showing a fire extinguisher\n"
    "    - Any other marker indicating this is a fire extinguisher location\n"
    "    - Red, yellow, or other color (color doesn't matter, visibility matters)\n"
    "\n"
    "  YES, sign is visible → Continue to Step 3\n"
    "  NO, no sign visible → VERDICT: FAIL (missing signage)\n"
    "\n"
    "STEP 3: Is the sign readable/legible from the current image angle?\n"
    "  'Readable' means:\n"
    "    - Text is not completely blurred or illegible\n"
    "    - The sign orientation shows it's meant to be a marker (not text upside-down or backwards)\n"
    "    - The sign is large enough to identify it as fire extinguisher signage\n"
    "\n"
    "  YES, readable → VERDICT: PASS\n"
    "  NO, sign is too faded/blurry/small to read → VERDICT: FAIL\n"
    "\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "\n"
    "IMPORTANT NOTES:\n"
    "  ⚠ '180 degrees viewable' does NOT mean the photo must show it from 180°.\n"
    "    It means the sign is DESIGNED to be seen from multiple angles.\n"
    "    If the sign is mounted flat against the wall and visible in this photo,\n"
    "    it's likely mounted for visibility from the room.\n"
    "\n"
    "  ⚠ You do NOT need to verify it's viewable from all 180°.\n"
    "    You only need to verify it EXISTS and is READABLE in the image provided.\n"
    "\n"
    "  ⚠ Single-sided flat sign is ACCEPTABLE.\n"
    "    Most fire extinguisher signs ARE single-sided flat signs on walls.\n"
    "    If it's visible and readable in the image, it PASSES.\n"
    "\n"
    "  ⚠ Small sign is ACCEPTABLE.\n"
    "    As long as it's legible in the image, size doesn't matter.\n"
    "\n"
    "EXAMPLES:\n"
    "\n"
    "  Example 1: Red 'Fire Extinguisher' sign above the unit, clearly visible and readable\n"
    "    Step 1: YES, extinguisher visible\n"
    "    Step 2: YES, red sign visible above unit\n"
    "    Step 3: YES, text is readable (not blurry, right-side-up)\n"
    "    VERDICT: PASS ✓\n"
    "    FINDING: 'Fire extinguisher has a clearly visible and readable sign above it.'\n"
    "    ACTION: 'No corrective action required.'\n"
    "\n"
    "  Example 2: Fire extinguisher visible, but no sign above or near it\n"
    "    Step 1: YES, extinguisher visible\n"
    "    Step 2: NO, no sign visible\n"
    "    VERDICT: FAIL ✗\n"
    "    FINDING: 'Fire extinguisher is visible but no signage is mounted above or adjacent to it.'\n"
    "    ACTION: 'Install a fire extinguisher sign above or next to the unit.'\n"
    "\n"
    "  Example 3: Sign is present but very faded and illegible\n"
    "    Step 1: YES, extinguisher visible\n"
    "    Step 2: YES, there is a sign shape visible above unit\n"
    "    Step 3: NO, text is too faded to read\n"
    "    VERDICT: FAIL ✗\n"
    "    FINDING: 'A sign is mounted above the extinguisher, but text is faded and illegible.'\n"
    "    ACTION: 'Replace or refresh the faded signage.'\n"
    "\n"
    "  Example 4: No extinguisher visible in image\n"
    "    Step 1: NO, cannot see extinguisher\n"
    "    VERDICT: NEED REVIEW ⚠\n"
    "    FINDING: 'Fire extinguisher is not visible in the image.'\n"
    "    ACTION: 'Please retake with the fire extinguisher clearly visible.'\n"
    "\n"
    "CRITICAL SAFETY CHECKS:\n"
    "  ⚠ If extinguisher is visible, you MUST answer PASS or FAIL.\n"
    "  ⚠ Do NOT use NEED REVIEW when you can see the extinguisher.\n"
    "  ⚠ Sign visible and readable = PASS (even if it's single-sided or at an angle).\n"
    "  ⚠ No sign visible = FAIL.\n"
    "  ⚠ Sign too faded to read = FAIL.\n"
),
    "5": (
        "RULE — SAFETY PIN AND TAMPER SEAL IN PLACE:\n\n"
        "IMPORTANT — THIS IS A WIDE/FAR SHOT:\n"
        "  This checklist item is assessed from a wide or moderate-distance shot of the extinguisher.\n"
        "  You do NOT need to read fine text or see microscopic detail.\n"
        "  You are looking for the PRESENCE or ABSENCE of the pin and seal as visible shapes.\n\n"
        "WHAT TO LOOK FOR:\n"
        "  THE PIN:\n"
        "    A safety pin is a small metal ring, loop, or straight pin that passes THROUGH the\n"
        "    trigger handle/lever at the top of the extinguisher.\n"
        "    From a distance it appears as: a small silver/metal ring, loop, or bar through the handle.\n"
        "    You do NOT need to see it clearly — if any ring/loop shape is visible in the handle area = pin present.\n\n"
        "  THE TAMPER SEAL:\n"
        "    A tamper seal is a plastic tag, colored string, zip-tie, or plastic loop that connects to\n"
        "    or hangs from the safety pin or handle area.\n"
        "    Common colors: yellow, red, green, white, blue, clear plastic.\n"
        "    From a distance it appears as: a small colored string, tag, or plastic loop near the handle top.\n"
        "    You do NOT need to read it — if ANY colored tag/string/loop is visible near handle = seal present.\n\n"
        "DISTANCE-AWARE EVALUATION LOGIC:\n"
        "  WIDE/FAR SHOT (full extinguisher visible, handle area is small in frame):\n"
        "    PASS if: A ring/loop shape OR colored tag/string is visible near the handle top area.\n"
        "    PASS if: The handle area exists and appears to have something through/on it.\n"
        "    FAIL if: The handle area is CLEARLY and COMPLETELY bare — a handle lever with\n"
        "             absolutely nothing through it, no ring, no string, no tag, nothing.\n"
        "    INCONCLUSIVE → PASS: If handle area is too small to assess at all.\n\n"
        "  CLOSE/MEDIUM SHOT (handle area takes up reasonable portion of frame):\n"
        "    PASS if: Pin ring/loop is clearly visible through the handle trigger mechanism.\n"
        "    PASS if: Tamper seal (plastic tag, string, zip-tie) is present near pin/handle.\n"
        "    FAIL if: Handle trigger is clearly visible and the hole through it is empty — no pin.\n"
        "    FAIL if: Pin is present but tamper seal is clearly torn off or absent.\n\n"
        "CRITICAL RULE:\n"
        "  From a wide shot — when in doubt, PASS and recommend close-up if needed.\n"
        "  Only fail if the handle is clearly bare and clearly missing both pin and seal."
    ),
    "6": (
        "RULE — GOOD CLEAN CONDITION, NO VISIBLE DAMAGE:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. The body shows NO visible dents, major scratches, rust spots, corrosion, or physical deformation.\n"
        "  3. The extinguisher appears clean — no heavy dust coating, grease, or grime visible.\n"
        "  4. The hose (if present) appears intact — not cracked, kinked, or detached.\n"
        "FAIL if:\n"
        "  - Visible rust, corrosion, or pitting on the cylinder.\n"
        "  - Dents or physical deformation on the body.\n"
        "  - Heavy dust, grime, or grease coating.\n"
        "  - Hose visibly cracked, detached, or missing.\n"
        "If image is too far away or blurry to assess condition, set pass=false and request a close-up retake."
    ),

 
"7": (
    "RULE — NOZZLE FREE OF BLOCKAGE:\n\n"
    "PHOTO GUIDANCE FOR USERS:\n"
    "For best results, the nozzle TIP should be pointed TOWARD the camera so the opening\n"
    "(the hole at the end) is visible. However, side-view nozzles are also acceptable if\n"
    "the end of the nozzle can be seen.\n\n"
    "STEP 1 — Locate the nozzle in the image.\n"
    "The nozzle is the cylindrical or conical part at the end of the hose.\n"
    "It may be:\n"
    "  - Held in a person's hand\n"
    "  - Hanging freely\n"
    "  - Resting somewhere\n"
    "Look for a small black plastic, brass, or metal cylinder/cone attached to the hose end.\n\n"
    "STEP 2 — Determine what verdict applies:\n\n"
    "PASS if ALL of the following are true:\n"
    "  ✓ A fire extinguisher is visible in the image.\n"
    "  ✓ A hose is connected to the extinguisher.\n"
    "  ✓ The nozzle is visible at the end of the hose (any angle is acceptable).\n"
    "  ✓ The nozzle appears to be its natural material color (black plastic, brass, metal).\n"
    "  ✓ NO bright foreign color (yellow, red, orange, blue, green) is attached to the tip.\n"
    "  ✓ NO tape, wrapping, or debris is visible on the nozzle.\n"
    "  ✓ The nozzle is physically intact (no cracks, breaks, melting).\n"
    "  ✓ The hose is not visibly kinked or crushed.\n\n"
    "FAIL if ANY of the following:\n"
    "  ✗ No fire extinguisher visible.\n"
    "  ✗ Hose is detached from the extinguisher.\n"
    "  ✗ A BRIGHT YELLOW, RED, ORANGE, BLUE, or other contrasting colored cap/plug is\n"
    "    attached to the nozzle.\n"
    "  ✗ Tape, plastic wrap, paper, or visible debris is on the nozzle.\n"
    "  ✗ Nozzle is visibly cracked, broken, or melted.\n"
    "  ✗ Hose is visibly kinked, crushed, or tied.\n\n"
    "NEED REVIEW only if:\n"
    "  ⚠ The nozzle is COMPLETELY not visible anywhere in the image (no hose end shown,\n"
    "    or the entire nozzle is hidden behind something).\n"
    "  ⚠ The image is too blurry to identify any part of the nozzle.\n\n"
    "CRITICAL INSTRUCTIONS:\n\n"
    "1. SIDE VIEW IS ACCEPTABLE:\n"
    "   If you see the SIDE of the nozzle (a cylindrical shape) without seeing directly\n"
    "   into the opening, this is STILL a valid inspection. You can determine obstruction\n"
    "   by checking if any FOREIGN COLORED OBJECT is attached to the nozzle.\n"
    "   - No foreign objects visible on the side = no obstruction = PASS\n"
    "   - Foreign object (yellow cap, tape, etc.) visible = obstruction = FAIL\n\n"
    "2. SMALL NOZZLE IN FRAME IS ACCEPTABLE:\n"
    "   The nozzle does NOT need to fill the frame. If you can identify a nozzle at the\n"
    "   end of the hose, even if it's small, you can make a verdict.\n\n"
    "3. THE 'COLOR TEST' IS YOUR PRIMARY TOOL:\n"
    "   Look at the nozzle overall. What color is it?\n"
    "   - All black/metal/brass with no foreign attachments → PASS\n"
    "   - Has a bright yellow/red/orange/blue/green attachment → FAIL\n"
    "   - Has tape/wrapping → FAIL\n"
    "   - Cannot find the nozzle at all in the image → NEED REVIEW\n\n"
    "4. DO NOT REQUEST RETAKE FOR ANGLE ISSUES:\n"
    "   Side view, angled view, or partial view of the nozzle are all acceptable.\n"
    "   Only request retake (NEED REVIEW) if the nozzle is COMPLETELY hidden from view.\n\n"
    "5. NEVER CONFABULATE:\n"
    "   If you see a yellow cap, say 'yellow cap', not 'clear opening'.\n"
    "   If you see a black nozzle, say 'black nozzle', not 'unclear'.\n"
    "   Describe what you actually see.\n\n"
    "EXAMPLES:\n\n"
    "  Example 1: Black plastic nozzle held sideways in hand, no foreign colors visible.\n"
    "    Reasoning: Nozzle is visible, all black plastic, no foreign attachments.\n"
    "    Verdict: PASS\n\n"
    "  Example 2: Nozzle with a bright yellow cap clearly attached to the tip.\n"
    "    Reasoning: Yellow object is a foreign cap, not part of the nozzle.\n"
    "    Verdict: FAIL — Remove yellow cap.\n\n"
    "  Example 3: Nozzle is in frame but white tape is wrapped around it.\n"
    "    Reasoning: White tape is foreign material covering the nozzle.\n"
    "    Verdict: FAIL — Remove tape.\n\n"
    "  Example 4: No nozzle visible anywhere — only extinguisher body shown, no hose end.\n"
    "    Reasoning: Cannot locate the nozzle in the image.\n"
    "    Verdict: NEED REVIEW — Retake to include nozzle.\n\n"
    "DEFAULT BEHAVIOR:\n"
    "  - If a nozzle is visible and looks all-natural-material with no foreign attachments\n"
    "    → PASS (regardless of viewing angle).\n"
    "  - If a foreign colored object is clearly attached → FAIL.\n"
    "  - Only NEED REVIEW if the nozzle truly cannot be located in the image."
),



 "8": (
    "RULE — PRESSURE GAUGE: IS THE EXTINGUISHER ADEQUATELY CHARGED?\n"
    "\n"
    "YOUR ONLY JOB: Find the pressure indicator and determine if it shows ADEQUATE pressure.\n"
    "\n"
    "════════════════════════════════════════════════\n"
    "SECTION 1 — THE 5 GAUGE TYPES YOU WILL SEE\n"
    "════════════════════════════════════════════════\n"
    "\n"
    "TYPE A — WHITE/GREY FACE DIAL GAUGE (needle pivots from center bottom):\n"
    "  Appearance: White or grey circular face, colored zone arc at top, needle pointing upward\n"
    "  Zones (left to right on the arc): [RED recharge] [GREEN adequate] [RED overcharge]\n"
    "  HOW TO READ: Find the thin needle from the bottom pivot. Where does its TIP touch the arc?\n"
    "    - Tip touches the GREEN band = PASS\n"
    "    - Tip touches RED on left = FAIL (recharge)\n"
    "    - Tip touches RED on right = FAIL (overcharge)\n"
    "  COMMON MISTAKE: The needle on this gauge points UPWARD. Do not say '9 o'clock'\n"
    "  just because the gauge is oriented sideways in the photo.\n"
    "\n"
    "TYPE B — RED-FACE DIAL GAUGE (entire face is red/orange, small green arc at top):\n"
    "  Appearance: Fully red or orange circular face. A small green arc band painted near the\n"
    "              top-center. Labels 'RECHARGE' on left and 'OVERCHARGED' on right.\n"
    "  HOW TO READ — THIS IS CRITICAL:\n"
    "    Step 1: Locate the green arc band. It is painted at the TOP of the dial.\n"
    "    Step 2: Find the needle (thin pointer from center, may be yellow, white, or black).\n"
    "    Step 3: Is the needle tip touching or inside the GREEN arc band?\n"
    "      - Needle tip is ON the green arc band = PASS (even if most of the face looks red)\n"
    "      - Needle tip is LEFT of the green band (toward 'RECHARGE') = FAIL\n"
    "      - Needle tip is RIGHT of the green band (toward 'OVERCHARGED') = FAIL\n"
    "  ⚠ CRITICAL MISTAKE TO AVOID: Do NOT look at where 'RECHARGE' text is printed.\n"
    "    The word 'RECHARGE' is a ZONE LABEL on the left side of the dial.\n"
    "    A needle pointing to 12 o'clock (straight up) on this gauge IS in the green zone.\n"
    "    Do NOT say 'needle near RECHARGE text' unless the needle is actually at the far left.\n"
    "\n"
    "TYPE C — CONCENTRIC RING / DECORATIVE RED-FACE GAUGE (no traditional needle):\n"
    "  Appearance: Red face with decorative concentric rings or circular patterns.\n"
    "              Small green arc painted near the top. May have a silver/grey central hub.\n"
    "              The 'needle' may look like a tiny white dot, notch, or mark on the rings.\n"
    "  HOW TO READ:\n"
    "    Step 1: Look for the green arc at the top of the dial.\n"
    "    Step 2: Is there any indicator (dot, notch, pointer, line) aligned with the green arc?\n"
    "      - Indicator aligned with the green arc at top = PASS\n"
    "      - No indicator visible at all = evaluate by green arc position (if green is top-center\n"
    "        and this is the normal resting position for this gauge type → PASS)\n"
    "    ⚠ DO NOT confuse the decorative concentric rings for a needle pointing left.\n"
    "       The rings are DECORATIVE. They do not move. They do not indicate pressure direction.\n"
    "    ⚠ If you see a green arc at the top and NO evidence the indicator is off-center → PASS.\n"
    "\n"
    "TYPE D — SEMICIRCULAR / ARC GAUGE (half-circle face, needle at bottom center):\n"
    "  Appearance: Semicircular face (like a D on its side). Numbers along the arc.\n"
    "              Colored zone arc at top: red zones at each end, green in middle.\n"
    "  HOW TO READ: Needle pivots from bottom center. Where does it point?\n"
    "    - Needle pointing to center of arc (roughly 12 o'clock from pivot) = GREEN = PASS\n"
    "    - Needle pointing far left (near 0, near left red zone) = FAIL (recharge)\n"
    "    - Needle pointing far right (past max, right red zone) = FAIL (overcharge)\n"
    "  NOTE: If the needle points STRAIGHT DOWN (6 o'clock from pivot) = near zero = FAIL.\n"
    "\n"
    "TYPE E — WINDOW INDICATOR (FireBoss style) or POPUP PIN:\n"
    "  Window: Green color visible in the window = PASS. Red = FAIL.\n"
    "  Pin: Pin flush with body = PASS. Pin raised/protruding = FAIL.\n"
    "\n"
    "════════════════════════════════════════════════\n"
    "SECTION 2 — CRITICAL ANTI-HALLUCINATION RULES\n"
    "════════════════════════════════════════════════\n"
    "\n"
    "RULE 1 — IGNORE PRINTED TEXT FOR ZONE DETERMINATION:\n"
    "  The words 'RECHARGE' and 'OVERCHARGED' are printed ZONE LABELS.\n"
    "  Do NOT determine the needle position by which label it is 'near' or 'below'.\n"
    "  ONLY look at where the needle TIP is physically pointing on the scale.\n"
    "\n"
    "RULE 2 — PRINTED NUMBERS ARE NOT THE NEEDLE:\n"
    "  Numbers like 0, 100, 195, 400, 1345, 2070 are FIXED scale labels.\n"
    "  The number '195' printed on the gauge does NOT mean the needle points there.\n"
    "  Find the actual thin moving pointer. It may be very small in the image.\n"
    "\n"
    "RULE 3 — CONCENTRIC RINGS ARE DECORATIVE:\n"
    "  Some red-face gauges have concentric circular grooves or rings.\n"
    "  These are decoration/machining marks on the gauge housing.\n"
    "  Do NOT interpret curved lines as a needle pointing in a direction.\n"
    "\n"
    "RULE 4 — THE RED FACE MEANS NOTHING:\n"
    "  A gauge with a completely red face is NOT automatically in the recharge zone.\n"
    "  The red face color is the gauge design. Only the needle/indicator position matters.\n"
    "\n"
    "RULE 5 — WHEN NEEDLE IS TRULY NOT VISIBLE (valid only for very small/distant gauges):\n"
    "  If the gauge is visible but the needle is too small to determine position:\n"
    "  → For a red-face gauge: Look at the green arc. If green arc is at top and nothing\n"
    "    clearly shows the indicator is off-center → PASS (adequate pressure is the normal state)\n"
    "  → For a white-face gauge: needle is usually visible; if not → request retake\n"
    "\n"
    "════════════════════════════════════════════════\n"
    "SECTION 3 — PASS / FAIL DECISION\n"
    "════════════════════════════════════════════════\n"
    "\n"
    "PASS if:\n"
    "  - A pressure indicator is visible AND\n"
    "  - The needle/indicator is within the GREEN zone/arc AND\n"
    "  - The gauge face is intact and readable\n"
    "\n"
    "FAIL if:\n"
    "  - No pressure indicator visible at all\n"
    "  - Needle is CLEARLY at the far left red recharge zone (near zero, needle near floor)\n"
    "  - Needle is CLEARLY at the far right red overcharge zone (past maximum)\n"
    "  - Gauge glass is cracked, missing, or completely unreadable\n"
    "\n"
    "WHEN IN DOUBT — GREEN ARC IS VISIBLE AT TOP, NEEDLE NOT CLEARLY OFF-CENTER → PASS\n"
    "Only fail when the needle is unmistakably at one extreme end."
    "- You are mistaking the printed white '195' line or outer white scale lines for the actual moving needle.\n\n"
    "Important:\n"
    "- A red-dominant gauge face is normal and is NOT a fail by itself.\n"
    "- A white, yellow, or black needle can still be PASS if it points in the green zone.\n"
    "- ANTI-HALLUCINATION: The printed white '195' mark and outer printed scale lines are NOT the needle. Do not follow them. If you mistake a printed white scale line for the needle, you will hallucinate a PASS for an overcharged gauge."
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
        "  - Label is covered in dust, grease, or grime.\n"
        "  - Label is partially or fully detached.\n"
        "  - No label is visible on the extinguisher body."
    ),
    "10": (
        "RULE — INSPECTION TAG FRONT SIDE — YEAR GRID AND DATE VERIFICATION (2025 OR LATER):\n"
        "\n"
        "NOTE: This is the FRONT side of the tag. The worker will also capture the BACK side separately.\n"
        "For this image, focus on: year grid, service date, tag attachment, and tag type.\n"
        "\n"
        "PASS if ALL of the following are true:\n"
        "  1. An inspection tag or service card is physically ATTACHED to the extinguisher.\n"
        "  2. The tag shows evidence of service in year 2025 or later.\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "SECTION 1 — HOW TO READ THE YEAR GRID\n"
        "(This is the most commonly misread part)\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "CRITICAL: Year grids on service tags are often printed SIDEWAYS / ROTATED.\n"
        "The entire year column may be rotated 90 degrees on the right edge of the tag.\n"
        "\n"
        "ROTATED GRID — HOW TO READ IT:\n"
        "  When years appear as VERTICAL TEXT on the right edge of the tag:\n"
        "  - The years run BOTTOM TO TOP: 2025 is at the bottom, 2026 above it,\n"
        "    2027 above that, 2028 above that, 2029 at the top.\n"
        "  - The BOTTOM year (2025) may be partially cut off by the image crop.\n"
        "    If the bottommost year label is partially hidden, it is likely 2025.\n"
        "  - Look at the VISIBLE year labels. If you can read 2026, 2027, 2028,\n"
        "    or 2029 → those are all 2025 or later → the tag is current → PASS.\n"
        "\n"
        "⚠ DO NOT say 'grid only shows through 2024' just because:\n"
        "  - The 2025 label is at the bottom and partially cut off, OR\n"
        "  - The text is rotated and hard to read, OR\n"
        "  - You can only clearly read 2026, 2027, 2028, 2029 and not 2025\n"
        "  If you can read ANY year that is 2025 or later → the tag is current.\n"
        "\n"
        "NORMAL (HORIZONTAL) GRID:\n"
        "  Years run left to right or top to bottom in normal reading orientation.\n"
        "  Find the most recent year with any mark (hole, dot, check, ink).\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "SECTION 2 — TAG TYPES AND HOW TO PASS THEM\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "TYPE A — PROFESSIONAL SERVICE CARD (most common real-world tag):\n"
        "  Recognizable by: company name, address, phone, certificate/registration\n"
        "  number, name of licensee, signature line, license number, type-of-work\n"
        "  checkboxes (MAINTENANCE / NEW EXTINGUISHER / SERVICE).\n"
        "\n"
        "  PASS SIGNALS — any ONE of these is enough when combined with attachment:\n"
        "    (a) Year grid shows any mark (hole, dot, tick, check) for 2025 or later\n"
        "    (b) A handwritten SIGNATURE is present on the Signature line\n"
        "    (c) ALL of these fields are filled in: Registration Number + Name + License Number\n"
        "        → A fully completed service card = evidence of professional service\n"
        "    (d) A TYPE OF WORK checkbox (MAINTENANCE / NEW EXTINGUISHER / SERVICE) is ticked\n"
        "        combined with a signature\n"
        "\n"
        "  WHY SIGNALS (b) and (c) MATTER:\n"
        "    A blank, never-used tag has NO filled fields and NO signature.\n"
        "    A completed tag with Registration Number + Licensee Name + License Number\n"
        "    + Signature is PROOF of professional service — these don't get filled by accident.\n"
        "    When the year grid is hard to read due to rotation/cropping, a fully\n"
        "    completed service card MUST be treated as a legitimate recent service record.\n"
        "\n"
        "TYPE B — MONTHLY PUNCH CARD:\n"
        "  Grid of year rows × month columns. Hole punch, ink, checkmark, dot, or\n"
        "  any mark in a 2025+ year cell = PASS.\n"
        "\n"
        "TYPE C — DATE STICKER OR WRITTEN DATE:\n"
        "  Sticker or handwritten/printed date showing 2025 or later = PASS.\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "SECTION 3 — WHAT COUNTS AS A VALID MARK\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "ANY of the following counts as a valid service mark:\n"
        "  ✓ Hole punched through the cell\n"
        "  ✓ Checkbox tick, checkmark, or X\n"
        "  ✓ Any ink mark, pen dot, or pencil mark\n"
        "  ✓ Rubber stamp or ink stamp impression\n"
        "  ✓ Sticker placed in the cell\n"
        "  ✓ Handwritten initials, date, or signature\n"
        "  ✓ ANY visible discoloration or marking in a year cell\n"
        "  ✓ A filled-in checkbox for MAINTENANCE / NEW EXTINGUISHER / SERVICE\n"
        "\n"
        "You do NOT need a hole punch. Any mark is valid.\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "SECTION 4 — PASS / FAIL DECISION\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "PASS if:\n"
        "  Tag is attached AND any ONE of the following:\n"
        "  (a) Year grid has any mark for 2025 or later\n"
        "  (b) Tag is a completed professional service card (registration + name +\n"
        "      license number all filled + signature present)\n"
        "  (c) Written or printed date shows 2025 or later\n"
        "\n"
        "FAIL only if:\n"
        "  - No tag is visible at all, OR\n"
        "  - Tag is completely unreadable, OR\n"
        "  - Tag is present, clearly readable, and conclusively shows only 2024 or\n"
        "    earlier with NO filled professional service fields\n"
        "\n"
        "DEFAULT: When in doubt on a professional service card with filled fields → PASS."
    ),
    "10_back": (
        "RULE — INSPECTION TAG BACK SIDE — INSPECTOR DETAILS VERIFICATION:\n"
        "\n"
        "NOTE: This is the BACK side of the inspection tag. The front side (year grid)\n"
        "has already been captured separately. For this image, focus on: inspector identity\n"
        "and inspection details recorded on the back of the tag.\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "WHAT TO LOOK FOR ON THE BACK\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "The back of the inspection tag typically shows WHO inspected the extinguisher.\n"
        "Look for any combination of the following:\n"
        "\n"
        "  1. INSPECTOR NAME or INITIALS — handwritten or printed name/initials\n"
        "     of the person who performed the monthly inspection.\n"
        "  2. INSPECTION DATE — month/year or full date of the last inspection.\n"
        "     This may appear as handwritten text, a punched month, or a stamped date.\n"
        "  3. COMPANY/SERVICER INFO — name of the inspection company,\n"
        "     license number, or technician ID.\n"
        "  4. MONTHLY GRID (back side) — some tags have a month-by-month grid\n"
        "     on the back where inspectors initial or punch each month.\n"
        "\n"
        "════════════════════════════════════════════════\n"
        "PASS / FAIL DECISION\n"
        "════════════════════════════════════════════════\n"
        "\n"
        "PASS if:\n"
        "  The back of the tag is visible AND at least ONE of the following:\n"
        "  (a) Inspector name or initials are present (handwritten or printed)\n"
        "  (b) An inspection date from 2025 or later is visible\n"
        "  (c) A monthly grid shows marks (initials, holes, ticks) for recent months\n"
        "  (d) Company/servicer information is legible\n"
        "\n"
        "FAIL if:\n"
        "  - The back of the tag is blank (no inspector info at all)\n"
        "  - The back is completely illegible, smeared, torn, or damaged\n"
        "  - No tag back is visible in the image\n"
        "  - The image shows the front side again instead of the back\n"
        "\n"
        "NOTE: The back side is typically less formal than the front. Even minimal\n"
        "handwritten initials count as valid inspector identification.\n"
        "\n"
        "DEFAULT: When initials or any inspector mark is present → PASS."
    ),
}

# ─────────────────────────────────────────────
# Keyword Validation per Checklist Item
# ─────────────────────────────────────────────
VALIDATION_KEYWORDS = {
    "1": ["extinguisher", "yellow tape", "yellow", "tape", "perimeter", "boundary", "50ft", "50 feet", "distance", "inside", "outside", "signage", "sign", "top", "visible", "fire"],
    "2": ["qr", "height", "mounted", "accessible", "code", "marker", "reference", "seated", "decoded"],
    "3": ["blocked", "clear", "accessible", "unobstructed", "path", "obstruction", "box", "pallet"],
    "4": ["sign", "signage", "visible", "above", "marker", "label", "red sign", "pictogram"],
    "5": [
        "pin", "ring", "loop", "bar", "metal", "inserted", "through", "handle",
        "seal", "tamper", "tag", "string", "tie", "zip", "plastic", "colored",
        "yellow", "red", "green", "white", "hanging", "attached", "present",
        "lever", "trigger", "mechanism", "top",
        "visible", "absent", "missing", "bare", "empty", "clear", "confirmed"
    ],
    "6": ["damage", "rust", "dirty", "clean", "wear", "dent", "corrosion", "scratch", "condition", "intact"],
    "7": ["nozzle", "hose", "blocked", "blockage", "obstructed", "clogged", "free", "clear",
          "visible", "intact", "opening", "debris", "cap", "covered",
          "attached", "connected", "extinguisher"],
    # Items 8, 9, 10: keyword validation disabled — enforce_component_checks is authoritative.
    "8": [],
    "9": [],
    "10": [],
    "10_back": [],
}

# ─────────────────────────────────────────────
# AI System Prompts
# ─────────────────────────────────────────────
IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a STRICT fire extinguisher safety inspector verifying images for OSHA compliance.
Your decisions affect worker safety. When in doubt, FAIL — never guess a pass.

═══════════════════════════════════════════════════════════════
GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a condition is met, set pass=false.
Never assume compliance from an unclear image.
═══════════════════════════════════════════════════════════════

CORE EVIDENCE POLICY:
- Judge only what is directly visible in the image.
- Do not infer compliance or failure from dominant background colors.
- For gauges, judge only whether the needle is pointing into the green zone.
- If the required evidence is visible and readable, use it even in a far shot or at an angle.
- If the required evidence is not clearly visible, fail.
- Never guess on safety-critical checks.

STEP 1 — PRESENCE CHECK (non-negotiable):
Confirm whether a physical fire extinguisher is clearly and unambiguously visible.

A real fire extinguisher:
  ✓ Cylindrical pressure vessel (usually red, silver, or yellow)
  ✓ 1–2 feet tall typically
  ✓ Has a nozzle or hose
  ✓ Has a pressure gauge on the body
  ✓ Has a handle and safety pin at the top
  ✓ Has an instruction label on the body

NOT a fire extinguisher:
  ✗ Walls, floors, ceilings, doors
  ✗ Boxes, shelves, pallets, furniture
  ✗ Laptops, phones, tools, cables, pipes
  ✗ People, hands, clothing
  ✗ Empty mounting brackets
  ✗ Water bottles, cans, cylinders that are NOT fire extinguishers
  ✗ Anything you are not 100% certain is a fire extinguisher

If you are not 100% certain → object_detected = "other", pass = false.

STEP 2 — CONDITION CHECK:
Only if Step 1 confirms a fire extinguisher, evaluate the specific checklist item
using the strict visual rule provided. Read the rule carefully — each item has
specific PASS and FAIL criteria. Follow them exactly.

ABSOLUTE RULES:
1. pass=true is ONLY valid when object_detected="fire_extinguisher" AND the condition is clearly met.
2. If object_detected is "other" or "unclear" → pass MUST be false.
3. Do NOT guess. Do NOT infer from context clues not visible in the image.
4. If the required condition is not clearly visible → pass=false.
5. If the image is blurry, dark, at a bad angle, or the item is partially hidden → pass=false.
6. reason MUST describe specifically what you see and why it passes or fails.
7. condition_checked must state exactly what visual condition you verified.
8. worker_message must be actionable — tell the worker what to do next.

Return JSON ONLY. No markdown. No extra text. No explanation outside the JSON.

Schema:
{
  "object_detected": "fire_extinguisher|other|unclear",
  "condition_checked": "<short string naming the condition verified, or 'not_visible'>",
  "pass": true|false,
  "confidence": <float 0.0–1.0>,
  "reason": "<one detailed sentence describing exactly what you see and why it passes or fails>",
  "worker_message": "<short actionable instruction for the worker, under 15 words>",
  "suggested_action": "<corrective action string, or null if passed>"
}
"""

COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a STRICT fire extinguisher component inspector for OSHA compliance.
You are analyzing images of specific components (pressure gauge, label, tag).
Your decisions directly affect worker safety.

═══════════════════════════════════════════════════════════════
GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a condition is met, set pass=false.
═══════════════════════════════════════════════════════════════

CRITICAL — WHAT "target_visible=true" MEANS:
  The component EXISTS somewhere in the image and you can describe what it looks like.
  It does NOT need to fill the frame or be in close-up.

  SET target_visible=true if:
  ✓ The gauge/label/tag is anywhere in the image, even small or at an angle
  ✓ You can describe the component's appearance
  ✓ The component is present but then FAILS its condition check

  SET target_visible=false ONLY if:
  ✗ The component is literally not present anywhere in the image
  ✗ The image shows something completely unrelated
  ✗ You genuinely cannot find the component anywhere after examining the full image

  KEY INSIGHT: target_visible=true and pass=false is a VALID and COMMON outcome.
  Do NOT conflate "I can't confirm it passes" with "I can't see it."

STEP 1 — COMPONENT PRESENCE:
  Is the target component identifiable anywhere in the image?
  If no → target_visible=false, pass=false, stop.
  If yes → target_visible=true, then evaluate its condition.

STEP 2 — CONDITION CHECK:
  Follow the strict visual rule for this checklist item EXACTLY.
  Apply each sub-condition. If any sub-condition fails → pass=false.

STEP 3 — RETURN CHECKS:
  Return every field in the checks object with explicit enum values.
  No nulls. If genuinely unclear → use "unclear" AND set pass=false.

ABSOLUTE RULES:
1. pass=true only when ALL required checks pass their required enum values.
2. "unclear" in any required check field → pass=false. No exceptions.
3. reason must describe what you specifically SEE for the component.
4. confidence: 0.85-1.0 if clearly visible, 0.5-0.85 if visible but challenging,
   0.3-0.5 if barely visible. Set >0 whenever you can see component.

Return JSON ONLY. No markdown. No extra text.

Schema:
{
  "object_detected": "target_component|other|unclear",
  "condition_checked": "<short string naming condition verified or 'not_visible'>",
  "pass": true|false,
  "confidence": <float 0.0–1.0>,
  "reason": "<detailed sentence: what you see for the component and why it passes/fails>",
  "worker_message": "<short actionable instruction, under 15 words>",
  "suggested_action": "<corrective action or null>",
  "checks": {
    "<item_specific_fields_exactly_as_contracted>": "<enum_value>"
  }
}
"""

VOICE_SYSTEM_PROMPT = """
You are a voice assistant for a fire extinguisher inspection app. Workers use voice commands hands-free while inspecting.

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
- If worker says something like "the extinguisher was behind a box" treat it as add_note and put that text in note_text.
- If ambiguous use clarify.
- Never mention helmets, PPE, or unrelated topics.
- If the current checklist item is 7, 8, 9, or 10, give short item-specific guidance about the visible condition:
    - item 7: nozzle or hose must be visible and not blocked, covered, clogged, or obstructed.
    - item 8: pressure gauge must be visible and needle should be in the green zone.
    - item 9: instruction label must be visible, properly aligned, facing outward, and readable.
    - item 10: inspection tag must be photographed from BOTH SIDES (front showing date, back showing inspector name). Two separate photos required.
    Keep the reply short and practical.
"""

def voice_item_hint(item_id: str, image_side: str = "") -> str:
    hints = {
        "7": "Check nozzle and hose. If blocked, covered, clogged, or obstructed, retake after clearing it.",
        "8": "Check pressure gauge. Keep needle visible and in the green zone.",
        "9": "Check instruction label. Keep it aligned, facing outward, and readable.",
        "10": "Check inspection tag. First, capture the FRONT of the tag showing the year and date. Then flip and capture the BACK showing the inspector name or initials.",
    }
    if str(item_id) == "10" and image_side == "back":
        return "Now capture the BACK of the inspection tag. Show the inspector name, initials, or monthly grid."
    return hints.get(str(item_id), "")

# ═══════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════

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
        template = load_checklist("fire-extinguisher", company_key, force_refresh=force_refresh)
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


def get_route(event):
    method = event.get("httpMethod", event.get("requestContext", {}).get("http", {}).get("method", "")).upper()
    path = event.get("path", event.get("rawPath", "")).rstrip("/")
    resource = event.get("resource", "")
    return method, path, resource


def path_endswith(path: str, suffix: str) -> bool:
    return path.rstrip("/").endswith(suffix.rstrip("/"))


def get_query(event, key, default=""):
    return (event.get("queryStringParameters") or {}).get(key, default)


def prepare_image_bytes(image_bytes: bytes, content_type: str = "image/jpeg", mode: str = "") -> Tuple[bytes, str]:
    """
    Resize and compress image for Bedrock.
    mode="component" uses higher resolution for close-up items (8, 9, 10).
    """
    if Image is None:
        return image_bytes, content_type
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")

        max_width = MAX_IMAGE_WIDTH_COMPONENT if mode == "component" else MAX_IMAGE_WIDTH
        quality = MAX_IMAGE_QUALITY_COMPONENT if mode == "component" else MAX_IMAGE_QUALITY

        if img.width > max_width:
            ratio = max_width / float(img.width)
            new_size = (max_width, max(1, int(img.height * ratio)))
            img = img.resize(new_size, Image.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning(f"Image preparation failed: {str(e)}")
        return image_bytes, content_type


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


def deep_copy_checklist(tenant_id="default"):
    return get_checklist_template(tenant_id)


# ═══════════════════════════════════════════════════════════════
# QR CODE UTILITIES (Checklist Item 2 — Height Verification)
# ═══════════════════════════════════════════════════════════════

def generate_qr_code_data(handle_height_cm: float, station_id: str = "", notes: str = "") -> dict:
    """Build the JSON payload to encode in the QR code."""
    return {
        "type": QR_CODE_TYPE_IDENTIFIER,
        "handle_height_cm": round(float(handle_height_cm), 1),
        "station_id": str(station_id or "").strip(),
        "notes": str(notes or "").strip(),
        "generated_at": now_iso(),
    }


def generate_qr_code_image_base64(data: dict) -> Optional[str]:
    """Generate a QR code PNG image as a base64-encoded data URI string."""
    if qrcode is None:
        logger.warning("qrcode library is not installed — cannot generate QR code")
        return None
    try:
        qr = qrcode.QRCode(
            version=None,
            error_correction=ERROR_CORRECT_H if ERROR_CORRECT_H is not None else 2,
            box_size=15,
            border=6,
        )
        qr.add_data(json.dumps(data, separators=(",", ":")))
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf)
        buf.seek(0)
        raw_b64 = base64.b64encode(buf.read()).decode("utf-8")
        return f"data:image/png;base64,{raw_b64}"
    except Exception as e:
        logger.exception(f"QR code generation failed: {e}")
        return None


def parse_qr_data_from_text(raw_text: str) -> Optional[dict]:
    """Parse raw QR text and check if it's a valid OSHA_FE_HEIGHT payload."""
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        cleaned = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict) and parsed.get("type") == QR_CODE_TYPE_IDENTIFIER:
            logger.info(f"[QR] Valid OSHA fire extinguisher height QR data: {parsed}")
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def validate_extinguisher_height(qr_data: dict) -> Tuple[bool, str, str, str]:
    """
    Validate the extinguisher height from decoded QR code data.
    Returns: (passed, finding, action_item, worker_message)
    """
    try:
        handle_height = float(qr_data.get("handle_height_cm", 0))
    except (ValueError, TypeError):
        return (
            False,
            "QR code data is malformed — handle_height_cm is not a valid number.",
            "Re-generate the QR code with correct height data and replace it.",
            "QR code data error. Contact supervisor.",
        )

    station_id = str(qr_data.get("station_id", "")).strip()
    station_label = f" (Station: {station_id})" if station_id else ""

    if handle_height <= 0:
        return (
            False,
            f"QR code has invalid height value: {handle_height} cm{station_label}.",
            "Re-measure and re-generate the QR code with the correct handle height.",
            "QR code has invalid height. Contact supervisor.",
        )

    if handle_height > MAX_ACCESSIBLE_HEIGHT_CM:
        return (
            False,
            f"Extinguisher handle is at {handle_height} cm from floor{station_label} — "
            f"exceeds maximum accessible height of {MAX_ACCESSIBLE_HEIGHT_CM} cm for seated access.",
            f"Lower the extinguisher so the top handle is at or below {MAX_ACCESSIBLE_HEIGHT_CM} cm from the floor.",
            f"Height {handle_height} cm exceeds {MAX_ACCESSIBLE_HEIGHT_CM} cm max. Reposition needed.",
        )

    return (
        True,
        f"Extinguisher handle is at {handle_height} cm from floor{station_label} — "
        f"within accessible range (max {MAX_ACCESSIBLE_HEIGHT_CM} cm). Height check passed.",
        "No corrective action required.",
        f"Height OK — {handle_height} cm (max {MAX_ACCESSIBLE_HEIGHT_CM} cm).",
    )


# ═══════════════════════════════════════════════════════════════
# DYNAMODB HELPERS
# ═══════════════════════════════════════════════════════════════

def load_inspection(inspection_id):
    """Load an inspection record from DynamoDB and convert decimals."""
    result = table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")
    if item:
        return convert_decimals(item)
    return None


def save_inspection(inspection):
    """Save an inspection record back to DynamoDB."""
    table.put_item(Item=sanitize_for_dynamodb(inspection))


def load_inspection_by_session_id(session_id: str):
    """
    Load inspection by session_id.
    First tries the sessions_table for a direct inspection_id lookup (fast),
    then falls back to a scan of the inspection table if needed.
    """
    session_id = str(session_id or "").strip()
    if not session_id:
        return None
    # Fast path: look up session record to get inspection_id directly
    try:
        session_resp = sessions_table.get_item(Key={"session_id": session_id})
        session = session_resp.get("Item")
        if session:
            inspection_id = str(session.get("inspection_id", "")).strip()
            if inspection_id:
                return load_inspection(inspection_id)
    except Exception:
        logger.exception("load_inspection_by_session_id fast-path failed, falling back to scan")
    # Fallback: scan inspection table
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
    """Try loading by inspection_id first, then by session_id."""
    id_value = str(id_value or "").strip()
    if not id_value:
        return None
    insp = load_inspection(id_value)
    if insp:
        return insp
    return load_inspection_by_session_id(id_value)


def find_item(inspection, item_id):
    """Find a checklist item by its ID. Returns (item, category_index, item_index) or (None, -1, -1)."""
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
            iid = int(item.get("id", 0))
            if iid in (11, 12):
                continue
            if not item.get("answer", "").strip() or item.get("blocked_by_wrong_image"):
                return cat_idx, item_idx
    return None


# ─────────────────────────────────────────────
# FIX 1: compute_status — now correctly returns "failed" when any item
# answers "No", matching File 1's production logic exactly.
# ─────────────────────────────────────────────
def compute_status(inspection):
    """
    Calculate inspection status.
    Returns: "in_progress" | "failed" | "passed"
    - "in_progress" if any item has an empty answer
    - "failed"      if all items answered but at least one is "No"
    - "passed"      if all items answered and none are "No"
    """
    items = get_all_items(inspection)
    if any(item.get("answer", "") == "" for item in items):
        return "in_progress"
    if any(item.get("answer") == "No" for item in items):
        return "failed"
    return "passed"


# ─────────────────────────────────────────────
# FIX 2: update_summary_items — now counts from evidence records
# (per-extinguisher) instead of per-checklist-item answers,
# matching File 1's production logic exactly.
# ─────────────────────────────────────────────
def update_summary_items(inspection):
    """
    Auto-calculate items 11 (total extinguishers inspected) and
    12 (total extinguishers compliant) from evidence records.
    Counts actual extinguisher evidence entries, not checklist answers.
    """
    total_inspected = 0
    total_compliant = 0
    for item in get_all_items(inspection):
        iid = int(item.get("id", 0))
        if iid in (11, 12):
            continue
        for ev in item.get("evidence", []):
            if isinstance(ev, dict) and ev.get("is_extinguisher"):
                total_inspected += 1
                if ev.get("is_compliant"):
                    total_compliant += 1

    item11, c11, i11 = find_item(inspection, "11")
    if item11 is not None:
        item11["answer"] = str(total_inspected)
        item11["finding"] = "Auto-calculated from inspected extinguisher images."
        inspection["categories"][c11]["items"][i11] = item11

    item12, c12, i12 = find_item(inspection, "12")
    if item12 is not None:
        item12["answer"] = str(total_compliant)
        item12["finding"] = "Auto-calculated from compliant extinguisher images."
        inspection["categories"][c12]["items"][i12] = item12


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

def checklist_rule_for_item(item_id: str, checklist_item: dict, image_side: str = "") -> str:
    """Return the visual rule for the given item. For item 10, supports front/back."""
    effective_key = str(item_id)
    if effective_key == "10" and image_side == "back":
        effective_key = "10_back"
    return CHECKLIST_VISUAL_RULES.get(effective_key, checklist_item.get("description", ""))


def expected_keywords_for_item(item_id: str) -> List[str]:
    return VALIDATION_KEYWORDS.get(str(item_id), [])


def required_yolo_class_for_item(item_id: str) -> Optional[str]:
    """Return None — YOLO is optional for this deployment.

    Android app uses YOLO optionally; here we explicitly disable
    any hard dependency on a YOLO class so the analyze flow works
    when no YOLO endpoint is configured.
    """
    return None


def item_zoom_hint(item_id: str, image_side: str = "") -> str:
    hints = {
        "7": "Show the full extinguisher with hose attached and nozzle tip clearly visible.",
        "8": "Zoom in on the pressure gauge. Keep the full gauge face and needle visible.",
        "9": "Zoom in on the instruction label. Keep label text facing camera, sharp, and readable.",
        "10": "Zoom in on the FRONT of the inspection tag. Keep year grid, date, and tag edges visible.",
    }
    if str(item_id) == "10" and image_side == "back":
        return "Flip the tag and zoom in on the BACK. Show inspector name, initials, or monthly grid."
    return hints.get(str(item_id), "")


def default_action_for_item(item_id: str) -> str:
    actions = {
        "1": "Ensure a fire extinguisher is installed at the designated location.",
        "2": "Adjust mounting height to seated-accessible range.",
        "3": "Remove obstruction and keep access path clear.",
        "4": "Install or reposition signage to be clearly visible.",
        "5": "Install/replace safety pin and tamper seal.",
        "6": "Clean unit and repair/replace damaged extinguisher.",
        "7": "Clear nozzle/hose blockage and retake close-up image.",
        "8": "Ensure gauge is clear/intact and needle is in green zone.",
        "9": "Align label outward and keep it clean/readable.",
        "10": "Attach intact tag and update with signed monthly details.",
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
    clean_reason = str(reason or "").strip()
    clean_action = str(suggested_action or "").strip()

    if blocked:
        finding = clean_reason or f"Image is not sufficient to verify checklist item {item_id}."
        action_item = clean_action or (item_zoom_hint(item_id) or default_action_for_item(item_id))
        return finding, action_item

    if passed:
        finding = clean_reason or f"Checklist item {item_id} passed. Condition verified: {condition_checked or 'verified'}."
        action_item = clean_action or "No corrective action required."
        return finding, action_item

    finding = clean_reason or f"Checklist item {item_id} failed. Condition not compliant: {condition_checked or 'not_verified'}."
    action_item = clean_action or default_action_for_item(item_id)
    return finding, action_item


def component_check_contract(item_id: str) -> str:
    """Returns the structured check contract for component items 7, 8, 9, 10."""
    contracts = {
        "7": (
            "Return checks with EXACTLY these fields and ONLY these enum values:\n"
            "  extinguisher_body_visible: true|false\n"
            "    true = a fire extinguisher cylindrical body is clearly visible in the image.\n"
            "    false = no extinguisher body visible.\n"
            "  hose_attached_to_extinguisher: true|false\n"
            "    true = the hose is visibly connected/attached to an extinguisher body in this image.\n"
            "    false = the hose is detached, held in isolation, or the extinguisher has no hose visible.\n"
            "  target_visible: true|false  (is nozzle/hose visible from ANY angle?)\n"
            "  nozzle_tip_visible: true|false  (can you see the nozzle tip from this angle?)\n"
            "  external_obstruction: none|tape|cap|cloth|debris|unclear\n"
            "    none = nothing covering the OUTSIDE of the nozzle tip = PASS\n"
            "    tape/cap/cloth/debris = something visibly covering the outside = FAIL\n"
            "    unclear = genuinely cannot tell if the outside is covered or not\n"
            "  hose_condition: clear|kinked|crushed|tied|unclear\n"
            "  nozzle_physical_condition: intact|cracked|melted|broken|unclear\n\n"
            "PASS rule: extinguisher_body_visible=true AND hose_attached_to_extinguisher=true AND "
            "target_visible=true AND nozzle_tip_visible=true AND "
            "external_obstruction=none AND hose_condition=clear AND nozzle_physical_condition=intact.\n\n"
            "CRITICAL RULES:\n"
            "  - If only a standalone hose or nozzle is shown with no extinguisher body → FAIL.\n"
            "  - external_obstruction=none if the outside of the tip is clean and uncovered.\n"
            "  - Shadow inside the opening ≠ blockage. external_obstruction=none in that case.\n"
            "  - A side-angle shot of a clean nozzle attached to an extinguisher body = PASS."
        ),
        "8": (
            "Return checks with EXACTLY these fields and ONLY these enum values.\n\n"
            "The gauge does NOT need to fill the frame. Evaluate it wherever it appears.\n\n"
            "══ MANDATORY PRE-ANALYSIS — DO THIS BEFORE FILLING FIELDS ══\n\n"
            "STEP A: What TYPE of gauge is this?\n"
            "  - White/grey face with colored arc at top and visible needle → TYPE_WHITE\n"
            "  - Red/orange face with green arc at top, labels RECHARGE/OVERCHARGED → TYPE_RED_FACE\n"
            "  - Red/orange face with concentric ring patterns, small green arc at top → TYPE_CONCENTRIC\n"
            "  - Semicircular face (D-shape), numbers along arc, needle from bottom → TYPE_SEMI\n"
            "  - Flat window or popup pin → TYPE_WINDOW_PIN\n\n"
            "STEP B: For TYPE_RED_FACE and TYPE_CONCENTRIC:\n"
            "  - Find the GREEN ARC. It is painted near the TOP of the dial.\n"
            "  - Find the needle or indicator. It may be very thin, yellow, white, or black.\n"
            "  - Is the needle tip ON the green arc? If yes → needle_zone = green.\n"
            "  - ⚠ The words 'RECHARGE' and 'OVERCHARGED' are just LABELS — ignore them for zone.\n"
            "  - ⚠ Concentric rings/grooves are DECORATIVE — not a needle pointing left.\n\n"
            "STEP C: For TYPE_SEMI:\n"
            "  - Needle pivots from bottom center.\n"
            "  - Needle pointing UP (12 o'clock from pivot) = green zone = PASS.\n"
            "  - Needle pointing DOWN (6 o'clock from pivot, near zero) = recharge = FAIL.\n\n"
            "══ FIELDS ══\n\n"
            "  target_visible: true|false\n"
            "    true  = Any pressure indicator is identifiable anywhere in the image.\n"
            "    false = No pressure indicator of any kind exists anywhere in the image.\n\n"
            "  gauge_type: dial|window_indicator|popup_pin|unclear\n"
            "    dial             = any needle/pointer gauge (white face, red face, semicircular,\n"
            "                       concentric ring style — ALL count as dial)\n"
            "    window_indicator = flat window (FireBoss), no needle\n"
            "    popup_pin        = pressure button/pin, no needle\n"
            "    unclear          = cannot determine type at all\n\n"
            "  needle_zone: green|yellow_caution|red_recharge|red_overcharge|unclear\n\n"
            "    DETERMINE BY GAUGE TYPE:\n\n"
            "    For TYPE_RED_FACE (red face, green arc at top, RECHARGE left / OVERCHARGED right):\n"
            "      green        = needle tip is ON or WITHIN the green arc band at the top\n"
            "                     OR needle appears to be pointing toward the top center of the dial\n"
            "      red_recharge = needle is CLEARLY pointing far LEFT, well past the green arc,\n"
            "                     nearly touching or past the 'RECHARGE' label at the left edge\n"
            "      red_overcharge = needle is CLEARLY pointing far RIGHT, past 'OVERCHARGED' label\n"
            "      unclear      = you genuinely cannot find any needle or indicator at all\n"
            "      ⚠ DEFAULT: If green arc is visible at top and needle is NOT clearly far left or far\n"
            "         right → use green. Do NOT use red_recharge just because the face is red.\n\n"
            "    For TYPE_CONCENTRIC (red face with decorative rings, no clear needle):\n"
            "      green        = green arc is visible at top of dial in normal position\n"
            "                     (this is the resting/normal state for this gauge type)\n"
            "      red_recharge = a clear indicator dot/notch/mark is visibly displaced FAR LEFT\n"
            "                     from the green arc\n"
            "      unclear      = cannot determine at all\n"
            "      ⚠ DEFAULT: Green arc visible at top, no displaced indicator → green.\n"
            "      ⚠ DO NOT say red_recharge because you see curved lines (those are decorative rings).\n\n"
            "    For TYPE_WHITE (white/grey face, colored arc at top):\n"
            "      Needle tip on green band = green.\n"
            "      Needle at far left near 0 = red_recharge.\n"
            "      Needle at far right past max = red_overcharge.\n\n"
            "    For TYPE_SEMI (semicircular, numbers along arc):\n"
            "      Needle pointing to center of arc / upward = green.\n"
            "      Needle pointing far left/low or straight down = red_recharge.\n"
            "      Needle pointing far right/past max = red_overcharge.\n\n"
            "    For window/pin:\n"
            "      Green in window or pin flush = green.\n"
            "      Red in window or pin raised = red_recharge.\n\n"
            "  gauge_face_readability: readable|unreadable|unclear\n"
            "    readable = you could determine the gauge type and approximate zone\n\n"
            "  gauge_glass_condition: intact|cracked|missing|unclear\n"
            "    For window/pin indicators with no glass: use intact\n\n"
            "PASS RULE:\n"
            "  target_visible=true AND needle_zone=green\n"
            "  AND gauge_face_readability=readable AND gauge_glass_condition=intact\n"
            "  → pass=true. Any other combination → pass=false.\n\n"
            "CONCRETE EXAMPLES FROM REAL GAUGES:\n"
            "  White semicircular gauge, needle pointing UP to center = needle_zone=green → PASS\n"
            "  White semicircular gauge, needle pointing straight DOWN near 0 = needle_zone=red_recharge → FAIL\n"
            "  Red-face gauge, green arc at top, needle pointing to top center = needle_zone=green → PASS\n"
            "  Red-face gauge, needle visibly far left near floor level = needle_zone=red_recharge → FAIL\n"
            "  Red-face concentric ring gauge, green arc at top, no displaced indicator = needle_zone=green → PASS\n"
            "  Red-face gauge with yellow needle pointing to ~12-1 o'clock green arc = needle_zone=green → PASS\n"
            "  Any gauge with green arc visible at top and needle not clearly displaced = needle_zone=green → PASS\n"
        ),
        "9": (
            "Return checks with EXACTLY these fields and ONLY these enum values:\n"
            "  target_visible: true|false\n"
            "  label_orientation: outward|not_outward|unclear\n"
            "  label_text_readability: readable|illegible|blurry|unclear\n"
            "  label_obstruction: clear|dust_covered|tag_covered|object_covered|unclear\n"
            "  label_physical_condition: intact|damaged|peeled|folded|torn|unclear\n"
            "  label_attachment_state: attached|loose|detached|unclear\n"
            "PASS rule: target_visible=true AND label_orientation=outward AND "
            "label_text_readability=readable AND label_obstruction=clear AND "
            "label_physical_condition=intact AND label_attachment_state=attached.\n"
            "Any other combination → pass=false."
        ),
        "10": (
            "Return checks with EXACTLY these fields and ONLY these enum values.\n\n"
            "══ MANDATORY PRE-ANALYSIS — DO THIS IN ORDER ══\n\n"
            "STEP 1: Is this a PROFESSIONAL SERVICE CARD?\n"
            "  Look for: company name/logo, Certificate of Registration Number field,\n"
            "  'Name of Licensee' field, 'Signature' line, 'License Number' field,\n"
            "  'Type of Work' checkboxes (MAINTENANCE / NEW EXTINGUISHER / SERVICE).\n"
            "  If YES → tag_format = service_card → proceed to STEP 2.\n"
            "  If NO  → tag_format = punch_grid or written_date → skip to STEP 4.\n\n"
            "STEP 2: For SERVICE CARDS — check completed fields:\n"
            "  Is the Registration/Certificate Number field filled in? (any text/number)\n"
            "  Is the Name of Licensee field filled in? (any name)\n"
            "  Is a Signature present on the Signature line? (any handwriting)\n"
            "  Is the License Number field filled in? (any number)\n"
            "  Is a Type of Work checkbox ticked? (MAINTENANCE / NEW / SERVICE)\n"
            "  → Count how many of these 5 signals are present.\n"
            "  → If 3 or more are present → service_card_completed = true\n\n"
            "STEP 3: For SERVICE CARDS — read the year grid:\n"
            "  The year grid is usually a column of years on the RIGHT EDGE of the tag.\n"
            "  ⚠ THE YEARS ARE OFTEN ROTATED 90 DEGREES — read them carefully:\n"
            "    - Years run BOTTOM TO TOP: 2025=bottom, 2026=above, 2027=above, 2028, 2029=top\n"
            "    - The bottom year (2025) may be PARTIALLY CUT OFF by image crop — that is NORMAL\n"
            "    - If you can see 2026, 2027, 2028, or 2029 labels → those ARE 2025+ years\n"
            "    - If the bottommost visible year is 2026 (because 2025 is cut off) → still 2025+\n"
            "  Look for any mark (hole, tick, dot, ink) in any year 2025 or later.\n"
            "  → Write that year in year_identified.\n\n"
            "STEP 4: For PUNCH GRID / WRITTEN DATE tags:\n"
            "  Find the most recent year with any mark. Write it in year_identified.\n\n"
            "══ KEY OVERRIDE RULE ══\n\n"
            "If tag_format=service_card AND service_card_completed=true (3+ fields filled):\n"
            "  → Set year_identified to the most recent year visible in the grid\n"
            "     (even if partially cut off or rotated)\n"
            "  → If year grid is unclear but grid shows labels 2026+ → year_identified='2026'\n"
            "  → recent_year_visible = yes\n"
            "  → pass = true\n"
            "REASON: A professional service card with all fields completed by a licensed\n"
            "technician IS proof of legitimate recent service. It cannot be from 2024 if\n"
            "it has a current license number and professional service company details.\n\n"
            "══ FIELDS ══\n\n"
            "  target_visible: true|false\n"
            "    true  = An inspection tag or service card is present in the image.\n"
            "    false = No tag of any kind visible.\n\n"
            "  tag_physically_attached: attached|detached|unclear\n"
            "    attached = hanging, wired, zip-tied, or on the extinguisher\n\n"
            "  tag_format: service_card|punch_grid|written_date|sticker|unclear\n"
            "    service_card = has company info, signature line, license number, work type\n\n"
            "  service_card_completed: true|false|not_applicable\n"
            "    true           = service card with 3+ of: reg.number + name + signature +\n"
            "                     license number + work type checkbox all filled in\n"
            "    false          = service card but mostly blank/unfilled\n"
            "    not_applicable = not a service card (punch_grid, written_date, etc.)\n\n"
            "  year_identified: <most recent year with evidence, e.g. '2025','2026','unclear','none'>\n"
            "    For SERVICE CARDS:\n"
            "      Read the rotated year grid on the right edge of the tag.\n"
            "      Years go bottom-to-top: 2025(bottom, may be cut off), 2026, 2027, 2028, 2029.\n"
            "      If you can see '2026' or higher anywhere in the grid → year_identified='2026' (or higher).\n"
            "      If grid is cut off but service_card_completed=true → year_identified='2026'.\n"
            "      DO NOT say 'none' or 'unclear' for a completed service card — it has a year.\n"
            "    For PUNCH GRIDS: find most recent year with any mark.\n"
            "    For WRITTEN/STICKER: read the year from the date.\n"
            "    'unclear' = tag present but genuinely no year readable anywhere.\n"
            "    'none' = ONLY if entire tag is clearly visible with NO 2025+ marks at all.\n\n"
            "  recent_year_visible: yes|no|unclear\n"
            "    yes     = year_identified is 2025 or later\n"
            "    no      = year_identified is before 2025 or ='none'\n"
            "    unclear = year_identified='unclear'\n"
            "    OVERRIDE: if service_card_completed=true → recent_year_visible=yes\n\n"
            "PASS RULE:\n"
            "  target_visible=true AND tag_physically_attached=attached\n"
            "  AND (recent_year_visible=yes OR service_card_completed=true)\n"
            "  → pass=true.\n\n"
            "CONCRETE EXAMPLE — THE EXACT TAG THAT WAS FAILING:\n"
            "  Tag: Elite Extinguisher Services service card\n"
            "  Visible: Company name, ECR-1751167 registration, BILLY MORRIS licensee,\n"
            "           signature present, FEL-A-2030850 license number, MAINTENANCE ticked\n"
            "  Year grid: rotated vertical on right edge, 2026 visible, 2025 partially cut off\n"
            "  CORRECT analysis:\n"
            "    target_visible = true\n"
            "    tag_physically_attached = attached\n"
            "    tag_format = service_card\n"
            "    service_card_completed = true  (reg.number + name + signature + license + work type)\n"
            "    year_identified = '2026'  (2026 label is visible in the rotated year grid)\n"
            "    recent_year_visible = yes\n"
            "    pass = true\n"
            "  WRONG analysis (what was happening before):\n"
            "    'grid shows years only through 2024' ← WRONG, 2025 was just cut off at bottom\n"
            "    'No evidence of 2025 or later' ← WRONG, 2026 is clearly in the grid\n\n"
            "MORE EXAMPLES:\n"
            "  Service card, all fields filled, 2026 in rotated grid → pass=true\n"
            "  Service card, all fields filled, year grid partially cut off → pass=true (completed card)\n"
            "  Punch card, 2025 row has hole punch → pass=true\n"
            "  Punch card, 2026 row has ink dot → pass=true\n"
            "  Tag only has marks through 2024, no professional fields → pass=false\n"
            "  No tag visible → pass=false\n"
        ),
        "10_back": (
            "Return checks with EXACTLY these fields and ONLY these enum values.\n\n"
            "NOTE: This is the BACK side of the inspection tag.\n"
            "The front side (year grid) has already been captured separately.\n\n"
            "══ FIELDS ══\n\n"
            "  target_visible: true|false\n"
            "    true  = The back of an inspection tag is visible in the image.\n"
            "    false = No tag back visible, or front side shown again.\n\n"
            "  inspector_info_present: yes|no|unclear\n"
            "    yes     = At least ONE of: inspector name/initials, inspection date,\n"
            "              company/servicer info, or monthly grid with marks is visible.\n"
            "    no      = Tag back is completely blank with no inspector info at all.\n"
            "    unclear = Tag back is too blurry/damaged to determine.\n\n"
            "  inspector_name_or_initials: present|absent|unclear\n"
            "    present = Handwritten or printed name/initials of inspector visible.\n"
            "    absent  = No name or initials visible on back.\n"
            "    unclear = Cannot determine due to image quality.\n\n"
            "  inspection_date_visible: yes|no|unclear\n"
            "    yes = A date (month/year or full date) from 2025 or later is visible.\n"
            "    no  = No date visible, or date is from before 2025.\n"
            "    unclear = Date field is present but illegible.\n\n"
            "  back_readability: readable|illegible|unclear\n"
            "    readable  = Can identify at least some inspector information.\n"
            "    illegible = Back is smeared, torn, or too damaged to read.\n"
            "    unclear   = Cannot determine.\n\n"
            "PASS RULE:\n"
            "  target_visible=true AND inspector_info_present=yes\n"
            "  AND back_readability=readable\n"
            "  → pass=true.\n\n"
            "EXAMPLES:\n"
            "  Handwritten initials 'JD' on back → pass=true\n"
            "  Inspection date '02/2025' written on back → pass=true\n"
            "  Monthly grid with initials in recent months → pass=true\n"
            "  Company name stamped on back → pass=true\n"
            "  Completely blank back → pass=false\n"
            "  Front side shown again instead of back → pass=false\n"
        ),
    }
    
    # Support "10_back" as a contract lookup key (for when image_side="back")
    if str(item_id) == "10_back":
        return contracts.get("10_back", contracts.get("10", "Return checks object for requested component with explicit enum states."))
    
    return contracts.get(str(item_id), "Return checks object for requested component with explicit enum states.")


def enforce_component_checks(
    item_id: str,
    analysis: dict,
    passed: bool,
    condition_checked: str,
    reason: str,
    worker_message: str,
    suggested_action: Optional[str],
    image_side: str = "",
) -> Tuple[bool, str, str, str, Optional[str]]:
    """
    Authoritative pass/fail enforcement for component items 7, 8, 9, 10.
    Reads the structured 'checks' dict returned by Claude and applies
    per-item rules regardless of what Claude set for 'pass'.
    """
    checks = analysis.get("checks") if isinstance(analysis, dict) else None
    checks = checks if isinstance(checks, dict) else {}

    def _state(key: str) -> str:
        return str(checks.get(key, "unclear")).strip().lower().replace(" ", "_")

    def _bool(key: str) -> Optional[bool]:
        v = checks.get(key)
        if isinstance(v, bool):
            return v
        t = str(v).strip().lower()
        if t in {"true", "yes"}:
            return True
        if t in {"false", "no"}:
            return False
        return None

    def force_fail(checked, fail_reason, msg, action) -> Tuple[bool, str, str, str, Optional[str]]:
        analysis["pass"] = False
        return False, checked, fail_reason, msg, action

    # Gate 1: target must be visible
    target_visible = _bool("target_visible")
    if target_visible is not True:
        return force_fail(
            "target_not_visible",
            reason or "Target component is not clearly visible in the image.",
            "Move closer. Keep the target component fully visible and in focus.",
            "Retake close-up image with target component fully visible and in sharp focus.",
        )

    item = str(item_id)

    # ── Item 7: Nozzle ──────────────────────────────────────────────────────
    if item == "7":
        extinguisher_body_visible     = _bool("extinguisher_body_visible")
        hose_attached_to_extinguisher = _bool("hose_attached_to_extinguisher")
        nozzle_tip_visible            = _bool("nozzle_tip_visible")
        external_obstruction          = _state("external_obstruction")
        hose_condition                = _state("hose_condition")
        nozzle_physical_condition     = _state("nozzle_physical_condition")

        if extinguisher_body_visible is not True:
            return force_fail(
                "no_extinguisher_body_visible",
                reason or "No fire extinguisher body is visible in the image.",
                "Point camera at the extinguisher. Hose must be attached and visible.",
                "Retake image showing the hose/nozzle attached to the fire extinguisher body.",
            )

        if hose_attached_to_extinguisher is not True:
            return force_fail(
                "hose_not_attached_to_extinguisher",
                reason or "The hose or nozzle appears detached from the fire extinguisher body.",
                "Reattach hose to extinguisher. Retake image showing it connected.",
                "Hose is detached or shown in isolation. Reconnect hose to extinguisher and retake.",
            )

        fail_conditions = []
        if nozzle_tip_visible is not True:
            fail_conditions.append("nozzle tip is not visible in the image")
        if external_obstruction not in ("none", ""):
            fail_conditions.append(f"external obstruction detected: {external_obstruction}")
        if hose_condition not in ("clear", ""):
            fail_conditions.append(f"hose condition: {hose_condition}")
        if nozzle_physical_condition != "intact":
            fail_conditions.append(f"nozzle physical condition: {nozzle_physical_condition}")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "external obstruction" in detail:
                msg    = "Nozzle tip is covered or blocked. Remove obstruction and retake."
                action = "Remove tape, cap, or any external material from nozzle tip and retake image."
            elif "hose" in detail:
                msg    = "Hose is kinked or blocked. Straighten hose and retake."
                action = "Straighten the hose so it is unobstructed and retake close-up."
            elif "physical condition" in detail:
                msg    = "Nozzle is physically damaged. Replace or service the extinguisher."
                action = "Nozzle is cracked or broken — extinguisher needs servicing."
            else:
                msg    = "Nozzle not clearly visible. Retake closer image of nozzle/hose tip."
                action = "Retake image with nozzle/hose tip clearly visible in frame."
            return force_fail("nozzle_failed", reason or f"Nozzle check failed: {detail}.", msg, action)

        analysis["pass"] = True
        return True, (condition_checked or "nozzle_tip_clear_and_intact"), reason, worker_message, suggested_action

    # ── Item 8: Pressure Gauge ─────────────────────────────────────────────
    if item == "8":
        needle_zone            = _state("needle_zone")
        gauge_face_readability = _state("gauge_face_readability")
        gauge_glass_condition  = _state("gauge_glass_condition")
        gauge_type             = _state("gauge_type")
        target_visible_raw     = _bool("target_visible")
        confidence_val         = float(analysis.get("confidence", 0.0) or 0.0)

        # Self-correction layers: if Claude evaluated the gauge but set target_visible=false,
        # correct it based on signal evidence.
        KNOWN_GAUGE_TYPES = {
            "dial", "window_indicator", "popup_pin",
            "full_circle_needle", "semicircular_bottom_pivot",
            "red_face_recharge_dial", "fireboss_window", "other_indicator", "needle_dial"
        }
        if target_visible_raw is not True and needle_zone in ("green", "yellow_caution", "red_recharge", "red_overcharge"):
            logger.warning(f"[ITEM8] target_visible=false but needle_zone={needle_zone} — correcting to true.")
            checks["target_visible"] = True
            target_visible_raw = True

        if target_visible_raw is not True and gauge_type in KNOWN_GAUGE_TYPES:
            logger.warning(f"[ITEM8] target_visible=false but gauge_type={gauge_type} — correcting to true.")
            checks["target_visible"] = True
            target_visible_raw = True

        if target_visible_raw is not True and gauge_face_readability == "readable":
            logger.warning(f"[ITEM8] target_visible=false but gauge_face_readability=readable — correcting to true.")
            checks["target_visible"] = True
            target_visible_raw = True

        if target_visible_raw is not True and confidence_val > 0.0:
            logger.warning(f"[ITEM8] target_visible=false but confidence={confidence_val:.2f} > 0 — correcting to true.")
            checks["target_visible"] = True
            target_visible_raw = True

        if target_visible_raw is not True:
            return force_fail(
                "target_not_visible",
                reason or "No pressure gauge or pressure indicator is visible in the image.",
                "Zoom in on the pressure gauge so it fills the frame clearly.",
                "Retake close-up image with the pressure gauge fully visible and in focus.",
            )

        # Zone resolution from trace signals (backward compatibility)
        needle_trace      = str(checks.get("needle_trace_description", "")).lower()
        needle_color_seen = str(checks.get("needle_color_zone_seen", "")).lower()
        combined_trace    = needle_trace + " " + needle_color_seen

        overcharge_signals = ["overcharg", "past max", "far right", "beyond max", "right side", "too high", "past normal", "past maximum", "past green", "beyond green"]
        recharge_signals   = ["recharge", "far left", "near zero", "low pressure", "left side", "too low", "empty side"]
        green_signals      = ["green zone", "green area", "green arc", "green background", "center zone", "middle", "normal range", "between recharge", "between the labels", "straight up", "upward", "center band", "center top", "center of scale"]

        if needle_zone == "unclear":
            if any(s in combined_trace for s in overcharge_signals):
                checks["needle_zone"] = "red_overcharge"; needle_zone = "red_overcharge"
            elif any(s in combined_trace for s in recharge_signals):
                checks["needle_zone"] = "red_recharge"; needle_zone = "red_recharge"
            elif any(s in combined_trace for s in green_signals):
                checks["needle_zone"] = "green"; needle_zone = "green"

        if needle_zone == "unclear" and reason:
            reason_lower = reason.lower()
            if any(s in reason_lower for s in green_signals):
                checks["needle_zone"] = "green"; needle_zone = "green"
            elif any(s in reason_lower for s in recharge_signals):
                checks["needle_zone"] = "red_recharge"; needle_zone = "red_recharge"
            elif any(s in reason_lower for s in overcharge_signals):
                checks["needle_zone"] = "red_overcharge"; needle_zone = "red_overcharge"

        # Correct red_recharge when all evidence points to green
        if (
            needle_zone == "red_recharge"
            and gauge_type in ("dial", "red_face_recharge_dial", "full_circle_needle", "needle_dial", "semicircular_bottom_pivot")
            and not any(s in combined_trace for s in recharge_signals)
            and not any(s in combined_trace for s in overcharge_signals)
            and any(s in combined_trace for s in green_signals)
        ):
            logger.warning("[ITEM8] Correcting red_recharge → green. No recharge/overcharge signals, green signals present.")
            checks["needle_zone"] = "green"; needle_zone = "green"

        fail_conditions = []
        if needle_zone != "green":
            if needle_zone in ("red_recharge", "red_left"):
                fail_conditions.append("pressure too LOW — recharge needed")
            elif needle_zone in ("red_overcharge", "red_right"):
                fail_conditions.append("pressure too HIGH — overcharged")
            elif needle_zone in ("yellow_caution", "yellow", "orange"):
                fail_conditions.append("pressure in CAUTION zone — marginal, service soon")
            else:
                fail_conditions.append(f"pressure zone '{needle_zone}' not confirmed adequate")

        if gauge_face_readability not in ("readable", ""):
            fail_conditions.append(f"gauge readability={gauge_face_readability}")
        if gauge_glass_condition not in ("intact", ""):
            fail_conditions.append(f"gauge glass={gauge_glass_condition}")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "LOW" in detail or "recharge" in detail.lower():
                msg = "Pressure too low. Remove from service immediately."
                action = "Extinguisher needs recharging — remove from service and replace."
            elif "HIGH" in detail or "overcharg" in detail.lower():
                msg = "Pressure too high. Remove from service for inspection."
                action = "Extinguisher is overcharged — remove from service and inspect."
            elif "CAUTION" in detail or "marginal" in detail:
                msg = "Pressure marginal. Schedule servicing soon."
                action = "Pressure in caution zone — schedule servicing before it drops further."
            elif "glass" in detail or "readability" in detail:
                msg = "Gauge is damaged or unreadable. Service the extinguisher."
                action = "Gauge damaged or unreadable — extinguisher needs servicing."
            else:
                msg = "Gauge unreadable. Retake a clearer close-up."
                action = "Retake clear close-up of gauge with full face and needle visible."
            return force_fail("gauge_failed", reason or f"Pressure gauge failed: {detail}.", msg, action)

        analysis["pass"] = True
        return True, (condition_checked or "gauge_pressure_adequate"), reason, worker_message, suggested_action

    # ── Item 9: Instruction Label ──────────────────────────────────────────
    if item == "9":
        label_orientation        = _state("label_orientation")
        label_text_readability   = _state("label_text_readability")
        label_obstruction        = _state("label_obstruction")
        label_physical_condition = _state("label_physical_condition")
        label_attachment_state   = _state("label_attachment_state")

        fail_conditions = []
        if label_orientation != "outward":
            fail_conditions.append(f"label_orientation={label_orientation}")
        if label_text_readability != "readable":
            fail_conditions.append(f"label_text_readability={label_text_readability}")
        if label_obstruction != "clear":
            fail_conditions.append(f"label_obstruction={label_obstruction}")
        if label_physical_condition != "intact":
            fail_conditions.append(f"label_physical_condition={label_physical_condition}")
        if label_attachment_state != "attached":
            fail_conditions.append(f"label_attachment_state={label_attachment_state}")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            return force_fail(
                "label_failed",
                reason or f"Instruction label failed: {detail}.",
                "Label is not readable or properly positioned. Fix and retake.",
                "Clean label, rotate to face outward, and retake clear image.",
            )
        analysis["pass"] = True
        return True, (condition_checked or "label_outward_and_readable"), reason, worker_message, suggested_action

    # ── Item 10: Inspection Tag (with front/back support) ──────────────────
    if item == "10":
        # Determine if this is front or back analysis
        # FIX 4b: Use image_side from request body; fall back to AI analysis for backward compat
        _image_side = image_side if image_side else str(analysis.get("_image_side", "front")).strip().lower()
        
        if _image_side == "back":
            # ── Back side enforcement ──────────────────────────────────────
            tag_side_shown = _state("tag_side_shown")
            inspector_info = _bool("inspector_info_present")
            back_legibility = _state("back_legibility")
            
            # If they captured the front side again, ask for the back
            if tag_side_shown == "front":
                return force_fail(
                    "wrong_side_captured",
                    reason or "Image shows the FRONT of the tag again. Need the BACK side.",
                    "Flip the tag over and capture the BACK side.",
                    "Flip the tag and retake showing the back with inspector details.",
                )
            
            fail_conditions = []
            if inspector_info is not True:
                fail_conditions.append("no inspector name, initials, or identification visible on back")
            if back_legibility not in ("readable", ""):
                fail_conditions.append(f"back legibility: {back_legibility}")
            
            if fail_conditions:
                detail = "; ".join(fail_conditions)
                if "inspector" in detail or "initials" in detail:
                    msg = "No inspector info on tag back. Add inspector details and retake."
                    action = "Back of tag must show inspector name or initials. Retake if blank."
                else:
                    msg = "Tag back is not readable. Clean or replace tag and retake."
                    action = "Ensure back of tag is legible with inspector details visible."
                return force_fail("tag_back_failed", reason or f"Tag back verification failed: {detail}.", msg, action)
            
            analysis["pass"] = True
            return True, (condition_checked or "tag_back_inspector_verified"), reason, worker_message, suggested_action
        
        # ── Front side enforcement (original logic) ───────────────────────
        tag_physically_attached = _state("tag_physically_attached")
        recent_year_visible     = _state("recent_year_visible")
        year_identified         = _state("year_identified")
        service_card_completed  = _state("service_card_completed")

        # Override: completed professional service card = auto-pass
        if tag_physically_attached == "attached" and service_card_completed == "true":
            confirmed_old_year = False
            if year_identified and year_identified not in ("none", "unclear", ""):
                try:
                    identified_yr = int(re.sub(r"[^0-9]", "", str(year_identified)))
                    if 1900 < identified_yr < 2025:
                        confirmed_old_year = True
                except (ValueError, TypeError):
                    pass
            if not confirmed_old_year:
                analysis["pass"] = True
                return True, (condition_checked or "tag_front_attached_completed_service_card"), reason, worker_message, suggested_action

        fail_conditions = []
        if tag_physically_attached != "attached":
            fail_conditions.append("tag is NOT attached to the extinguisher")
        if recent_year_visible != "yes":
            fail_conditions.append("no year from 2025 onwards is visible on the tag")

        # Override: Claude said yes but identified a year before 2025
        if recent_year_visible == "yes" and year_identified and year_identified not in ("none", "unclear", ""):
            try:
                identified_year = int(re.sub(r'[^0-9]', '', str(year_identified)))
                if identified_year > 0 and identified_year < 2025:
                    fail_conditions.append(f"year identified ({identified_year}) is before 2025")
                    recent_year_visible = "no"
            except (ValueError, TypeError):
                pass

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "NOT attached" in detail:
                msg    = "Tag is missing or detached. Attach a new inspection tag."
                action = "Attach a new inspection tag with current date and initials."
            elif "2025" in detail or "year" in detail.lower():
                msg    = "Inspection tag does not show a 2025 or later date. Update and retake."
                action = "Update tag with current year and initials. Retake close-up of the tag."
            else:
                msg    = "Inspection tag failed condition check. Fix and retake."
                action = "Replace or update inspection tag with current signed monthly details."
            return force_fail("tag_front_failed", reason or f"Inspection tag front failed: {detail}.", msg, action)

        analysis["pass"] = True
        return True, (condition_checked or "tag_front_attached_legible_dated"), reason, worker_message, suggested_action

    # ── Item 10_back: Inspection Tag Back Side (legacy support) ────────────
    # Note: This is kept for backward compatibility if item_id="10_back" is explicitly passed
    # The preferred approach is to use item_id="10" with image_side="back" parameter
    if item == "10_back":
        inspector_info_present = _state("inspector_info_present")
        back_readability       = _state("back_readability")

        fail_conditions = []
        if inspector_info_present != "yes" and inspector_info_present is not True:
            fail_conditions.append("no inspector information visible on tag back")
        if back_readability not in ("readable", ""):
            fail_conditions.append(f"back readability: {back_readability}")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "no inspector information" in detail:
                msg    = "Tag back is blank or missing inspector details. Add inspector info and retake."
                action = "Write inspector name/initials and date on tag back, then retake close-up."
            elif "illegible" in detail:
                msg    = "Tag back is too damaged or smeared to read. Replace tag and retake."
                action = "Replace inspection tag with a clean one showing inspector details on back."
            else:
                msg    = "Tag back failed condition check. Fix and retake."
                action = "Ensure inspector name/initials are visible on tag back and retake clear image."
            return force_fail("tag_back_failed", reason or f"Inspection tag back failed: {detail}.", msg, action)

        analysis["pass"] = True
        return True, (condition_checked or "tag_back_inspector_info_visible"), reason, worker_message, suggested_action

    # Fallback
    return passed, condition_checked, reason, worker_message, suggested_action


# ═══════════════════════════════════════════════════════════════
# BEDROCK / CLAUDE INTEGRATION
# ═══════════════════════════════════════════════════════════════

def invoke_claude_json(system_prompt, user_text, image_bytes=None, media_type="image/jpeg", model_id=None, max_tokens=220):
    """Call Claude via Bedrock. Returns parsed JSON dict."""
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

    # ─────────────────────────────────────────────
    # FIX 6: Guard against missing AWS_LAMBDA_FUNCTION_NAME env var.
    # Without this guard a missing env var causes an unhandled exception
    # inside lambda_client.invoke, crashing the request with a 500.
    # ─────────────────────────────────────────────
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
        return build_response(500, {
            "error": "Failed to queue async analysis job. Please retry.",
            "job_id": job_id,
            "job_status": "failed",
        })

    return build_response(202, {
        "job_id": job_id,
        "job_status": "pending",
        "message": "Analysis started. Poll GET /fire-extinguisher/analyze/status/{job_id}",
    })


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
    return build_response(200, {
        "job_id":     job.get("session_id", job_id),
        "job_status": job.get("job_status", "unknown"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "error":      job.get("error"),
        "result":     result,
    })


# ═══════════════════════════════════════════════════════════════
# QR CODE GENERATION ENDPOINT
# ═══════════════════════════════════════════════════════════════

def generate_height_qr(event):
    """Generate a QR code image for fire extinguisher height verification."""
    body = parse_body(event) if (event.get("httpMethod") or "").upper() == "POST" else {}
    params = event.get("queryStringParameters") or {}

    raw_height = body.get("handle_height_cm") or params.get("handle_height_cm")
    station_id = str(body.get("station_id") or params.get("station_id") or "").strip()
    notes = str(body.get("notes") or params.get("notes") or "").strip()

    if raw_height is None:
        return build_response(400, {"error": "handle_height_cm is required"})
    try:
        handle_height_cm = float(raw_height)
    except (ValueError, TypeError):
        return build_response(400, {"error": "handle_height_cm must be a number"})
    if handle_height_cm <= 0 or handle_height_cm > 300:
        return build_response(400, {"error": "handle_height_cm must be between 1 and 300"})

    data = generate_qr_code_data(handle_height_cm, station_id, notes)
    within_limit = handle_height_cm <= MAX_ACCESSIBLE_HEIGHT_CM

    response_body = {
        "qr_data": data,
        "handle_height_cm": handle_height_cm,
        "station_id": station_id,
        "max_accessible_height_cm": MAX_ACCESSIBLE_HEIGHT_CM,
        "within_osha_limit": within_limit,
        "warning": None if within_limit else (
            f"Height {handle_height_cm} cm exceeds OSHA limit of "
            f"{MAX_ACCESSIBLE_HEIGHT_CM} cm. This station will fail inspection."
        ),
    }

    qr_b64 = generate_qr_code_image_base64(data)
    if qr_b64:
        response_body["qr_image_base64"] = qr_b64
    else:
        response_body["qr_image_base64"] = None
        response_body["qr_note"] = (
            "qrcode library not installed in this environment. "
            "Use the qr_data JSON to generate the QR code on the frontend."
        )

    return build_response(200, response_body)


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
            key = file_key
            if key.startswith("s3://"):
                parts = key.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ""
            obj = s3.get_object(Bucket=bucket, Key=key)
            image_bytes = obj["Body"].read()
            content_type = obj.get("ContentType", "image/jpeg")
            return image_bytes, content_type
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(f"S3 key not found: {file_key}")
            raise
        except Exception:
            raise

    return None, "image/jpeg"


# ═══════════════════════════════════════════════════════════════
# API 1: GET /fire-extinguisher-inspection/checklist
# ═══════════════════════════════════════════════════════════════
def get_checklist(event):
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key, force_refresh=True)
    if filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return build_response(200, template)


# ═══════════════════════════════════════════════════════════════
# API 2: POST /fire-extinguisher-inspection
# ═══════════════════════════════════════════════════════════════
def create_inspection(event):
    """
    Creates a new fire extinguisher inspection record linked to an existing session.
    If an inspection_id is supplied and a record already exists, merges categories
    instead of overwriting — preserving previously captured answers and evidence.
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    session_id    = body.get("session_id", "").strip()
    inspection_id = body.get("inspection_id", "").strip()
    team          = body.get("team", [])
    general_results = body.get("general_results", [])
    notes         = body.get("notes", "").strip() if isinstance(body.get("notes", ""), str) else ""
    categories    = body.get("categories", [])

    if not session_id and not inspection_id:
        return build_response(400, {"error": "session_id or inspection_id is required"})
    if len(notes) > 5000:
        return build_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return build_response(400, {"error": "general_results must be a list"})
    if not categories or not isinstance(categories, list):
        return build_response(400, {"error": "categories must be a non-empty list"})

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

    # Resolve session metadata
    auditor_name  = ""
    facility_area = ""
    date_of_audit = ""
    location      = ""
    station       = ""

    if session_id:
        session_resp = sessions_table.get_item(Key={"session_id": session_id})
        session = session_resp.get("Item")
        if session:
            auditor_name  = str(session.get("auditor_name",  body.get("auditor_name",  ""))).strip()
            facility_area = str(session.get("facility_area", body.get("facility_area", ""))).strip()
            date_of_audit = str(session.get("date_of_audit", body.get("date_of_audit", ""))).strip()
            location      = str(session.get("location",      body.get("location",      ""))).strip()
            station       = str(session.get("station",       body.get("station",       ""))).strip()
            station_id    = str(session.get("station_id",    body.get("station_id",    ""))).strip()
            # If session holds an inspection_id and none was provided, use it
            if not inspection_id:
                inspection_id = str(session.get("inspection_id", "")).strip()
        else:
            # Session not found — fall back to body fields if provided
            auditor_name  = str(body.get("auditor_name",  "")).strip()
            facility_area = str(body.get("facility_area", "")).strip()
            date_of_audit = str(body.get("date_of_audit", "")).strip()
            location      = str(body.get("location",      "")).strip()
            station       = str(body.get("station",       "")).strip()
            station_id    = str(body.get("station_id",    "")).strip()
    else:
        auditor_name  = str(body.get("auditor_name",  "")).strip()
        facility_area = str(body.get("facility_area", "")).strip()
        date_of_audit = str(body.get("date_of_audit", "")).strip()
        location      = str(body.get("location",      "")).strip()
        station       = str(body.get("station",       "")).strip()
        station_id    = str(body.get("station_id",    "")).strip()

    # ─────────────────────────────────────────────
    # FIX 4: Merge with existing inspection instead of always creating new.
    # If an inspection_id is known, load existing record and merge categories
    # so that previously captured answers and evidence are not overwritten.
    # ─────────────────────────────────────────────
    existing = load_inspection(inspection_id) if inspection_id else None

    if existing:
        # Merge incoming categories into existing
        merged_cats = merge_categories(
            copy.deepcopy(existing.get("categories", [])),
            categories,
        )
        # Merge general_results
        existing_gr = existing.get("general_results", [])
        if isinstance(general_results, list) and general_results:
            merged_gr = []
            for idx, result_item in enumerate(general_results):
                if not isinstance(result_item, dict):
                    continue
                base = existing_gr[idx] if idx < len(existing_gr) and isinstance(existing_gr[idx], dict) else {}
                merged_r = dict(base)
                for k, v in result_item.items():
                    if isinstance(v, str):
                        if v.strip():
                            merged_r[k] = v
                    elif v is not None:
                        merged_r[k] = v
                merged_gr.append(merged_r)
            if not merged_gr:
                merged_gr = copy.deepcopy(existing_gr)
        else:
            merged_gr = copy.deepcopy(existing_gr)

        merged_notes = notes if notes else str(existing.get("notes", "")).strip()
        merged_team  = team if isinstance(team, list) and team else existing.get("team", [])
        created_at   = existing.get("created_at", now_iso())
        updated_at   = now_iso()

        record = dict(existing)
        record.update({
            "session_id":         session_id or existing.get("session_id", ""),
            "auditor_name":       auditor_name  or existing.get("auditor_name",  ""),
            "facility_area":      facility_area or existing.get("facility_area", ""),
            "date_of_audit":      date_of_audit or existing.get("date_of_audit", ""),
            "location":           location      or existing.get("location",      ""),
            "station":            station       or existing.get("station",       ""),
            "team":               merged_team,
            "categories":         merged_cats,
            "general_results":    merged_gr,
            "notes":              merged_notes,
            "status":             compute_status({"categories": merged_cats}),
            "updated_at":         updated_at,
        })
        save_inspection(record)
        return build_response(200, {
            "inspection_id": inspection_id,
            "session_id":    session_id or existing.get("session_id", ""),
            "created_at":    created_at,
            "updated_at":    updated_at,
            "status":        record["status"],
            "message":       "Inspection updated and merged successfully.",
        })

    # No existing record — create brand new
    if not inspection_id:
        inspection_id = str(uuid.uuid4())
        # Persist newly generated inspection_id back to session record if possible
        if session_id:
            try:
                sessions_table.update_item(
                    Key={"session_id": session_id},
                    UpdateExpression="SET inspection_id = :iid",
                    ExpressionAttributeValues={":iid": inspection_id},
                )
            except Exception:
                logger.exception("Failed to persist generated inspection_id back to sessions_table")

    created_at = now_iso()
    record = {
        "inspection_id":      inspection_id,
        "session_id":         session_id,
        "auditor_name":       auditor_name,
        "facility_area":      facility_area,
        "date_of_audit":      date_of_audit,
        "location":           location,
        "station":            station,
        "station_id":         station_id,
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
    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id":    session_id,
        "created_at":    created_at,
        "status":        "not_started",
    })


# ═══════════════════════════════════════════════════════════════
# API 3: GET /fire-extinguisher-inspections
# ═══════════════════════════════════════════════════════════════
def list_inspections(event):
    result = table.scan()
    items = result.get("Items", [])
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    items = convert_decimals(items)

    summary_list = []
    for item in items:
        summary_list.append({
            "inspection_id":      item.get("inspection_id"),
            "session_id":         item.get("session_id"),
            "auditor_name":       item.get("auditor_name"),
            "facility_area":      item.get("facility_area"),
            "date_of_audit":      item.get("date_of_audit"),
            "location":           item.get("location"),
            "station":            item.get("station"),
            "team":               item.get("team", []),
            "status":             item.get("status", "unknown"),
            "created_at":         item.get("created_at"),
            # ─────────────────────────────────────────────
            # FIX 5: Added current_item_index and notes to list summary.
            # Mobile app needs current_item_index to resume an inspection
            # from where the worker left off, and notes for display.
            # ─────────────────────────────────────────────
            "current_item_index": item.get("current_item_index", 0),
            "notes":              item.get("notes", ""),
        })

    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return build_response(200, summary_list)


# ═══════════════════════════════════════════════════════════════
# API 4: GET /fire-extinguisher-inspection/{id}
# ═══════════════════════════════════════════════════════════════
def get_inspection(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()

    # ─────────────────────────────────────────────
    # FIX: Use load_inspection_by_any_id so that a session_id passed as the
    # path parameter also resolves correctly, matching File 1 behaviour.
    # ─────────────────────────────────────────────
    item = load_inspection_by_any_id(inspection_id)
    if not item:
        return build_response(404, {"error": "Inspection not found"})

    if company_key and company_key != "default" and sync_inspection_with_template is not None:
        synced = sync_inspection_with_template(item, "fire-extinguisher", company_key)
        if synced.get("categories") != item.get("categories"):
            item = synced
            item["updated_at"] = now_iso()
            save_inspection(item)

    description_lookup = build_description_lookup(company_key or "default")
    categories = item.get("categories", [])
    for category in categories:
        for checklist_item in category.get("items", []):
            item_id = checklist_item.get("id")
            if item_id in description_lookup:
                checklist_item["description"] = description_lookup[item_id]

    ordered_item = {
        "inspection_id": item.get("inspection_id"),
        "session_id":    item.get("session_id"),
        "auditor_name":  item.get("auditor_name"),
        "facility_area": item.get("facility_area"),
        "date_of_audit": item.get("date_of_audit"),
        "location":      item.get("location"),
        "station":       item.get("station"),
        "team":          item.get("team", []),
        "categories":    categories,
        "general_results": item.get("general_results", []),
        "notes":         item.get("notes", ""),
        "status":        item.get("status", "unknown"),
        "created_at":    item.get("created_at"),
        "updated_at":    item.get("updated_at", ""),
    }

    return build_response(200, ordered_item)


# ═══════════════════════════════════════════════════════════════
# API 5: PATCH /fire-extinguisher/session/{id}/items/{item_id}
# ═══════════════════════════════════════════════════════════════
def update_checklist_item(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id = path_params.get("item_id", "")
    body = parse_body(event)

    if not inspection_id or not item_id:
        return build_response(400, {"error": "inspection_id and item_id are required"})

    # ─────────────────────────────────────────────
    # FIX 3a: Guard against manually updating auto-calculated items 11 and 12.
    # These are always computed from evidence records; manual writes would corrupt counts.
    # ─────────────────────────────────────────────
    if str(item_id) in ["11", "12"]:
        return build_response(400, {"error": "Items 11 and 12 are auto-calculated from inspection evidence and cannot be updated manually"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": f"Checklist item {item_id} not found"})

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

    # ─────────────────────────────────────────────
    # FIX 3b: Handle blocked_by_wrong_image and clear_block.
    # blocked_by_wrong_image lets the frontend flag/unflag a blocked item.
    # clear_block=true explicitly unblocks an item so the worker can proceed.
    # Without this, once blocked an item can never be manually unblocked.
    # ─────────────────────────────────────────────
    blocked_by_wrong_image = body.get("blocked_by_wrong_image", None)
    clear_block = bool(body.get("clear_block", False))

    if blocked_by_wrong_image is not None:
        checklist_item["blocked_by_wrong_image"] = bool(blocked_by_wrong_image)
    if clear_block:
        checklist_item["blocked_by_wrong_image"] = False

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    update_summary_items(inspection)
    inspection["status"] = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "message":          f"Item {item_id} updated",
        "updated_item":     checklist_item,
        "inspection_status": inspection["status"],
        "categories":       inspection.get("categories", []),
        "inspection":       inspection,
    })


# ═══════════════════════════════════════════════════════════════
# API 6: PATCH /fire-extinguisher/session/{id}/items/{item_id}/note
# ═══════════════════════════════════════════════════════════════
def add_note_to_item(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")
    item_id = path_params.get("item_id", "")
    body = parse_body(event)
    note_text = body.get("note", "").strip()

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

    timestamp = now_iso()
    existing_finding = checklist_item.get("finding", "").strip()
    new_note = f"[{timestamp}] {note_text}"
    checklist_item["finding"] = f"{existing_finding}\n{new_note}" if existing_finding else new_note

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return build_response(200, {
        "message": f"Note added to item {item_id}",
        "updated_item": checklist_item,
    })


# ═══════════════════════════════════════════════════════════════
# API 7: GET /fire-extinguisher/session/{id}/report
# ═══════════════════════════════════════════════════════════════
def get_inspection_report(event):
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    # ─────────────────────────────────────────────
    # FIX 7: Tiered response size management to stay under Lambda's 6 MB
    # response limit. Large inspections with many evidence records were
    # silently returning oversized responses that API Gateway would reject.
    #
    # Tier 1 (> 4000 KB): strip evidence to file_key + slim metadata only.
    # Tier 2 (> 5500 KB): nuclear — remove all evidence arrays entirely.
    # ─────────────────────────────────────────────

    # Slim evidence for normal response: keep last record only with key fields
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            cleaned_evidence = []
            for ev in item.get("evidence", []):
                if isinstance(ev, dict):
                    slim_ev = {
                        "file_key":          str(ev.get("file_key", ""))[:300],
                        "analyzed_at":       ev.get("analyzed_at", ""),
                        "object_detected":   ev.get("object_detected", ""),
                        "condition_checked": ev.get("condition_checked", ""),
                        "pass":              ev.get("pass", False),
                        "is_compliant":      ev.get("is_compliant", False),
                        "confidence":        ev.get("confidence", 0.0),
                        "blocked":           ev.get("blocked", False),
                    }
                    cleaned_evidence.append(slim_ev)
                elif isinstance(ev, str) and ev.strip():
                    cleaned_evidence.append({"file_key": str(ev)[:300]})
            # Keep only the most recent evidence entry to reduce size
            item["evidence"] = cleaned_evidence[-1:] if cleaned_evidence else []

            for field in ["finding", "action_item", "responsible"]:
                if field in item:
                    item[field] = str(item.get(field, ""))[:200]

    if "notes" in inspection:
        inspection["notes"] = str(inspection.get("notes", ""))[:1000]

    # Check size after first-pass slim
    body_str = json.dumps(inspection, default=str)
    size_kb = len(body_str.encode("utf-8")) / 1024
    logger.info(f"[REPORT] inspection_id={inspection_id} response_size={size_kb:.1f}KB")

    if size_kb > 4000:
        logger.warning(f"[REPORT] Response {size_kb:.1f}KB > 4000KB — stripping evidence to file_key only")
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = [
                    {"file_key": str(ev.get("file_key", "") if isinstance(ev, dict) else ev)[:300]}
                    for ev in item.get("evidence", [])
                    if (ev.get("file_key") if isinstance(ev, dict) else ev)
                ]
        body_str = json.dumps(inspection, default=str)
        size_kb = len(body_str.encode("utf-8")) / 1024
        logger.info(f"[REPORT] After tier-1 strip: {size_kb:.1f}KB")

    if size_kb > 5500:
        logger.error(f"[REPORT] Still {size_kb:.1f}KB > 5500KB — removing all evidence (nuclear)")
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = []

    return build_response(200, inspection)


# ═══════════════════════════════════════════════════════════════
# API 10a: DELETE /fire-extinguisher-inspection/{inspection_id}
# ═══════════════════════════════════════════════════════════════
def delete_inspection(event):
    """
    Deletes a fire extinguisher inspection record by inspection_id.
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
        logger.exception("Error deleting inspection")
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
            logger.warning(f"Warning: Failed to delete linked session {session_id}: {e}")

    return build_response(200, {
        "message": "Inspection deleted successfully",
        "inspection_id": inspection_id,
        "session_id": session_id,
        "session_deleted": deleted_session,
    })


# ═══════════════════════════════════════════════════════════════
# API 10b: DELETE /fire-extinguisher/session/{session_id}
# ═══════════════════════════════════════════════════════════════
def delete_session(event):
    path_params = event.get("pathParameters", {}) or {}
    session_id = str(path_params.get("session_id", "")).strip()

    if not session_id:
        return build_response(400, {"error": "session_id is required"})

    delete_inspections_flag = str(get_query(event, "delete_inspections", "true")).strip().lower()
    delete_inspections = delete_inspections_flag in {"1", "true", "yes", "y"}

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
                    iid = str(it.get("inspection_id", "")).strip()
                    if iid:
                        table.delete_item(Key={"inspection_id": iid})
                        deleted_inspection_ids.append(iid)
        except Exception as e:
            logger.exception("Failed deleting linked inspections")
            return build_response(500, {"error": f"Failed deleting linked inspections: {str(e)}"})

    try:
        sessions_table.delete_item(Key={"session_id": session_id})
    except Exception as e:
        logger.exception("Failed deleting session")
        return build_response(500, {"error": f"Failed deleting session: {str(e)}"})

    return build_response(200, {
        "message":                    "Session deleted successfully",
        "session_id":                 session_id,
        "deleted_linked_inspections": delete_inspections,
        "deleted_inspection_count":   len(deleted_inspection_ids),
        "deleted_inspection_ids":     deleted_inspection_ids,
    })


# ═══════════════════════════════════════════════════════════════
# API 11: POST /fire-extinguisher/voice
# ═══════════════════════════════════════════════════════════════
def voice_command(event):
    """
    Parses a voice command. Tries fast local regex first,
    falls back to Claude Haiku for complex/ambiguous commands.
    Also incorporates item-specific guidance for items 7–10.
    """
    body = parse_body(event)
    text = body.get("text", "").strip()
    current_item_id = str(body.get("current_item_id", "")).strip()
    inspection_id = str(body.get("inspection_id", "")).strip()

    if not text:
        return build_response(400, {"error": "text is required"})

    # Parallel: kick off DynamoDB load in background thread
    def _load_inspection_and_item():
        if not (inspection_id and current_item_id):
            return None, None
        insp = load_inspection_by_any_id(inspection_id)
        if not insp:
            return None, None
        item, _, _ = find_item(insp, current_item_id)
        return insp, item

    dynamo_future = _THREAD_POOL.submit(_load_inspection_and_item)

    # Try fast local regex first (runs while DynamoDB loads)
    local = _parse_voice_local(text.lower())

    # Collect DynamoDB result
    try:
        inspection, current_item = dynamo_future.result(timeout=5)
    except Exception:
        inspection, current_item = None, None

    if local:
        # Guard: don't advance if item is blocked
        if local.get("intent") == "next_item" and current_item and current_item.get("blocked_by_wrong_image"):
            zoom_hint = item_zoom_hint(current_item_id)
            return build_response(200, {
                "intent": "clarify",
                "confidence": 0.99,
                "message": (f"{zoom_hint} Remove obstruction and retake first." if zoom_hint else "Please retake the photo for this item first."),
                "move_to_item": False,
                "note_text": None,
            })

        # Attach zoom/condition hints for items 7-10
        zoom_hint = item_zoom_hint(current_item_id)
        if zoom_hint and local.get("intent") in ["capture_photo", "retake_photo", "repeat_item", "help"]:
            if current_item and current_item.get("blocked_by_wrong_image"):
                local["message"] = f"{zoom_hint} Remove obstruction and retake."
            else:
                local["message"] = zoom_hint

        return build_response(200, local)

    # Fallback to Haiku
    item_context = ""
    if current_item:
        item_context = f"Current checklist item: {current_item.get('description', '')}"
        zoom_hint = item_zoom_hint(current_item_id)
        if zoom_hint:
            item_context += f"\nZoom guidance: {zoom_hint}"
        condition_hint = voice_item_hint(current_item_id)
        if condition_hint:
            item_context += f"\nCondition guidance: {condition_hint}"

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
            "intent": "clarify",
            "confidence": 0.2,
            "message": "Please repeat the command.",
            "move_to_item": False,
            "note_text": None,
        })


def _parse_voice_local(text):
    """Fast local regex parser for common voice commands."""
    text = text.strip().lower()

    if re.search(r"\b(next item|move next|go forward|continue)\b", text):
        return {"intent": "next_item", "confidence": 0.99, "message": "Moving to next item.", "move_to_item": True, "note_text": None}
    if re.search(r"\bnext\b", text) and len(text.split()) <= 2:
        return {"intent": "next_item", "confidence": 0.95, "message": "Moving to next item.", "move_to_item": True, "note_text": None}

    if re.search(r"\b(previous item|go back|back to previous)\b", text):
        return {"intent": "previous_item", "confidence": 0.99, "message": "Going back one item.", "move_to_item": True, "note_text": None}
    if re.search(r"\bback\b", text) and len(text.split()) <= 2:
        return {"intent": "previous_item", "confidence": 0.95, "message": "Going back one item.", "move_to_item": True, "note_text": None}

    if re.search(r"\b(capture photo|take photo|take picture|capture image|scan)\b", text):
        return {"intent": "capture_photo", "confidence": 0.99, "message": "Capture the photo now.", "move_to_item": False, "note_text": None}

    if re.search(r"\b(retake photo|retake|try again|take again|redo|capture again)\b", text):
        return {"intent": "retake_photo", "confidence": 0.99, "message": "Retaking. Point camera at extinguisher.", "move_to_item": False, "note_text": None}

    if re.search(r"\b(add note|add comment|add finding|note this|write note|add remark)\b", text):
        return {"intent": "add_note", "confidence": 0.95, "message": "Go ahead, add your note.", "move_to_item": False, "note_text": None}

    if re.search(r"\b(repeat|say again|read again|read item|what is this item)\b", text):
        return {"intent": "repeat_item", "confidence": 0.95, "message": "Repeating current item.", "move_to_item": False, "note_text": None}

    if re.search(r"\b(help|what can i say|show commands|list commands)\b", text):
        return {"intent": "help", "confidence": 0.95, "message": "Say next, back, capture, retake, add note, or repeat.", "move_to_item": False, "note_text": None}

    note_match = re.search(r"\b(?:note|add note|comment|remark)[:\s]+(.+)", text)
    if note_match:
        return {"intent": "add_note", "confidence": 0.90, "message": "Note saved.", "move_to_item": False, "note_text": note_match.group(1).strip()}

    return None  # let Haiku handle it


# ═══════════════════════════════════════════════════════════════
# API 12: POST /fire-extinguisher/analyze
# AI Image Analysis — Claude 3.5 Sonnet via Bedrock
# ═══════════════════════════════════════════════════════════════
def analyze_item_image(event, _is_async=False):
    if not _is_async and not event.get("async_worker"):
        return start_async_analyze_job(event)

    t0 = time.time()
    logger.info("[TIMING] analyze_item_image START")
    body = parse_body(event)
    logger.info(
        "[REQUEST] analyze called: inspection_id=%s item_id=%s image_side=%s has_file_key=%s has_image_base64=%s",
        body.get("inspection_id"),
        body.get("item_id"),
        body.get("image_side", ""),
        bool(body.get("file_key") or body.get("fileKey")),
        bool(body.get("image_base64")),
    )

    inspection_id     = str(body.get("inspection_id", "")).strip()
    item_id           = str(body.get("item_id", "")).strip()
    expected_location = str(body.get("expected_location", "")).strip()
    image_side        = str(body.get("image_side", "")).strip().lower()

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required"})
    if not item_id:
        return build_response(400, {"error": "item_id is required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return build_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return build_response(404, {"error": "Checklist item not found"})

    # Items 11 and 12 are auto-calculated — no image needed
    if str(item_id) in ["11", "12"]:
        update_summary_items(inspection)
        inspection["status"]     = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)
        item11, _, _ = find_item(inspection, "11")
        item12, _, _ = find_item(inspection, "12")
        return build_response(200, {
            "inspection_id":      inspection_id,
            "item_id":            item_id,
            "blocked":            False,
            "move_next":          True,
            "message":            "Summary auto-calculated from inspection evidence.",
            "updated_item_11":    item11,
            "updated_item_12":    item12,
            "inspection_status":  inspection["status"],
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection":         inspection,
            "categories":         inspection.get("categories", []),
        })

    # Extract image bytes
    try:
        image_bytes, content_type = _extract_image_from_request(body)
    except FileNotFoundError as e:
        return build_response(400, {"error": f"Invalid image input: {str(e)}"})
    except ClientError as e:
        return build_response(400, {"error": f"S3 read failed: {e.response.get('Error', {}).get('Message', str(e))}"})
    except Exception as e:
        return build_response(400, {"error": f"Invalid image input: {str(e)}"})

    if image_bytes is None:
        return build_response(400, {"error": "Provide image_base64 or file_key"})

    original_image_bytes = image_bytes

    # ── Custom checklist items: generic visual compliance check ───────────
    if str(item_id).startswith("custom_"):
        bedrock_image_bytes, bedrock_type = prepare_image_bytes(
            original_image_bytes, content_type, mode="",
        )
        question_text = (
            checklist_item.get("description")
            or checklist_item.get("title")
            or "custom safety requirement"
        )
        custom_prompt = (
            f"Inspect this workplace image against this custom safety question:\n"
            f"Question: {question_text}\n\n"
            f"Determine if the image provides enough visual evidence to answer YES.\n"
            f"Return JSON only with keys: pass (bool), confidence (0-1), reason, "
            f"worker_message, suggested_action, condition_checked (use 'custom_requirement')."
        )
        try:
            analysis = invoke_claude_json(
                "You are a workplace safety inspector. Return JSON only.",
                custom_prompt,
                image_bytes=bedrock_image_bytes,
                media_type=bedrock_type,
            )
        except Exception as e:
            return build_response(500, {"error": f"AI analysis failed: {str(e)}"})

        passed = bool(analysis.get("pass", False))
        confidence = float(analysis.get("confidence", 0.0) or 0.0)
        reason = str(analysis.get("reason", "")).strip()
        worker_message = str(analysis.get("worker_message", "")).strip()
        suggested_action = analysis.get("suggested_action")
        if suggested_action is not None:
            suggested_action = str(suggested_action).strip() or None
        condition_checked = str(analysis.get("condition_checked", "custom_requirement")).strip()
        blocked = confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD
        finding_text, action_text = build_finding_and_action(
            item_id, passed, blocked, reason, suggested_action, condition_checked,
        )

        evidence_record = {
            "file_key": body.get("file_key") or body.get("fileKey") or "",
            "analyzed_at": now_iso(),
            "object_detected": "custom_requirement",
            "condition_checked": condition_checked,
            "pass": passed and not blocked,
            "is_compliant": passed and not blocked,
            "confidence": confidence,
            "reason": reason,
            "worker_message": worker_message,
            "suggested_action": suggested_action or "",
            "blocked": blocked,
        }
        checklist_item.setdefault("evidence", [])
        checklist_item["evidence"].append(evidence_record)

        if blocked:
            checklist_item["blocked_by_wrong_image"] = True
            checklist_item["answer"] = ""
            checklist_item["finding"] = finding_text
            checklist_item["action_item"] = action_text
        else:
            checklist_item["answer"] = "Yes" if passed else "No"
            checklist_item["blocked_by_wrong_image"] = False
            checklist_item["finding"] = finding_text
            checklist_item["action_item"] = action_text

        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        update_summary_items(inspection)
        inspection["status"] = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        _ck = str(body.get("company_key", "")).strip()
        _verdict_label = "need_review"
        if (blocked or not passed) and _ck and get_company_config:
            try:
                _verdict_label = get_company_config(_ck).get("blocked_verdict_label", "need_review")
            except Exception:
                pass

        resp = {
            "inspection_id": inspection_id,
            "item_id": item_id,
            "blocked": blocked,
            "move_next": not blocked,
            "pass": passed and not blocked,
            "confidence": confidence,
            "condition_checked": condition_checked,
            "message": worker_message or "Custom item analyzed.",
            "reason": finding_text,
            "suggested_action": action_text,
            "updated_item": checklist_item,
            "inspection_status": inspection["status"],
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        }
        if blocked or not passed:
            resp["blocked_verdict_label"] = _verdict_label
        return build_response(200, resp)

    # ── Item 2: Frontend-decoded QR + AI extinguisher check ──────────────
    if str(item_id) == "2":
        logger.info("[ITEM2] Frontend-decoded QR flow")

        def _item2_fail(finding, action, message, obj_det, cond, blocked=True):
            ev = {
                "file_key":          body.get("file_key") or body.get("fileKey") or "",
                "analyzed_at":       now_iso(),
                "object_detected":   obj_det,
                "condition_checked": cond,
                "is_extinguisher":   True,
                "pass":              False,
                "is_compliant":      False,
                "confidence":        Decimal("0.0"),
                "reason":            finding,
                "worker_message":    message,
                "suggested_action":  action,
                "blocked":           blocked,
                "inference_source":  "frontend_qr_decode",
            }
            checklist_item.setdefault("evidence", [])
            checklist_item["evidence"].append(ev)
            checklist_item["blocked_by_wrong_image"] = blocked
            checklist_item["answer"]      = "" if blocked else "No"
            checklist_item["finding"]     = finding
            checklist_item["action_item"] = action
            inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
            inspection["updated_at"] = now_iso()
            save_inspection(inspection)
            return build_response(200, {
                "inspection_id":      inspection_id,
                "item_id":            item_id,
                "blocked":            blocked,
                "move_next":          False,
                "pass":               False,
                "object_detected":    obj_det,
                "condition_checked":  cond,
                "confidence":         0.0,
                "message":            message,
                "reason":             finding,
                "suggested_action":   action,
                "updated_item":       checklist_item,
                "inspection_status":  inspection.get("status", "in_progress"),
                "current_item_index": inspection.get("current_item_index", 0),
                "inference_source":   "frontend_qr_decode",
                "inspection":         inspection,
                "categories":         inspection.get("categories", []),
            })

        qr_height_cm_raw = body.get("qr_height_cm")
        station_id_raw   = str(body.get("station_id") or "").strip()

        if qr_height_cm_raw is None:
            return _item2_fail(
                "No QR height data received. The QR code may not be visible or readable in the image.",
                "Place the printed height QR code clearly beside the fire extinguisher and retake the photo.",
                "No QR found. Place QR card beside extinguisher and retake.",
                "no_qr_code", "qr_not_detected",
            )

        try:
            handle_height = float(qr_height_cm_raw)
        except (ValueError, TypeError):
            return _item2_fail(
                f"QR height value '{qr_height_cm_raw}' is not a valid number.",
                "Regenerate the QR code with a valid numeric height and replace it.",
                "Invalid QR data. Contact supervisor.",
                "qr_invalid", "qr_data_invalid",
            )

        if handle_height <= 0 or handle_height > 300:
            return _item2_fail(
                f"QR height value {handle_height} cm is out of plausible range (0–300 cm).",
                "Regenerate the QR code with the correct measured height.",
                "QR height value is implausible. Contact supervisor.",
                "qr_invalid", "qr_height_implausible",
            )

        qr_data = {"type": QR_CODE_TYPE_IDENTIFIER, "handle_height_cm": handle_height, "station_id": station_id_raw}
        qr_passed, qr_finding, qr_action, qr_message = validate_extinguisher_height(qr_data)
        if not qr_passed:
            return _item2_fail(qr_finding, qr_action, qr_message, "height_out_of_range", "height_exceeds_osha_limit", blocked=False)

        # Claude confirms fire extinguisher is physically visible beside the QR
        ITEM2_FE_CHECK_SYSTEM = (
            "You are a visual analysis assistant. Your ONLY job is to determine whether "
            "a REAL, PHYSICAL, 3D fire extinguisher cylinder is present in the image. "
            "Answer honestly. If the object is clearly visible, say so. If not, say so."
        )
        bedrock_img, bedrock_type = prepare_image_bytes(original_image_bytes, content_type)
        fe_prompt = (
            "Look at this image carefully.\n"
            "Is there a REAL, PHYSICAL fire extinguisher clearly visible in this photo?\n\n"
            "A REAL fire extinguisher is:\n"
            "  ✓ A 3D cylindrical pressure vessel (typically red, silver, or yellow)\n"
            "  ✓ Usually 1-2 feet tall, standing upright or mounted on a wall\n"
            "  ✓ Has physical features: hose/nozzle, pressure gauge, handle/lever, instruction label\n"
            "  ✓ A solid, tangible, three-dimensional object — NOT a flat image or print\n\n"
            "NOT a fire extinguisher (MUST return false for these):\n"
            "  ✗ A QR code — even if it says 'fire extinguisher' on it\n"
            "  ✗ A printed image or picture of an extinguisher on paper, card, or screen\n"
            "  ✗ A sign, poster, label, or sticker showing an extinguisher icon\n"
            "  ✗ Just a wall, floor, door, or empty bracket\n"
            "  ✗ Only a QR code card with no real extinguisher beside it\n\n"
            "You MUST see the actual 3D metal/plastic cylinder body of a fire extinguisher.\n"
            "A QR code alone — even with extinguisher text — is NOT an extinguisher.\n\n"
            "CRITICAL CONSISTENCY RULE:\n"
            "  - If your reason describes seeing a REAL 3D fire extinguisher → set fire_extinguisher_visible = true\n"
            "  - If you only see a QR code, paper, sign, or no extinguisher → set fire_extinguisher_visible = false\n"
            "  - Your reason text and fire_extinguisher_visible MUST agree.\n\n"
            "Return JSON only — no markdown, no extra text:\n"
            '{"fire_extinguisher_visible": true|false, '
            '"confidence": <float 0.0-1.0>, '
            '"reason": "<one sentence describing what you see>"}'
        )
        try:
            ai_result = invoke_claude_json(
                system_prompt=ITEM2_FE_CHECK_SYSTEM,
                user_text=fe_prompt,
                image_bytes=bedrock_img,
                media_type=bedrock_type,
                max_tokens=150,
            )
        except Exception as e:
            logger.exception(f"[ITEM2] Bedrock call failed: {e}")
            return _item2_fail(
                f"AI extinguisher check failed: {str(e)}",
                "Retry the inspection.", "Analysis failed. Please retry.",
                "error", "ai_error",
            )

        fe_visible    = bool(ai_result.get("fire_extinguisher_visible", False))
        fe_confidence = float(ai_result.get("confidence", 0.0) or 0.0)
        ai_reason     = str(ai_result.get("reason", "")).strip()

        # Contradiction detection — only override if reason clearly describes a
        # PHYSICAL extinguisher (not just mentioning the word in context of QR/sign)
        if not fe_visible and ai_reason:
            reason_lower = ai_reason.lower()

            # Negative filter: if reason talks about QR-only or no extinguisher, trust the false
            qr_only_indicators = [
                "only a qr", "only qr", "qr code only", "no fire extinguisher",
                "no extinguisher", "not visible", "cannot see", "not present",
                "no physical", "just a qr", "qr card", "paper", "printed",
            ]
            is_qr_only_description = any(ind in reason_lower for ind in qr_only_indicators)

            if not is_qr_only_description:
                # Positive descriptors for a real physical extinguisher
                fe_descriptors = [
                    "extinguisher is clearly visible",
                    "extinguisher is visible",
                    "red cylindrical",
                    "pressure vessel",
                    "cylindrical pressure",
                    "extinguisher with a hose",
                    "extinguisher with a nozzle",
                    "mounted on the wall",
                    "standing upright",
                    "pressure gauge",
                ]
                descriptor_hits = sum(1 for d in fe_descriptors if d in reason_lower)
                # Require 2+ descriptor hits to override — avoids false positives
                if descriptor_hits >= 2:
                    logger.warning(
                        f"[ITEM2] Contradiction detected — Claude said visible=false "
                        f"but reason describes a physical fire extinguisher ({descriptor_hits} "
                        f"descriptor hits). Overriding to visible=true. Reason: {ai_reason}"
                    )
                    fe_visible = True
                    fe_confidence = max(fe_confidence, 0.75)

        logger.info(
            f"[ITEM2] FE visibility: visible={fe_visible}, "
            f"confidence={fe_confidence:.2f}, reason={ai_reason}"
        )

        if not fe_visible or fe_confidence < 0.5:
            return _item2_fail(
                f"QR height is valid ({handle_height} cm) but no fire extinguisher is clearly "
                f"visible in the photo. {ai_reason}",
                "Retake the photo showing BOTH the fire extinguisher AND the QR card in frame.",
                "No extinguisher visible. Retake with extinguisher and QR card in frame.",
                "no_extinguisher_visible", "extinguisher_not_in_frame",
            )

        station_label = f" (Station: {station_id_raw})" if station_id_raw else ""
        pass_finding  = (
            f"QR decoded by device: handle at {handle_height} cm{station_label}. "
            f"Within OSHA accessible range (max {MAX_ACCESSIBLE_HEIGHT_CM} cm). "
            f"Fire extinguisher confirmed present. {ai_reason}"
        )
        pass_message = f"Height OK — {handle_height} cm. Extinguisher verified."

        ev = {
            "file_key":          body.get("file_key") or body.get("fileKey") or "",
            "analyzed_at":       now_iso(),
            "object_detected":   "fire_extinguisher_with_qr",
            "condition_checked": "height_accessible_extinguisher_present",
            "is_extinguisher":   True,
            "pass":              True,
            "is_compliant":      True,
            "confidence":        Decimal(str(fe_confidence)),
            "reason":            pass_finding,
            "worker_message":    pass_message,
            "suggested_action":  "No corrective action required.",
            "blocked":           False,
            "inference_source":  "frontend_qr_decode",
            "qr_height_cm":      handle_height,
            "station_id":        station_id_raw,
        }
        checklist_item.setdefault("evidence", [])
        checklist_item["evidence"].append(ev)
        checklist_item["answer"]               = "Yes"
        checklist_item["blocked_by_wrong_image"] = False
        checklist_item["finding"]              = pass_finding
        checklist_item["action_item"]          = "No corrective action required."

        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        next_pos = next_unanswered_index(inspection)
        inspection["current_item_index"] = next_pos[1] if next_pos else item_idx
        update_summary_items(inspection)
        inspection["status"]     = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        item11, _, _ = find_item(inspection, "11")
        item12, _, _ = find_item(inspection, "12")

        return build_response(200, {
            "inspection_id":      inspection_id,
            "item_id":            item_id,
            "blocked":            False,
            "move_next":          True,
            "pass":               True,
            "object_detected":    "fire_extinguisher_with_qr",
            "condition_checked":  "height_accessible_extinguisher_present",
            "confidence":         fe_confidence,
            "message":            pass_message,
            "reason":             pass_finding,
            "suggested_action":   "No corrective action required.",
            "updated_item":       checklist_item,
            "summary_item_11":    item11,
            "summary_item_12":    item12,
            "inspection_status":  inspection["status"],
            "current_item_index": inspection.get("current_item_index", 0),
            "next_item_index":    inspection.get("current_item_index", 0),
            "inference_source":   "frontend_qr_decode",
            "qr_height_cm":       handle_height,
            "inspection":         inspection,
            "categories":         inspection.get("categories", []),
        })

    # ── Items 3–10 (excluding 2): Bedrock image analysis ─────────────────
    COMPONENT_ITEMS = {"8", "9", "10"}
    component_focus = str(item_id) in COMPONENT_ITEMS

    bedrock_image_bytes, bedrock_type = prepare_image_bytes(
        original_image_bytes,
        content_type,
        mode="component" if component_focus else "",
    )
    logger.info(f"[TIMING] after image extract/prepare: {time.time() - t0:.2f}s")

    rule = checklist_rule_for_item(item_id, checklist_item, image_side=image_side)

    # Build prompt
    if component_focus:
        effective_item_key = f"{item_id}_back" if str(item_id) == "10" and image_side == "back" else str(item_id)
        contract_text = component_check_contract(effective_item_key)
        COMPONENT_TARGET_NAMES = {"8": "pressure gauge", "9": "instruction label", "10": "inspection tag", "10_back": "inspection tag back"}
        target_name = COMPONENT_TARGET_NAMES.get(effective_item_key, "component")
        prompt = (
            f"Checklist item to inspect (component close-up expected):\n"
            f"- item_id: {checklist_item['id']}\n"
            f"- item_description: {checklist_item['description']}\n"
            f"- target_component: {target_name}\n"
            f"- strict_visual_rule: {rule}\n"
            f"- required_checks_contract: {contract_text}\n"
            f"- expected_location_hint: {expected_location or 'not provided'}\n\n"
            f"STEP 1: Is the target_component identifiable anywhere in this image?\n"
            f"  Remember: target_visible=true even if the component is small or the full\n"
            f"  extinguisher body is also visible. Only set target_visible=false if the\n"
            f"  component is completely absent from the image.\n"
            f"STEP 2: If yes, does it satisfy the strict_visual_rule? Evaluate each sub-condition.\n"
            f"STEP 3: Return checks object exactly per required_checks_contract.\n"
            f"Do not guess. If the required condition is not clearly visible, fail.\n"
            f"Return JSON only."
        )
    elif str(item_id) == "7":
        prompt = (
            f"Checklist item to inspect:\n"
            f"- item_id: {checklist_item['id']}\n"
            f"- item_description: {checklist_item['description']}\n"
            f"- strict_visual_rule: {rule}\n"
            f"- expected_location_hint: {expected_location or 'not provided'}\n\n"
            f"STEP 1: Is a fire extinguisher clearly visible in this image?\n"
            f"STEP 2: Is the hose physically connected to the extinguisher body AND visible?\n"
            f"STEP 3: Is the nozzle tip or hose end LITERALLY VISIBLE in the image pixels?\n"
            f"STEP 4: If nozzle tip IS visible — is it free of tape, cap, cloth, debris?\n\n"
            f"CRITICAL: You must answer each step based ONLY on what you can literally see.\n"
            f"Do NOT assume the nozzle is fine because the extinguisher body looks good.\n"
            f"If you cannot see the nozzle tip or hose end → nozzle_tip_visible = false.\n\n"
            f"Return JSON ONLY in this exact schema:\n"
            f"{{\n"
            f'  "object_detected": "fire_extinguisher|other|unclear",\n'
            f'  "condition_checked": "<short string>",\n'
            f'  "pass": true|false,\n'
            f'  "confidence": <float 0.0-1.0>,\n'
            f'  "reason": "<what you literally see for each component>",\n'
            f'  "worker_message": "<under 15 words>",\n'
            f'  "suggested_action": "<corrective action or null>",\n'
            f'  "checks": {{\n'
            f'    "extinguisher_body_visible": true|false,\n'
            f'    "hose_visible_in_image": true|false,\n'
            f'    "nozzle_tip_visible": true|false,\n'
            f'    "external_obstruction": "none|tape|cap|cloth|debris|unclear",\n'
            f'    "hose_condition": "clear|kinked|crushed|tied|unclear",\n'
            f'    "nozzle_physical_condition": "intact|cracked|melted|broken|unclear"\n'
            f'  }}\n'
            f"}}\n"
            f"Return JSON only."
        )
    else:
        prompt = (
            f"Checklist item to inspect:\n"
            f"- item_id: {checklist_item['id']}\n"
            f"- item_description: {checklist_item['description']}\n"
            f"- strict_visual_rule: {rule}\n"
            f"- expected_location_hint: {expected_location or 'not provided'}\n\n"
            f"STEP 1: Is a fire extinguisher clearly and unambiguously visible in this image?\n"
            f"STEP 2: If yes, does the image clearly satisfy the strict_visual_rule above?\n"
            f"Do not approve just because an extinguisher is present.\n"
            f"Do not guess. If the required condition is not clearly visible, fail.\n"
            f"Ignore all people, PPE, and background.\n"
            f"Return JSON only."
        )

    # Call Bedrock
    try:
        bedrock_t = time.time()
        _max_tokens = 600 if component_focus else 220
        analysis = invoke_claude_json(
            system_prompt=COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT if component_focus else IMAGE_ANALYSIS_SYSTEM_PROMPT,
            user_text=prompt,
            image_bytes=bedrock_image_bytes,
            media_type=bedrock_type,
            max_tokens=_max_tokens,
        )
        logger.info(f"[TIMING] Bedrock done: {time.time() - bedrock_t:.2f}s total: {time.time() - t0:.2f}s")
        # Optional debug capture: log and attach full Bedrock analysis when enabled
        try:
            if str(os.getenv("DEBUG_CAPTURE_ANALYSIS", "")).strip().lower() in {"1", "true", "yes"}:
                try:
                    logger.info(f"[DEBUG_AI] analysis (truncated) for item={item_id}: {json.dumps(analysis)[:4000]}")
                except Exception:
                    logger.info(f"[DEBUG_AI] analysis present for item={item_id} (unable to JSON-dump)")
        except Exception:
            logger.exception("Failed to process DEBUG_CAPTURE_ANALYSIS")
        # --- Blur/clarity guard for component items (7-10) ---
        try:
            if str(item_id) in {"7", "8", "9", "10"} and isinstance(bedrock_image_bytes, (bytes, bytearray)):
                try:
                    img = Image.open(io.BytesIO(bedrock_image_bytes)).convert("L")
                    img = img.resize((300, 300))
                    edges = img.filter(ImageFilter.FIND_EDGES)
                    pixels = list(edges.getdata())
                    if pixels:
                        mean = sum(pixels) / len(pixels)
                        var = sum((p - mean) ** 2 for p in pixels) / len(pixels)
                    else:
                        var = 0.0
                    logger.info(f"[BLUR_CHECK] item={item_id} edge-variance={var:.2f}")
                    BLUR_THRESHOLD = float(os.getenv("BLUR_EDGE_VARIANCE_THRESHOLD", "200.0"))
                    if var < BLUR_THRESHOLD:
                        checks = analysis.get("checks") if isinstance(analysis, dict) else {}
                        checks = checks if isinstance(checks, dict) else {}
                        if str(item_id) == "8":
                            if checks.get("gauge_face_readability") != "readable":
                                checks["gauge_face_readability"] = "unreadable"
                                logger.info(f"[BLUR_CHECK] Marked gauge_face_readability=unreadable for item {item_id}")
                        elif str(item_id) == "7":
                            if checks.get("nozzle_tip_visible") is not False:
                                checks["nozzle_tip_visible"] = False
                                logger.info(f"[BLUR_CHECK] Marked nozzle_tip_visible=false for item {item_id}")
                        elif str(item_id) == "9":
                            if checks.get("label_text_readability") != "blurry":
                                checks["label_text_readability"] = "blurry"
                                logger.info(f"[BLUR_CHECK] Marked label_text_readability=blurry for item {item_id}")
                        elif str(item_id) == "10":
                            if checks.get("recent_year_visible") != "no":
                                checks["recent_year_visible"] = "no"
                                checks["year_identified"] = "unclear"
                                logger.info(f"[BLUR_CHECK] Marked recent_year_visible=no for item {item_id}")
                        analysis["checks"] = checks
                except Exception:
                    logger.exception("Failed to compute blur metric")
        except Exception:
            logger.exception("Unexpected error in blur-check wrapper")
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

    # ── Item 7 structured check enforcement ───────────────────────────────
    if str(item_id) == "7" and isinstance(analysis.get("checks"), dict):
        checks7 = analysis["checks"]

        def _b7(key):
            v = checks7.get(key)
            if isinstance(v, bool): return v
            t = str(v).strip().lower()
            if t in {"true", "yes"}: return True
            if t in {"false", "no"}: return False
            return None

        def _s7(key):
            return str(checks7.get(key, "unclear")).strip().lower()

        extinguisher_visible = _b7("extinguisher_body_visible")
        hose_visible         = _b7("hose_visible_in_image")
        nozzle_tip_visible   = _b7("nozzle_tip_visible")
        obstruction          = _s7("external_obstruction")
        hose_cond            = _s7("hose_condition")
        nozzle_cond          = _s7("nozzle_physical_condition")

        item7_fail_reason = None
        item7_msg         = None
        item7_action      = None

        if extinguisher_visible is not True:
            item7_fail_reason = "No fire extinguisher body is visible in the image."
            item7_msg         = "Point camera at the fire extinguisher and retake."
            item7_action      = "Retake image showing the full fire extinguisher."
        elif hose_visible is not True:
            item7_fail_reason = "The hose is not visible in the image. Cannot verify nozzle condition."
            item7_msg         = "Tilt camera down to show the hose and nozzle tip."
            item7_action      = "Retake showing hose attached to extinguisher and nozzle tip visible."
        elif nozzle_tip_visible is not True:
            item7_fail_reason = "The nozzle tip is not visible in the image. Cannot confirm it is unobstructed."
            item7_msg         = "Move closer or tilt camera to show the nozzle tip clearly."
            item7_action      = "Retake with nozzle tip clearly visible in the frame."
        elif obstruction not in ("none", ""):
            item7_fail_reason = f"Nozzle tip has external obstruction: {obstruction}."
            item7_msg         = "Remove obstruction from nozzle tip and retake."
            item7_action      = "Remove tape, cap, or debris from nozzle tip and retake."
        elif hose_cond not in ("clear", ""):
            item7_fail_reason = f"Hose condition is {hose_cond}."
            item7_msg         = "Fix hose condition and retake."
            item7_action      = "Straighten or repair hose and retake image."
        elif nozzle_cond != "intact":
            item7_fail_reason = f"Nozzle physical condition is {nozzle_cond}."
            item7_msg         = "Nozzle is damaged. Service or replace extinguisher."
            item7_action      = "Nozzle damaged — extinguisher needs servicing."

        if item7_fail_reason:
            passed           = False
            analysis["pass"] = False
            reason           = item7_fail_reason
            worker_message   = item7_msg
            suggested_action = item7_action

    # ── Component checks enforcement (items 8, 9, 10) ─────────────────────
    if component_focus:
        passed, condition_checked, reason, worker_message, suggested_action = enforce_component_checks(
            item_id=item_id,
            analysis=analysis,
            passed=passed,
            condition_checked=condition_checked,
            reason=reason,
            worker_message=worker_message,
            suggested_action=suggested_action,
            image_side=image_side,
        )

    # ── Hard safety guard — non-component items only ───────────────────────
    if (not component_focus) and passed and object_detected != "fire_extinguisher":
        # Special-case: item 4 (signage). If the AI explicitly verified signage
        # (reason contains expected signage keywords), allow the pass to stand.
        if str(item_id) == "4":
            try:
                expected_kw = expected_keywords_for_item(item_id)
            except Exception:
                expected_kw = []
            lower_reason = str(reason or "").lower()
            if expected_kw and any(kw in lower_reason for kw in expected_kw):
                logger.info(f"Item 4: signage keywords present in reason; allowing pass despite object_detected={object_detected}.")
            else:
                logger.warning(f"AI returned pass=true with object_detected={object_detected} — overriding. item_id={item_id}")
                passed            = False
                object_detected   = "other"
                confidence        = 0.0
                condition_checked = "not_visible"
                analysis["pass"]              = False
                analysis["object_detected"]   = "other"
                analysis["confidence"]        = 0.0
                analysis["condition_checked"] = "not_visible"
                worker_message   = "No fire extinguisher detected. Point camera directly at the extinguisher."
                reason           = "Object in image is not a fire extinguisher."
                suggested_action = "Point the camera directly at the fire extinguisher and retake."
        else:
            logger.warning(f"AI returned pass=true with object_detected={object_detected} — overriding. item_id={item_id}")
            passed            = False
            object_detected   = "other"
            confidence        = 0.0
            condition_checked = "not_visible"
            analysis["pass"]              = False
            analysis["object_detected"]   = "other"
            analysis["confidence"]        = 0.0
            analysis["condition_checked"] = "not_visible"
            worker_message   = "No fire extinguisher detected. Point camera directly at the extinguisher."
            reason           = "Object in image is not a fire extinguisher."
            suggested_action = "Point the camera directly at the fire extinguisher and retake."

    wrong_image = False if component_focus else object_detected != "fire_extinguisher"

    if (not component_focus) and object_detected == "fire_extinguisher" and confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD:
        logger.warning(f"fire_extinguisher detected but confidence={confidence} < threshold={IMAGE_CONFIDENCE_BLOCK_THRESHOLD} — treating as unclear. item_id={item_id}")
        object_detected   = "unclear"
        wrong_image       = True
        worker_message    = "Extinguisher detected but image is not clear enough. Move closer and retake."
        condition_checked = "not_visible"

    # ── Keyword validation — non-component items only ──────────────────────
    expected_keywords = expected_keywords_for_item(item_id)
    # Only apply brittle keyword gating when AI DID NOT detect a fire extinguisher.
    # This prevents legitimate structured passes (and clear detections) from being
    # overturned simply because the free-text `reason` didn't include exact keywords.
    if passed and expected_keywords and not component_focus:
        lower_reason = reason.lower()
        if object_detected != "fire_extinguisher":
            if not any(keyword in lower_reason for keyword in expected_keywords):
                logger.warning(f"Checklist condition not validated in reason -> overriding FAIL. item_id={item_id}, reason={reason}")
                passed           = False
                analysis["pass"] = False
                worker_message   = "Condition not clearly visible. Retake image properly."
                reason           = "Checklist condition not clearly verified from image."
                suggested_action = "Retake the photo with the checklist item clearly visible."

    # ── Confidence gate ────────────────────────────────────────────────────
    if component_focus:
        low_confidence = confidence < COMPONENT_CONFIDENCE_BLOCK_THRESHOLD
        logger.info(f"[BEDROCK] component confidence gate item={item_id} confidence={confidence:.2f} threshold={COMPONENT_CONFIDENCE_BLOCK_THRESHOLD:.2f}")
        if str(item_id) == "8" and passed and isinstance(analysis.get("checks"), dict):
            checks8 = analysis.get("checks", {})
            target_visible8 = checks8.get("target_visible")
            if isinstance(target_visible8, str):
                target_visible8 = target_visible8.strip().lower() in {"true", "yes", "1"}
            needle_zone8 = str(checks8.get("needle_zone", "")).strip().lower().replace(" ", "_")
            face_readability8 = str(checks8.get("gauge_face_readability", "")).strip().lower().replace(" ", "_")
            glass_condition8 = str(checks8.get("gauge_glass_condition", "")).strip().lower().replace(" ", "_")
            if (
                target_visible8 is True
                and needle_zone8 == "green"
                and face_readability8 == "readable"
                and glass_condition8 == "intact"
            ):
                logger.info(
                    f"[ITEM8] Structured checks confirm a valid green gauge; "
                    f"overriding low-confidence block (confidence={confidence:.2f})."
                )
                low_confidence = False
    else:
        low_confidence = confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD

    blocked = wrong_image or low_confidence
    zoom_hint = item_zoom_hint(item_id, image_side=image_side)
    blocked_suggested_action = (
        suggested_action or
        (f"{zoom_hint} Remove obstruction and retake." if zoom_hint else "Point the camera directly at the fire extinguisher and retake.")
    )

    finding_text, action_text = build_finding_and_action(
        item_id=item_id,
        passed=passed,
        blocked=blocked,
        reason=reason,
        suggested_action=suggested_action,
        condition_checked=condition_checked,
    )

    # ── Special case: item 1 — extinguisher genuinely missing ─────────────
    if blocked and str(item_id) == "1":
        reason_text = reason or ""
        missing_keywords = ["missing", "empty", "no extinguisher", "not present", "absent", "bracket"]
        is_clearly_missing = any(kw in reason_text.lower() for kw in missing_keywords)
        if is_clearly_missing:
            logger.info("Item 1: extinguisher confirmed missing at location. Recording as fail.")
            checklist_item["answer"]                = "No"
            checklist_item["blocked_by_wrong_image"] = False
            checklist_item["finding"]               = reason or "Fire extinguisher not found at this location."
            checklist_item["action_item"]           = suggested_action or "Replace missing fire extinguisher immediately and report to safety officer."
            blocked = False
            passed  = False

    # Build and append evidence record
    evidence_record = {
        "file_key":          body.get("file_key") or body.get("fileKey") or "",
        "analyzed_at":       now_iso(),
        "object_detected":   object_detected,
        "condition_checked": condition_checked,
        "is_extinguisher":   (object_detected == "fire_extinguisher") or component_focus,
        "pass":              passed,
        "is_compliant":      passed and not blocked,
        "confidence":        confidence,
        "reason":            reason,
        "worker_message":    worker_message,
        "suggested_action":  suggested_action or "",
        "blocked":           blocked,
    }
    # Attach full analysis to evidence when explicitly requested via env var.
    try:
        if str(os.getenv("DEBUG_CAPTURE_ANALYSIS", "")).strip().lower() in {"1", "true", "yes"}:
            evidence_record["analysis"] = analysis
    except Exception:
        logger.exception("Failed to attach analysis to evidence_record")
    checklist_item.setdefault("evidence", [])
    checklist_item["evidence"].append(evidence_record)

    if blocked:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"]                  = ""
        checklist_item["finding"]                 = finding_text
        checklist_item["action_item"]             = action_text

        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        # Resolve company-level blocked verdict label
        _company_key = str(body.get("company_key", "")).strip()
        _verdict_label = "need_review"
        if _company_key and get_company_config:
            try:
                _cfg = get_company_config(_company_key)
                _verdict_label = _cfg.get("blocked_verdict_label", "need_review")
            except Exception:
                pass

        return build_response(200, {
            "inspection_id":      inspection_id,
            "item_id":            item_id,
            "blocked":            True,
            "move_next":          False,
            "pass":               False,
            "object_detected":    object_detected,
            "condition_checked":  condition_checked,
            "confidence":         confidence,
            "blocked_verdict_label": _verdict_label,
            "message":            worker_message or ("Target component not clear. Move closer and retake." if component_focus else "No fire extinguisher detected. Point camera directly at the extinguisher."),
            "reason":             finding_text,
            "suggested_action":   blocked_suggested_action or action_text,
            "updated_item":       checklist_item,
            "inspection_status":  inspection.get("status", "in_progress"),
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection":         inspection,
            "categories":         inspection.get("categories", []),
        })

    # Record pass/fail answer
    if str(item_id) in ["1"]:
        checklist_item["answer"] = "N/A"
    else:
        checklist_item["answer"] = "Yes" if passed else "No"

    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"]                = finding_text
    checklist_item["action_item"]            = action_text

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item

    next_pos = next_unanswered_index(inspection)
    if next_pos is not None:
        inspection["current_item_index"] = next_pos[1]
    else:
        inspection["current_item_index"] = item_idx

    update_summary_items(inspection)
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    item11, _, _ = find_item(inspection, "11")
    item12, _, _ = find_item(inspection, "12")

    # Resolve company-level verdict label for non-pass results
    _verdict_label_final = None
    if not passed:
        _ck = str(body.get("company_key", "")).strip()
        _verdict_label_final = "need_review"
        if _ck and get_company_config:
            try:
                _verdict_label_final = get_company_config(_ck).get("blocked_verdict_label", "need_review")
            except Exception:
                pass

    resp_body = {
        "inspection_id":      inspection_id,
        "item_id":            item_id,
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
        "summary_item_11":    item11,
        "summary_item_12":    item12,
        "inspection_status":  inspection["status"],
        "current_item_index": inspection.get("current_item_index", 0),
        "next_item_index":    inspection.get("current_item_index", 0),
        "inspection":         inspection,
        "categories":         inspection.get("categories", []),
    }
    if _verdict_label_final is not None:
        resp_body["blocked_verdict_label"] = _verdict_label_final

    return build_response(200, resp_body)


# ═══════════════════════════════════════════════════════════════
# PAUSE / RESUME SESSION
# ═══════════════════════════════════════════════════════════════

def compute_progress(inspection):
    """Compute inspection progress, skipping auto-calculated items 11 & 12."""
    total = 0
    answered = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = int(item.get("id", 0))
            if iid in (11, 12):
                continue
            total += 1
            if item.get("answer", "").strip():
                answered += 1
    percentage = round((answered / total * 100), 1) if total else 0
    return {"total": total, "answered": answered, "percentage": percentage}


def find_next_unanswered(inspection):
    """Return the item ID of the next unanswered checklist item, skipping 11 & 12."""
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            iid = int(item.get("id", 0))
            if iid in (11, 12):
                continue
            if not item.get("answer", "").strip():
                return iid
    return None


def pause_session(event):
    """POST handler — pause an in-progress inspection session."""
    try:
        session_id = (event.get("pathParameters") or {}).get("session_id", "")
        if not session_id:
            return build_response(400, {"error": "Missing session_id"})

        inspection = load_inspection_by_any_id(session_id)
        if not inspection:
            return build_response(404, {"error": f"Inspection not found for session {session_id}"})

        # ── Merge any partial data submitted with the pause request ──
        body = {}
        raw = event.get("body", "")
        if raw:
            try:
                body = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                body = {}

        if body.get("categories"):
            inspection["categories"] = merge_categories(
                inspection.get("categories", []), body["categories"]
            )
        if "general_results" in body:
            inspection["general_results"] = body["general_results"]
        if "notes" in body:
            inspection["notes"] = body["notes"]

        # ── Compute progress ─────────────────────────────────────────
        progress = compute_progress(inspection)

        # ── Update inspection record ─────────────────────────────────
        ts = now_iso()
        inspection["status"] = "paused"
        inspection["last_paused_at"] = ts
        inspection["updated_at"] = ts
        save_inspection(inspection)

        # ── Update session table ─────────────────────────────────────
        try:
            sessions_table.update_item(
                Key={"session_id": inspection.get("session_id", session_id)},
                UpdateExpression="SET #status = :status, progress = :progress, inspection_type = :itype, updated_at = :updated_at, last_paused_at = :paused_at",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": "paused",
                    ":progress": sanitize_for_dynamodb(progress),
                    ":itype": "fire-extinguisher",
                    ":updated_at": now_iso(),
                    ":paused_at": now_iso(),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to update session table: {e}")

        return build_response(200, {
            "session_id": inspection.get("session_id", session_id),
            "inspection_id": inspection.get("inspection_id"),
            "status": "paused",
            "progress": progress,
            "last_paused_at": ts,
        })
    except Exception:
        logger.exception("pause_session failed")
        return build_response(500, {"error": "Internal error while pausing session"})


def resume_session(event):
    """GET handler — resume a paused inspection session."""
    try:
        session_id = (event.get("pathParameters") or {}).get("session_id", "")
        if not session_id:
            return build_response(400, {"error": "Missing session_id"})

        inspection = load_inspection_by_any_id(session_id)
        if not inspection:
            return build_response(404, {"error": f"Inspection not found for session {session_id}"})

        # ── Compute progress & next item ─────────────────────────────
        progress = compute_progress(inspection)
        next_item_id = find_next_unanswered(inspection)

        # ── Update inspection record ─────────────────────────────────
        ts = now_iso()
        inspection["status"] = "in_progress"
        inspection["resumed_at"] = ts
        inspection["updated_at"] = ts
        save_inspection(inspection)

        # ── Update session table ─────────────────────────────────────
        try:
            sessions_table.update_item(
                Key={"session_id": inspection.get("session_id", session_id)},
                UpdateExpression="SET #status = :status, updated_at = :updated_at, resumed_at = :resumed_at",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": "in_progress",
                    ":updated_at": now_iso(),
                    ":resumed_at": now_iso(),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to update session table: {e}")

        result = dict(inspection)
        result["progress"] = progress
        result["next_unanswered_item_id"] = next_item_id
        return build_response(200, result)
    except Exception:
        logger.exception("resume_session failed")
        return build_response(500, {"error": "Internal error while resuming session"})


# ═══════════════════════════════════════════════════════════════
# LAMBDA HANDLER — Main Router
# ═══════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    """
    Main entry point. Routes the request based on HTTP method and path.
    """
    # Async self-invoke: Lambda called itself for background AI work
    if event.get("async_worker"):
        return process_async_analyze_worker(event)

    http_method = event.get("httpMethod", "")
    resource    = event.get("resource", "")
    path        = event.get("path", "")

    logger.info(f"Received: {http_method} {resource} (path: {path})")

    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # ── Original CRUD routes ──────────────────────────────────────────────
    if http_method == "GET" and resource == "/fire-extinguisher-inspection/checklist":
        return get_checklist(event)

    if http_method == "POST" and resource == "/fire-extinguisher-inspection":
        return create_inspection(event)

    if http_method == "GET" and resource == "/fire-extinguisher-inspections":
        return list_inspections(event)

    if http_method == "GET" and resource == "/fire-extinguisher-inspection/{inspection_id}":
        return get_inspection(event)

    # ── Delete inspection by ID (CRUD) ────────────────────────────────────
    if http_method == "DELETE" and resource == "/fire-extinguisher-inspection/{inspection_id}":
        return delete_inspection(event)

    # ── PATCH item ────────────────────────────────────────────────────────
    if http_method == "PATCH" and "/items/" in path and "/note" not in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            items_idx   = parts.index("items")
            inspection_id = parts[session_idx + 1]
            item_id       = parts[items_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            event["pathParameters"]["item_id"]       = item_id
            return update_checklist_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH item path"})

    # ── PATCH note ────────────────────────────────────────────────────────
    if http_method == "PATCH" and "/note" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            items_idx   = parts.index("items")
            inspection_id = parts[session_idx + 1]
            item_id       = parts[items_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            event["pathParameters"]["item_id"]       = item_id
            return add_note_to_item(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid PATCH note path"})

    # ── Slimmed report ────────────────────────────────────────────────────
    if http_method == "GET" and "/report" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx   = parts.index("session")
            inspection_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["inspection_id"] = inspection_id
            return get_inspection_report(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid report path"})

    # ── Delete session ────────────────────────────────────────────────────
    if http_method == "DELETE" and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            session_id  = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = session_id
            return delete_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid delete path"})

    # ── Pause session ─────────────────────────────────────────────────────
    if http_method == "POST" and "/pause" in path and "/session/" in path:
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
    if http_method == "GET" and "/resume" in path and "/session/" in path:
        parts = path.rstrip("/").split("/")
        try:
            session_idx = parts.index("session")
            session_id = parts[session_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["session_id"] = session_id
            return resume_session(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid resume path"})

    # ── Voice command ─────────────────────────────────────────────────────
    if http_method == "POST" and "voice" in path:
        return voice_command(event)

    # ── QR code generation ────────────────────────────────────────────────
    if http_method in ("GET", "POST") and "qr/generate" in path:
        return generate_height_qr(event)

    # ── AI Analyze (async — returns job_id immediately) ───────────────────
    if http_method == "POST" and "analyze" in path and "status" not in path:
        return analyze_item_image(event)

    # ── AI Analyze status polling ─────────────────────────────────────────
    if http_method == "GET" and "analyze/status" in path:
        parts = path.rstrip("/").split("/")
        try:
            status_idx = parts.index("status")
            job_id     = parts[status_idx + 1]
            event.setdefault("pathParameters", {})
            event["pathParameters"]["job_id"] = job_id
            return get_analyze_job_status(event)
        except (ValueError, IndexError):
            return build_response(400, {"error": "Invalid analyze status path"})

    return build_response(404, {"error": f"Route not found: {http_method} {resource}"})
