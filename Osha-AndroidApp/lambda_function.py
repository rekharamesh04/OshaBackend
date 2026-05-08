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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
INSPECTION_TABLE_NAME = os.getenv("INSPECTION_TABLE_NAME", "osha-fire-extinguisher-inspections1")
SESSION_TABLE_NAME = os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions1")
EVIDENCE_BUCKET = os.getenv("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media1")
MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()
YOLO_ENDPOINT_NAME = os.getenv("YOLO_ENDPOINT_NAME", "").strip()
YOLO_DETECTION_CONFIDENCE_THRESHOLD = float(os.getenv("YOLO_DETECTION_CONFIDENCE_THRESHOLD", "0.10"))
YOLO_HARD_BLOCK_THRESHOLD = float(os.getenv("YOLO_HARD_BLOCK_THRESHOLD", "0.10"))

UPLOAD_URL_EXPIRY = int(os.getenv("UPLOAD_URL_EXPIRY", "900"))
DOWNLOAD_URL_EXPIRY = int(os.getenv("DOWNLOAD_URL_EXPIRY", "3600"))


MAX_IMAGE_WIDTH = 800
MAX_IMAGE_QUALITY = 85  # higher quality for component items
YOLO_IMAGE_SIZE = 800  # match your training imgsz exactly

# Single source of truth for blocking low-confidence detections on items 1-6.
# Kept low so Claude's valid pass decisions are not overridden by a tight confidence gate.
IMAGE_CONFIDENCE_BLOCK_THRESHOLD = 0.35

ALLOWED_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/heic",
    "image/heif",
}

# ------------------------------------------------------------------------------
# AWS clients
# ------------------------------------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
sagemaker_runtime = boto3.client("sagemaker-runtime", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

inspection_table = dynamodb.Table(INSPECTION_TABLE_NAME)
session_table = dynamodb.Table(SESSION_TABLE_NAME)

# Module-level thread pool — reused across warm Lambda invocations.
# max_workers=4 is safe for Lambda (128–512 MB RAM). Each thread only
# blocks on I/O (PIL resize, network), so GIL contention is minimal.
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# ------------------------------------------------------------------------------
# Checklist — 12 items
# ------------------------------------------------------------------------------
FIRE_EXTINGUISHER_CHECKLIST = {
    "inspection_type": "Fire Extinguisher Monthly Inspection",
    "general_information": {
        "location": "",
        "start_date": "",
        "checklist": "Fire Extinguisher Monthly Inspection",
        "leader": "",
        "team": []
    },
    "available_answers": ["Yes", "No", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Fire Extinguisher Inspection",
            "items": [
                {
                    "id": 1,
                    "description": "Extinguishers are at minimum within 50 feet of areas of risk.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 2,
                    "description": "Extinguishers are mounted in height that is accessible from a seated position.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 3,
                    "description": "Extinguishers are not obstructed and are easily accessible.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 4,
                    "description": "Extinguishers are marked with proper signage (above the unit and viewable from 180 degrees).",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 5,
                    "description": "Extinguishers' pins and seals are in place.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 6,
                    "description": "Extinguishers are in good, clean condition. No visible damage to units. Units are wiped down & clean.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 7,
                    "description": "Extinguishers' nozzles are free of blockage.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 8,
                    "description": "Extinguishers are fully charged. Pressure gauges show adequate pressure (within green zone) and the gauge glass is intact, clean, and readable.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 9,
                    "description": "Extinguishers' instructions face outward for visibility and are clean, readable, and not blurry, dusty, folded, peeled, or damaged.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 10,
                    "description": "Extinguisher tags are initialed and dated certifying monthly visual inspection took place. Tags must be attached, legible, clean, and not torn, dusty, dirty, blurry, or missing date/initials. Any extinguisher(s) that did not pass, need to be noted in this inspection and brought to compliance through corrective actions.",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 11,
                    "description": "Number of extinguishers inspected:",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                },
                {
                    "id": 12,
                    "description": "Number of extinguishers compliant:",
                    "answer": "", "finding": "", "action_item": "", "responsible": "",
                    "due_date": "", "evidence": [], "blocked_by_wrong_image": False
                }
            ]
        }
    ],
    "general_results": [
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""},
        {"finding": "", "action_item": "", "responsible": "", "due_date": ""}
    ],
    "notes": ""
}

# ------------------------------------------------------------------------------
# Explicit visual rules per checklist item
# ------------------------------------------------------------------------------
CHECKLIST_VISUAL_RULES = {
    "1": "Check if a fire extinguisher is present at the designated location. If the location is empty or the bracket is empty, it is a fail.",
    "1": (
        "RULE — PRESENCE WITHIN 50 FEET OF RISK AREA:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A physical fire extinguisher is clearly and unambiguously visible.\n"
        "  2. The extinguisher is mounted on a wall, bracket, or stand — NOT on the floor unsecured.\n"
        "  3. No obvious risk area (machinery, electrical panel, fuel, chemicals) is visible at a distance "
        "     that looks clearly beyond one normal room length from the extinguisher.\n"
        "FAIL if:\n"
        "  - The bracket or mount is empty.\n"
        "  - No extinguisher is visible at all.\n"
        "  - The extinguisher is lying on the floor with no bracket.\n"
        "NOTE: You cannot measure 50 feet from a photo. If presence is confirmed, mark PASS "
        "and note in reason: 'Distance to risk area could not be verified from image — field validation required.' "
        "Do NOT silently pass without this disclaimer."
    ),
    "2": (
        "RULE — MOUNTED HEIGHT ACCESSIBLE FROM SEATED POSITION:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. The extinguisher body or its handle is at roughly waist-to-shoulder height "
        "     relative to the wall or bracket it is mounted on — NOT near the ceiling.\n"
        "  3. There is no evidence the extinguisher is mounted so high that a person in a wheelchair "
        "     or seated position could not reach the handle.\n"
        "FAIL if:\n"
        "  - The extinguisher handle appears higher than roughly 5 feet from the floor.\n"
        "  - The extinguisher appears mounted near the ceiling or very high up.\n"
        "If height cannot be judged from the image, set pass=false, condition_checked='height_not_verifiable', "
        "and reason must say 'Height could not be verified — retake showing full wall from floor to extinguisher.'"
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
        "RULE — PROPER SIGNAGE VISIBLE FROM 180 DEGREES:\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher is clearly visible.\n"
        "  2. A sign or marker (typically red with white text, or a pictogram of a fire extinguisher) "
        "     is visibly mounted ABOVE or near the extinguisher.\n"
        "  3. The sign appears oriented to be readable from multiple angles.\n"
        "FAIL if:\n"
        "  - No signage is visible above or near the extinguisher.\n"
        "  - The sign is present but facing away, or is so small/faded it cannot be read.\n"
        "If sign visibility is uncertain from the image angle, set pass=false and ask for a retake."
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
        "  The evaluation changes based on how close the shot is:\n\n"
        "  WIDE/FAR SHOT (full extinguisher visible, handle area is small in frame):\n"
        "    PASS if: A ring/loop shape OR colored tag/string is visible near the handle top area.\n"
        "    PASS if: The handle area exists and appears to have something through/on it,\n"
        "             even if you cannot confirm exact type at this distance.\n"
        "    FAIL if: The handle area is CLEARLY and COMPLETELY bare — a handle lever with\n"
        "             absolutely nothing through it, no ring, no string, no tag, nothing.\n"
        "    INCONCLUSIVE → PASS (not fail): If handle area is too small to assess at all,\n"
        "             treat as PASS and note that close-up verification is recommended.\n"
        "             Do NOT fail just because you cannot confirm from this distance.\n\n"
        "  CLOSE/MEDIUM SHOT (handle area takes up reasonable portion of frame):\n"
        "    PASS if: Pin ring/loop is clearly visible through the handle trigger mechanism.\n"
        "    PASS if: Tamper seal (plastic tag, string, zip-tie) is present near pin/handle.\n"
        "    FAIL if: Handle trigger is clearly visible and the hole through it is empty — no pin.\n"
        "    FAIL if: Pin is present but tamper seal is clearly torn off or absent.\n\n"
        "PASS CONDITIONS (any of these = PASS):\n"
        "  1. A metal ring, loop, or bar shape is visible through the handle area.\n"
        "  2. A colored string, tag, or plastic element is visible hanging near the handle/pin.\n"
        "  3. Handle area is too small in frame to assess — benefit of doubt = PASS with note.\n"
        "  4. Any element that could be a pin or seal is visible near the handle top.\n\n"
        "FAIL CONDITIONS (ALL must be true to fail):\n"
        "  1. The extinguisher handle and trigger area is CLEARLY visible in the image, AND\n"
        "  2. The handle hole/trigger mechanism appears COMPLETELY EMPTY — no ring, no loop, AND\n"
        "  3. No colored tag, string, or plastic element is visible anywhere near the handle, AND\n"
        "  4. You are confident this is not just a distance/clarity issue.\n\n"
        "CRITICAL RULE:\n"
        "  From a wide shot — when in doubt, PASS and recommend close-up if needed.\n"
        "  The reason: a missing pin is rare and obvious even from distance (bare handle).\n"
        "  A present pin/seal may be hard to confirm from distance but is still there.\n"
        "  Do NOT fail item 5 from a wide shot just because you cannot confirm detail.\n"
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
        "RULE — NOZZLE/HOSE FREE OF BLOCKAGE AND PHYSICALLY ATTACHED TO A FIRE EXTINGUISHER:\n\n"
        "CRITICAL PREREQUISITE — EXTINGUISHER MUST BE PRESENT AND HOSE MUST BE ATTACHED:\n"
        "  This check is ONLY valid when the hose/nozzle is visibly connected to a fire extinguisher body.\n"
        "  A fire extinguisher body is: a red (or silver/yellow) cylindrical pressure vessel with a gauge and handle.\n"
        "  FAIL immediately if:\n"
        "    - No fire extinguisher body is visible in the image at all.\n"
        "    - The hose or nozzle is detached, removed, or held separately from the extinguisher.\n"
        "    - Only a standalone hose or nozzle is shown without an extinguisher body it connects to.\n"
        "    - The extinguisher body is present but the hose is clearly disconnected from it.\n\n"
        "WHAT YOU ARE CHECKING (ONLY after above prerequisite is confirmed):\n"
        "  Whether the OUTSIDE of the nozzle tip is free of any external covering or blockage,\n"
        "  and whether the hose and nozzle are physically undamaged and still attached to the extinguisher.\n"
        "  You are NOT required to see INTO the nozzle bore — this is physically impossible\n"
        "  from most photo angles and is NOT part of this check.\n\n"
        "ACCEPTED PHOTO ANGLES:\n"
        "  Front-facing, side-angle, top-down — all are valid.\n"
        "  The extinguisher body does NOT need to fill the entire frame,\n"
        "  but it MUST be visibly present and the hose MUST be connected to it.\n\n"
        "PASS only if ALL of the following are true:\n"
        "  1. A fire extinguisher body is clearly visible in the image.\n"
        "  2. The hose is visibly connected/attached to the extinguisher body (not detached or held separately).\n"
        "  3. The nozzle tip or hose end is visible from any angle.\n"
        "  4. No material is visibly COVERING the OUTSIDE of the nozzle tip:\n"
        "     (no tape wrapped around it, no plastic cap, no cloth tied over it, no packed debris at the tip).\n"
        "  5. The hose is not kinked, crushed, or tied shut.\n"
        "  6. The nozzle/horn is physically intact — not cracked, melted, or broken off.\n\n"
        "FAIL if:\n"
        "  - No fire extinguisher body is visible.\n"
        "  - The hose is detached from the extinguisher or only shown in isolation.\n"
        "  - The nozzle tip or hose end is NOT visible anywhere in the image — \n"
        "    even if the extinguisher body is present and looks fine.\n"
        "  - Only the top handle/pin area of an extinguisher is visible with NO hose visible at all.\n"
        "  - Tape, a cap, cloth, or any material is visibly COVERING the outside of the nozzle tip.\n"
        "  - The hose is kinked, crushed, or tied so it cannot discharge.\n"
        "  - The nozzle tip is cracked, melted, or broken.\n\n"
        "CRITICAL ANTI-HALLUCINATION RULE:\n"
        "  If you cannot SEE the actual nozzle tip or hose end in the image, you CANNOT confirm\n"
        "  it is 'unobstructed' or 'securely attached'. Do NOT assume compliance.\n"
        "  A beautiful extinguisher body with NO visible hose/nozzle = FAIL.\n"
        "  You must literally see the nozzle or hose end to pass this item.\n\n"
        "DO NOT FAIL because:\n"
        "  - You cannot see INTO the nozzle opening (not required).\n"
        "  - The photo is taken from the side or an angle (any angle is fine as long as extinguisher is present).\n"
        "  - There is shadow inside the nozzle opening (shadow ≠ blockage)."
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
        "- The gauge is broken, cracked, or missing.\n"
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
        "ITEM 10 — INSPECTION TAG:\n\n"
        "DEFAULT ASSUMPTION: Tags with a visible grid ARE valid inspection records.\n"
        "A pre-printed year grid (2025/2026/2027/2028/2029) with ANY physical mark, "
        "hole, or darkening in a recent year cell = PASS.\n\n"
        "PASS if ALL are true:\n"
        "1. A tag is physically attached and visible.\n"
        "2. ANY of the following is true for year 2025 or later:\n"
        "   a. A punched hole is visible in that year's cell.\n"
        "   b. A written/stamped mark, pen stroke, or ink mark is in that year's cell.\n"
        "   c. The cell appears darker, circled, or physically marked vs blank cells.\n"
        "   d. You can see any indication that the 2026 (or later) cell was acted upon.\n\n"
        "FAIL ONLY IF:\n"
        "- No tag is visible at all, OR\n"
        "- The tag is completely destroyed/unreadable, OR\n"
        "- ALL year cells from 2025 onward are clearly and completely blank with zero marks.\n\n"
        "DO NOT FAIL because:\n"
        "- You cannot see a clean circular hole (punch holes vary in appearance).\n"
        "- A year was skipped (2026 punched, 2025 blank = VALID).\n"
        "- Future years (2027–2029) are blank (normal, grid is pre-printed).\n"
        "- You cannot read initials or a specific month.\n"
        "- The image is slightly blurry but a mark is still visible.\n\n"
        "IMPORTANT: When you see '2026 JAN' or similar on a tag that appears marked, "
        "that IS the inspection record. Set recent_year_visible=yes.\n"
    ),
}

VALIDATION_KEYWORDS = {
    "1": ["extinguisher", "present", "mounted", "bracket", "visible", "fire"],
    "2": ["height", "mounted", "accessible", "reach", "high", "low", "wall", "bracket"],
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
    "8": ["gauge", "pressure", "needle", "green"],
    "9": ["label", "instruction", "instructions", "facing outward", "front-facing", "aligned",
           "readable", "visible", "clear", "clean", "blurry", "dust", "damaged", "peeled",
           "folded", "attached", "legible"],
    "10": ["tag", "date", "dated", "initial", "initialed", "torn", "dusty", "dirty",
            "missing", "attached", "legible", "blurry", "illegible", "clean", "intact",
            "visible", "month", "signed", "inspection"],
}

# ------------------------------------------------------------------------------
# System prompts
# ------------------------------------------------------------------------------
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
   - BAD reason: "The extinguisher looks fine."
   - GOOD reason: "The safety pin is clearly visible through the handle, and a yellow plastic tamper seal is intact around the pin."
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
You are analyzing CLOSE-UP images of specific components (pressure gauge, label, tag).
Your decisions directly affect worker safety. When in doubt, FAIL.

═══════════════════════════════════════════════════════════════
GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a condition is met, set pass=false.
Never assume compliance from an unclear or partial image.
═══════════════════════════════════════════════════════════════

IMPORTANT CONTEXT:
  - For items 8–10, the full extinguisher body does NOT need to be visible.
  - You are evaluating a SPECIFIC COMPONENT at close range.
  - Even a perfectly clear extinguisher body is irrelevant — only the target component matters.

STEP 1 — TARGET COMPONENT VISIBILITY:
  Confirm whether the requested target component is clearly and fully visible.
  If it is blurry, partially cut off, in shadow, or not present → set pass=false.

STEP 2 — CONDITION CHECK:
  Follow the strict visual rule for this checklist item EXACTLY.
  Apply each sub-condition. If any sub-condition fails → overall pass=false.

STEP 3 — CHECKS OBJECT:
  Return the required `checks` fields per the contract in the user prompt.
  Every field must be present with an explicit enum value — no nulls, no omissions.
  If a field state is unclear → use "unclear" as the value AND set pass=false.

ABSOLUTE RULES:
1. pass=true only when ALL required checks pass their strict enum values.
2. "unclear" in any check field → pass=false. No exceptions.
3. Do NOT guess. Use only what is visibly clear in the image.
4. reason must describe what you specifically see for the component.
5. worker_message must be actionable and specific to what was wrong.

CORE EVIDENCE POLICY:
- Judge only what is directly visible in the image.
- Do not infer compliance or failure from dominant background colors.
- For gauges, judge only whether the needle is pointing into the green zone.
- If the required evidence is visible and readable, use it even in a far shot or at an angle.
- If the required evidence is not clearly visible, fail.
- Never guess on safety-critical checks.




ITEM 8 SPECIAL RULE — PRESSURE GAUGE:
  Judge needle zone by where it points, not by the color of the needle itself.
  A yellow, black, or white needle is normal.

  TYPE B GAUGE (red-dominant face):
    The gauge face may be mostly red and that is normal.
    CRITICAL: Do NOT mistake the printed white outer scale lines or the printed "195" marking as the moving needle.
    If you mistake the printed lines for the needle, you will hallucinate a PASS on an overcharged gauge!
    Identify the true needle attached to the center pivot.
    PASS only when the TRUE needle is clearly inside the green band.
    Needle clearly inside the green band = PASS

  TYPE A GAUGE (common in Asia/India, e.g. Fire Boss):
    Only two zones. RED on LEFT = recharge = FAIL. GREEN on RIGHT = PASS.
    Needle pointing right = PASS.

  TYPE C GAUGE (European sweep gauges: SAFESTAR, Gloria, Thomas):
    Numeric scale sweeps in arc. Read the number the needle points to.
    If within the labeled green band (typically 10–20 bar) = needle_zone = green = PASS.
    Red zone takes up most of the arc — do NOT assume red just because red is dominant.

  FOR ALL TYPES:
    needle_zone = green → PASS (if glass is intact and readable)
    needle_zone = red_recharge or red_overcharge or yellow → FAIL
    needle_zone = unclear → FAIL (inconclusive = fail)
    Fogged, cracked, or dusty glass → gauge_glass_condition ≠ intact → FAIL

ITEM 10 SPECIAL RULE — INSPECTION TAG:
  MAKE THIS CHECK EXTREMELY FORGIVING BUT REQUIRE A PUNCH.
  If a tag is visible and you can confirm a PUNCH HOLE or MARK on ANY RECENT YEAR (2025, 2026, 2027+) in the grid, PASS it.
  - Do NOT fail if a year is skipped (e.g. 2026 is punched but 2025 is empty).
  - Do NOT fail if you see future years (e.g. 2028, 2029) unpunched. The printed grid itself is normal context.
  - A punched hole in the year grid = VALID INSPECTION MARK.
  - DO NOT require a specific month, inspector signature, or initials to be visible.
  - Fail only if there is no tag, or if there is NO punch on 2025, NO punch on 2026, and NO punch on any future year.

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
    - item 10: inspection tag must be present, attached, clean enough to read, and not torn or missing.
    Keep the reply short and practical.
"""


def voice_item_hint(item_id: str) -> str:
    hints = {
        "7": "Check nozzle and hose. If blocked, covered, clogged, or obstructed, retake after clearing it.",
        "8": "Check pressure gauge. Keep needle visible and in the green zone.",
        "9": "Check instruction label. Keep it aligned, facing outward, and readable.",
        "10": "Check inspection tag. Keep it present, attached, clean, and readable; replace if torn or missing.",
    }
    return hints.get(str(item_id), "")


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        },
        "body": json.dumps(body, default=str),
    }


def deep_copy_checklist() -> dict:
    return copy.deepcopy(FIRE_EXTINGUISHER_CHECKLIST)


def get_query(event: dict, key: str, default: str = "") -> str:
    return (event.get("queryStringParameters") or {}).get(key, default) or default


def parse_body(event: dict) -> dict:
    body = event.get("body")
    if body in (None, ""):
        return {}
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, dict):
        return body
    return json.loads(body)


def safe_json_parse(text: str) -> dict:
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
            return json.loads(text[start: end + 1])
        except Exception:
            return {"error": "parse_failed", "raw": text}
    return {"error": "parse_failed", "raw": text}


def get_route(event: dict) -> Tuple[str, str, str]:
    method = (event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
    path = event.get("path") or event.get("rawPath") or ""
    resource = event.get("resource") or ""
    return method, path, resource


def path_endswith(path: str, suffix: str) -> bool:
    return path.rstrip("/").endswith(suffix.rstrip("/"))


def _normalized_headers(event: dict) -> dict:
    headers = event.get("headers") or {}
    return {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in headers.items()}


def require_api_key(event: dict) -> Optional[dict]:
    if not EXPECTED_API_KEY:
        return json_response(500, {"error": "Server API_KEY env var is not configured"})
    headers = _normalized_headers(event)
    provided_key = (
        headers.get("x-api-key") or headers.get("x_api_key") or
        headers.get("api_key") or headers.get("apikey") or ""
    ).strip()
    if not provided_key or provided_key != EXPECTED_API_KEY:
        return json_response(403, {"error": "Forbidden", "message": "Invalid or missing API key"})
    return None


def get_all_items(inspection: dict) -> List[dict]:
    items = []
    for cat in inspection.get("categories", []):
        items.extend(cat.get("items", []))
    return items


def find_item(inspection: dict, item_id: str) -> Tuple[Optional[dict], Optional[int], Optional[int]]:
    for cat_idx, cat in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(cat.get("items", [])):
            if str(item.get("id")) == str(item_id):
                return item, cat_idx, item_idx
    return None, None, None


def next_unanswered_index(inspection: dict) -> Optional[Tuple[int, int]]:
    for cat_idx, cat in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(cat.get("items", [])):
            if item.get("answer", "") == "":
                return cat_idx, item_idx
    return None


def compute_status(inspection: dict) -> str:
    items = get_all_items(inspection)
    if any(item.get("answer", "") == "" for item in items):
        return "in_progress"
    if any(item.get("answer") == "No" for item in items):
        return "failed"
    return "passed"


def extract_text_from_claude_response(resp: dict) -> str:
    content = resp.get("content") or []
    if not content:
        return ""
    first = content[0]
    if isinstance(first, dict):
        return first.get("text", "")
    return ""



def prepare_image_bytes(image_bytes: bytes, content_type: str = "image/jpeg", for_yolo: bool = False) -> Tuple[bytes, str]:
    """
    Resize image for sending to backend services.
    for_yolo=True:  Resize longest side to YOLO_IMAGE_SIZE, preserve aspect ratio.
                  Do NOT letterbox here — SageMaker YOLO endpoint handles its own letterbox internally.
                  Use higher quality to preserve small component details.
    for_yolo=False: Standard resize for Bedrock (Claude), max width 800px.
    """
    if Image is None:
        return image_bytes, content_type
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")

        if for_yolo:
            # Resize longest side to YOLO_IMAGE_SIZE, keep aspect ratio.
            # YOLO endpoint preprocesses internally with its own letterbox — do NOT add one here.
            longest = max(img.width, img.height)
            if longest > YOLO_IMAGE_SIZE:
                scale = YOLO_IMAGE_SIZE / longest
                new_w = max(1, int(img.width * scale))
                new_h = max(1, int(img.height * scale))
                img = img.resize((new_w, new_h), Image.LANCZOS)
            quality = 92  # high quality — small components (nozzle, gauge) need detail
        else:
            # Standard resize for Bedrock
            if img.width > MAX_IMAGE_WIDTH:
                ratio = MAX_IMAGE_WIDTH / float(img.width)
                new_size = (MAX_IMAGE_WIDTH, max(1, int(img.height * ratio)))
                img = img.resize(new_size, Image.LANCZOS)
            quality = MAX_IMAGE_QUALITY

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning(f"Image preparation failed: {str(e)}")
        return image_bytes, content_type


def convert_floats_to_decimal(obj: Any) -> Any:
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(v) for v in obj]
    return obj


def sanitize_for_dynamodb(item: dict) -> dict:
    return convert_floats_to_decimal(item)


def load_inspection(inspection_id: str) -> Optional[dict]:
    resp = inspection_table.get_item(Key={"inspection_id": inspection_id})
    item = resp.get("Item")
    return convert_floats_to_decimal(item) if item else None


def load_inspection_by_session_id(session_id: str) -> Optional[dict]:
    session_id = str(session_id or "").strip()
    if not session_id:
        return None

    try:
        session = session_table.get_item(Key={"session_id": session_id}).get("Item")
    except Exception:
        return None

    if not session:
        return None

    inspection_id = str(session.get("inspection_id", "")).strip()
    if not inspection_id:
        return None

    return load_inspection(inspection_id)


def load_inspection_by_any_id(value: str) -> Optional[dict]:
    value = str(value or "").strip()
    if not value:
        return None
    item = load_inspection(value)
    if item:
        return item
    return load_inspection_by_session_id(value)


def save_inspection(item: dict) -> None:
    inspection_table.put_item(Item=sanitize_for_dynamodb(item))


def merge_item_records(existing_item: dict, incoming_item: dict) -> dict:
    merged = copy.deepcopy(existing_item or {})
    incoming_item = incoming_item or {}

    for key, incoming_value in incoming_item.items():
        if key == "evidence":
            if isinstance(incoming_value, list) and incoming_value:
                merged[key] = incoming_value
            elif key not in merged:
                merged[key] = []
            continue

        if key == "blocked_by_wrong_image":
            merged[key] = bool(incoming_value)
            continue

        if isinstance(incoming_value, str):
            if incoming_value.strip():
                merged[key] = incoming_value
            elif key not in merged:
                merged[key] = incoming_value
            continue

        if isinstance(incoming_value, list):
            if incoming_value:
                merged[key] = incoming_value
            elif key not in merged:
                merged[key] = incoming_value
            continue

        if incoming_value is not None:
            merged[key] = incoming_value

    return merged


def merge_categories(existing_categories: List[dict], incoming_categories: List[dict]) -> List[dict]:
    existing_by_id = {str(cat.get("id")): cat for cat in (existing_categories or []) if isinstance(cat, dict)}
    merged_categories: List[dict] = []

    for incoming_cat in incoming_categories or []:
        if not isinstance(incoming_cat, dict):
            continue

        cat_id = str(incoming_cat.get("id", ""))
        existing_cat = existing_by_id.get(cat_id, {})
        merged_cat = copy.deepcopy(existing_cat) if existing_cat else {}

        for key, value in incoming_cat.items():
            if key == "items":
                continue
            if isinstance(value, str):
                if value.strip():
                    merged_cat[key] = value
                elif key not in merged_cat:
                    merged_cat[key] = value
            elif value is not None:
                merged_cat[key] = value

        existing_items = existing_cat.get("items", []) if isinstance(existing_cat, dict) else []
        existing_items_by_id = {
            str(item.get("id")): item for item in existing_items if isinstance(item, dict)
        }

        merged_items: List[dict] = []
        for incoming_item in incoming_cat.get("items", []) if isinstance(incoming_cat.get("items", []), list) else []:
            if not isinstance(incoming_item, dict):
                continue
            item_id = str(incoming_item.get("id", ""))
            existing_item = existing_items_by_id.get(item_id, {})
            merged_items.append(merge_item_records(existing_item, incoming_item))

        if not merged_items and isinstance(existing_items, list):
            merged_items = copy.deepcopy(existing_items)

        merged_cat["items"] = merged_items
        merged_categories.append(merged_cat)

    return merged_categories if merged_categories else copy.deepcopy(existing_categories or [])


def save_async_job(job_id: str, status: str, payload: Optional[dict] = None, result: Optional[dict] = None, error: Optional[str] = None) -> None:
    record = {
        "session_id": job_id,
        "job_type": "analyze",
        "job_status": status,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    if payload is not None:
        record["payload"] = payload
    if result is not None:
        record["result"] = result
    if error is not None:
        record["error"] = error
    session_table.put_item(Item=sanitize_for_dynamodb(record))


def load_async_job(job_id: str) -> Optional[dict]:
    resp = session_table.get_item(Key={"session_id": job_id})
    item = resp.get("Item")
    return convert_floats_to_decimal(item) if item else None


def start_async_analyze_job(payload: dict) -> dict:
    job_id = f"job_{uuid.uuid4().hex}"
    save_async_job(job_id, "queued", payload=payload)

    worker_event = {
        "async_worker": True,
        "job_id": job_id,
        "payload": payload,
    }

    function_name = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "")
    if not function_name:
        save_async_job(job_id, "failed", payload=payload, error="AWS_LAMBDA_FUNCTION_NAME is not configured")
        return {
            "job_id": job_id,
            "status": "failed",
            "message": "Async worker could not be started.",
        }

    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(worker_event).encode("utf-8"),
        )
    except Exception as e:
        logger.exception("Failed to enqueue async analyze job")
        save_async_job(job_id, "failed", payload=payload, error=str(e))
        return {
            "job_id": job_id,
            "status": "failed",
            "message": f"Failed to queue async job: {str(e)}",
        }

    return {
        "job_id": job_id,
        "status": "queued",
        "message": "Analysis queued. Poll the status endpoint.",
        "status_url": f"/fire-extinguisher/analyze/status/{job_id}",
    }


def process_async_analyze_worker(event: dict) -> dict:
    job_id = str(event.get("job_id", "")).strip()
    payload = event.get("payload") or {}
    if not job_id:
        return {"ok": False, "error": "job_id is required"}

    save_async_job(job_id, "processing", payload=payload)

    try:
        analyze_event = {
            "body": json.dumps(payload),
            "isBase64Encoded": False,
            "headers": {},
        }
        response = analyze_item_image(analyze_event)
        response_body = response.get("body") if isinstance(response, dict) else response
        parsed_body = safe_json_parse(response_body) if isinstance(response_body, str) else response_body
        job_status = "completed"
        if isinstance(parsed_body, dict) and parsed_body.get("blocked") is True:
            job_status = "completed"

        # NOTE: Do NOT attach full_inspection to the stored job result.
        # Storing the full inspection inside the DynamoDB job record doubles
        # the serialised payload and causes 502 errors when the polling client
        # reads it back.  The polling handler (get_analyze_job_status) re-fetches
        # a fresh inspection on its own when the job is complete.
        save_async_job(job_id, job_status, payload=payload, result=parsed_body)
        return {"ok": True, "job_id": job_id, "status": job_status}
    except Exception as e:
        logger.exception("Async analyze worker failed")
        save_async_job(job_id, "failed", payload=payload, error=str(e))
        return {"ok": False, "job_id": job_id, "error": str(e)}


def generate_s3_download_url(file_key: str) -> Optional[str]:
    try:
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": EVIDENCE_BUCKET, "Key": file_key},
            ExpiresIn=DOWNLOAD_URL_EXPIRY,
        )
    except Exception:
        return None


def checklist_rule_for_item(item_id: str, checklist_item: dict) -> str:
    return CHECKLIST_VISUAL_RULES.get(str(item_id), checklist_item.get("description", ""))


def expected_keywords_for_item(item_id: str) -> List[str]:
    return VALIDATION_KEYWORDS.get(str(item_id), [])


def required_yolo_class_for_item(item_id: str) -> Optional[str]:
    return None  # Claude handles all items directly — no YOLO gate


def item_zoom_hint(item_id: str) -> str:
    hints = {
        "7": "Show the full extinguisher with hose attached and nozzle tip clearly visible.",
        "8": "Zoom in on the pressure gauge. Use the actual moving pressure needle, keep the gauge glass intact, and verify it is in the green zone.",
        "9": "Zoom in on the instruction label. Keep label text facing camera, sharp, and readable.",
        "10": "Zoom in on the inspection tag. Keep date, initials, and tag edges visible and readable.",
    }
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
    contracts = {
        "7": (
            "Return checks with EXACTLY these fields and ONLY these enum values:\n"
            "  extinguisher_body_visible: true|false\n"
            "    true = a fire extinguisher cylindrical body is clearly visible in the image.\n"
            "    false = no extinguisher body visible (e.g. only a standalone hose, only a hand holding nozzle, empty frame).\n"
            "  hose_attached_to_extinguisher: true|false\n"
            "    true = the hose is visibly connected/attached to an extinguisher body in this image.\n"
            "    false = the hose is detached, held in isolation, or the extinguisher has no hose visible.\n"
            "  target_visible: true|false  (is nozzle/hose visible from ANY angle?)\n"
            "  nozzle_tip_visible: true|false  (can you see the nozzle tip from this angle? any angle counts)\n"
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
            "  - If only a standalone hose or nozzle is shown with no extinguisher body → extinguisher_body_visible=false, hose_attached_to_extinguisher=false → FAIL.\n"
            "  - If the extinguisher body is present but only the top (handle/pin) is visible and NO hose is in frame → hose_attached_to_extinguisher=false → FAIL.\n"
            "  - external_obstruction=none if the outside of the tip is clean and uncovered.\n"
            "  - Do NOT set external_obstruction=unclear just because you cannot see into the bore.\n"
            "  - Shadow inside the opening ≠ blockage. external_obstruction=none in that case.\n"
            "  - A side-angle shot of a clean nozzle attached to an extinguisher body = PASS."
        ),
        "8": (
            "Return checks with EXACTLY these fields and ONLY these enum values:\n"
            "  target_visible: true|false\n"
            "    true  = a pressure gauge is clearly visible in the image.\n"
            "    false = no gauge visible, or gauge is cut off / completely obscured.\n"
            "  needle_zone: green|red_recharge|red_overcharge|unclear\n"
            "    green          = the physical needle tip is pointing INTO the green zone on the gauge face.\n"
            "    red_recharge   = the needle tip is in a red zone on the LOW pressure side.\n"
            "    red_overcharge = the needle tip is in a red zone on the HIGH pressure side.\n"
            "    unclear        = cannot confidently determine which zone the needle tip is in.\n"
            "  gauge_face_readability: readable|unreadable|unclear\n"
            "    readable   = gauge face and colored zones are visible and legible.\n"
            "    unreadable = heavily fogged, dusty, or obscured so zones cannot be read.\n"
            "  gauge_glass_condition: intact|cracked|missing|unclear\n"
            "    intact  = gauge glass is whole with no cracks.\n"
            "    cracked = gauge glass is visibly cracked or broken.\n\n"
            "HOW TO DETERMINE needle_zone (this is the only decision that matters):\n"
            "  Step 1: Find the physical needle. It is the MOVING pointer attached to the center pivot.\n"
            "          It is NOT a printed scale line, number, or arc printed on the gauge face.\n"
            "  Step 2: Look at which COLORED ZONE the tip of that needle is pointing into.\n"
            "  Step 3: Set needle_zone to that color. The zone color is the only factor.\n"
            "  - Tip in GREEN zone → needle_zone = green\n"
            "  - Tip in RED zone   → needle_zone = red_recharge or red_overcharge\n"
            "  - Cannot clearly tell → needle_zone = unclear\n\n"
            "PASS rule: target_visible=true AND needle_zone=green AND "
            "gauge_face_readability=readable AND gauge_glass_condition=intact.\n"
            "ANY other combination → pass=false.\n\n"
        "CRITICAL: A red-dominant gauge FACE is normal on many gauge types.\n"
            " - Do NOT set needle_zone=red just because the face background is red.\n"
            "- Judge ONLY the colored zone the needle tip points into."
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
            "Return checks with EXACTLY these fields and ONLY these enum values:\n"
            "  target_visible: true|false\n"
            "  tag_physically_attached: attached|detached|unclear\n"
            "  recent_year_visible: yes|no|unclear\n"
            "    recent_year_visible=yes if ANY mark, hole, or darkening is visible\n"
            "    in ANY year cell from 2025 onward. Be generous — marks vary in appearance.\n"
            "  year_identified: <the year you see marked, e.g. '2026', or 'none'>\n"
            "PASS rule: target_visible=true AND tag_physically_attached=attached AND recent_year_visible=yes.\n"
            "CRITICAL: If you can see '2026' marked in any way on the tag, year_identified='2026' "
            "and recent_year_visible=yes. Do not require a perfect punch hole.\n"
        ),
    }
    return contracts.get(str(item_id), "Return checks object for requested component with explicit enum states.")


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "yes", "1"}:
        return True
    if text in {"false", "no", "0"}:
        return False
    return None


def _to_state(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def enforce_component_checks(
    item_id: str,
    analysis: dict,
    passed: bool,
    condition_checked: str,
    reason: str,
    worker_message: str,
    suggested_action: Optional[str],
) -> Tuple[bool, str, str, str, Optional[str]]:
    """
    Enforce strict component-level pass/fail rules regardless of what Claude returned.
    This is the safety net that catches cases where Claude's pass=true is incorrect.
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

    def force_fail(
        checked: str,
        fail_reason: str,
        msg: str,
        action: str,
    ) -> Tuple[bool, str, str, str, Optional[str]]:
        analysis["pass"] = False
        return False, checked, fail_reason, msg, action

    # ── Gate 1: target must be visible ──────────────────────────────────────
    target_visible = _bool("target_visible")
    if target_visible is not True:
        return force_fail(
            "target_not_visible",
            reason or "Target component is not clearly visible in the image.",
            "Move closer. Keep the target component fully visible and in focus.",
            "Retake close-up image with target component fully visible and in sharp focus.",
        )

    item = str(item_id)

    # ── Item 7: Nozzle ───────────────────────────────────────────────────────
    if item == "7":
        extinguisher_body_visible      = _bool("extinguisher_body_visible")
        hose_attached_to_extinguisher  = _bool("hose_attached_to_extinguisher")
        nozzle_tip_visible             = _bool("nozzle_tip_visible")
        external_obstruction           = _state("external_obstruction")
        hose_condition                 = _state("hose_condition")
        nozzle_physical_condition      = _state("nozzle_physical_condition")

        # Gate 0: Extinguisher body must be present
        if extinguisher_body_visible is not True:
            return force_fail(
                "no_extinguisher_body_visible",
                reason or "No fire extinguisher body is visible in the image. The hose/nozzle must be shown attached to an extinguisher.",
                "Point camera at the extinguisher. Hose must be attached and visible.",
                "Retake image showing the hose/nozzle attached to the fire extinguisher body.",
            )

        # Gate 1: Hose must be attached to extinguisher
        if hose_attached_to_extinguisher is not True:
            return force_fail(
                "hose_not_attached_to_extinguisher",
                reason or "The hose or nozzle appears detached from the fire extinguisher body. It must be physically connected to the extinguisher to pass.",
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
            return force_fail(
                "nozzle_failed",
                reason or f"Nozzle check failed: {detail}.",
                msg,
                action,
            )
        analysis["pass"] = True
        return True, (condition_checked or "nozzle_tip_clear_and_intact"), reason, worker_message, suggested_action

    # ── Item 8: Pressure Gauge ───────────────────────────────────────────────
    if item == "8":
        needle_zone = _state("needle_zone")
        gauge_face_readability = _state("gauge_face_readability")
        gauge_glass_condition = _state("gauge_glass_condition")

        fail_conditions = []
        if needle_zone != "green":
            if needle_zone == "red_recharge":
                fail_conditions.append("needle in RED RECHARGE zone — extinguisher needs recharging")
            elif needle_zone == "red_overcharge":
                fail_conditions.append("needle in RED OVERCHARGE zone — pressure is too high")
            elif needle_zone == "yellow":
                fail_conditions.append("needle in YELLOW zone — pressure is marginal")
            else:
                fail_conditions.append(f"needle zone is '{needle_zone}' — not confirmed as green")

        if gauge_face_readability not in ("readable", ""):
            fail_conditions.append(f"gauge face readability={gauge_face_readability}")
        if gauge_glass_condition not in ("intact", ""):
            fail_conditions.append(f"gauge glass condition={gauge_glass_condition}")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "recharge" in detail:
                action = "Extinguisher needs recharging — remove from service and replace immediately."
                msg    = "Pressure too low. Extinguisher needs recharging. Remove from service."
            elif "overcharge" in detail:
                action = "Extinguisher is overcharged — remove from service and have it inspected."
                msg    = "Pressure too high. Remove from service for inspection."
            elif "glass" in detail:
                action = "Gauge glass is damaged. Replace or service the extinguisher."
                msg    = "Gauge glass is damaged. Extinguisher needs servicing."
            else:
                action = "Retake clear close-up of gauge with needle and zone clearly visible."
                msg    = "Gauge unreadable. Retake closer, clearer image of gauge face."
            return force_fail(
                "gauge_failed",
                reason or f"Pressure gauge failed: {detail}.",
                msg,
                action,
            )
        analysis["pass"] = True
        return True, (condition_checked or "gauge_needle_in_green_zone"), reason, worker_message, suggested_action

    # ── Item 9: Instruction Label ────────────────────────────────────────────
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

    # ── Item 10: Inspection Tag ──────────────────────────────────────────────
    if item == "10":
        tag_physically_attached = _state("tag_physically_attached")
        recent_year_visible     = _state("recent_year_visible")

        fail_conditions = []
        if tag_physically_attached != "attached":
            fail_conditions.append("tag is NOT attached to the extinguisher")
        if recent_year_visible != "yes":
            fail_conditions.append("a recent year (2025 or later) is not visible on the tag")

        if fail_conditions:
            detail = "; ".join(fail_conditions)
            if "NOT attached" in detail:
                msg    = "Tag is missing or detached. Attach a new inspection tag."
                action = "Attach a new inspection tag with current date and initials."
            elif "DATE" in detail or "INITIALS" in detail:
                msg    = "Tag is present but date or initials are not readable. Update and retake."
                action = "Update tag with current date and initials. Retake close-up of the tag."
            else:
                msg    = "Inspection tag failed condition check. Fix and retake."
                action = "Replace or update inspection tag with current signed monthly details."
            return force_fail(
                "tag_failed",
                reason or f"Inspection tag failed: {detail}.",
                msg,
                action,
            )
        analysis["pass"] = True
        return True, (condition_checked or "tag_attached_legible_dated_initialed"), reason, worker_message, suggested_action

    # ── Fallback for unexpected item_ids ────────────────────────────────────
    return passed, condition_checked, reason, worker_message, suggested_action


def best_detection_confidence(detections: List[dict], class_name: str) -> float:
    best = 0.0
    for d in detections or []:
        detected_class = str(d.get("class_name", "")).strip().lower().replace("-", "_")
        if detected_class != class_name:
            continue
        try:
            conf = float(d.get("confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        if conf > best:
            best = conf
    return best


# ------------------------------------------------------------------------------
# YOLO endpoint integration
# ------------------------------------------------------------------------------
def _normalize_yolo_detections(payload: dict) -> List[dict]:
    normalized = []
    if not isinstance(payload, dict):
        return normalized

    detections = payload.get("detections")
    if isinstance(detections, list):
        for d in detections:
            if not isinstance(d, dict):
                continue
            class_name = str(
                d.get("class_name") or d.get("class") or d.get("label") or ""
            ).strip().lower().replace("-", "_")
            if not class_name:
                continue
            confidence = d.get("confidence", d.get("score", 0.0))
            try:
                confidence = float(confidence)
            except Exception:
                confidence = 0.0
            bbox = d.get("bbox", d.get("box", []))
            normalized.append({"class_name": class_name, "confidence": confidence, "bbox": bbox})

    class_confidences = payload.get("class_confidences")
    if isinstance(class_confidences, dict):
        for k, v in class_confidences.items():
            class_name = str(k).strip().lower().replace("-", "_")
            try:
                confidence = float(v)
            except Exception:
                confidence = 0.0
            normalized.append({"class_name": class_name, "confidence": confidence, "bbox": []})

    classes = payload.get("classes")
    if isinstance(classes, list):
        for c in classes:
            class_name = str(c).strip().lower().replace("-", "_")
            if class_name:
                normalized.append({"class_name": class_name, "confidence": 1.0, "bbox": []})

    return normalized


def invoke_yolo_endpoint(image_bytes: bytes, content_type: str = "image/jpeg") -> Optional[List[dict]]:
    if not YOLO_ENDPOINT_NAME:
        return None
    started = time.time()
    try:
        logger.info(f"[YOLO] invoking endpoint={YOLO_ENDPOINT_NAME} image_size={len(image_bytes)} content_type={content_type}")
        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=YOLO_ENDPOINT_NAME,
            ContentType=content_type,
            Body=image_bytes,
        )
        raw = response.get("Body").read().decode("utf-8")
        logger.info(f"[YOLO] done in {time.time() - started:.2f}s raw_preview={raw[:200]}")
        payload = safe_json_parse(raw)
        if payload.get("error"):
            logger.warning(f"YOLO endpoint parse failed: {payload}")
            return None
        return _normalize_yolo_detections(payload)
    except Exception as e:
        logger.warning(f"[YOLO] FAILED in {time.time() - started:.2f}s error={str(e)}")
        return None


def evaluate_item_by_yolo_detections(item_id: str, detections: List[dict]) -> dict:
    best_conf_by_class = {}
    for d in detections or []:
        class_name = str(d.get("class_name", "")).strip().lower().replace("-", "_")
        if not class_name:
            continue
        try:
            conf = float(d.get("confidence", 0.0) or 0.0)
        except Exception:
            conf = 0.0
        if class_name not in best_conf_by_class or conf > best_conf_by_class[class_name]:
            best_conf_by_class[class_name] = conf

    def present(label: str) -> bool:
        return best_conf_by_class.get(label, 0.0) >= YOLO_DETECTION_CONFIDENCE_THRESHOLD

    if str(item_id) == "7":
        if present("nozzle"):
            return {"status": "pass", "reason": "Nozzle is detected and appears visible.", "condition_checked": "nozzle_visible", "confidence": best_conf_by_class.get("nozzle", 0.0)}
        return {"status": "fail", "reason": "Nozzle is not detected clearly.", "condition_checked": "nozzle_not_visible", "confidence": best_conf_by_class.get("nozzle", 0.0), "suggested_action": "Retake image with nozzle/hose clearly visible."}

    if str(item_id) == "8":
        if not present("pressure_gauge"):
            return {"status": "fail", "reason": "Pressure gauge is not detected clearly.", "condition_checked": "gauge_not_visible", "confidence": best_conf_by_class.get("pressure_gauge", 0.0), "suggested_action": "Retake image focused on pressure gauge."}
        return {"status": "inconclusive", "reason": "Gauge detected, but green-zone needle state needs visual condition validation.", "condition_checked": "gauge_visible_zone_unknown", "confidence": best_conf_by_class.get("pressure_gauge", 0.0)}

    if str(item_id) == "9":
        if not present("label"):
            return {"status": "fail", "reason": "Instruction label is not detected clearly.", "condition_checked": "label_not_visible", "confidence": best_conf_by_class.get("label", 0.0), "suggested_action": "Retake image showing the instruction label."}
        return {"status": "inconclusive", "reason": "Label detected, but facing direction/readability is not guaranteed by detection alone.", "condition_checked": "label_visible_orientation_unknown", "confidence": best_conf_by_class.get("label", 0.0)}

    if str(item_id) == "10":
        if not present("inspection_tag"):
            return {"status": "fail", "reason": "Inspection tag is not detected clearly.", "condition_checked": "tag_not_visible", "confidence": best_conf_by_class.get("inspection_tag", 0.0), "suggested_action": "Retake image so inspection tag is visible."}
        return {"status": "inconclusive", "reason": "Inspection tag detected, but initials/date validity needs OCR/visual verification.", "condition_checked": "tag_visible_date_initial_unknown", "confidence": best_conf_by_class.get("inspection_tag", 0.0)}

    return {"status": "inconclusive", "reason": "No YOLO rule for this item.", "condition_checked": "not_applicable", "confidence": 0.0}


# ------------------------------------------------------------------------------
# Bedrock
# ------------------------------------------------------------------------------
def invoke_claude_json(
    system_prompt: str,
    user_text: str,
    image_bytes: Optional[bytes] = None,
    media_type: str = "image/jpeg",
    max_tokens: int = 220,
) -> dict:
    content = []
    if image_bytes is not None:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": base64.b64encode(image_bytes).decode("utf-8"),
            },
        })
    content.append({"type": "text", "text": user_text})
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": content}],
    }
    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(payload),
        contentType="application/json",
        accept="application/json",
    )
    body = json.loads(response["body"].read())
    return safe_json_parse(extract_text_from_claude_response(body))


# ------------------------------------------------------------------------------
# Summary counters for items 11 and 12
# ------------------------------------------------------------------------------
def compute_summary_counts(inspection: dict) -> Tuple[int, int]:
    total = 0
    compliant = 0
    for item in get_all_items(inspection):
        for ev in item.get("evidence", []):
            if ev.get("is_extinguisher"):
                total += 1
                if ev.get("is_compliant"):
                    compliant += 1
    return total, compliant


def update_summary_items(inspection: dict) -> None:
    total, compliant = compute_summary_counts(inspection)
    item11, c11, i11 = find_item(inspection, "11")
    item12, c12, i12 = find_item(inspection, "12")
    if item11 is not None:
        item11["answer"] = str(total)
        item11["finding"] = "Auto-calculated from inspected extinguisher images."
        inspection["categories"][c11]["items"][i11] = item11
    if item12 is not None:
        item12["answer"] = str(compliant)
        item12["finding"] = "Auto-calculated from compliant extinguisher images."
        inspection["categories"][c12]["items"][i12] = item12


# ------------------------------------------------------------------------------
# Checklist / session
# ------------------------------------------------------------------------------
def get_checklist(event: dict) -> dict:
    return json_response(200, FIRE_EXTINGUISHER_CHECKLIST)


def create_session(event: dict) -> dict:
    body = parse_body(event)
    auditor_name = str(body.get("auditor_name", "")).strip()
    facility_area = str(body.get("facility_area", "")).strip()
    date_of_audit = str(body.get("date_of_audit", "")).strip()
    notes = str(body.get("notes", "")).strip()

    if not auditor_name:
        return json_response(400, {"error": "auditor_name is required"})
    if not facility_area:
        return json_response(400, {"error": "facility_area is required"})
    if not date_of_audit:
        return json_response(400, {"error": "date_of_audit is required"})

    session_id = str(uuid.uuid4())
    inspection_id = str(uuid.uuid4())
    created_at = now_iso()

    general_info = copy.deepcopy(FIRE_EXTINGUISHER_CHECKLIST.get("general_information", {}))
    general_info["location"] = facility_area
    general_info["start_date"] = date_of_audit
    general_info["leader"] = auditor_name
    general_info["team"] = []

    session_table.put_item(Item=sanitize_for_dynamodb({
        "session_id": session_id,
        "inspection_id": inspection_id,
        "auditor_name": auditor_name,
        "facility_area": facility_area,
        "date_of_audit": date_of_audit,
        "created_at": created_at,
    }))

    inspection_table.put_item(Item=sanitize_for_dynamodb({
        "inspection_id": inspection_id,
        "session_id": session_id,
        "inspection_type": "fire-extinguisher",
        "auditor_name": auditor_name,
        "facility_area": facility_area,
        "date_of_audit": date_of_audit,
        "status": "in_progress",
        "current_item_index": 0,
        "general_information": general_info,
        "categories": deep_copy_checklist()["categories"],
        "general_results": deep_copy_checklist()["general_results"],
        "notes": notes,
        "created_at": created_at,
        "updated_at": created_at,
    }))

    return json_response(201, {
        "session_id": session_id,
        "inspection_id": inspection_id,
        "created_at": created_at,
        "report_url": f"/fire-extinguisher/session/{inspection_id}/report",
        "checklist": FIRE_EXTINGUISHER_CHECKLIST,
    })


def create_inspection_from_session_payload(event: dict) -> dict:
    """
    Submit or update an inspection with a categories payload.

    Lookup priority for finding the right inspection record:
      1. session_id  → session table → session["inspection_id"]  (original path)
      2. inspection_id from request body                          (fallback)

    This means QR/Voice/Auditor flows that save inspection_id in InspectionStore
    but do NOT send session_id in the payload will still update the correct,
    already-created record instead of creating a duplicate.

    No new endpoints are added. The router below decides which flows reach here.
    """
    body = parse_body(event)

    session_id         = str(body.get("session_id", "")).strip()
    body_inspection_id = str(body.get("inspection_id", "")).strip()   # ← fallback
    team               = body.get("team", [])
    categories         = body.get("categories", [])
    general_results    = body.get("general_results", [])
    notes              = str(body.get("notes", "")).strip() if isinstance(body.get("notes", ""), str) else ""

    # --- validation (unchanged) -----------------------------------------------
    if len(notes) > 5000:
        return json_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return json_response(400, {"error": "general_results must be a list"})
    if not categories or not isinstance(categories, list):
        return json_response(400, {"error": "categories must be a non-empty list"})

    for i, cat in enumerate(categories):
        if not isinstance(cat, dict):
            return json_response(400, {"error": f"Category at index {i} must be an object"})
        if "id" not in cat:
            return json_response(400, {"error": f"Category at index {i} is missing id"})
        if "name" not in cat:
            return json_response(400, {"error": f"Category at index {i} is missing name"})
        items = cat.get("items", [])
        if not isinstance(items, list):
            return json_response(400, {"error": f"Category '{cat.get('name')}' items must be a list"})
        for j, item in enumerate(items):
            if not isinstance(item, dict):
                return json_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' must be an object"})
            if "id" not in item:
                return json_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' is missing id"})
            if "answer" not in item:
                return json_response(400, {"error": f"Item at index {j} in category '{cat.get('name')}' is missing answer"})

    # --- resolve session + inspection_id ---------------------------------------
    #
    # Path A: session_id provided → look up session table → get inspection_id
    # Path B: session not found / session_id blank → use body inspection_id directly
    #         and build a minimal synthetic session so the rest of the function
    #         can run without any code changes.
    # Path C: neither found → 404
    #
    session       = None
    inspection_id = ""

    if session_id:
        session_resp = session_table.get_item(Key={"session_id": session_id})
        session      = session_resp.get("Item")
        if session:
            inspection_id = str(session.get("inspection_id", "")).strip()
            logger.info(f"[SUBMIT] path A — session_id={session_id} inspection_id={inspection_id}")

    if not inspection_id and body_inspection_id:
        # Path B: client already knows its inspection_id (QR / Voice / Auditor flows).
        # Load the existing record to pull auditor_name / facility_area / date_of_audit
        # so the merge below has accurate metadata.
        inspection_id       = body_inspection_id
        existing_for_meta   = load_inspection(inspection_id)
        if existing_for_meta:
            session = {
                "auditor_name":  str(existing_for_meta.get("auditor_name",  body.get("auditor_name",  ""))).strip(),
                "facility_area": str(existing_for_meta.get("facility_area", body.get("facility_area", ""))).strip(),
                "date_of_audit": str(existing_for_meta.get("date_of_audit", body.get("date_of_audit", ""))).strip(),
            }
        else:
            # Brand-new inspection that somehow has an ID but no record yet —
            # build session purely from body fields.
            session = {
                "auditor_name":  str(body.get("auditor_name",  "")).strip(),
                "facility_area": str(body.get("facility_area", "")).strip(),
                "date_of_audit": str(body.get("date_of_audit", "")).strip(),
            }
        logger.info(f"[SUBMIT] path B — body inspection_id={inspection_id} (session_id was blank or not found)")

    if not session:
        return json_response(404, {"error": "Session not found. Create session first."})

    if not inspection_id:
        # Safety net — should not normally be reached
        inspection_id = str(uuid.uuid4())
        logger.warning(f"[SUBMIT] no inspection_id resolved — generating new: {inspection_id}")
        if session_id:
            try:
                session_table.update_item(
                    Key={"session_id": session_id},
                    UpdateExpression="SET inspection_id = :iid",
                    ExpressionAttributeValues={":iid": inspection_id},
                )
            except Exception:
                logger.exception("Failed to persist generated inspection_id back to session table")

    # --- load existing record and merge (logic unchanged) ---------------------
    existing_inspection = load_inspection(inspection_id)

    created_at = str(existing_inspection.get("created_at", "")).strip() if existing_inspection else now_iso()
    updated_at = now_iso()

    merged_categories = merge_categories(
        existing_inspection.get("categories", []) if existing_inspection else [],
        categories,
    )

    existing_general_results = existing_inspection.get("general_results", []) if existing_inspection else []
    if isinstance(general_results, list) and general_results:
        merged_general_results = []
        for idx, result_item in enumerate(general_results):
            if not isinstance(result_item, dict):
                continue
            base_result   = existing_general_results[idx] if idx < len(existing_general_results) and isinstance(existing_general_results[idx], dict) else {}
            merged_result = copy.deepcopy(base_result)
            for key, value in result_item.items():
                if isinstance(value, str):
                    if value.strip():
                        merged_result[key] = value
                    elif key not in merged_result:
                        merged_result[key] = value
                elif value is not None:
                    merged_result[key] = value
            merged_general_results.append(merged_result)
        if not merged_general_results:
            merged_general_results = copy.deepcopy(existing_general_results)
    else:
        merged_general_results = copy.deepcopy(existing_general_results)

    merged_notes = notes if notes else str(existing_inspection.get("notes", "")).strip() if existing_inspection else ""
    merged_team  = team if isinstance(team, list) and team else (existing_inspection.get("team", []) if existing_inspection else [])

    # Preserve the original session_id stored on the record when the submit
    # came via Path B (session_id blank in the payload).
    preserved_session_id = session_id or (str(existing_inspection.get("session_id", "")).strip() if existing_inspection else "")

    merged_record = copy.deepcopy(existing_inspection) if existing_inspection else {}
    merged_record.update({
        "inspection_id":      inspection_id,
        "session_id":         preserved_session_id,
        "inspection_type":    "fire-extinguisher",
        "auditor_name":       session.get("auditor_name", ""),
        "facility_area":      session.get("facility_area", ""),
        "date_of_audit":      session.get("date_of_audit", ""),
        "team":               merged_team if isinstance(merged_team, list) else [],
        "categories":         merged_categories,
        "general_results":    merged_general_results,
        "notes":              merged_notes,
        "status":             compute_status({"categories": merged_categories}),
        "current_item_index": next_unanswered_index({"categories": merged_categories})[1] if next_unanswered_index({"categories": merged_categories}) is not None else 0,
        "created_at":         created_at,
        "updated_at":         updated_at,
    })

    inspection_table.put_item(Item=sanitize_for_dynamodb(merged_record))

    # Re-read what was actually written so the client gets exact data
    saved_inspection = load_inspection(inspection_id) or merged_record

    status_code = 201 if not existing_inspection else 200
    # Return only lightweight fields — the Android app navigates immediately
    # after submit and does not use the full inspection blob here.
    # Returning the full inspection (with all evidence) was pushing the
    # response past API Gateway's 6 MB limit and causing 502 errors.
    return json_response(status_code, {
        "inspection_id": inspection_id,
        "session_id":    preserved_session_id,
        "created_at":    created_at,
        "updated_at":    updated_at,
        "report_url":    f"/fire-extinguisher/session/{inspection_id}/report",
        "status":         saved_inspection.get("status", "in_progress"),
    })


def get_inspection(event: dict) -> dict:
    path_params = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})
    item = load_inspection_by_any_id(inspection_id)
    if not item:
        return json_response(404, {"error": "Inspection not found"})
    return json_response(200, item)


def list_inspections(event: dict) -> dict:
    try:
        result = inspection_table.scan()
        items  = result.get("Items", [])
        while "LastEvaluatedKey" in result:
            result = inspection_table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
            items.extend(result.get("Items", []))
    except Exception as e:
        logger.exception("Failed to scan inspections")
        return json_response(500, {"error": f"Failed to list inspections: {str(e)}"})

    items = convert_floats_to_decimal(items)
    summaries = []
    for item in items:
        summaries.append({
            "inspection_id":      item.get("inspection_id"),
            "session_id":         item.get("session_id"),
            "auditor_name":       item.get("auditor_name"),
            "facility_area":      item.get("facility_area"),
            "date_of_audit":      item.get("date_of_audit"),
            "status":             item.get("status", compute_status(item)),
            "created_at":         item.get("created_at"),
            "current_item_index": item.get("current_item_index", 0),
            "notes":              item.get("notes", ""),
        })
    summaries.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return json_response(200, summaries)


def update_checklist_item(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    item_id       = path_params.get("item_id") or ""

    if not inspection_id or not item_id:
        return json_response(400, {"error": "inspection_id and item_id are required"})

    body                   = parse_body(event)
    answer                 = str(body.get("answer", "")).strip()
    finding                = str(body.get("finding", "")).strip()
    action_item            = str(body.get("action_item", "")).strip()
    responsible            = str(body.get("responsible", "")).strip()
    due_date               = str(body.get("due_date", "")).strip()
    evidence               = body.get("evidence", [])
    blocked_by_wrong_image = body.get("blocked_by_wrong_image", None)
    clear_block            = bool(body.get("clear_block", False))

    if str(item_id) in ["11", "12"]:
        return json_response(400, {"error": "Items 11 and 12 are auto-calculated from inspection evidence"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    if answer and answer not in ["Yes", "No", "N/A"]:
        return json_response(400, {"error": "answer must be Yes, No, or N/A"})

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
    if blocked_by_wrong_image is not None:
        checklist_item["blocked_by_wrong_image"] = bool(blocked_by_wrong_image)
    if clear_block:
        checklist_item["blocked_by_wrong_image"] = False

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    update_summary_items(inspection)
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return json_response(200, {
        "inspection_id": inspection_id,
        "item_id":       item_id,
        "updated_item":  checklist_item,
        "inspection":    inspection,
        "categories":    inspection.get("categories", []),
        "status":        inspection.get("status", "in_progress"),
        "updated_at":    inspection.get("updated_at", ""),
    })


def add_note_to_item(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    item_id       = path_params.get("item_id") or ""

    if not inspection_id or not item_id:
        return json_response(400, {"error": "inspection_id and item_id are required"})

    body = parse_body(event)
    note = str(body.get("note", "")).strip()
    if not note:
        return json_response(400, {"error": "note is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    existing_finding          = checklist_item.get("finding", "")
    checklist_item["finding"] = (existing_finding + " | " if existing_finding else "") + f"Worker note: {note}"

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    update_summary_items(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return json_response(200, {
        "inspection_id": inspection_id,
        "item_id":       item_id,
        "finding":       checklist_item["finding"],
        "message":       "Note saved successfully.",
    })


# ------------------------------------------------------------------------------
# S3 URLs
# ------------------------------------------------------------------------------
def generate_upload_url(event: dict) -> dict:
    filename        = get_query(event, "filename", "").strip()
    content_type    = get_query(event, "contentType", "").strip()
    inspection_id   = get_query(event, "inspectionId", "").strip()
    item_id         = get_query(event, "itemId", "").strip()
    inspection_type = get_query(event, "inspectionType", "fire-extinguisher").strip()

    if not filename:
        return json_response(400, {"error": "filename is required"})
    if not content_type:
        return json_response(400, {"error": "contentType is required"})
    if content_type not in ALLOWED_CONTENT_TYPES:
        return json_response(400, {"error": f"Unsupported contentType: {content_type}"})
    if not inspection_id:
        return json_response(400, {"error": "inspectionId is required"})
    if not item_id:
        return json_response(400, {"error": "itemId is required"})

    safe_filename = filename.replace(" ", "_")
    date_prefix   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key           = f"evidence/{inspection_type}/{inspection_id}/item-{item_id}/{date_prefix}/{uuid.uuid4().hex[:8]}_{safe_filename}"

    try:
        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": EVIDENCE_BUCKET, "Key": key, "ContentType": content_type},
            ExpiresIn=UPLOAD_URL_EXPIRY,
        )
    except Exception as e:
        logger.exception("Failed to generate upload URL")
        return json_response(500, {"error": f"Failed to generate upload URL: {str(e)}"})

    return json_response(200, {"upload_url": upload_url, "file_key": key, "expires_in": UPLOAD_URL_EXPIRY})


def generate_download_url(event: dict) -> dict:
    file_key = get_query(event, "fileKey", "").strip() or get_query(event, "file_key", "").strip()
    if not file_key:
        return json_response(400, {"error": "fileKey is required"})

    try:
        s3.head_object(Bucket=EVIDENCE_BUCKET, Key=file_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return json_response(404, {"error": "File not found"})
        logger.exception("Could not verify file existence")
        return json_response(500, {"error": "Could not verify file"})
    except Exception as e:
        logger.exception("Could not verify file existence")
        return json_response(500, {"error": f"Could not verify file: {str(e)}"})

    download_url = generate_s3_download_url(file_key)
    if not download_url:
        return json_response(500, {"error": "Failed to generate download URL"})

    return json_response(200, {"download_url": download_url, "expires_in": DOWNLOAD_URL_EXPIRY})


# ------------------------------------------------------------------------------
# Image extraction
# ------------------------------------------------------------------------------
def _extract_image_from_request(body: dict) -> Tuple[Optional[bytes], str]:
    image_base64 = str(body.get("image_base64", "")).strip()
    file_key     = str(body.get("file_key", "")).strip() or str(body.get("fileKey", "")).strip()

    if image_base64:
        if "," in image_base64 and image_base64.startswith("data:"):
            image_base64 = image_base64.split(",", 1)[1]
        return base64.b64decode(image_base64), "image/jpeg"

    if file_key:
        try:
            obj          = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=file_key)
            image_bytes  = obj["Body"].read()
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


# ------------------------------------------------------------------------------
# Image analysis
# ------------------------------------------------------------------------------
def analyze_item_image(event: dict) -> dict:
    t0 = time.time()
    logger.info("[TIMING] analyze_item_image START")
    body = parse_body(event)
    logger.info(
        "[REQUEST] analyze called: inspection_id=%s item_id=%s has_file_key=%s has_image_base64=%s file_key=%s",
        body.get("inspection_id"),
        body.get("item_id"),
        bool(body.get("file_key") or body.get("fileKey")),
        bool(body.get("image_base64")),
        str(body.get("file_key") or body.get("fileKey") or "")[:80],
    )

    inspection_id     = str(body.get("inspection_id", "")).strip()
    item_id           = str(body.get("item_id", "")).strip()
    expected_location = str(body.get("expected_location", "")).strip()

    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})
    if not item_id:
        return json_response(400, {"error": "item_id is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    # Items 11 and 12 are auto-calculated — no image needed
    if str(item_id) in ["11", "12"]:
        update_summary_items(inspection)
        inspection["status"]     = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)
        item11, _, _ = find_item(inspection, "11")
        item12, _, _ = find_item(inspection, "12")
        return json_response(200, {
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

    # Extract image
    try:
        image_bytes, content_type = _extract_image_from_request(body)
    except FileNotFoundError as e:
        return json_response(400, {"error": f"Invalid image input: {str(e)}"})
    except ClientError as e:
        return json_response(400, {"error": f"S3 read failed: {e.response.get('Error', {}).get('Message', str(e))}"})
    except Exception as e:
        return json_response(400, {"error": f"Invalid image input: {str(e)}"})

    if image_bytes is None:
        return json_response(400, {"error": "Provide image_base64 or file_key"})

    # Prepare YOLO and Bedrock images from original bytes
    original_image_bytes = image_bytes
    is_yolo_item = bool(required_yolo_class_for_item(item_id))

    if is_yolo_item:
        # Prepare YOLO-sized and Bedrock-sized images IN PARALLEL.
        # PIL resize is I/O-light but CPU-bound; running both at once on
        # separate threads saves ~100-200ms per items-7-10 request.
        yolo_future   = _THREAD_POOL.submit(prepare_image_bytes, original_image_bytes, content_type, True)
        bedrock_future = _THREAD_POOL.submit(prepare_image_bytes, original_image_bytes, content_type, False)
        yolo_image_bytes,   normalized_type = yolo_future.result(timeout=30)
        bedrock_image_bytes, bedrock_type   = bedrock_future.result(timeout=30)
        image_bytes = yolo_image_bytes  # used for YOLO gate below
    else:
        image_bytes, normalized_type = prepare_image_bytes(
            original_image_bytes, content_type, for_yolo=False
        )
        bedrock_image_bytes = image_bytes
        bedrock_type = normalized_type
    logger.info(f"[TIMING] after image extract/prepare (parallel): {time.time() - t0:.2f}s")

    # YOLO gate for items 7-10
    yolo_detections: List[dict] = []
    yolo_target      = required_yolo_class_for_item(item_id)
    yolo_target_conf = 0.0

    if yolo_target:
        if not YOLO_ENDPOINT_NAME:
            return json_response(500, {"error": "YOLO endpoint is required for checklist items 7-10 but YOLO_ENDPOINT_NAME is not configured"})

        logger.info(f"[TIMING] calling YOLO endpoint: {YOLO_ENDPOINT_NAME}")
        yolo_t      = time.time()
        yolo_result = invoke_yolo_endpoint(image_bytes, normalized_type)
        logger.info(f"[TIMING] YOLO done: {time.time() - yolo_t:.2f}s total: {time.time() - t0:.2f}s result={'None' if yolo_result is None else len(yolo_result)}")

        if yolo_result is None:
            return json_response(502, {"error": "YOLO endpoint unavailable. Please retry."})

        yolo_detections  = yolo_result
        yolo_target_conf = best_detection_confidence(yolo_detections, yolo_target)

        if yolo_target_conf < YOLO_DETECTION_CONFIDENCE_THRESHOLD:
            zoom_hint      = item_zoom_hint(item_id)
            worker_message = (
                f"{zoom_hint} Remove obstruction and retake."
                if zoom_hint else
                "Target object not detected clearly. Retake image."
            )
            reason = (
                f"YOLO did not detect required object '{yolo_target}' with sufficient confidence "
                f"(confidence={yolo_target_conf:.2f}, required>={YOLO_DETECTION_CONFIDENCE_THRESHOLD:.2f})."
            )

            evidence_record = {
                "file_key":               body.get("file_key") or body.get("fileKey") or "",
                "analyzed_at":            now_iso(),
                "object_detected":        "unclear",
                "condition_checked":      f"{yolo_target}_not_visible",
                "is_extinguisher":        True,
                "pass":                   False,
                "is_compliant":           False,
                "confidence":             yolo_target_conf,
                "reason":                 reason,
                "worker_message":         worker_message,
                "suggested_action":       zoom_hint or "Retake image with target object clearly visible.",
                "blocked":                True,
                "inference_source":       "yolo_gate",
                "yolo_target":            yolo_target,
                "yolo_target_confidence": yolo_target_conf,
                "yolo_detections":        yolo_detections,
            }

            checklist_item.setdefault("evidence", [])
            checklist_item["evidence"].append(evidence_record)
            checklist_item["blocked_by_wrong_image"] = True
            checklist_item["answer"]                  = ""

            inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
            inspection["updated_at"] = now_iso()
            save_inspection(inspection)

            return json_response(200, {
                "inspection_id":          inspection_id,
                "item_id":                item_id,
                "blocked":                True,
                "move_next":              False,
                "pass":                   False,
                "object_detected":        "unclear",
                "condition_checked":      f"{yolo_target}_not_visible",
                "confidence":             yolo_target_conf,
                "message":                worker_message or ("Target component not clear. Move closer and retake." if component_focus else "No fire extinguisher detected. Point camera directly at the extinguisher."),
                "reason":                 reason,
                "suggested_action":       zoom_hint or "Retake image with target object clearly visible.",
                "updated_item":           checklist_item,
                "inspection_status":      inspection.get("status", "in_progress"),
                "current_item_index":     inspection.get("current_item_index", 0),
                "inference_source":       "yolo_gate",
                "yolo_target":            yolo_target,
                "yolo_target_confidence": yolo_target_conf,
                "inspection":             inspection,
                "categories":             inspection.get("categories", []),
            })

    COMPONENT_ITEMS = {"8", "9", "10"}
    component_focus = str(item_id) in COMPONENT_ITEMS
    rule            = checklist_rule_for_item(item_id, checklist_item)

    if component_focus:
        contract_text = component_check_contract(item_id)
        COMPONENT_TARGET_NAMES = {
            "8": "pressure gauge",
            "9": "instruction label",
            "10": "inspection tag",
        }
        target_name = COMPONENT_TARGET_NAMES.get(str(item_id), "component")
        prompt = (
            f"Checklist item to inspect (component close-up expected):\n"
            f"- item_id: {checklist_item['id']}\n"
            f"- item_description: {checklist_item['description']}\n"
            f"- target_component: {target_name}\n"
            f"- strict_visual_rule: {rule}\n"
            f"- required_checks_contract: {contract_text}\n"
            f"- expected_location_hint: {expected_location or 'not provided'}\n\n"
            f"STEP 1: Is the target_component clearly visible in this image?\n"
            f"STEP 2: If yes, does the image clearly satisfy strict_visual_rule?\n"
            f"STEP 3: Return checks object exactly per required_checks_contract.\n"
            f"Do NOT require full extinguisher body visibility.\n"
            f"Do not guess. If the required condition is not clearly visible, fail.\n"
            f"Ignore all people, PPE, and background.\n"
            f"Return JSON only."
        )
    else:
        if str(item_id) == "7":
            prompt = (
                f"Checklist item to inspect:\n"
                f"- item_id: {checklist_item['id']}\n"
                f"- item_description: {checklist_item['description']}\n"
                f"- strict_visual_rule: {rule}\n"
                f"- expected_location_hint: {expected_location or 'not provided'}\n\n"
                f"STEP 1: Is a fire extinguisher clearly visible in this image?\n"
                f"STEP 2: Is the hose physically connected to the extinguisher body AND visible?\n"
                f"STEP 3: Is the nozzle tip or hose end LITERALLY VISIBLE in the image pixels? "
                f"Not assumed — actually visible as a shape you can describe.\n"
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
                f"nozzle_tip_visible = true ONLY if you can literally describe the shape of the nozzle tip or hose end in the image.\n"
                f"If the extinguisher is small, far away, or only the body/top is visible → nozzle_tip_visible = false.\n"
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

    try:
        bedrock_t = time.time()
        analysis = invoke_claude_json(
            system_prompt=COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT if component_focus else IMAGE_ANALYSIS_SYSTEM_PROMPT,
            user_text=prompt,
            image_bytes=bedrock_image_bytes,   # use bedrock-sized version
            media_type=bedrock_type,
            max_tokens=220,
        )
        logger.info(f"[TIMING] Bedrock done: {time.time() - bedrock_t:.2f}s total: {time.time() - t0:.2f}s")
    except Exception as e:
        logger.exception("Bedrock analysis failed")
        return json_response(502, {"error": f"Bedrock failed: {str(e)}"})

    object_detected   = str(analysis.get("object_detected", "unclear")).lower().strip()
    condition_checked = str(analysis.get("condition_checked", "not_visible")).strip()
    passed            = bool(analysis.get("pass", False))
    confidence        = float(analysis.get("confidence", 0.0) or 0.0)
    reason            = str(analysis.get("reason", "")).strip()
    worker_message    = str(analysis.get("worker_message", "")).strip()
    suggested_action  = analysis.get("suggested_action", None)

    if suggested_action is not None:
        suggested_action = str(suggested_action).strip() or None

    # ── Item 7 structured check enforcement (hallucination prevention) ──────
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
            logger.warning(f"[ITEM7] Structured check enforcement → FAIL. reason={item7_fail_reason}")

    if component_focus:
        passed, condition_checked, reason, worker_message, suggested_action = enforce_component_checks(
            item_id=item_id,
            analysis=analysis,
            passed=passed,
            condition_checked=condition_checked,
            reason=reason,
            worker_message=worker_message,
            suggested_action=suggested_action,
        )

    # Hard safety guard — non-component items only
    if (not component_focus) and passed and object_detected != "fire_extinguisher":
        logger.warning(f"AI returned pass=true with object_detected={object_detected} confidence={confidence} — overriding to false. item_id={item_id}")
        passed              = False
        object_detected     = "other"
        confidence          = 0.0
        condition_checked   = "not_visible"
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

    expected_keywords = expected_keywords_for_item(item_id)
    if passed and expected_keywords:
        lower_reason = reason.lower()
        if not any(keyword in lower_reason for keyword in expected_keywords):
            logger.warning(f"Checklist condition not validated in reason -> overriding FAIL. item_id={item_id}, reason={reason}")
            passed           = False
            analysis["pass"] = False
            worker_message   = "Condition not clearly visible. Retake image properly."
            reason           = "Checklist condition not clearly verified from image."
            suggested_action = "Retake the photo with the checklist item clearly visible."

    if component_focus:
        effective_confidence = max(confidence, yolo_target_conf)
        low_confidence       = effective_confidence < YOLO_DETECTION_CONFIDENCE_THRESHOLD
        logger.info(
            f"[YOLO] component confidence gate item={item_id} "
            f"bedrock_conf={confidence:.2f} yolo_conf={yolo_target_conf:.2f} "
            f"effective_conf={effective_confidence:.2f} threshold={YOLO_DETECTION_CONFIDENCE_THRESHOLD:.2f}"
        )
    else:
        low_confidence = confidence < IMAGE_CONFIDENCE_BLOCK_THRESHOLD

    blocked                  = wrong_image or low_confidence
    zoom_hint                = item_zoom_hint(item_id)
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

    # Special case: item 1 — extinguisher genuinely missing from location
    if blocked and str(item_id) == "1":
        reason_text        = reason or ""
        missing_keywords   = ["missing", "empty", "no extinguisher", "not present", "absent", "bracket"]
        is_clearly_missing = any(kw in reason_text.lower() for kw in missing_keywords)
        if is_clearly_missing:
            logger.info("Item 1: extinguisher confirmed missing at location. Recording as fail.")
            checklist_item["answer"]                = "No"
            checklist_item["blocked_by_wrong_image"] = False
            checklist_item["finding"]               = reason or "Fire extinguisher not found at this location."
            checklist_item["action_item"]           = suggested_action or "Replace missing fire extinguisher immediately and report to safety officer."
            blocked = False
            passed  = False

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
    if yolo_target:
        evidence_record["inference_source"]       = "yolo_then_bedrock"
        evidence_record["yolo_target"]            = yolo_target
        evidence_record["yolo_target_confidence"] = yolo_target_conf
        evidence_record["yolo_detections"]        = yolo_detections

    checklist_item.setdefault("evidence", [])
    checklist_item["evidence"].append(evidence_record)

    # BLOCKED
    if blocked:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"]                  = ""
        checklist_item["finding"]                 = finding_text
        checklist_item["action_item"]             = action_text

        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        return json_response(200, {
            "inspection_id":      inspection_id,
            "item_id":            item_id,
            "blocked":            True,
            "move_next":          False,
            "pass":               False,
            "object_detected":    object_detected,
            "condition_checked":  condition_checked,
            "confidence":         confidence,
            "message":            worker_message or ("Target component not clear. Move closer and retake." if component_focus else "No fire extinguisher detected. Point camera directly at the extinguisher."),
            "reason":             finding_text,
            "suggested_action":   blocked_suggested_action or action_text,
            "updated_item":       checklist_item,
            "inspection_status":  inspection.get("status", "in_progress"),
            "current_item_index": inspection.get("current_item_index", 0),
            "inspection":         inspection,
            "categories":         inspection.get("categories", []),
        })

    # SCORED
    checklist_item["answer"]               = "Yes" if passed else "No"
    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"]              = finding_text
    checklist_item["action_item"]          = action_text

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

    return json_response(200, {
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
    })


def get_analyze_job_status(event: dict) -> dict:
    path_params = event.get("pathParameters") or {}
    job_id      = path_params.get("job_id") or ""
    if not job_id:
        return json_response(400, {"error": "job_id is required"})

    job = load_async_job(job_id)
    if not job:
        return json_response(404, {"error": "Job not found"})

    result = job.get("result")

    # Re-fetch fresh inspection when job is done so polling clients get latest state
    if job.get("job_status") == "completed" and isinstance(result, dict):
        inspection_id = str(result.get("inspection_id", "")).strip()
        if inspection_id:
            fresh_inspection = load_inspection(inspection_id)
            if fresh_inspection:
                result                      = dict(result)
                result["inspection"]        = fresh_inspection
                result["categories"]        = fresh_inspection.get("categories", [])
                result["inspection_status"] = fresh_inspection.get("status", "in_progress")

    return json_response(200, {
        "job_id":     job.get("session_id", job_id),
        "job_status": job.get("job_status", "unknown"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "error":      job.get("error"),
        "result":     result,
    })


# ------------------------------------------------------------------------------
# Dashboard report
# ------------------------------------------------------------------------------
def get_inspection_report(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    # ── AGGRESSIVELY strip heavy fields ─────────────────────────────────────
    # Root cause: repeated photo retakes accumulate evidence entries that
    # collectively push the response past API Gateway's 6 MB hard limit.
    # Strategy:
    #   1. Keep ONLY the latest evidence entry per item (most recent analysis).
    #   2. Drop verbose string fields (reason, worker_message) from evidence.
    #   3. Cap all item-level string fields.
    #   4. Fallback tier 1 → strip to file_key only  (>4 MB)
    #   5. Fallback tier 2 → nuclear metadata-only   (>5.5 MB)
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            cleaned_evidence = []
            for ev in item.get("evidence", []):
                # Handle evidence as either dict or string (data corruption edge case)
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
                        # reason / worker_message / suggested_action intentionally omitted
                    }
                    cleaned_evidence.append(slim_ev)
                elif isinstance(ev, str) and ev.strip():
                    # Fallback: if evidence is a string (file path), treat as file_key
                    slim_ev = {
                        "file_key": str(ev)[:300],
                        "analyzed_at": "",
                        "object_detected": "",
                        "condition_checked": "",
                        "pass": False,
                        "is_compliant": False,
                        "confidence": 0.0,
                        "blocked": False,
                    }
                    cleaned_evidence.append(slim_ev)
            # Keep only the latest evidence entry — historical ones bloat the response
            item["evidence"] = cleaned_evidence[-1:] if cleaned_evidence else []

            # Cap string fields on the item itself
            for field in ["finding", "action_item", "responsible"]:
                if field in item:
                    item[field] = str(item.get(field, ""))[:200]

    # Cap notes
    if "notes" in inspection:
        inspection["notes"] = str(inspection.get("notes", ""))[:1000]

    # Slim general_results (remove it first, re-add slimmed)
    raw_general_results = inspection.pop("general_results", []) or []
    slim_results = []
    for r in raw_general_results:
        slim_results.append({
            "finding":     str(r.get("finding", ""))[:200],
            "action_item": str(r.get("action_item", ""))[:200],
            "responsible": str(r.get("responsible", ""))[:100],
            "due_date":    r.get("due_date", ""),
        })
    inspection["general_results"] = slim_results

    # Measure serialised size
    body_str = json.dumps(inspection, default=str)
    size_kb  = len(body_str.encode("utf-8")) / 1024
    logger.info(f"[REPORT] inspection_id={inspection_id} response_size={size_kb:.1f}KB")

    # Fallback tier 1 — strip evidence down to file_key only
    if size_kb > 4000:
        logger.error(f"[REPORT] Still too large ({size_kb:.1f}KB) — stripping all evidence to file_key only")
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = [
                    {"file_key": str(ev.get("file_key", "") if isinstance(ev, dict) else ev)[:300]}
                    for ev in item.get("evidence", [])
                    if (ev.get("file_key") if isinstance(ev, dict) else ev)
                ]
        body_str = json.dumps(inspection, default=str)
        size_kb  = len(body_str.encode("utf-8")) / 1024
        logger.info(f"[REPORT] After tier-1 strip: {size_kb:.1f}KB")

    # Fallback tier 2 (nuclear) — return metadata + categories only, no evidence at all
    if size_kb > 5500:
        logger.error(f"[REPORT] NUCLEAR: {size_kb:.1f}KB — returning metadata-only response")
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = []
        return json_response(200, {
            "inspection_id":     inspection_id,
            "session_id":        inspection.get("session_id", ""),
            "auditor_name":      inspection.get("auditor_name", ""),
            "facility_area":     inspection.get("facility_area", ""),
            "date_of_audit":     inspection.get("date_of_audit", ""),
            "status":            inspection.get("status", ""),
            "general_information": inspection.get("general_information", {}),
            "categories":        inspection.get("categories", []),
            "notes":             inspection.get("notes", ""),
            "general_results":   slim_results,
            "created_at":        inspection.get("created_at", ""),
            "updated_at":        inspection.get("updated_at", ""),
        })

    return json_response(200, inspection)


def delete_session(event: dict) -> dict:
    path_params = event.get("pathParameters") or {}
    session_id  = str(path_params.get("session_id") or "").strip()
    if not session_id:
        return json_response(400, {"error": "session_id is required"})

    delete_inspections_flag = str(get_query(event, "delete_inspections", "true")).strip().lower()
    delete_inspections      = delete_inspections_flag in {"1", "true", "yes", "y"}

    existing = session_table.get_item(Key={"session_id": session_id}).get("Item")
    if not existing:
        return json_response(404, {"error": "Session not found"})

    deleted_inspection_ids: List[str] = []
    if delete_inspections:
        try:
            scan_res = inspection_table.scan()
            items    = scan_res.get("Items", [])
            while "LastEvaluatedKey" in scan_res:
                scan_res = inspection_table.scan(ExclusiveStartKey=scan_res["LastEvaluatedKey"])
                items.extend(scan_res.get("Items", []))

            for it in items:
                if str(it.get("session_id", "")).strip() == session_id:
                    inspection_id = str(it.get("inspection_id", "")).strip()
                    if inspection_id:
                        inspection_table.delete_item(Key={"inspection_id": inspection_id})
                        deleted_inspection_ids.append(inspection_id)
        except Exception as e:
            logger.exception("Failed deleting linked inspections")
            return json_response(500, {"error": f"Failed deleting linked inspections: {str(e)}"})

    try:
        session_table.delete_item(Key={"session_id": session_id})
    except Exception as e:
        logger.exception("Failed deleting session")
        return json_response(500, {"error": f"Failed deleting session: {str(e)}"})

    return json_response(200, {
        "message":                    "Session deleted successfully",
        "session_id":                 session_id,
        "deleted_linked_inspections": delete_inspections,
        "deleted_inspection_count":   len(deleted_inspection_ids),
        "deleted_inspection_ids":     deleted_inspection_ids,
    })


# ------------------------------------------------------------------------------
# Voice commands
# ------------------------------------------------------------------------------
def parse_voice_intent_locally(text: str) -> Optional[dict]:
    t = (text or "").strip().lower()
    if not t:
        return {"intent": "clarify", "confidence": 1.0, "message": "Please repeat the command.", "move_to_item": False, "note_text": None}

    if any(p in t for p in ["next item", "move next", "go forward", "continue"]):
        return {"intent": "next_item", "confidence": 0.99, "message": "Moving to next item.", "move_to_item": True, "note_text": None}
    if re.search(r"\bnext\b", t) and len(t.split()) <= 2:
        return {"intent": "next_item", "confidence": 0.95, "message": "Moving to next item.", "move_to_item": True, "note_text": None}

    if any(p in t for p in ["previous item", "go back", "back to previous"]):
        return {"intent": "previous_item", "confidence": 0.99, "message": "Going back one item.", "move_to_item": True, "note_text": None}
    if re.search(r"\bback\b", t) and len(t.split()) <= 2:
        return {"intent": "previous_item", "confidence": 0.95, "message": "Going back one item.", "move_to_item": True, "note_text": None}

    if any(p in t for p in ["capture photo", "take photo", "take picture", "capture image", "scan"]):
        return {"intent": "capture_photo", "confidence": 0.99, "message": "Capture the photo now.", "move_to_item": False, "note_text": None}

    if any(p in t for p in ["retake photo", "retake", "try again", "take again", "redo", "capture again"]):
        return {"intent": "retake_photo", "confidence": 0.99, "message": "Retaking. Point camera at extinguisher.", "move_to_item": False, "note_text": None}

    if any(p in t for p in ["add note", "add comment", "add finding", "note this", "write note", "add remark"]):
        return {"intent": "add_note", "confidence": 0.95, "message": "Go ahead, add your note.", "move_to_item": False, "note_text": None}

    if any(p in t for p in ["repeat", "say again", "read again", "read item", "what is this item"]):
        return {"intent": "repeat_item", "confidence": 0.95, "message": "Repeating current item.", "move_to_item": False, "note_text": None}

    if any(p in t for p in ["help", "what can i say", "show commands", "list commands"]):
        return {"intent": "help", "confidence": 0.95, "message": "Say next, back, capture, retake, add note, or repeat.", "move_to_item": False, "note_text": None}

    return None


def voice_command(event: dict) -> dict:
    body            = parse_body(event)
    inspection_id   = str(body.get("inspection_id", "")).strip()
    current_item_id = str(body.get("current_item_id", "")).strip()
    text            = str(body.get("text", "")).strip()

    if not text:
        return json_response(400, {"error": "text is required"})

    # ── Parallel: DynamoDB load + local intent parse run simultaneously ────────
    # Local parsing is instant (regex), but we also kick off the DynamoDB read
    # at the same time. On cache-miss this saves ~300ms of sequential wait.
    def _load_inspection_and_item():
        if not (inspection_id and current_item_id):
            return None, None
        insp = load_inspection(inspection_id)
        if not insp:
            return None, None
        item, _, _ = find_item(insp, current_item_id)
        return insp, item

    dynamo_future = _THREAD_POOL.submit(_load_inspection_and_item)
    local         = parse_voice_intent_locally(text)   # instant — runs on this thread

    # Now resolve the DynamoDB result (may already be done)
    try:
        inspection, current_item = dynamo_future.result(timeout=5)
    except Exception:
        inspection, current_item = None, None

    if local:
        if local["intent"] == "next_item" and current_item and current_item.get("blocked_by_wrong_image"):
            zoom_hint = item_zoom_hint(current_item_id)
            return json_response(200, {
                "intent":       "clarify",
                "confidence":   0.99,
                "message":      (f"{zoom_hint} Remove obstruction and retake first." if zoom_hint else "Please retake the photo for this item first."),
                "move_to_item": False,
                "note_text":    None,
            })

        zoom_hint = item_zoom_hint(current_item_id)
        if zoom_hint and local.get("intent") in ["capture_photo", "retake_photo", "repeat_item", "help"]:
            if current_item and current_item.get("blocked_by_wrong_image"):
                local["message"] = f"{zoom_hint} Remove obstruction and retake."
            else:
                local["message"] = zoom_hint

        return json_response(200, local)

    # Local parse missed — send to Claude for NLU
    item_context = ""
    if current_item:
        item_context   = f"Current checklist item: {current_item.get('description', '')}"
        zoom_hint      = item_zoom_hint(current_item_id)
        if zoom_hint:
            item_context = f"{item_context}\nZoom guidance: {zoom_hint}"
        condition_hint = voice_item_hint(current_item_id)
        if condition_hint:
            item_context = f"{item_context}\nCondition guidance: {condition_hint}"

    prompt = f"{item_context}\nWorker said: {text}\nReturn JSON only."
    try:
        result = invoke_claude_json(system_prompt=VOICE_SYSTEM_PROMPT, user_text=prompt, max_tokens=150)
    except Exception:
        result = {"intent": "clarify", "confidence": 0.2, "message": "Please repeat the command.", "move_to_item": False, "note_text": None}

    if "intent" not in result:
        result = {"intent": "clarify", "confidence": 0.2, "message": "Please repeat the command.", "move_to_item": False, "note_text": None}

    return json_response(200, result)


# ------------------------------------------------------------------------------
# Router
# ------------------------------------------------------------------------------
def lambda_handler(event, context):
    if event.get("async_worker"):
        return process_async_analyze_worker(event)

    method, path, resource = get_route(event)

    if method == "OPTIONS":
        return json_response(200, {"message": "ok"})

    auth_error = require_api_key(event)
    if auth_error:
        return auth_error

    route_key = resource or path

    if method == "GET" and (
        route_key == "/fire-extinguisher/checklist"
        or route_key == "/fire-extinguisher-inspection/checklist"
        or path_endswith(path, "/fire-extinguisher/checklist")
        or path_endswith(path, "/fire-extinguisher-inspection/checklist")
    ):
        return get_checklist(event)

    if method == "POST" and (
        route_key == "/fire-extinguisher/session"
        or route_key == "/fire-extinguisher-inspection"
        or path_endswith(path, "/fire-extinguisher/session")
        or path_endswith(path, "/fire-extinguisher-inspection")
    ):
        body = parse_body(event)

        # FIX: Route to the update/merge path when EITHER session_id OR
        # inspection_id is present alongside a categories list.
        #
        # Previously the condition was:
        #   if session_id and categories → create_inspection_from_session_payload
        #
        # That caused QR / Voice / Auditor flows (which store inspection_id in
        # InspectionStore but do not always put session_id in the payload due to
        # the sessionCreatedByAuditorScreen guard) to fall through to
        # create_session — creating a duplicate record every time.
        #
        # With inspection_id accepted as a fallback key inside
        # create_inspection_from_session_payload, we can safely widen this
        # condition without touching any Android code.
        has_session    = bool(str(body.get("session_id",    "")).strip())
        has_insp_id    = bool(str(body.get("inspection_id", "")).strip())
        has_categories = isinstance(body.get("categories"), list)

        if (has_session or has_insp_id) and has_categories:
            return create_inspection_from_session_payload(event)
        return create_session(event)

    if method == "GET" and (
        route_key == "/fire-extinguisher/session/{inspection_id}"
        or route_key == "/fire-extinguisher-inspection/{inspection_id}"
        or re.search(r"/fire-extinguisher/session/[^/]+$", path)
        or re.search(r"/fire-extinguisher-inspection/[^/]+$", path)
    ):
        inspection_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"inspection_id": inspection_id}
        return get_inspection(event)

    if method == "DELETE" and (
        route_key == "/fire-extinguisher/session/{session_id}"
        or route_key == "/fire-extinguisher-inspection/session/{session_id}"
        or re.search(r"/fire-extinguisher/session/[^/]+$", path)
        or re.search(r"/fire-extinguisher-inspection/session/[^/]+$", path)
    ):
        session_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"session_id": session_id}
        return delete_session(event)

    if method == "GET" and re.search(r"/fire-extinguisher/session/[^/]+/report$", path):
        parts         = path.rstrip("/").split("/")
        inspection_id = parts[-2]
        event["pathParameters"] = {"inspection_id": inspection_id}
        return get_inspection_report(event)

    if method == "GET" and (
        route_key == "/fire-extinguisher/inspections"
        or route_key == "/fire-extinguisher-inspections"
        or path_endswith(path, "/fire-extinguisher/inspections")
        or path_endswith(path, "/fire-extinguisher-inspections")
    ):
        return list_inspections(event)

    if method == "PATCH" and re.search(r"/fire-extinguisher/session/[^/]+/items/[^/]+/note$", path):
        parts         = path.rstrip("/").split("/")
        inspection_id = parts[-4]
        item_id       = parts[-2]
        event["pathParameters"] = {"inspection_id": inspection_id, "item_id": item_id}
        return add_note_to_item(event)

    if method == "PATCH" and re.search(r"/fire-extinguisher/session/[^/]+/items/[^/]+$", path):
        parts         = path.rstrip("/").split("/")
        inspection_id = parts[-3]
        item_id       = parts[-1]
        event["pathParameters"] = {"inspection_id": inspection_id, "item_id": item_id}
        return update_checklist_item(event)

    if method == "GET" and (route_key == "/fire-extinguisher/evidence/upload-url" or path_endswith(path, "/fire-extinguisher/evidence/upload-url")):
        return generate_upload_url(event)

    if method == "GET" and (route_key == "/fire-extinguisher/evidence/download-url" or path_endswith(path, "/fire-extinguisher/evidence/download-url")):
        return generate_download_url(event)

    if method == "POST" and (route_key == "/fire-extinguisher/analyze" or path_endswith(path, "/fire-extinguisher/analyze")):
        return analyze_item_image(event)

    if method == "GET" and re.search(r"/fire-extinguisher/analyze/status/[^/]+$", path):
        job_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"job_id": job_id}
        return get_analyze_job_status(event)

    if method == "POST" and (route_key == "/fire-extinguisher/voice" or path_endswith(path, "/fire-extinguisher/voice")):
        return voice_command(event)

    return json_response(404, {"error": f"No route for {method} {path or resource}"})
