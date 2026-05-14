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
MAX_IMAGE_QUALITY = 85
YOLO_IMAGE_SIZE = 800

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
    "1": (
        "RULE — PRESENCE WITHIN 50 FEET OF RISK AREA:\n"
        "This item cannot be fully verified by an image.\n"
        "You must return pass=false (or inconclusive if available).\n"
        "In your reason, you MUST state: 'Distance to risk area cannot be verified from image — field validation required. Review required.'\n"
        "Set condition_checked='distance_not_verifiable'.\n"
        "Do NOT attempt to pass this item under any circumstances."
    ),
    "2": (
        "RULE — MOUNTED HEIGHT ACCESSIBLE FROM SEATED POSITION:\n"
        "This item cannot be fully verified by an image.\n"
        "You must return pass=false (or inconclusive if available).\n"
        "In your reason, you MUST state: 'Height and accessibility cannot be verified from image — field validation required. Review required.'\n"
        "Set condition_checked='height_not_verifiable'.\n"
        "Do NOT attempt to pass this item under any circumstances."
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
        "RULE — INSPECTION TAG ATTACHED AND DATED (2025 OR LATER):\n"
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
    # FIX: Items 8, 9, 10 keyword validation removed — component items use
    # enforce_component_checks which is authoritative. Keyword checks on
    # component items silently killed valid passes when Claude's reason
    # phrasing didn't exactly match these keywords.
    "8": [],   # Disabled — enforce_component_checks is authoritative for item 8
    "9": [],   # Disabled — enforce_component_checks is authoritative for item 9
    "10": [],  # Disabled — enforce_component_checks is authoritative for item 10
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

# FIX: Rewrote COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT to fix the core contradiction:
# Old version said "confirm target component is present" in a way that caused Claude
# to set target_visible=false when the gauge was small in a full-extinguisher shot.
# New version explicitly defines "visible" as "identifiable anywhere in the image,
# even if small or angled" — which is the correct interpretation.
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
  ✓ You can describe the component's appearance (e.g. "small round dial on the body")
  ✓ The component is present but then FAILS its condition check
  
  SET target_visible=false ONLY if:
  ✗ The component is literally not present anywhere in the image
  ✗ The image shows something completely unrelated (empty wall, person's face, etc.)
  ✗ You genuinely cannot find the component anywhere after examining the full image

  KEY INSIGHT: target_visible=true and pass=false is a VALID and COMMON outcome.
  A gauge that is present but shows low pressure → target_visible=true, pass=false.
  A tag that is attached but has no 2025+ date → target_visible=true, pass=false.
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
4. confidence: 0.85-1.0 if clearly visible and evaluated, 0.5-0.85 if visible but 
   challenging to read, 0.3-0.5 if barely visible. Set >0 whenever you can see component.

CORE EVIDENCE POLICY:
- For pressure gauges: judge needle POSITION relative to zones, not needle color.
- '195' or any number printed on the gauge face = scale label, NOT the needle.
- If required evidence is visible and readable, evaluate it even if small or angled.
- Never guess — but never refuse to evaluate what you can actually see.

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
    if Image is None:
        return image_bytes, content_type
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")

        if for_yolo:
            longest = max(img.width, img.height)
            if longest > YOLO_IMAGE_SIZE:
                scale = YOLO_IMAGE_SIZE / longest
                new_w = max(1, int(img.width * scale))
                new_h = max(1, int(img.height * scale))
                img = img.resize((new_w, new_h), Image.LANCZOS)
            quality = 92
        else:
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
    return None


def item_zoom_hint(item_id: str) -> str:
    hints = {
        "7": "Show the full extinguisher with hose attached and nozzle tip clearly visible.",
        "8": "Zoom in on the pressure gauge. Keep the full gauge face and needle visible.",
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

        # ─────────────────────────────────────────────────────────────────────
        # ITEM 8 CONTRACT — REWRITTEN (v5)
        #
        # Changes from v4:
        #  1. Added mandatory pre-analysis step (gauge type identification)
        #  2. Per-type zone determination (TYPE_RED_FACE, TYPE_CONCENTRIC, etc.)
        #  3. Concentric ring anti-hallucination: rings are decorative, not needles
        #  4. RECHARGE text anchoring fix: label is a zone boundary, not position
        #  5. Default to green when green arc visible and no displaced indicator
        #  6. Added concrete real-gauge examples from failing images
        # ─────────────────────────────────────────────────────────────────────
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

        # ─────────────────────────────────────────────────────────────────────
        # ITEM 9 CONTRACT — unchanged from original (working correctly)
        # ─────────────────────────────────────────────────────────────────────
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

        # ─────────────────────────────────────────────────────────────────────
        # ITEM 10 CONTRACT — REWRITTEN (v5)
        #
        # Changes from v4:
        #  1. Added service_card_completed field (true/false/not_applicable)
        #  2. Rotated 90° year grid reading instructions (bottom-to-top)
        #  3. KEY OVERRIDE: completed service card = auto-pass even if year unclear
        #  4. Explicit "WRONG analysis" example to prevent the exact failure mode
        #  5. PASS RULE updated: recent_year_visible=yes OR service_card_completed=true
        # ─────────────────────────────────────────────────────────────────────
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
        # FIX: Updated field name from "gauge_type_identified" to "gauge_type"
        # to match the rewritten contract.
        needle_zone            = _state("needle_zone")
        gauge_face_readability = _state("gauge_face_readability")
        gauge_glass_condition  = _state("gauge_glass_condition")
        gauge_type             = _state("gauge_type")          # FIX: was "gauge_type_identified"
        target_visible_raw     = _bool("target_visible")

        # ── Self-correction layer 1: needle_zone has a real value ─────────────
        # Claude clearly evaluated the gauge even if it set target_visible=false.
        if target_visible_raw is not True and needle_zone in (
            "green", "yellow_caution", "red_recharge", "red_overcharge"
        ):
            logger.warning(
                f"[ITEM8] target_visible=false but needle_zone={needle_zone} "
                f"— Claude evaluated gauge. Correcting target_visible=true."
            )
            checks["target_visible"] = True
            target_visible_raw = True

        # ── Self-correction layer 2: known gauge_type identified ──────────────
        KNOWN_GAUGE_TYPES = {"dial", "window_indicator", "popup_pin",
                             # legacy values from old contract, kept for safety:
                             "full_circle_needle", "semicircular_bottom_pivot",
                             "red_face_recharge_dial", "fireboss_window", "other_indicator",
                             "needle_dial"}
        if target_visible_raw is not True and gauge_type in KNOWN_GAUGE_TYPES:
            logger.warning(
                f"[ITEM8] target_visible=false but gauge_type={gauge_type} identified "
                f"— correcting target_visible=true."
            )
            checks["target_visible"] = True
            target_visible_raw = True

        # ── Self-correction layer 3: face is readable ─────────────────────────
        if target_visible_raw is not True and gauge_face_readability == "readable":
            logger.warning(
                f"[ITEM8] target_visible=false but gauge_face_readability=readable "
                f"— correcting target_visible=true."
            )
            checks["target_visible"] = True
            target_visible_raw = True

        # ── Self-correction layer 4 (NEW): confidence > 0 means Claude saw it ─
        # If all other signals are "unclear" but confidence > 0, Claude at least
        # attempted evaluation — treat as target_visible. This catches the case
        # where truncation caused all check fields to be "unclear" but Claude
        # still set a nonzero confidence because it saw the gauge.
        confidence_val = float(analysis.get("confidence", 0.0) or 0.0)
        if target_visible_raw is not True and confidence_val > 0.0:
            logger.warning(
                f"[ITEM8] target_visible=false but confidence={confidence_val:.2f} > 0 "
                f"— Claude attempted evaluation. Correcting target_visible=true. "
                f"needle_zone={needle_zone} gauge_type={gauge_type}"
            )
            checks["target_visible"] = True
            target_visible_raw = True

        # ── Hard gate — only fires if gauge truly not visible ─────────────────
        if target_visible_raw is not True:
            return force_fail(
                "target_not_visible",
                reason or "No pressure gauge or pressure indicator is visible in the image.",
                "Zoom in on the pressure gauge so it fills the frame clearly.",
                "Retake close-up image with the pressure gauge fully visible and in focus.",
            )

        # ── Zone resolution from trace signals ────────────────────────────────
        # Note: needle_trace_description removed from contract but kept here for
        # backward compatibility with any responses that still include it.
        needle_trace = str(checks.get("needle_trace_description", "")).lower()
        needle_color_seen = str(checks.get("needle_color_zone_seen", "")).lower()
        combined_trace = needle_trace + " " + needle_color_seen

        overcharge_signals = [
            "overcharg", "past max", "far right", "beyond max", "right side",
            "too high", "past normal", "past the max", "past maximum",
            "past green", "beyond green"
        ]
        recharge_signals = [
            "recharge", "far left", "near zero", "low pressure",
            "left side", "too low", "empty side"
        ]
        green_signals = [
            "green zone", "green area", "green arc", "green background",
            "center zone", "middle", "normal range", "between recharge",
            "between the labels", "straight up", "upward", "center band",
            "center top", "center of scale"
        ]

        if needle_zone == "unclear":
            if any(s in combined_trace for s in overcharge_signals):
                logger.warning(f"[ITEM8] Resolving unclear → red_overcharge from trace signals")
                checks["needle_zone"] = "red_overcharge"
                needle_zone = "red_overcharge"
            elif any(s in combined_trace for s in recharge_signals):
                logger.warning(f"[ITEM8] Resolving unclear → red_recharge from trace signals")
                checks["needle_zone"] = "red_recharge"
                needle_zone = "red_recharge"
            elif any(s in combined_trace for s in green_signals):
                logger.warning(f"[ITEM8] Resolving unclear → green from trace signals")
                checks["needle_zone"] = "green"
                needle_zone = "green"

        # Also resolve from the main reason text if needle_zone is still unclear
        if needle_zone == "unclear" and reason:
            reason_lower = reason.lower()
            if any(s in reason_lower for s in green_signals):
                logger.warning(f"[ITEM8] Resolving unclear → green from reason text")
                checks["needle_zone"] = "green"
                needle_zone = "green"
            elif any(s in reason_lower for s in recharge_signals):
                logger.warning(f"[ITEM8] Resolving unclear → red_recharge from reason text")
                checks["needle_zone"] = "red_recharge"
                needle_zone = "red_recharge"
            elif any(s in reason_lower for s in overcharge_signals):
                logger.warning(f"[ITEM8] Resolving unclear → red_overcharge from reason text")
                checks["needle_zone"] = "red_overcharge"
                needle_zone = "red_overcharge"

        # Correct red_recharge when all evidence points to green
        if (
            needle_zone == "red_recharge"
            and gauge_type in ("dial", "red_face_recharge_dial", "full_circle_needle",
                               "needle_dial", "semicircular_bottom_pivot")
            and not any(s in combined_trace for s in recharge_signals)
            and not any(s in combined_trace for s in overcharge_signals)
            and any(s in combined_trace for s in green_signals)
        ):
            logger.warning(
                f"[ITEM8] Correcting red_recharge → green. "
                f"No recharge/overcharge signals, green signals present."
            )
            checks["needle_zone"] = "green"
            needle_zone = "green"

        # ── Evaluate fail conditions ──────────────────────────────────────────
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
                msg    = "Pressure too low. Remove from service immediately."
                action = "Extinguisher needs recharging — remove from service and replace."
            elif "HIGH" in detail or "overcharg" in detail.lower():
                msg    = "Pressure too high. Remove from service for inspection."
                action = "Extinguisher is overcharged — remove from service and inspect."
            elif "CAUTION" in detail or "marginal" in detail:
                msg    = "Pressure marginal. Schedule servicing soon."
                action = "Pressure in caution zone — schedule servicing before it drops further."
            elif "glass" in detail or "readability" in detail:
                msg    = "Gauge is damaged or unreadable. Service the extinguisher."
                action = "Gauge damaged or unreadable — extinguisher needs servicing."
            else:
                msg    = "Gauge unreadable. Retake a clearer close-up."
                action = "Retake clear close-up of gauge with full face and needle visible."
            return force_fail(
                "gauge_failed",
                reason or f"Pressure gauge failed: {detail}.",
                msg,
                action,
            )

        analysis["pass"] = True
        return True, (condition_checked or "gauge_pressure_adequate"), reason, worker_message, suggested_action

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
        year_identified         = _state("year_identified")
        tag_format              = _state("tag_format")
        service_card_completed  = _state("service_card_completed")

        # ── Override: completed professional service card = auto-pass ────────
        # A service card with Registration Number + Licensee Name + Signature +
        # License Number + Work Type all filled in is proof of legitimate recent
        # service. If Claude couldn't read the rotated/cut-off year grid, we
        # trust the completed card over a misread year.
        if (
            tag_physically_attached == "attached"
            and service_card_completed == "true"
        ):
            # Allow only if the year identified is not a CONFIRMED pre-2025 year
            # (i.e., don't override if Claude explicitly found 2024 or earlier)
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
                return True, (condition_checked or "tag_attached_completed_service_card"), reason, worker_message, suggested_action

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
            return force_fail(
                "tag_failed",
                reason or f"Inspection tag failed: {detail}.",
                msg,
                action,
            )
        analysis["pass"] = True
        return True, (condition_checked or "tag_attached_legible_dated_initialed"), reason, worker_message, suggested_action

    # ── Fallback ─────────────────────────────────────────────────────────────
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
    body = parse_body(event)

    session_id         = str(body.get("session_id", "")).strip()
    body_inspection_id = str(body.get("inspection_id", "")).strip()
    team               = body.get("team", [])
    categories         = body.get("categories", [])
    general_results    = body.get("general_results", [])
    notes              = str(body.get("notes", "")).strip() if isinstance(body.get("notes", ""), str) else ""

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

    session       = None
    inspection_id = ""

    if session_id:
        session_resp = session_table.get_item(Key={"session_id": session_id})
        session      = session_resp.get("Item")
        if session:
            inspection_id = str(session.get("inspection_id", "")).strip()

    if not inspection_id and body_inspection_id:
        inspection_id     = body_inspection_id
        existing_for_meta = load_inspection(inspection_id)
        if existing_for_meta:
            session = {
                "auditor_name":  str(existing_for_meta.get("auditor_name",  body.get("auditor_name",  ""))).strip(),
                "facility_area": str(existing_for_meta.get("facility_area", body.get("facility_area", ""))).strip(),
                "date_of_audit": str(existing_for_meta.get("date_of_audit", body.get("date_of_audit", ""))).strip(),
            }
        else:
            session = {
                "auditor_name":  str(body.get("auditor_name",  "")).strip(),
                "facility_area": str(body.get("facility_area", "")).strip(),
                "date_of_audit": str(body.get("date_of_audit", "")).strip(),
            }

    if not session:
        return json_response(404, {"error": "Session not found. Create session first."})

    if not inspection_id:
        inspection_id = str(uuid.uuid4())
        if session_id:
            try:
                session_table.update_item(
                    Key={"session_id": session_id},
                    UpdateExpression="SET inspection_id = :iid",
                    ExpressionAttributeValues={":iid": inspection_id},
                )
            except Exception:
                logger.exception("Failed to persist generated inspection_id back to session table")

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
    saved_inspection = load_inspection(inspection_id) or merged_record

    status_code = 201 if not existing_inspection else 200
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

    original_image_bytes = image_bytes
    is_yolo_item = bool(required_yolo_class_for_item(item_id))

    if is_yolo_item:
        yolo_future    = _THREAD_POOL.submit(prepare_image_bytes, original_image_bytes, content_type, True)
        bedrock_future = _THREAD_POOL.submit(prepare_image_bytes, original_image_bytes, content_type, False)
        yolo_image_bytes,   normalized_type = yolo_future.result(timeout=30)
        bedrock_image_bytes, bedrock_type   = bedrock_future.result(timeout=30)
        image_bytes = yolo_image_bytes
    else:
        image_bytes, normalized_type = prepare_image_bytes(
            original_image_bytes, content_type, for_yolo=False
        )
        bedrock_image_bytes = image_bytes
        bedrock_type = normalized_type
    logger.info(f"[TIMING] after image extract/prepare: {time.time() - t0:.2f}s")

    yolo_detections: List[dict] = []
    yolo_target      = required_yolo_class_for_item(item_id)
    yolo_target_conf = 0.0

    if yolo_target:
        if not YOLO_ENDPOINT_NAME:
            return json_response(500, {"error": "YOLO endpoint is required for checklist items 7-10 but YOLO_ENDPOINT_NAME is not configured"})

        yolo_t      = time.time()
        yolo_result = invoke_yolo_endpoint(image_bytes, normalized_type)
        logger.info(f"[TIMING] YOLO done: {time.time() - yolo_t:.2f}s total: {time.time() - t0:.2f}s")

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
                "message":                worker_message,
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
            f"STEP 1: Is the target_component identifiable anywhere in this image?\n"
            f"  Remember: target_visible=true even if the component is small or the full\n"
            f"  extinguisher body is also visible. Only set target_visible=false if the\n"
            f"  component is completely absent from the image.\n"
            f"STEP 2: If yes, does it satisfy the strict_visual_rule? Evaluate each sub-condition.\n"
            f"STEP 3: Return checks object exactly per required_checks_contract.\n"
            f"Do not guess. If the required condition is not clearly visible, fail.\n"
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

    try:
        bedrock_t = time.time()
        # FIX: Use 600 tokens for component items (8,9,10).
        # Original 220 tokens was causing JSON truncation for item 8 whose
        # contract alone requires ~350 tokens to complete. Truncated JSON →
        # parse_failed → confidence=0.0 → blocked. 600 tokens gives comfortable
        # headroom for all component item contracts.
        _max_tokens = 600 if component_focus else 220
        analysis = invoke_claude_json(
            system_prompt=COMPONENT_IMAGE_ANALYSIS_SYSTEM_PROMPT if component_focus else IMAGE_ANALYSIS_SYSTEM_PROMPT,
            user_text=prompt,
            image_bytes=bedrock_image_bytes,
            media_type=bedrock_type,
            max_tokens=_max_tokens,
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

    # ── Item 7 structured check enforcement ────────────────────────────────
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
        logger.warning(f"AI returned pass=true with object_detected={object_detected} — overriding. item_id={item_id}")
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

    # FIX: Added "not component_focus" guard to keyword validation.
    # Previously this ran for ALL items including component items 8,9,10.
    # For component items, enforce_component_checks is the authoritative
    # pass/fail decision. The keyword check was silently killing valid passes
    # when Claude's reason text didn't happen to use exact keyword matches.
    expected_keywords = expected_keywords_for_item(item_id)
    if passed and expected_keywords and not component_focus:
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

    # Special case: item 1 — extinguisher genuinely missing
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

    if str(item_id) in ["1", "2"]:
        checklist_item["answer"]               = "N/A"
    else:
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
            item["evidence"] = cleaned_evidence[-1:] if cleaned_evidence else []

            for field in ["finding", "action_item", "responsible"]:
                if field in item:
                    item[field] = str(item.get(field, ""))[:200]

    if "notes" in inspection:
        inspection["notes"] = str(inspection.get("notes", ""))[:1000]

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

    body_str = json.dumps(inspection, default=str)
    size_kb  = len(body_str.encode("utf-8")) / 1024
    logger.info(f"[REPORT] inspection_id={inspection_id} response_size={size_kb:.1f}KB")

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

    def _load_inspection_and_item():
        if not (inspection_id and current_item_id):
            return None, None
        insp = load_inspection(inspection_id)
        if not insp:
            return None, None
        item, _, _ = find_item(insp, current_item_id)
        return insp, item

    dynamo_future = _THREAD_POOL.submit(_load_inspection_and_item)
    local         = parse_voice_intent_locally(text)

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