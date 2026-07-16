import base64
import copy
import io
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
import concurrent.futures

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Environment ────────────────────────────────────────────────────────────────
AWS_REGION            = os.getenv("AWS_REGION", "ap-south-1")
INSPECTION_TABLE_NAME = os.getenv("EYEWASH_INSPECTION_TABLE_NAME", "osha-eyewash-inspections")
SESSION_TABLE_NAME    = os.getenv("SESSION_TABLE_NAME", "osha-inspection-sessions")
EVIDENCE_BUCKET       = os.getenv("EVIDENCE_S3_BUCKET", "osha-inspection-evidence-media")
MODEL_ID              = os.getenv("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
EXPECTED_API_KEY      = os.getenv("API_KEY", "").strip()
UPLOAD_URL_EXPIRY     = int(os.getenv("UPLOAD_URL_EXPIRY", "900"))
DOWNLOAD_URL_EXPIRY   = int(os.getenv("DOWNLOAD_URL_EXPIRY", "3600"))
MAX_IMAGE_WIDTH       = 800
MAX_IMAGE_QUALITY     = 85
BATCH_WORKER_COUNT    = int(os.getenv("BATCH_WORKER_COUNT", "4"))

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}

# ── AWS Clients ────────────────────────────────────────────────────────────────
dynamodb         = boto3.resource("dynamodb", region_name=AWS_REGION)
s3               = boto3.client("s3", region_name=AWS_REGION)
bedrock          = boto3.client("bedrock-runtime", region_name=AWS_REGION)
inspection_table = dynamodb.Table(INSPECTION_TABLE_NAME)
session_table    = dynamodb.Table(SESSION_TABLE_NAME)

# ── Checklist (Fallback — used when DynamoDB is unreachable) ─────────────────
_FALLBACK_CHECKLIST = {
    "inspection_type": "Eyewash/Emergency Shower Weekly Inspection",
    "general_information": {"location": "", "start_date": "", "checklist": "Eyewash/Emergency Shower Weekly Inspection", "leader": "", "team": []},
    "available_answers": ["Yes", "No", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Gravity Fed Eyewash Station(s)",
            "items": [
                {"id": 1,  "description": "Location is accessible within 10 seconds of the hazard.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 2,  "description": "Path of travel is free of obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 3,  "description": "Location is well lit and identified with a highly visible sign.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 4,  "description": "Eyewash Station is free of clutter.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 5,  "description": "Exterior of station is satisfactory. No signs of damage, leaks or corrosion.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 6,  "description": "Wipe down eyewash station with clean cloth/disinfectant wipe to keep clean and free of debris.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 7,  "description": "IF station is a cartridge unit- do NOT flush. CHECK cartridge expiration dates to ensure fluid is still within date range. If within 1 month of expiration, order replacement cartridges.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 8,  "description": "IF still using liquid preservative do NOT flush. CHECK water is at MIN FILL line and there are no contaminants. CHECK date of last preservative and drain/replenish according to preservative instructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 9,  "description": "IF unit has a cleansing stick, the unit is activated to verify proper flow out of both openings. Top off activated unit with additional clean water. CHECK date of last stick maintenance (every year). Stick is good for 3 years.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 10, "description": "Sign and date inspection tag attached to eyewash station.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 2,
            "name": "Plumbed Eyewash Station(s)",
            "items": [
                {"id": 11, "description": "Location is accessible within 10 seconds of the hazard.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 12, "description": "Path of travel is free of obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 13, "description": "Location is well lit and identified with a highly visible sign.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 14, "description": "IF shut-off valves are installed in the supply line, provisions are made to prevent unauthorized shut off.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 15, "description": "Eyewash Station is free of clutter.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 16, "description": "Wipe down eyewash station with clean cloth/disinfectant wipe to keep clean and free of debris.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 17, "description": "Nozzle caps are present and in good condition to prevent contamination.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 18, "description": "Hands-free valve activates in one second or less. Water flows steadily until manually closed. If sediment is present, flush thoroughly.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 19, "description": "When activated, both nozzle caps come off with the pressure of the water flow.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 20, "description": "Stream flow is equal and high enough to reach eyes. Verify pressure would not be injurious to the eyes.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 21, "description": "Temperature of the water is tepid (between 60 to 100 degrees F).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 22, "description": "Sign and date inspection tag attached to eyewash station.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 3,
            "name": "Emergency Shower Inspection",
            "items": [
                {"id": 23, "description": "Location is accessible within 10 seconds of the hazard.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 24, "description": "Path of travel is free of obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 25, "description": "Location is well lit and identified with a highly visible sign.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 26, "description": "IF shut-off valves are installed in the supply line, provisions are made to prevent unauthorized shut off.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 27, "description": "Station is free of clutter.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 28, "description": "Wipe down station with clean cloth/disinfectant wipe to keep clean and free of debris.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 29, "description": "Temperature of the fluid is tepid (between 60 to 100 degrees F).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 30, "description": "Hands-free valve activates in one second or less. Water flows steadily until manually closed. For eyewash/shower combos, verify eyewash flow is not affected when shower is activated.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 31, "description": "Sign and date inspection tag attached to eyewash station.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 4,
            "name": "Supplemental Eyewash Bottles",
            "items": [
                {"id": 32, "description": "Confirm acknowledgement that supplemental eyewash bottles do not meet requirements for an eyewash and you have an additional eyewash station (plumbed or flooded) within 10 seconds of your charging/filling station(s).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 33, "description": "Path of travel is free of obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 34, "description": "Location is well lit and identified with a highly visible sign.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 35, "description": "Station is free of clutter.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 36, "description": "Wipe down station with clean cloth/disinfectant wipe to keep clean and free of debris.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 37, "description": "Check solution expiration date. Solution is NOT expired. If bottles are expired, dispose and replace. Do NOT bulk order. Order replacement bottles 1 month prior to expiration.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 38, "description": "Sign and date inspection tag attached to eyewash station.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
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

# ── Visual rules per item ──────────────────────────────────────────────────────
CHECKLIST_VISUAL_RULES = {
    "1": """
ITEM: Gravity-fed eyewash accessibility within 10 seconds of hazard.
LOOK FOR: The eyewash station in relation to its surrounding environment.
PASS if: Station is in open area with no locked doors, gates, turnstiles, or fixed barriers.
FAIL if: Locked door/gate visible between station and work area; station inside a separate locked room;
  heavy permanent shelving/machinery completely blocks direct approach; station not visible at all.
NOTE: Evaluate visible barriers only. Cannot measure 10 seconds from a photo.
""",
    "2": """
ITEM: Path of travel to gravity-fed eyewash is free of obstructions.
LOOK FOR: Floor space and aisle leading directly to station (within ~3 feet).
PASS if: Floor directly in front of and leading to station is completely clear.
FAIL if: ANY object on floor within direct path (boxes, bins, carts, equipment);
  hoses/cords crossing approach path; item placed directly in front of basin.
""",
    "3": """
ITEM: Gravity-fed eyewash location is well-lit with a highly visible sign.
LOOK FOR: (A) Green/white eyewash safety sign with eyewash pictogram. (B) Adequate ambient lighting.
PASS if: Green eyewash sign clearly visible on/above/beside station AND area adequately lit.
FAIL if: No sign visible; sign covered/obscured/wrong direction; wrong color/symbol;
  area visibly dark; sign torn/vandalized/unreadable.
""",
    "4": """
ITEM: Gravity-fed eyewash station is free of clutter.
LOOK FOR: Top surface, basin, shelf, and area within 12 inches of unit.
PASS if: Basin completely empty. No items on/in/around unit. Area clean and dedicated.
FAIL if: Any item resting on station; items stored on attached shelf;
  personal belongings hung on/leaning against unit; supplies on basin/ledge.
""",
    "5": """
ITEM: Gravity-fed eyewash exterior shows no damage, leaks, or corrosion.
LOOK FOR: Outer housing, basin, lid, nozzle area, visible fittings and seams.
PASS if: All surfaces intact, uniform color, no rust/mineral deposits/cracks/leaks.
FAIL if: Orange/brown rust visible; white/green crusty mineral deposits; visible crack/dent;
  water stains/puddles indicating leak; lid missing/broken; significant discoloration.
""",
    "6": """
ITEM: Gravity-fed eyewash station has been wiped down and is visibly clean.
LOOK FOR: Outer casing, basin interior, lid surface, nozzle area, surrounding mount.
PASS if: All surfaces appear clean with no grime/dust buildup/biological growth.
FAIL if: Visible dark grime or grease; dust accumulation on top/lid/nozzle;
  green or black biological growth; debris inside basin.
""",
    "7": """
ITEM: Cartridge unit — cartridge expiration dates visible and within valid range.
LOOK FOR: Expiration date label/sticker on cartridge(s) inside or attached to unit.
PASS if: Clear expiration date label visible AND legible AND date is in the future AND cartridge properly seated.
FAIL if: No expiration label visible; label exists but unreadable; date clearly expired;
  cartridge missing or improperly seated.
NOTE: If date present but numbers unreadable → FAIL.
""",
    "8": """
ITEM: Liquid preservative unit — water at MIN FILL line, clear and uncontaminated.
LOOK FOR: Water level indicator on basin side AND visual quality of water.
PASS if: Water level clearly at or above MIN FILL line AND water appears clear/colorless with no debris.
FAIL if: Water level below MIN FILL line; no visible indicator; water yellow/brown/green/cloudy;
  visible debris/sediment/growth; basin appears empty.
""",
    "9": """
ITEM: Cleansing stick unit — unit activated, proper flow from both openings, stick present.
LOOK FOR: (A) Cleansing stick mechanism. (B) Water flow from BOTH nozzle openings. (C) Date tag.
PASS if: Cleansing stick visibly present AND water flow actively visible from BOTH nozzles in photo.
FAIL if: No cleansing stick visible; only one nozzle flowing; no water flow visible;
  stick damaged/improperly inserted; cannot confirm both openings flowing.
NOTE: Static photo at rest cannot confirm activation → FAIL if no active flow visible.
""",
    "10": """
ITEM: Signed and dated inspection tag attached to gravity-fed eyewash station.
LOOK FOR: Physical tag/label on unit showing recent inspection dates and inspector initials.
PASS if: Tag clearly visible AND attached AND at least one legible date entry AND recent date
  within inspection cycle AND signature/initials present.
FAIL if: No tag visible; hanging loose/detached; all date fields blank; date unreadable;
  most recent date clearly old; no signature/initials — date stamp alone insufficient; tag damaged.
""",
    "11": """
ITEM: Plumbed eyewash station is accessible within 10 seconds of hazard.
LOOK FOR: Station in environment — assess physical barriers only.
PASS if: Station in open accessible space with no locked barriers/enclosed rooms.
FAIL if: Locked door/security gate between station and work area; inside a key-access room;
  fixed equipment creates maze path; station not clearly visible.
""",
    "12": """
ITEM: Path of travel to plumbed eyewash station is free of obstructions.
LOOK FOR: Floor space directly in front of and leading to plumbed eyewash station.
PASS if: Completely clear floor path (min 28 inches wide per ANSI) visible with no items.
FAIL if: Any movable or fixed item on floor in direct approach path; hoses/cords crossing path;
  cart/bin/box parked within 3 feet; permanently mounted equipment blocking approach.
""",
    "13": """
ITEM: Plumbed eyewash location is well-lit with a highly visible sign.
LOOK FOR: (A) Green/white eyewash safety sign. (B) Active lighting or natural light.
PASS if: Green eyewash sign clearly legible/properly oriented/unobstructed AND area well-lit.
FAIL if: No green eyewash sign near plumbed station; sign faded/torn/wrong color/facing away;
  area poorly lit — station in shadow; sign blocked by shelving/pipes.
""",
    "14": """
ITEM: Plumbed shut-off valves have anti-tampering/unauthorized shutoff provisions.
LOOK FOR: Valves on supply piping, then padlocks/chain locks/valve covers/warning signage on them.
PASS if: No intermediate shutoff valve visible OR valves present AND secured with visible lock/chain/cover.
FAIL if: Valve visible with zero security device; valve appears closed with no lock;
  lock/chain present but visibly open/broken/removed.
NOTE: If no valve visible → confidence low (0.3), pass=false (inconclusive).
""",
    "15": """
ITEM: Plumbed eyewash station is free of clutter.
LOOK FOR: Basin, bowl/heads, and area directly surrounding unit.
PASS if: Basin empty. No items stored on/in/directly against unit.
FAIL if: Items on/inside basin; anything hung on activation handle;
  signage taped over basin area; work materials leaning against unit.
""",
    "16": """
ITEM: Plumbed eyewash station has been wiped down and is visibly clean.
LOOK FOR: Nozzle heads, basin bowl, push handle, body casing, mounting surface.
PASS if: All surfaces clean with no grime/biological growth/heavy mineral deposits. Mild water spots acceptable.
FAIL if: Green/black biological growth around nozzles/basin; heavy lime scale on nozzle heads;
  dark grease on activation handle; significant dust; debris inside basin.
""",
    "17": """
ITEM: Plumbed eyewash nozzle caps are present and in good condition.
LOOK FOR: Two small protective caps atop each nozzle head (typically white or yellow).
PASS if: BOTH nozzle caps clearly visible, fully seated, undamaged — no cracks/chips/deformation.
FAIL if: One or both caps missing; cap visibly cracked/chipped/deformed; cap loose/not seated;
  caps so discolored/brittle integrity is in doubt; cannot clearly see both nozzle areas → FAIL.
""",
    "18": """
ITEM: Hands-free valve activates in one second; water flows steadily until manually closed.
LOOK FOR: Water streaming from both nozzles AND hands-free activation mechanism type.
PASS if: Active water flow clearly visible FROM BOTH nozzles AND mechanism is hands-free type
  (push paddle/foot bar/auto-sensor — NOT a manual twist/turn knob).
FAIL if: No active water flow visible in photo; only one nozzle flowing; flow weak/intermittent;
  mechanism requires hands to hold open; water discolored/sediment visible; manual twist valve.
NOTE: Static image at rest CANNOT confirm this item → FAIL if no flow visible.
""",
    "19": """
ITEM: Both nozzle caps come off automatically with water pressure when activated.
LOOK FOR: Active photo of eyewash being operated — caps should be off (popped, not manually removed).
PASS if: Photo taken during activation AND both caps clearly off AND water flowing from both nozzles.
FAIL if: Static shot with no activation (inconclusive); caps still on during apparent activation;
  only one cap off; caps appear manually removed before photo.
""",
    "20": """
ITEM: Stream flow equal from both nozzles, sufficient height to reach eyes, pressure not injurious.
LOOK FOR: Both water streams during active flow test — compare heights and volumes.
PASS if: Both streams clearly visible AND equal height/volume AND reach eye-level height AND
  appear gentle/controlled (not jet-like blast).
FAIL if: One stream visibly higher/stronger; streams angled away from center; streams too low;
  streams appear high-pressure/injurious; no active flow visible; mineral buildup deflecting streams.
""",
    "21": """
ITEM: Water temperature is tepid (60–100°F / 16–38°C).
LOOK FOR: Visual temperature cues — steam (too hot), frost/ice (too cold), readable gauge.
PASS if: No steam rising; no frost/ice/heavy condensation on pipes; no HOT warning label;
  OR thermometer visible and reads 60–100°F.
FAIL if: Steam rising from water/unit/pipes; frost/ice on pipes or unit; temperature gauge reads outside range;
  CAUTION HOT WATER label posted on or near unit.
NOTE: Without steam, ice, or readable gauge → FAIL (inconclusive). Low confidence (0.3–0.4) appropriate.
""",
    "22": """
ITEM: Signed and dated inspection tag attached to plumbed eyewash station.
LOOK FOR: Physical hanging tag/adhesive label/laminated card on unit with date and initials fields.
PASS if: Tag clearly attached AND at least one legible date row filled AND date appears recent
  (within weekly cycle) AND signature/initials present.
FAIL if: No tag on unit; all date/signature fields blank; tag detached/in basin;
  date or initials unreadable; most recent date visibly old (>2 weeks); laminated but fields empty.
""",
    "23": """
ITEM: Emergency shower is accessible within 10 seconds of hazard.
LOOK FOR: Overhead shower head with pull handle/chain. Assess barriers from visible work areas.
PASS if: Shower in open accessible space with no locked barriers/enclosed rooms from visible work zone.
FAIL if: Locked door/security gate between shower and work area; inside a key-access room;
  fixed equipment creates maze path; shower head or pull rod not visible.
""",
    "24": """
ITEM: Path of travel to emergency shower is free of obstructions.
LOOK FOR: Floor directly beneath shower and 3-foot radius around drain area plus approach aisle.
PASS if: Floor beneath shower head completely clear. Approach aisle has no items. Pull rod hangs freely.
FAIL if: Equipment/pallet/bin/cart beneath or in front of shower; items in approach aisle;
  pull chain wrapped around pipe/inaccessible; hoses/cords on floor in approach zone.
""",
    "25": """
ITEM: Emergency shower location is well-lit with a highly visible sign.
LOOK FOR: Green/white emergency shower sign (larger format) above/near unit AND adequate lighting.
PASS if: Green emergency shower sign clearly visible/properly oriented/unobstructed AND area well-lit.
FAIL if: No green emergency shower sign visible; sign obscured by pipes/conduit/hardware;
  sign color faded to white/gray; area visibly dark; sign shows wrong symbol.
""",
    "26": """
ITEM: Emergency shower shut-off valves have anti-tampering/unauthorized shutoff provisions.
LOOK FOR: Valves on supply pipe, then padlocks/chain locks/lockout covers/DO NOT CLOSE signage.
PASS if: No intermediate shutoff valve visible OR valves present AND secured with visible lock/chain/cover.
FAIL if: Valve present with zero security device; lock visible but open/unlatched;
  valve handle in closed position; security chain broken/removed and valve unsecured.
""",
    "27": """
ITEM: Emergency shower station area is free of clutter.
LOOK FOR: Area within 4-foot radius of shower base/drain, shower head, and pull rod/chain.
PASS if: Area beneath shower completely clear. Nothing hangs from or stored near pull chain.
FAIL if: Boxes/bins/equipment stored beneath shower; items hanging from pull-chain;
  hoses/tubing/signage draped over shower arm; shower inside storage area with crowded items.
""",
    "28": """
ITEM: Emergency shower has been wiped down and is visibly clean.
LOOK FOR: Shower head diffuser plate, pull arm/handle, supply pipe, drain cover.
PASS if: Shower head free of heavy mineral scale/biological growth/corrosion. Pull handle clean.
FAIL if: Green/brown biological growth on shower head diffuser; heavy white lime scale coating holes;
  significant rust on supply pipe/pull arm/shower head; dark grime on pull handle; drain clogged/missing.
""",
    "29": """
ITEM: Emergency shower water temperature is tepid (60–100°F / 16–38°C).
LOOK FOR: Visual temperature cues — steam, frost/ice on pipes, temperature warning labels, readable gauge.
PASS if: No steam visible; no frost/ice on pipes; no hot/cold warning labels; OR gauge reads 60–100°F.
FAIL if: Steam rising from shower head/pipes; ice/frost/heavy condensation on cold pipes;
  HOT WATER or HIGH TEMPERATURE label on unit; gauge shows temperature outside safe range.
NOTE: Inherently difficult to assess from static photo. Unless confirming evidence exists → FAIL (0.3 confidence).
""",
    "30": """
ITEM: Hands-free shower valve activates in one second; water flows steadily; does not affect eyewash.
LOOK FOR: (A) Pull handle/treadle bar (hands-free). (B) Water flowing from shower head in photo.
  (C) For combo units: eyewash also flowing simultaneously.
PASS if: Active shower flow visible in photo AND activation mechanism is pull-rod type (hands-free)
  AND if combo unit, both shower and eyewash are flowing.
FAIL if: No active water flow visible (static photo cannot confirm); flow weak/inconsistent;
  pull rod missing/damaged/inaccessible; combo: eyewash inactive while shower runs.
""",
    "31": """
ITEM: Signed and dated inspection tag attached to emergency shower.
LOOK FOR: Tag hanging from pull rod/attached to supply pipe/mounted on bracket near shower.
  Should show date fields and inspector initials/signature.
PASS if: Tag clearly visible AND securely attached to shower structure AND legible date entry
  from current inspection cycle AND inspector initials/signature present.
FAIL if: No tag on or near shower; date fields blank; tag detached/on floor;
  date entries not legible; last date >2 weeks old; tag belongs to different equipment.
""",
    "32": """
ITEM: Supplemental bottles present AND compliant plumbed/gravity station confirmed within 10 seconds.
LOOK FOR: Green wall-mounted holder with squeeze bottles AND evidence of permanent eyewash station nearby.
PASS if: Supplemental bottles clearly visible AND a separate plumbed or gravity-fed eyewash station
  is also visible in same image within apparent 10-second reach.
FAIL if: Only eyewash bottles visible with no permanent station in image; bottles missing from holder;
  holder present but empty; cannot confirm permanent station exists within 10 seconds.
NOTE: Supplemental bottles are NOT a substitute for a permanent station. If bottles are only eyewash option → FAIL.
""",
    "33": """
ITEM: Path to supplemental eyewash bottle station is free of obstructions.
LOOK FOR: Floor space and aisle within 3 feet of bottle holder.
PASS if: Clear, open floor path directly to bottle station. No items blocking approach.
FAIL if: Items on floor within direct approach path; bins/carts/equipment directly in front of holder;
  locked cabinet prevents immediate bottle retrieval.
""",
    "34": """
ITEM: Supplemental bottle station has a visible sign and is well-lit.
LOOK FOR: Green eyewash sign near/on bottle holder AND adequate lighting.
PASS if: Green eyewash/emergency wash sign clearly visible at/above bottle station AND area adequately lit.
FAIL if: No green eyewash sign near bottle station; station in dark corner; sign faded/wrong color/unreadable.
""",
    "35": """
ITEM: Supplemental eyewash bottle station is free of clutter.
LOOK FOR: Holder, shelves around holder, and immediate surrounding area.
PASS if: Area around bottle holder clear. No non-eyewash items on same shelf or blocking bottle access.
FAIL if: Other chemicals/cleaning supplies/equipment stored alongside bottles;
  items stacked in front obscuring access; non-eyewash materials inside holder bracket.
""",
    "36": """
ITEM: Supplemental eyewash bottles are externally clean and free of contamination.
LOOK FOR: Exterior of each bottle — body, cap/nozzle area, and the holder itself.
PASS if: Bottle exteriors clean with no grime/chemical splash/discoloration. Caps/nozzles sealed/undamaged.
FAIL if: Visible chemical residue/staining/grime on bottle exteriors; caps missing/open/broken;
  bottles discolored/sticky/coated; holder heavily dirty or corroded.
""",
    "37": """
ITEM: Supplemental eyewash bottle solution is NOT expired; expiration date is visible.
LOOK FOR: Expiration date label on each bottle (format MM/YYYY or MM/DD/YYYY — on side or bottom).
PASS if: Expiration date clearly readable on at least one bottle AND date is in the future.
  All visible bottles should show unexpired dates.
FAIL if: Expiration date label not visible; date present but unreadable (faded/smudged/too small);
  any bottle shows expired date; bottles have no expiration label; date appears tampered with.
NOTE: If even one visible bottle is expired → FAIL. If date unreadable for any bottle → FAIL.
""",
    "38": """
ITEM: Signed and dated inspection tag attached to supplemental eyewash bottle station.
LOOK FOR: Tag on bottle holder bracket, mounted on wall beside it, or attached via zip tie.
  Should show inspection dates and inspector identification.
PASS if: Tag clearly attached to bottle station AND legible recent date entry AND signature/initials present.
FAIL if: No tag on or near bottle holder; tag blank (all fields empty); tag belongs to different equipment;
  date unreadable or clearly outdated; signature/initials field empty — date alone insufficient.
""",
}

# ─────────────────────────────────────────────
# AI System Prompts (Eyewash / Emergency Shower)
# ─────────────────────────────────────────────
IMAGE_ANALYSIS_SYSTEM_PROMPT = """
You are a CERTIFIED OSHA EYEWASH & EMERGENCY SHOWER SAFETY INSPECTOR with 15 years of field experience.
Your job is to evaluate one or more photographs (frames from video or multiple images) against one specific ANSI Z358.1 checklist item.
Each image is analyzed independently; the system aggregates results using the best (highest confidence, most favorable) outcome.

PRIME DIRECTIVE — ZERO TOLERANCE FOR AMBIGUITY:
• INCONCLUSIVE = FAIL. Always.
• PARTIALLY VISIBLE = FAIL. Always.
• CANNOT READ LABEL/TAG/DATE = FAIL. Always.
• OBSTRUCTED VIEW = FAIL. Always.
• If you are forming the thought "it probably is fine" → that is a FAIL.
• pass=true requires CLEAR, UNAMBIGUOUS, DIRECT visual confirmation.

STEP 1 — SUBJECT PRESENCE CHECK:
Identify what safety equipment is present:
  • eyewash_station   → wall-mounted or pedestal basin with dual nozzles
  • emergency_shower  → overhead shower head with pull rod/handle
  • eyewash_bottle    → portable squeeze bottle(s) in green holder
  • combo_unit        → both shower and eyewash combined
  • other             → not eyewash safety equipment
  • unclear           → cannot determine
If the item to evaluate is NOT clearly the primary subject → object_detected="other"/"unclear", pass=false, stop.

STEP 2 — ITEM-SPECIFIC CONDITION CHECK:
Only proceed if Step 1 confirmed correct equipment type.
Apply ONLY the strict_visual_rule provided. Do not evaluate outside its scope.

HARD RULES (override everything):
  R1. Rust, corrosion, mineral deposits, visible cracks → ALWAYS fail items 5, 17, 20
  R2. Missing nozzle caps → ALWAYS fail item 17
  R3. Unequal or missing water streams → ALWAYS fail item 20
  R4. Any obstruction in travel path → ALWAYS fail items 2, 12, 24, 33
  R5. No visible green eyewash sign → ALWAYS fail items 3, 13, 25, 34
  R6. Inspection tag missing/not dated/date unreadable → ALWAYS fail items 10, 22, 31, 38
  R7. Expiration date unreadable or past → ALWAYS fail items 7, 37
  R8. Water level below MIN FILL line → ALWAYS fail item 8
  R9. Nozzle caps require manual removal → ALWAYS fail item 19

CONFIDENCE CALIBRATION:
  1.0 = Crystal clear image, condition unambiguously confirmed or denied
  0.8 = Clear image, minor uncertainty about one small detail
  0.6 = Adequate image, some details slightly unclear
  0.4 = Image blurry, partial, or condition only partially visible
  0.2 = Very poor image quality or subject barely visible
  If confidence < 0.5, you MUST set pass=false regardless of what you think you see.

Return JSON ONLY. No markdown. No extra text.
{
  "object_detected": "eyewash_station|emergency_shower|eyewash_bottle|combo_unit|other|unclear",
  "condition_checked": "<10-word max summary of what was evaluated>",
  "pass": true|false,
  "confidence": <float 0.0-1.0>,
  "reason": "<2-3 sentences: exactly what you see and pixel-level evidence for pass or fail>",
  "worker_message": "<actionable instruction under 12 words>",
  "suggested_action": "<specific corrective action, or null if passed>"
You are a STRICT component-level inspector for eyewash and emergency shower images.
You analyze specific components (nozzle caps, tags, labels, gauge-like indicators on cartridges).

═══════════════════════════════════════════════════════════════
GOLDEN RULE: INCONCLUSIVE = FAIL
If you cannot clearly confirm a component condition is met, set pass=false.
═══════════════════════════════════════════════════════════════

CRITICAL — WHAT "target_visible=true" MEANS:
    The component EXISTS somewhere in the image and you can describe it.
    It does NOT need to fill the frame or be a close-up.

SET target_visible=true if:
    ✓ The component is anywhere in the image and describable (even small/angled).
    ✓ You can identify the component (e.g., "small white nozzle cap on right head").

SET target_visible=false ONLY if:
    ✗ The component is literally not present anywhere in the image.

Follow the same JSON schema as the main prompt, but include `target_visible` when relevant.
"""


# ── AI Enablement Policy (from Eyewash_Inspection_AI_Enablement.xlsx) ─────────
MANUAL_ONLY_ITEMS = {"6", "16", "21", "28", "29", "36"}
PARTIAL_AI_ITEMS  = {"7", "8", "9", "18", "20", "30"}

ITEM_CONFIDENCE_LEVEL = {
    "1": "medium", "2": "high", "3": "high", "4": "high", "5": "high",
    "6": "n/a", "7": "medium", "8": "medium", "9": "medium", "10": "high",
    "11": "medium", "12": "high", "13": "high", "14": "high", "15": "high",
    "16": "n/a", "17": "high", "18": "medium", "19": "high", "20": "medium",
    "21": "n/a", "22": "high", "23": "medium", "24": "high", "25": "high",
    "26": "high", "27": "high", "28": "n/a", "29": "n/a", "30": "medium",
    "31": "high", "32": "medium", "33": "high", "34": "high", "35": "high",
    "36": "n/a", "37": "high", "38": "high",
}

ITEM_MEDIA_REQUIREMENT = {
    "7": "clear_label_photo",
    "8": "clear_level_and_label_photo",
    "9": "short_video_or_two_phase_images",
    "18": "short_video",
    "20": "active_flow_photo_or_video",
    "30": "short_video",
}

MANUAL_REVIEW_REASON = {
    "7": "AI can read visible dates, but inspector must confirm replacement action and unclear labels.",
    "8": "AI can estimate fill line and visible label details, but inspector must confirm preservative maintenance action.",
    "9": "AI can verify visible flow and stick/date clues, but physical activation and final sign-off are manual.",
    "18": "AI needs activation evidence and can only estimate timing; inspector must confirm one-second response and stable flow.",
    "20": "AI can estimate stream symmetry visually, but pressure safety and final usability judgment are manual.",
    "30": "AI needs activation evidence and can only estimate timing/combination behavior; inspector must confirm final compliance.",
}

MANUAL_ONLY_REASON = {
    "6": "Physical wipe-down is a maintenance action and cannot be completed by AI.",
    "16": "Physical wipe-down is a maintenance action and cannot be completed by AI.",
    "21": "Water temperature requires a thermometer or probe and cannot be measured from images.",
    "28": "Physical wipe-down is a maintenance action and cannot be completed by AI.",
    "29": "Water temperature requires a thermometer or probe and cannot be measured from images.",
    "36": "Physical wipe-down is a maintenance action and cannot be completed by AI.",
}


def get_item_ai_policy(item_id: str) -> dict:
    sid = str(item_id)
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
        "mode": "ai",
        "manual_review_required": False,
        "confidence_level": ITEM_CONFIDENCE_LEVEL.get(sid, "high"),
        "media_requirement": "photo",
        "reason": "Full visual AI inspection is feasible with a clear image.",
    }


def get_checklist_template(company_key="default", force_refresh=False):
    """Load checklist from DynamoDB with company overlay fallback. Falls back to hardcoded."""
    if load_checklist is not None:
        template = load_checklist("eyewash", company_key, force_refresh=force_refresh)
        if template is not None:
            return template
    return copy.deepcopy(_FALLBACK_CHECKLIST)


def get_ai_enablement_matrix(event: dict) -> dict:
    rows = []
    checklist = get_checklist_template()
    for category in checklist.get("categories", []):
        for item in category.get("items", []):
            sid = str(item.get("id", ""))
            policy = get_item_ai_policy(sid)
            rows.append({
                "item_id": item.get("id"),
                "section": category.get("name", ""),
                "description": item.get("description", ""),
                "ai_mode": policy["mode"],
                "manual_review_required": policy["manual_review_required"],
                "confidence_level": policy["confidence_level"],
                "media_requirement": policy["media_requirement"],
                "reason": policy["reason"],
            })
    return json_response(200, {"ai_enablement_matrix": rows})


def build_item_prompt(checklist_item: dict, rule: str, policy: Optional[dict] = None) -> str:
    """Builds a structured analysis prompt for a single checklist item."""
    item_id   = checklist_item.get("id", "?")
    item_desc = checklist_item.get("description", "")
    policy = policy or get_item_ai_policy(str(item_id))

    parts = [
        f"You are evaluating checklist item #{item_id} in an OSHA eyewash/emergency shower inspection.",
        "",
        "CHECKLIST ITEM DETAILS",
        f"Item ID    : {item_id}",
        f"Description: {item_desc}",
        "",
        "STRICT VISUAL RULE FOR THIS ITEM",
        rule.strip(),
        "",
        "YOUR EVALUATION TASK",
        "STEP 1: Is the correct type of safety equipment CLEARLY VISIBLE as the main subject?",
        "        If not, set object_detected=other/unclear, pass=false, confidence < 0.4.",
        "STEP 2: Does the image CLEARLY SATISFY every requirement in the STRICT VISUAL RULE?",
        "        Apply HARD RULES from system instructions if relevant to this item.",
        "STEP 3: Score confidence (0.0-1.0) based on image clarity. If confidence < 0.5, pass must be false.",
        "",
        "REMINDERS: INCONCLUSIVE = FAIL. Do NOT assume what is behind an obstruction. "
        "Report ONLY what you directly observe. Return JSON only.",
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


# ── Utilities ──────────────────────────────────────────────────────────────────
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

def parse_body(event: dict) -> dict:
    body = event.get("body")
    if body in (None, ""):
        return {}
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, dict):
        return body
    try:
        return json.loads(body)
    except Exception:
        return {}

def get_query(event: dict, key: str, default: str = "") -> str:
    return (event.get("queryStringParameters") or {}).get(key, default) or default

def get_route(event: dict) -> Tuple[str, str, str]:
    method   = (event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
    path     = event.get("path") or event.get("rawPath") or ""
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
    headers     = _normalized_headers(event)
    provided    = (headers.get("x-api-key") or headers.get("x_api_key") or headers.get("apikey") or "").strip()
    if not provided or provided != EXPECTED_API_KEY:
        return json_response(403, {"error": "Forbidden", "message": "Invalid or missing API key"})
    return None

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
    end   = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return {"error": "parse_failed", "raw": text}
    return {"error": "parse_failed", "raw": text}

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

def extract_text_from_claude_response(resp: dict) -> str:
    content = resp.get("content") or []
    if not content:
        return ""
    first = content[0]
    return first.get("text", "") if isinstance(first, dict) else ""

def deep_copy_checklist(tenant_id="default") -> dict:
    return get_checklist_template(tenant_id)

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
    """
    Calculate inspection status.
    Returns: "in_progress" | "completed"
    - "in_progress" if any item has an empty answer
    - "completed"   if all items have been answered (pass OR fail outcomes
                    are both unified under "completed" so the dashboard and
                    mobile API correctly mark the station as done).
    """
    items = get_all_items(inspection)
    if not items:
        return "in_progress"
    if any(item.get("answer", "") == "" for item in items):
        return "in_progress"
    return "completed"

def prepare_image_bytes(image_bytes: bytes, content_type: str = "image/jpeg") -> Tuple[bytes, str]:
    if Image is None:
        return image_bytes, content_type
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")
        if img.width > MAX_IMAGE_WIDTH:
            ratio    = MAX_IMAGE_WIDTH / float(img.width)
            new_size = (MAX_IMAGE_WIDTH, max(1, int(img.height * ratio)))
            img      = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=MAX_IMAGE_QUALITY, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning(f"Image preparation failed: {e}")
        return image_bytes, content_type

# ── DynamoDB helpers ───────────────────────────────────────────────────────────
def load_inspection(inspection_id: str) -> Optional[dict]:
    resp = inspection_table.get_item(Key={"inspection_id": inspection_id})
    item = resp.get("Item")
    return convert_floats_to_decimal(item) if item else None

def load_inspection_by_session(session_id: str) -> Optional[dict]:
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
    return load_inspection_by_session(value)

def save_inspection(item: dict) -> None:
    inspection_table.put_item(Item=sanitize_for_dynamodb(item))

def merge_item_records(existing: dict, incoming: dict) -> dict:
    merged = copy.deepcopy(existing or {})
    for key, val in (incoming or {}).items():
        if key == "evidence":
            if isinstance(val, list) and val:
                merged[key] = val
            elif key not in merged:
                merged[key] = []
            continue
        if isinstance(val, str):
            if val.strip():
                merged[key] = val
            elif key not in merged:
                merged[key] = val
            continue
        if isinstance(val, list):
            if val:
                merged[key] = val
            elif key not in merged:
                merged[key] = val
            continue
        if val is not None:
            merged[key] = val
    return merged

def merge_categories(existing_cats: List[dict], incoming_cats: List[dict]) -> List[dict]:
    existing_by_id = {str(c.get("id")): c for c in (existing_cats or []) if isinstance(c, dict)}
    merged_cats: List[dict] = []
    for inc_cat in incoming_cats or []:
        if not isinstance(inc_cat, dict):
            continue
        cat_id     = str(inc_cat.get("id", ""))
        exist_cat  = existing_by_id.get(cat_id, {})
        merged_cat = copy.deepcopy(exist_cat) if exist_cat else {}
        for key, val in inc_cat.items():
            if key == "items":
                continue
            if isinstance(val, str) and val.strip():
                merged_cat[key] = val
            elif val is not None and key != "items":
                merged_cat[key] = val
        existing_items    = exist_cat.get("items", []) if isinstance(exist_cat, dict) else []
        exist_items_by_id = {str(i.get("id")): i for i in existing_items if isinstance(i, dict)}
        merged_items: List[dict] = []
        for inc_item in inc_cat.get("items", []):
            if not isinstance(inc_item, dict):
                continue
            item_id = str(inc_item.get("id", ""))
            merged_items.append(merge_item_records(exist_items_by_id.get(item_id, {}), inc_item))
        if not merged_items and isinstance(existing_items, list):
            merged_items = copy.deepcopy(existing_items)
        merged_cat["items"] = merged_items
        merged_cats.append(merged_cat)
    return merged_cats if merged_cats else copy.deepcopy(existing_cats or [])

# ── Bedrock ────────────────────────────────────────────────────────────────────
def invoke_claude_json(system_prompt: str, user_text: str, image_bytes: Optional[bytes] = None,
                       media_type: str = "image/jpeg", max_tokens: int = 200) -> dict:
    content = []
    if image_bytes is not None:
        content.append({"type": "image", "source": {"type": "base64", "media_type": media_type,
                        "data": base64.b64encode(image_bytes).decode("utf-8")}})
    content.append({"type": "text", "text": user_text})
    payload  = {"anthropic_version": "bedrock-2023-05-31", "max_tokens": max_tokens,
                "system": system_prompt, "messages": [{"role": "user", "content": content}]}
    response = bedrock.invoke_model(modelId=MODEL_ID, body=json.dumps(payload),
                                    contentType="application/json", accept="application/json")
    body = json.loads(response["body"].read())
    return safe_json_parse(extract_text_from_claude_response(body))

# ── Image extraction ───────────────────────────────────────────────────────────
def _extract_image(body: dict) -> Tuple[Optional[bytes], str]:
    image_base64 = str(body.get("image_base64", "")).strip()
    file_key     = str(body.get("file_key", "") or body.get("fileKey", "")).strip()
    if image_base64:
        if "," in image_base64 and image_base64.startswith("data:"):
            image_base64 = image_base64.split(",", 1)[1]
        return base64.b64decode(image_base64), "image/jpeg"
    if file_key:
        try:
            obj  = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=file_key)
            data = obj["Body"].read()
            ct   = obj.get("ContentType", "image/jpeg")
            return data, ct
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(f"S3 key not found: {file_key}")
            raise
    return None, "image/jpeg"

# ── S3 URL helpers ─────────────────────────────────────────────────────────────
def generate_s3_download_url(file_key: str) -> Optional[str]:
    try:
        return s3.generate_presigned_url("get_object",
               Params={"Bucket": EVIDENCE_BUCKET, "Key": file_key}, ExpiresIn=DOWNLOAD_URL_EXPIRY)
    except Exception:
        return None

# ── Route handlers ─────────────────────────────────────────────────────────────

def get_checklist(event: dict) -> dict:
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key, force_refresh=True)
    if build_mobile_checklist_response is not None and template is not None:
        template = build_mobile_checklist_response(template, "eyewash", company_key)
    elif filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return json_response(200, template)



def create_inspection_from_session_payload(event: dict) -> dict:
    body               = parse_body(event)
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

    session = None
    inspection_id = ""

    if session_id:
        sess_resp = session_table.get_item(Key={"session_id": session_id})
        session   = sess_resp.get("Item")
        if session:
            inspection_id = str(session.get("inspection_id", "")).strip()

    if not inspection_id and body_inspection_id:
        inspection_id     = body_inspection_id
        existing_for_meta = load_inspection(inspection_id)
        if existing_for_meta:
            session = {
                "auditor_name":  str(existing_for_meta.get("auditor_name",  body.get("auditor_name",  ""))).strip(),
                "location":      str(existing_for_meta.get("location",      body.get("location",      ""))).strip(),
                "facility_area": str(existing_for_meta.get("facility_area", body.get("facility_area", ""))).strip(),
                "station":       str(existing_for_meta.get("station",       body.get("station",       ""))).strip(),
                "date_of_audit": str(existing_for_meta.get("date_of_audit", body.get("date_of_audit", ""))).strip(),
            }
        else:
            session = {
                "auditor_name":  str(body.get("auditor_name",  "")).strip(),
                "location":      str(body.get("location",      "")).strip(),
                "facility_area": str(body.get("facility_area", "")).strip(),
                "station":       str(body.get("station",       "")).strip(),
                "date_of_audit": str(body.get("date_of_audit", "")).strip(),
            }

    if not session:
        return json_response(404, {"error": "Session not found. Create session first."})

    if not inspection_id:
        inspection_id = str(uuid.uuid4())

    existing   = load_inspection(inspection_id)
    created_at = str(existing.get("created_at", "")).strip() if existing else now_iso()
    updated_at = now_iso()

    merged_cats    = merge_categories(existing.get("categories", []) if existing else [], categories)
    merged_results = general_results if general_results else (existing.get("general_results", []) if existing else [])
    merged_notes   = notes if notes else (str(existing.get("notes", "")).strip() if existing else "")
    merged_team    = team if isinstance(team, list) and team else (existing.get("team", []) if existing else [])
    preserved_sid  = session_id or (str(existing.get("session_id", "")).strip() if existing else "")

    record = copy.deepcopy(existing) if existing else {}
    record.update({
        "inspection_id": inspection_id, "session_id": preserved_sid,
        "inspection_type": "eyewash",
        "auditor_name": session.get("auditor_name", ""),
        "location": session.get("location", ""),
        "facility_area": session.get("facility_area", ""),
        "station": session.get("station", ""),
        "station_id": session.get("station_id", ""),
        "date_of_audit": session.get("date_of_audit", ""),
        "team": merged_team, "categories": merged_cats,
        "general_results": merged_results, "notes": merged_notes,
        "status": compute_status({"categories": merged_cats}),
        "current_item_index": 0,
        "created_at": created_at, "updated_at": updated_at,
    })

    # Stamp completed_at the first time the inspection reaches "completed"
    derived_status = record.get("status", "in_progress")
    if derived_status == "completed" and not record.get("completed_at"):
        record["completed_at"] = updated_at
        logger.info(
            "Inspection %s (eyewash) marked completed at %s",
            inspection_id, updated_at,
        )

    save_inspection(record)

    # Stamp shared session so Dashboard autosave can resolve the inspection table.
    if preserved_sid:
        try:
            session_table.update_item(
                Key={"session_id": preserved_sid},
                UpdateExpression="SET inspection_id = :iid, inspection_type = :itype, updated_at = :u",
                ExpressionAttributeValues={
                    ":iid": inspection_id,
                    ":itype": "eyewash",
                    ":u": updated_at,
                },
            )
        except Exception:
            logger.exception(
                "Failed to link session %s to inspection %s (eyewash)",
                preserved_sid, inspection_id,
            )

    status_code = 201 if not existing else 200
    return json_response(status_code, {
        "inspection_id": inspection_id, "session_id": preserved_sid,
        "created_at": created_at, "updated_at": updated_at,
        "status": record.get("status", "in_progress"),
        "message": "Inspection updated and merged successfully." if existing else "Inspection created.",
    })


def get_inspection(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})

    params = event.get("queryStringParameters") or {}
    company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()

    item = load_inspection_by_any_id(inspection_id)
    if not item:
        return json_response(404, {"error": "Inspection not found"})

    if company_key and company_key != "default" and sync_inspection_with_template is not None:
        synced = sync_inspection_with_template(item, "eyewash", company_key)
        if synced.get("categories") != item.get("categories"):
            item = synced
            item["updated_at"] = now_iso()
            save_inspection(item)

    return json_response(200, item)


def list_inspections(event: dict) -> dict:
    try:
        result = inspection_table.scan()
        items  = result.get("Items", [])
        while "LastEvaluatedKey" in result:
            result = inspection_table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
            items.extend(result.get("Items", []))
    except Exception as e:
        return json_response(500, {"error": f"Failed to list inspections: {str(e)}"})

    items     = convert_floats_to_decimal(items)
    summaries = []
    for item in items:
        summaries.append({
            "inspection_id": item.get("inspection_id"),
            "session_id":    item.get("session_id"),
            "auditor_name":  item.get("auditor_name"),
            "location":      item.get("location"),
            "facility_area": item.get("facility_area"),
            "station":       item.get("station"),
            "date_of_audit": item.get("date_of_audit"),
            "status":        item.get("status", compute_status(item)),
            "created_at":    item.get("created_at"),
            "notes":         item.get("notes", ""),
        })
    summaries.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return json_response(200, summaries)


def get_completion_readiness(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = str(path_params.get("inspection_id") or "").strip()

    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})

    inspection = load_inspection_by_any_id(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    general_info = inspection.get("general_information", {}) or {}
    required_general_fields = ["location", "start_date", "leader"]
    missing_general_fields = [
        field
        for field in required_general_fields
        if not str(general_info.get(field, "")).strip()
    ]

    items = get_all_items(inspection)
    missing_item_ids = []
    blocked_item_ids = []
    for item in items:
        item_id = str(item.get("id", "")).strip()
        if not item_id:
            continue
        if item.get("blocked_by_wrong_image"):
            blocked_item_ids.append(item_id)
        if not str(item.get("answer", "")).strip():
            missing_item_ids.append(item_id)

    progress = compute_progress(inspection)
    ready_for_completion = not missing_general_fields and not missing_item_ids and not blocked_item_ids

    missing_fields = []
    if missing_general_fields:
        missing_fields.append({"section": "general_information", "fields": missing_general_fields})
    if missing_item_ids:
        missing_fields.append({"section": "checklist_items", "item_ids": missing_item_ids})
    if blocked_item_ids:
        missing_fields.append({"section": "blocked_items", "item_ids": blocked_item_ids})

    return json_response(200, {
        "inspection_id": inspection.get("inspection_id", inspection_id),
        "session_id": inspection.get("session_id", ""),
        "ready_for_completion": ready_for_completion,
        "can_submit": ready_for_completion,
        "inspection_status": inspection.get("status", compute_status(inspection)),
        "completion_progress": progress,
        "missing_fields": missing_fields,
        "missing_general_fields": missing_general_fields,
        "missing_item_ids": missing_item_ids,
        "blocked_item_ids": blocked_item_ids,
        "message": (
            "Inspection is ready for completion." if ready_for_completion
            else "Inspection is not ready for completion. Resolve missing fields and blocked items."
        ),
    })


def update_checklist_item(event: dict) -> dict:
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    item_id       = path_params.get("item_id") or ""

    if not inspection_id or not item_id:
        return json_response(400, {"error": "inspection_id and item_id are required"})

    body        = parse_body(event)
    answer      = str(body.get("answer", "")).strip()
    finding     = str(body.get("finding", "")).strip()
    action_item = str(body.get("action_item", "")).strip()
    responsible = str(body.get("responsible", "")).strip()
    due_date    = str(body.get("due_date", "")).strip()
    evidence    = body.get("evidence", [])
    clear_block = bool(body.get("clear_block", False))

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    if answer and answer not in ["Yes", "No", "N/A"]:
        return json_response(400, {"error": "answer must be Yes, No, or N/A"})

    if answer:      checklist_item["answer"]      = answer
    if finding:     checklist_item["finding"]     = finding
    if action_item: checklist_item["action_item"] = action_item
    if responsible: checklist_item["responsible"] = responsible
    if due_date:    checklist_item["due_date"]    = due_date
    if isinstance(evidence, list) and evidence:
        checklist_item.setdefault("evidence", [])
        checklist_item["evidence"].extend(evidence)
    if clear_block:
        checklist_item["blocked_by_wrong_image"] = False

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return json_response(200, {
        "inspection_id": inspection_id, "item_id": item_id,
        "updated_item": checklist_item, "inspection": inspection,
        "categories": inspection.get("categories", []),
        "status": inspection.get("status", "in_progress"),
        "updated_at": inspection.get("updated_at", ""),
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

    existing = checklist_item.get("finding", "")
    checklist_item["finding"] = (existing + " | " if existing else "") + f"Worker note: {note}"
    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    return json_response(200, {
        "inspection_id": inspection_id, "item_id": item_id,
        "finding": checklist_item["finding"], "message": "Note saved successfully.",
    })


def confirm_manual_answer(event: dict) -> dict:
    """Confirm/reject AI suggestion for partial-AI items.

    POST body: { "answer": "Yes"|"No"|"N/A", "finding": "...", "action_item": "..." }
    """
    path_params   = event.get("pathParameters") or {}
    inspection_id = path_params.get("inspection_id") or ""
    item_id       = path_params.get("item_id") or ""

    if not inspection_id or not item_id:
        return json_response(400, {"error": "inspection_id and item_id are required"})

    body        = parse_body(event)
    answer      = str(body.get("answer", "")).strip()
    finding     = str(body.get("finding", "")).strip()
    action_item = str(body.get("action_item", "")).strip()

    if not answer or answer not in ["Yes", "No", "N/A"]:
        return json_response(400, {"error": "answer must be Yes, No, or N/A"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    policy = get_item_ai_policy(item_id)

    # Only partial/manual items with manual_review_required can use this endpoint
    if not policy.get("manual_review_required"):
        return json_response(400, {
            "error": "This item does not require manual confirmation (auto-AI item)"
        })

    # Confirm the inspector's manual decision
    ai_suggested = str(checklist_item.get("ai_suggested_answer", "")).strip()
    inspection_timestamp = now_iso()

    checklist_item["answer"] = answer
    if finding:
        checklist_item["finding"] = finding
    if action_item:
        checklist_item["action_item"] = action_item

    # Append manual confirmation record to evidence
    checklist_item.setdefault("evidence", [])
    checklist_item["evidence"].append({
        "confirmed_at": inspection_timestamp,
        "ai_mode": policy.get("mode", "partial"),
        "manual_review_required": True,
        "ai_suggested_answer": ai_suggested,
        "inspector_confirmed_answer": answer,
        "inspector_finding": finding,
        "inspector_action_item": action_item,
        "confirmation_type": "manual_override" if ai_suggested and ai_suggested != answer else "manual_confirm",
    })

    # Keep ai_suggested_answer for audit trail, but item is now answered
    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
    inspection["status"] = compute_status(inspection)
    inspection["updated_at"] = inspection_timestamp
    save_inspection(inspection)

    return json_response(200, {
        "inspection_id": inspection_id,
        "item_id": item_id,
        "message": "Manual confirmation recorded successfully.",
        "inspector_answer": answer,
        "ai_suggested_answer": ai_suggested,
        "confirmation_type": "manual_override" if ai_suggested and ai_suggested != answer else "manual_confirm",
        "updated_item": checklist_item,
        "inspection_status": inspection["status"],
        "inspection": inspection,
        "categories": inspection.get("categories", []),
    })


def analyze_item_image(event: dict) -> dict:
    body          = parse_body(event)
    inspection_id = str(body.get("inspection_id", "")).strip()
    item_id       = str(body.get("item_id", "")).strip()

    if not inspection_id: return json_response(400, {"error": "inspection_id is required"})
    if not item_id:       return json_response(400, {"error": "item_id is required"})

    inspection = load_inspection(inspection_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    checklist_item, cat_idx, item_idx = find_item(inspection, item_id)
    if checklist_item is None:
        return json_response(404, {"error": "Checklist item not found"})

    policy = get_item_ai_policy(item_id)

    if policy.get("mode") == "manual":
        checklist_item.setdefault("evidence", [])
        checklist_item["evidence"].append({
            "analyzed_at": now_iso(),
            "ai_mode": "manual",
            "manual_review_required": True,
            "reason": policy.get("reason", "Manual inspection required."),
        })
        checklist_item["ai_suggested_answer"] = ""
        checklist_item["finding"] = policy.get("reason", "Manual inspection required.")
        checklist_item["action_item"] = "Complete manual check and submit final answer."
        checklist_item["blocked_by_wrong_image"] = False

        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["status"] = compute_status(inspection)
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        return json_response(200, {
            "inspection_id": inspection_id,
            "item_id": item_id,
            "blocked": False,
            "move_next": False,
            "pass": False,
            "ai_mode": "manual",
            "manual_review_required": True,
            "confidence_level": policy.get("confidence_level", "n/a"),
            "media_requirement": policy.get("media_requirement", "manual_action"),
            "message": "Manual inspection required for this checklist item.",
            "reason": policy.get("reason", "Manual inspection required."),
            "updated_item": checklist_item,
            "inspection_status": inspection.get("status", "in_progress"),
            "inspection": inspection,
            "categories": inspection.get("categories", []),
        })

    # Support multiple images per item: `file_keys` (list) or `image_base64s` (list),
    # or single `file_key` / `image_base64` (backwards compatible).
    image_sources = []  # list of tuples: (source_type, payload)
    if isinstance(body.get("file_keys"), list) and body.get("file_keys"):
        for fk in body.get("file_keys"):
            if fk:
                image_sources.append(("file_key", str(fk).strip()))
    elif isinstance(body.get("image_base64s"), list) and body.get("image_base64s"):
        for b64 in body.get("image_base64s"):
            if b64:
                image_sources.append(("image_base64", str(b64).strip()))
    else:
        # fallback to single image behavior
        image_sources.append(("single_body", body))

    if not image_sources:
        return json_response(400, {"error": "Provide file_key(s) or image_base64(s)"})

    per_image_results = []
    VALID_OBJECTS = {"eyewash_station", "emergency_shower", "eyewash_bottle"}
    rule = CHECKLIST_VISUAL_RULES.get(str(item_id), checklist_item.get("description", ""))

    for idx, src in enumerate(image_sources):
        try:
            if src[0] == "file_key":
                fk = src[1]
                obj = s3.get_object(Bucket=EVIDENCE_BUCKET, Key=fk)
                image_bytes = obj["Body"].read()
                content_type = obj.get("ContentType", "image/jpeg")
                file_key_for_record = fk
            elif src[0] == "image_base64":
                b64 = src[1]
                if "," in b64 and b64.startswith("data:"):
                    b64 = b64.split(",", 1)[1]
                image_bytes = base64.b64decode(b64)
                content_type = "image/jpeg"
                file_key_for_record = f"inline_{idx}"
            else:
                # single_body: reuse existing extractor
                image_bytes, content_type = _extract_image(src[1])
                file_key_for_record = src[1].get("file_key") or src[1].get("fileKey") or f"inline_{idx}"
        except FileNotFoundError as e:
            return json_response(400, {"error": str(e)})
        except Exception as e:
            return json_response(400, {"error": f"Invalid image input: {str(e)}"})

        if image_bytes is None:
            continue

        image_bytes_prepared, media_type = prepare_image_bytes(image_bytes, content_type)
        prompt = build_item_prompt(checklist_item, rule, policy)

        try:
            analysis = invoke_claude_json(IMAGE_ANALYSIS_SYSTEM_PROMPT, prompt,
                                          image_bytes=image_bytes_prepared, media_type=media_type, max_tokens=200)
        except Exception as e:
            return json_response(502, {"error": f"Bedrock failed: {str(e)}"})

        passed = bool(analysis.get("pass", False))
        confidence = float(analysis.get("confidence", 0.0) or 0.0)
        object_detected = str(analysis.get("object_detected", "unclear")).lower().strip()
        condition_checked = str(analysis.get("condition_checked", "not_visible")).strip()
        reason = str(analysis.get("reason", "")).strip()
        worker_message = str(analysis.get("worker_message", "")).strip()
        suggested_action = analysis.get("suggested_action", None)
        if suggested_action is not None:
            suggested_action = str(suggested_action).strip() or None

        wrong_image = object_detected not in VALID_OBJECTS
        low_confidence = confidence < 0.35
        blocked = wrong_image or low_confidence

        per_image_results.append({
            "file_key": file_key_for_record,
            "analyzed_at": now_iso(),
            "ai_mode": policy.get("mode", "ai"),
            "manual_review_required": bool(policy.get("manual_review_required", False)),
            "confidence_level": policy.get("confidence_level", "high"),
            "media_requirement": policy.get("media_requirement", "photo"),
            "object_detected": object_detected,
            "condition_checked": condition_checked,
            "pass": passed,
            "is_compliant": passed and not blocked,
            "confidence": confidence,
            "reason": reason,
            "worker_message": worker_message,
            "suggested_action": suggested_action or "",
            "blocked": blocked,
        })

    # Merge per-image results into a single decision: prefer any non-blocked pass with highest confidence.
    if not per_image_results:
        return json_response(400, {"error": "No valid images were provided or extracted."})

    # Choose best result by highest confidence where not blocked and pass==True; fallback to highest confidence overall
    non_block_pass = [r for r in per_image_results if (r.get("pass") and not r.get("blocked"))]
    if non_block_pass:
        best = max(non_block_pass, key=lambda r: r.get("confidence", 0.0))
    else:
        best = max(per_image_results, key=lambda r: r.get("confidence", 0.0))

    # Append all per-image evidence records
    checklist_item.setdefault("evidence", [])
    for r in per_image_results:
        checklist_item["evidence"].append(r)

    blocked_overall = all(r.get("blocked", False) for r in per_image_results)

    if blocked_overall:
        checklist_item["blocked_by_wrong_image"] = True
        checklist_item["answer"] = ""
        checklist_item["finding"] = best.get("reason") or "Image(s) not sufficient."
        checklist_item["action_item"] = best.get("suggested_action") or "Retake clear images or short video."
        inspection["categories"][cat_idx]["items"][item_idx] = checklist_item
        inspection["updated_at"] = now_iso()
        save_inspection(inspection)

        # Resolve company-level blocked verdict label
        _company_key = str(body.get("company_key", "")).strip()
        _verdict_label, _verdict_display = ("need_review", "Need Verification")
        if resolve_verdict_fields is not None:
            _verdict_label, _verdict_display = resolve_verdict_fields(_company_key, False, True)

        blocked_resp = {
            "inspection_id": inspection_id, "item_id": item_id,
            "blocked": True, "move_next": False, "pass": False,
            "object_detected": best.get("object_detected"), "condition_checked": best.get("condition_checked"),
            "confidence": best.get("confidence"),
            "blocked_verdict_label": _verdict_label,
            "verdict_display": _verdict_display,
            "message": best.get("worker_message") or "Station not clearly visible. Retake images.",
            "reason": best.get("reason"), "suggested_action": best.get("suggested_action"),
            "updated_item": checklist_item,
            "inspection_status": inspection.get("status", "in_progress"),
            "inspection": inspection, "categories": inspection.get("categories", []),
        }
        if _company_key:
            blocked_resp["company_key"] = _company_key
        return json_response(200, blocked_resp)

    passed_overall = bool(best.get("pass", False)) and not best.get("blocked", False)
    checklist_item["answer"] = "Yes" if passed_overall else "No"
    checklist_item["blocked_by_wrong_image"] = False
    checklist_item["finding"] = best.get("reason") or ""
    checklist_item["action_item"] = best.get("suggested_action") or ("" if passed_overall else "Correct the issue and retake.")

    manual_review_required = bool(policy.get("manual_review_required", False))
    if manual_review_required:
        checklist_item["ai_suggested_answer"] = "Yes" if best.get("pass") else "No"
        checklist_item["answer"] = ""
        checklist_item["finding"] = (
            f"AI suggestion: {'Pass' if best.get('pass') else 'Fail'}. "
            f"{best.get('reason') or ''}"
        ).strip()
        checklist_item["action_item"] = (
            best.get("suggested_action") or policy.get("reason", "Human confirmation required.")
        )

    inspection["categories"][cat_idx]["items"][item_idx] = checklist_item

    next_pos = next_unanswered_index(inspection)
    inspection["current_item_index"] = next_pos[1] if next_pos else item_idx
    inspection["status"]     = compute_status(inspection)
    inspection["updated_at"] = now_iso()
    save_inspection(inspection)

    # Resolve company-level verdict label for non-pass results
    _verdict_label_final = None
    _verdict_display = "Pass" if passed_overall else "Fail"
    if not passed_overall and resolve_verdict_fields is not None:
        _ck = str(body.get("company_key", "")).strip()
        _verdict_label_final, _verdict_display = resolve_verdict_fields(_ck, False, False)

    resp_body = {
        "inspection_id": inspection_id, "item_id": item_id,
        "blocked": False, "move_next": not manual_review_required, "pass": passed_overall,
        "ai_mode": policy.get("mode", "ai"),
        "manual_review_required": manual_review_required,
        "confidence_level": policy.get("confidence_level", "high"),
        "media_requirement": policy.get("media_requirement", "photo"),
        "object_detected": best.get("object_detected"), "condition_checked": best.get("condition_checked"),
        "confidence": best.get("confidence"),
        "message": (
            (best.get("worker_message") or "Item analyzed.")
            if not manual_review_required
            else "AI suggestion ready. Manual confirmation required before finalizing."
        ),
        "reason": best.get("reason"), "suggested_action": best.get("suggested_action"),
        "updated_item": checklist_item,
        "inspection_status": inspection["status"],
        "current_item_index": inspection.get("current_item_index", 0),
        "inspection": inspection, "categories": inspection.get("categories", []),
        "verdict_display": _verdict_display,
    }
    if _verdict_label_final is not None:
        resp_body["blocked_verdict_label"] = _verdict_label_final
    _ck = str(body.get("company_key", "")).strip()
    if _ck:
        resp_body["company_key"] = _ck

    return json_response(200, resp_body)


def batch_analyze_items(event: dict) -> dict:
    """Batch analyze multiple item images in parallel.

    Expects JSON body: { "inspection_id": "...", "images": [ {"item_id": "1", "file_key": "..."}, ... ] }
    """
    body = parse_body(event)
    inspection_id = str(body.get("inspection_id", "")).strip()
    images = body.get("images", [])

    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required"})
    if not isinstance(images, list) or not images:
        return json_response(400, {"error": "images must be a non-empty list of {item_id,file_key|image_base64}"})

    # Prepare per-item events
    child_events = []
    for img in images:
        iid = str(img.get("item_id", "")).strip()
        if not iid:
            continue
        child_body = {"inspection_id": inspection_id, "item_id": iid}
        # Support multiple images per item: file_keys or image_base64s (lists)
        if "file_keys" in img and isinstance(img.get("file_keys"), list):
            child_body["file_keys"] = img["file_keys"]
        elif "file_key" in img:
            child_body["file_key"] = img["file_key"]
        if "image_base64s" in img and isinstance(img.get("image_base64s"), list):
            child_body["image_base64s"] = img["image_base64s"]
        elif "image_base64" in img:
            child_body["image_base64"] = img["image_base64"]
        child_events.append({"body": child_body})

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_WORKER_COUNT) as ex:
        futures = {ex.submit(analyze_item_image, ev): ev for ev in child_events}
        for fut in concurrent.futures.as_completed(futures):
            ev = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                # best-effort: record the exception for the specific item
                item_id = ev.get("body", {}).get("item_id")
                results[item_id] = {"error": str(e)}
                continue
            # analyze_item_image returns a json_response dict
            body = res.get("body") if isinstance(res, dict) else None
            try:
                parsed = json.loads(body) if isinstance(body, str) else (body or {})
            except Exception:
                parsed = body
            item_id = ev.get("body", {}).get("item_id")
            results[item_id] = parsed

    return json_response(200, {"inspection_id": inspection_id, "results": results})


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
            item["evidence"] = cleaned[-1:] if cleaned else []
            for field in ["finding", "action_item", "responsible"]:
                if field in item:
                    item[field] = str(item.get(field, ""))[:200]

    if "notes" in inspection:
        inspection["notes"] = str(inspection.get("notes", ""))[:1000]

    body_str = json.dumps(inspection, default=str)
    size_kb  = len(body_str.encode("utf-8")) / 1024
    logger.info(f"[REPORT] eyewash inspection_id={inspection_id} size={size_kb:.1f}KB")

    if size_kb > 5500:
        for cat in inspection.get("categories", []):
            for item in cat.get("items", []):
                item["evidence"] = []

    return json_response(200, inspection)


def delete_inspection(event: dict) -> dict:
    """
    Deletes an eyewash inspection record by inspection_id.
    Also deletes the associated session record from the sessions table.

    Path parameter:
        inspection_id (required) — The inspection ID to delete

    Query parameter:
        delete_session (optional, default true) — Also delete the linked session
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = str(path_params.get("inspection_id", "")).strip()

    if not inspection_id:
        return json_response(400, {"error": "inspection_id is required in the URL path"})

    # Fetch the inspection to verify it exists and get session_id
    result = inspection_table.get_item(Key={"inspection_id": inspection_id})
    item = result.get("Item")

    if not item:
        return json_response(404, {"error": "Inspection not found"})

    session_id = str(item.get("session_id", "")).strip()

    # Delete the inspection
    try:
        inspection_table.delete_item(Key={"inspection_id": inspection_id})
    except Exception as e:
        logger.exception("Error deleting inspection")
        return json_response(500, {"error": f"Failed to delete inspection: {str(e)}"})

    # Optionally delete the linked session
    delete_session_flag = str(
        (event.get("queryStringParameters") or {}).get("delete_session", "true")
    ).strip().lower()
    deleted_session = False
    if delete_session_flag in {"1", "true", "yes", "y"} and session_id:
        try:
            session_table.delete_item(Key={"session_id": session_id})
            deleted_session = True
        except Exception as e:
            logger.warning(f"Warning: Failed to delete linked session {session_id}: {e}")

    return json_response(200, {
        "message": "Inspection deleted successfully",
        "inspection_id": inspection_id,
        "session_id": session_id,
        "session_deleted": deleted_session,
    })


def delete_session(event: dict) -> dict:
    path_params = event.get("pathParameters") or {}
    session_id  = str(path_params.get("session_id") or "").strip()
    if not session_id:
        return json_response(400, {"error": "session_id is required"})

    existing = session_table.get_item(Key={"session_id": session_id}).get("Item")
    if not existing:
        return json_response(404, {"error": "Session not found"})

    delete_flag        = str(get_query(event, "delete_inspections", "true")).lower()
    delete_inspections = delete_flag in {"1", "true", "yes", "y"}
    deleted_ids: List[str] = []

    if delete_inspections:
        try:
            scan_res = inspection_table.scan()
            all_items = scan_res.get("Items", [])
            while "LastEvaluatedKey" in scan_res:
                scan_res = inspection_table.scan(ExclusiveStartKey=scan_res["LastEvaluatedKey"])
                all_items.extend(scan_res.get("Items", []))
            for it in all_items:
                if str(it.get("session_id", "")).strip() == session_id:
                    iid = str(it.get("inspection_id", "")).strip()
                    if iid:
                        inspection_table.delete_item(Key={"inspection_id": iid})
                        deleted_ids.append(iid)
        except Exception as e:
            return json_response(500, {"error": f"Failed deleting linked inspections: {str(e)}"})

    session_table.delete_item(Key={"session_id": session_id})
    return json_response(200, {
        "message": "Session deleted successfully", "session_id": session_id,
        "deleted_linked_inspections": delete_inspections,
        "deleted_inspection_count": len(deleted_ids),
        "deleted_inspection_ids": deleted_ids,
    })


# ── Router ─────────────────────────────────────────────────────────────────────

def get_linear_checklist_items(inspection: dict) -> List[dict]:
    """Flatten checklist categories into a linear list for voice navigation."""
    flattened = []
    for cat_idx, category in enumerate(inspection.get("categories", [])):
        for item_idx, item in enumerate(category.get("items", [])):
            flattened.append({
                "category_index": cat_idx,
                "sub_section_index": None,
                "item_index": item_idx,
                "item": item,
            })
        for sub_idx, sub_section in enumerate(category.get("sub_sections", [])):
            for item_idx, item in enumerate(sub_section.get("items", [])):
                flattened.append({
                    "category_index": cat_idx,
                    "sub_section_index": sub_idx,
                    "item_index": item_idx,
                    "item": item,
                })
    return flattened


def clamp_voice_index(index: int, total: int) -> int:
    if total <= 0:
        return 0
    return max(0, min(index, total - 1))


def get_current_voice_item(inspection: dict) -> Tuple[Optional[dict], Optional[int]]:
    """Return the item pointed to by current_item_index."""
    items = get_linear_checklist_items(inspection)
    if not items:
        return None, None

    try:
        current_index = int(inspection.get("current_item_index", 0) or 0)
    except Exception:
        current_index = 0

    current_index = clamp_voice_index(current_index, len(items))
    return items[current_index], current_index


def eyewash_voice_hint(item_id: str, item: Optional[dict] = None) -> str:
    hints = {
        "1": "Capture a wide shot of the eyewash station and surrounding area.",
        "2": "Capture the full path to the eyewash station.",
        "3": "Show the green eyewash sign and lighting.",
        "4": "Show the station basin and surrounding surface.",
        "5": "Show the station exterior for damage or leaks.",
        "6": "Show the station after wiping it clean.",
        "7": "Zoom in on the cartridge expiration label.",
        "8": "Zoom in on the water level and water clarity.",
        "9": "Show the cleansing stick flowing from both openings.",
        "10": "Show the signed and dated inspection tag.",
        "11": "Capture the plumbed eyewash from a wider angle.",
        "12": "Show the plumbed eyewash path is clear.",
        "13": "Show the plumbed eyewash sign and lighting.",
        "14": "Show any shut-off valve security.",
        "15": "Show the plumbed eyewash area is free of clutter.",
        "16": "Show the plumbed eyewash is clean and free of buildup.",
        "17": "Zoom in on both nozzle caps.",
        "18": "Show active flow from the plumbed eyewash.",
        "19": "Show both nozzle caps popping off during activation.",
        "20": "Show both water streams at the same height.",
        "21": "Show the water temperature evidence.",
        "22": "Show the signed plumbed eyewash tag.",
        "23": "Capture the emergency shower from a wide angle.",
        "24": "Show the shower path is clear.",
        "25": "Show the emergency shower sign and lighting.",
        "26": "Show any shut-off valve security.",
        "27": "Show the shower area is free of clutter.",
        "28": "Show the shower is clean and free of buildup.",
        "29": "Show the shower temperature evidence.",
        "30": "Show the shower operating correctly.",
        "31": "Show the signed emergency shower tag.",
        "32": "Show the bottle station and nearby permanent eyewash.",
        "33": "Show the bottle path is clear.",
        "34": "Show the bottle sign and lighting.",
        "35": "Show the bottle area is free of clutter.",
        "36": "Show the bottle station is clean and intact.",
        "37": "Zoom in on the bottle expiration date.",
        "38": "Show the signed supplemental bottle tag.",
    }
    if str(item_id) in hints:
        return hints[str(item_id)]
    if item and item.get("description"):
        return item["description"]
    return "Capture a clear image of the current checklist item."


def parse_voice_intent_locally(text: str) -> Optional[dict]:
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


def save_inspection(item: dict) -> None:
    inspection_table.put_item(Item=sanitize_for_dynamodb(item))


def handle_voice_command(event: dict) -> dict:
    """Process a voice transcript for the eyewash workflow."""
    body = parse_body(event)
    transcript = str(
        body.get("voice_text", "") or body.get("text", "") or body.get("transcript", "") or body.get("speech_text", "")
    ).strip()
    inspection_id = str(body.get("inspection_id", "") or body.get("inspectionId", "") or "").strip()
    session_id = str(body.get("session_id", "") or body.get("sessionId", "") or "").strip()
    item_id = str(body.get("item_id", "") or body.get("itemId", "") or "").strip()

    if not inspection_id and not session_id:
        return json_response(400, {"error": "inspection_id or session_id is required"})
    if not transcript:
        return json_response(400, {"error": "voice_text, text, or transcript is required"})

    inspection = None
    if inspection_id:
        inspection = load_inspection_by_any_id(inspection_id)
    elif session_id:
        inspection = load_inspection_by_session(session_id)

    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    items = get_linear_checklist_items(inspection)
    if not items:
        return json_response(400, {"error": "Checklist items are not available"})

    parsed = parse_voice_intent_locally(transcript)
    current_ctx, current_index = get_current_voice_item(inspection)
    if current_index is None:
        current_index = 0

    target_index = current_index
    target_ctx = current_ctx
    if item_id:
        for idx, entry in enumerate(items):
            if str(entry["item"].get("id")) == item_id:
                target_index = idx
                target_ctx = entry
                break

    intent = parsed.get("intent", "unknown")
    move_to_item = bool(parsed.get("move_to_item", False))
    note_text = parsed.get("note_text")

    if intent == "next_item":
        target_index = clamp_voice_index(target_index + 1, len(items))
        move_to_item = True
    elif intent == "previous_item":
        target_index = clamp_voice_index(target_index - 1, len(items))
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
    hint = eyewash_voice_hint(item_id_resolved, item)

    return json_response(200, {
        "inspection_id": inspection.get("inspection_id", inspection_id or ""),
        "session_id": inspection.get("session_id", session_id or ""),
        "intent": intent,
        "confidence": parsed.get("confidence", 0.0),
        "message": parsed.get("message", ""),
        "move_to_item": move_to_item,
        "voice_text": transcript,
        "note_text": note_text,
        "current_item_index": inspection.get("current_item_index", current_index),
        "current_item": {
            "id": item.get("id"),
            "description": item.get("description", ""),
            "answer": item.get("answer", ""),
        },
        "hint": hint,
        "inspection_status": inspection.get("status", compute_status(inspection)),
        "available_commands": ["next", "back", "repeat", "capture", "retake", "help", "add note <text>"],
    })


def compute_progress(inspection: dict) -> dict:
    """Compute progress stats from inspection categories."""
    total = 0
    answered = 0
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            total += 1
            if item.get("answer", "") != "":
                answered += 1
    percentage = round((answered / total) * 100, 1) if total > 0 else 0
    return {"total": total, "answered": answered, "percentage": percentage}


def find_next_unanswered(inspection: dict) -> Optional[str]:
    """Find the next unanswered item ID in the inspection."""
    for cat in inspection.get("categories", []):
        for item in cat.get("items", []):
            if item.get("answer", "") == "":
                return str(item.get("id", ""))
    return None


def pause_session_handler(event: dict) -> dict:
    """POST handler — pause an in-progress inspection session."""
    path_params = event.get("pathParameters") or {}
    session_id = path_params.get("session_id", "").strip()
    if not session_id:
        return json_response(400, {"error": "session_id is required"})

    inspection = load_inspection_by_any_id(session_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    body = parse_body(event)

    # Merge incoming categories if provided
    if isinstance(body.get("categories"), list) and body["categories"]:
        inspection["categories"] = merge_categories(
            inspection.get("categories", []), body["categories"]
        )

    # Update general_results if provided
    if isinstance(body.get("general_results"), list) and body["general_results"]:
        inspection["general_results"] = body["general_results"]

    # Update notes if provided
    if isinstance(body.get("notes"), str) and body["notes"].strip():
        inspection["notes"] = body["notes"].strip()

    progress = compute_progress(inspection)

    inspection["status"] = "paused"
    inspection["last_paused_at"] = now_iso()
    inspection["updated_at"] = now_iso()

    save_inspection(inspection)

    # Update session table record
    try:
        session_table.update_item(
            Key={"session_id": inspection.get("session_id", session_id)},
            UpdateExpression="SET #status = :status, progress = :progress, inspection_type = :itype, updated_at = :updated_at, last_paused_at = :paused_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "paused",
                ":progress": sanitize_for_dynamodb(progress),
                ":itype": "eyewash",
                ":updated_at": now_iso(),
                ":paused_at": now_iso(),
            },
        )
    except Exception as e:
        logger.warning(f"Failed to update session table: {e}")

    return json_response(200, {
        "session_id": inspection.get("session_id", session_id),
        "inspection_id": inspection.get("inspection_id", ""),
        "status": "paused",
        "progress": progress,
        "last_paused_at": inspection["last_paused_at"],
    })


def resume_session_handler(event: dict) -> dict:
    """GET handler — resume a paused inspection session."""
    path_params = event.get("pathParameters") or {}
    session_id = path_params.get("session_id", "").strip()
    if not session_id:
        return json_response(400, {"error": "session_id is required"})

    inspection = load_inspection_by_any_id(session_id)
    if not inspection:
        return json_response(404, {"error": "Inspection not found"})

    progress = compute_progress(inspection)
    next_item_id = find_next_unanswered(inspection)

    inspection["status"] = "in_progress"
    inspection["resumed_at"] = now_iso()
    inspection["updated_at"] = now_iso()

    save_inspection(inspection)

    # Update session table record
    try:
        session_table.update_item(
            Key={"session_id": inspection.get("session_id", session_id)},
            UpdateExpression="SET #status = :status, progress = :progress, updated_at = :updated_at, resumed_at = :resumed_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "in_progress",
                ":progress": sanitize_for_dynamodb(progress),
                ":updated_at": now_iso(),
                ":resumed_at": now_iso(),
            },
        )
    except Exception as e:
        logger.warning(f"Failed to update session table: {e}")

    response_data = copy.deepcopy(inspection)
    response_data["progress"] = progress
    response_data["next_unanswered_item_id"] = next_item_id

    return json_response(200, response_data)


def lambda_handler(event, context):
    # ── JWT Security Middleware ──────────────────────────────────────────
    # If a Cognito JWT Authorizer is attached, extract the verified
    # company_key from the token and forcefully inject it into the event
    # so that ALL downstream functions use the secure, verified key.
    _claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    if _claims:
        _secure_company = _claims.get("custom:company_key", "")
        _groups = _claims.get("cognito:groups", "")
        if isinstance(_groups, str):
            _groups = [g.strip() for g in _groups.split(",")]
        elif not _groups:
            _groups = []

        # SuperAdmins don't have a company_key in their token;
        # they pass it via query params or body — that's allowed.
        if _secure_company and "SuperAdmin" not in _groups:
            # Overwrite query params
            if event.get("queryStringParameters") is None:
                event["queryStringParameters"] = {}
            event["queryStringParameters"]["company_key"] = _secure_company
            event["queryStringParameters"]["tenant_id"] = _secure_company

            # Overwrite body
            import json as _json
            _raw_body = event.get("body") or "{}"
            try:
                _body_obj = _json.loads(_raw_body) if isinstance(_raw_body, str) else _raw_body
                if isinstance(_body_obj, dict):
                    _body_obj["company_key"] = _secure_company
                    event["body"] = _json.dumps(_body_obj)
            except Exception:
                pass
    # ── End JWT Security Middleware ──────────────────────────────────────

    method, path, resource = get_route(event)

    if method == "OPTIONS":
        return json_response(200, {"message": "ok"})

    auth_error = require_api_key(event)
    if auth_error:
        return auth_error

    if method == "GET" and (
        path_endswith(path, "/eyewash/checklist") or
        path_endswith(path, "/eyewash-inspection/checklist")
    ):
        return get_checklist(event)

    if method == "GET" and (
        path_endswith(path, "/eyewash/ai-enablement") or
        path_endswith(path, "/eyewash-inspection/ai-enablement")
    ):
        return get_ai_enablement_matrix(event)

    if method == "POST" and path_endswith(path, "/eyewash-inspection"):
        return create_inspection_from_session_payload(event)

    if method == "GET" and path_endswith(path, "/eyewash-inspections"):
        return list_inspections(event)

    if method == "GET" and re.search(r"/eyewash-inspection/[^/]+/completion-readiness$", path):
        parts = path.rstrip("/").split("/")
        event["pathParameters"] = {"inspection_id": parts[-2]}
        return get_completion_readiness(event)

    if method == "GET" and re.search(r"/eyewash-inspection/[^/]+$", path):
        inspection_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"inspection_id": inspection_id}
        return get_inspection(event)

    if method == "POST" and path_endswith(path, "/eyewash/analyze"):
        return analyze_item_image(event)

    if method == "POST" and path_endswith(path, "/eyewash/analyze-batch"):
        return batch_analyze_items(event)

    if method == "POST" and path_endswith(path, "/eyewash/voice"):
        return handle_voice_command(event)

    if method == "POST" and re.search(r"/eyewash-inspection/[^/]+/items/[^/]+/confirm$", path):
        parts = path.rstrip("/").split("/")
        event["pathParameters"] = {"inspection_id": parts[-4], "item_id": parts[-2]}
        return confirm_manual_answer(event)

    if method == "PATCH" and re.search(r"/(?:eyewash/session|eyewash-inspection)/[^/]+/items/[^/]+/note$", path):
        parts = path.rstrip("/").split("/")
        event["pathParameters"] = {"inspection_id": parts[-4], "item_id": parts[-2]}
        return add_note_to_item(event)

    if method == "PATCH" and re.search(r"/(?:eyewash/session|eyewash-inspection)/[^/]+/items/[^/]+$", path):
        parts = path.rstrip("/").split("/")
        event["pathParameters"] = {"inspection_id": parts[-3], "item_id": parts[-1]}
        return update_checklist_item(event)

    if method == "DELETE" and re.search(r"/eyewash-inspection/[^/]+$", path):
        inspection_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"inspection_id": inspection_id}
        return delete_inspection(event)

    if method == "DELETE" and re.search(r"/eyewash/session/[^/]+$", path):
        session_id = path.rstrip("/").split("/")[-1]
        event["pathParameters"] = {"session_id": session_id}
        return delete_session(event)

    if method == "POST" and re.search(r"/eyewash/session/[^/]+/pause$", path):
        session_id = path.rstrip("/").split("/")[-2]
        event["pathParameters"] = {"session_id": session_id}
        return pause_session_handler(event)

    if method == "GET" and re.search(r"/eyewash/session/[^/]+/resume$", path):
        session_id = path.rstrip("/").split("/")[-2]
        event["pathParameters"] = {"session_id": session_id}
        return resume_session_handler(event)

    return json_response(404, {"error": f"No route for {method} {path or resource}"})
