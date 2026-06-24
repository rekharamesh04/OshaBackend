"""
checklist_loader.py — Shared Checklist Template Loader (v3 — Company-Level Overlay)

Fetches inspection checklist templates from the 'osha-checklist-templates'
DynamoDB table with company-level overlay support:

  1. Always load the "default" master template
  2. If company_key != "default", load the company's overlay record
  3. Merge: mark disabled items (is_enabled=false), append custom items
  4. Cache in Lambda memory for warm invocation reuse
  5. Return deep copy so callers can mutate freely

Overlay record schema (company != "default"):
  {
    "tenant_id": "cigroupusa",       # DynamoDB PK — stores company_key value
    "checklist_type": "fire-extinguisher",
    "disabled_items": [3, 7],        # IDs of default items to hide
    "custom_items": [                 # Additional company-specific questions
      {"id": "custom_1719093000", "description": "...", "category_id": 1, ...}
    ]
  }

Place this file alongside lambda_function.py in each Lambda's deployment folder.
"""

import copy
import logging
import os
from decimal import Decimal

import boto3

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# In-memory cache — survives across warm Lambda invocations
# ─────────────────────────────────────────────
_CACHE = {}


def _convert_decimals(obj):
    """Convert DynamoDB Decimal types to Python int/float."""
    if isinstance(obj, list):
        return [_convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: _convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def _get_table():
    """Get the DynamoDB table resource (created once per cold start)."""
    table_name = os.getenv("CHECKLIST_TEMPLATE_TABLE", "osha-checklist-templates")
    region = os.getenv("AWS_REGION", "ap-south-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    return dynamodb.Table(table_name)


def _fetch_item(table, tenant_id, checklist_type):
    """Fetch a single item from DynamoDB. Returns dict or None."""
    resp = table.get_item(Key={
        "tenant_id": tenant_id,
        "checklist_type": checklist_type,
    })
    item = resp.get("Item")
    if item:
        return _convert_decimals(item)
    return None


def _normalize_id(item_id):
    """Normalize an item ID for comparison (int or string)."""
    if isinstance(item_id, (int, float, Decimal)):
        return int(item_id)
    return item_id


def _apply_overlay(default_template, overlay):
    """
    Merge an overlay record onto a default template.

    - Adds is_enabled=true to all default items
    - Sets is_enabled=false for items in overlay["disabled_items"]
    - Appends overlay["custom_items"] to their target categories
    - Returns a new dict (does not mutate inputs)
    """
    result = copy.deepcopy(default_template)

    disabled_set = set()
    for item_id in overlay.get("disabled_items", []):
        disabled_set.add(_normalize_id(item_id))

    custom_items = overlay.get("custom_items", [])

    # Mark is_enabled on all default items
    for category in result.get("categories", []):
        for item in category.get("items", []):
            item["is_enabled"] = _normalize_id(item.get("id")) not in disabled_set

        # Also handle sub_sections if present
        for sub in category.get("sub_sections", []):
            for item in sub.get("items", []):
                item["is_enabled"] = _normalize_id(item.get("id")) not in disabled_set

    # Append custom items to their target categories
    if custom_items:
        cat_map = {}
        for cat in result.get("categories", []):
            cat_map[_normalize_id(cat.get("id"))] = cat

        for custom in custom_items:
            custom_copy = copy.deepcopy(custom)
            custom_copy["is_enabled"] = True
            custom_copy["is_custom"] = True
            if "title" not in custom_copy:
                custom_copy["title"] = "Custom Field"

            target_cat_id = custom_copy.pop("category_id", None)
            if target_cat_id is not None and _normalize_id(target_cat_id) in cat_map:
                cat_map[_normalize_id(target_cat_id)]["items"].append(custom_copy)
            else:
                # If no matching category, append to the last category
                cats = result.get("categories", [])
                if cats:
                    cats[-1]["items"].append(custom_copy)

    return result


def _mark_all_enabled(template):
    """Add is_enabled=true to every item in a template (no overlay case)."""
    result = copy.deepcopy(template)
    for category in result.get("categories", []):
        for item in category.get("items", []):
            item["is_enabled"] = True
        for sub in category.get("sub_sections", []):
            for item in sub.get("items", []):
                item["is_enabled"] = True
    return result


def filter_disabled_items(template):
    """
    Remove items where is_enabled=false from the template.
    Used by inspection endpoints (mobile app should only see enabled items).
    Returns a new dict.
    """
    result = copy.deepcopy(template)
    for category in result.get("categories", []):
        category["items"] = [
            item for item in category.get("items", [])
            if item.get("is_enabled", True)
        ]
        for sub in category.get("sub_sections", []):
            sub["items"] = [
                item for item in sub.get("items", [])
                if item.get("is_enabled", True)
            ]
    return result


def load_checklist(checklist_type: str, company_key: str = "default") -> dict:
    """
    Fetch a checklist template from DynamoDB with company overlay merge.

    1. Load the "default" master template
    2. If company_key != "default", load the company's overlay
    3. Merge: apply disabled_items + custom_items onto the default
    4. All items get is_enabled flag (true/false)
    5. Cache result in memory for warm Lambda reuse

    Returns None if default template not found (caller should fall back to hardcoded).
    """
    cache_key = f"{company_key}:{checklist_type}"
    if cache_key in _CACHE:
        return copy.deepcopy(_CACHE[cache_key])

    try:
        table = _get_table()

        # Always load the default template first
        default_item = _fetch_item(table, "default", checklist_type)

        if not default_item:
            return None

        # Remove DynamoDB metadata fields
        for key in ("tenant_id", "checklist_type", "created_at", "updated_at"):
            default_item.pop(key, None)

        if company_key == "default":
            # No overlay — just mark all items as enabled
            result = _mark_all_enabled(default_item)
        else:
            # Load company overlay (stored under tenant_id column in DynamoDB)
            overlay = _fetch_item(table, company_key, checklist_type)

            if overlay and ("disabled_items" in overlay or "custom_items" in overlay):
                # Apply overlay onto default
                result = _apply_overlay(default_item, overlay)
            else:
                # No overlay for this company — use default with all enabled
                result = _mark_all_enabled(default_item)

        _CACHE[cache_key] = result
        return copy.deepcopy(result)

    except Exception as e:
        logger.error(
            "Failed to load checklist '%s' for company '%s': %s",
            checklist_type, company_key, e,
        )

    return None


def load_company_overlay(checklist_type: str, company_key: str) -> dict:
    """
    Load the raw company overlay record from DynamoDB.
    Returns the overlay dict or None if not found.
    Used by admin CRUD endpoints.
    """
    if company_key == "default":
        return None

    try:
        table = _get_table()
        item = _fetch_item(table, company_key, checklist_type)
        if item and ("disabled_items" in item or "custom_items" in item):
            return item
    except Exception as e:
        logger.error(
            "Failed to load overlay for '%s' company '%s': %s",
            checklist_type, company_key, e,
        )
    return None


def get_default_item_ids(checklist_type: str) -> set:
    """
    Get the set of all valid item IDs from the default template.
    Used for validating toggle-item requests.
    """
    try:
        table = _get_table()
        default_item = _fetch_item(table, "default", checklist_type)
        if not default_item:
            return set()

        ids = set()
        for cat in default_item.get("categories", []):
            for item in cat.get("items", []):
                ids.add(_normalize_id(item.get("id")))
            for sub in cat.get("sub_sections", []):
                for item in sub.get("items", []):
                    ids.add(_normalize_id(item.get("id")))
        return ids
    except Exception:
        return set()


def clear_cache():
    """Clear the in-memory cache. Useful after CRUD updates or for testing."""
    _CACHE.clear()
