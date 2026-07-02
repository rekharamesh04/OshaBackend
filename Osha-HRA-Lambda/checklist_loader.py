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
    "disabled_custom_items": ["custom_1719093000"],
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
    return str(item_id)


def _item_id_matches(stored_id, target_id):
    """Compare item IDs supporting both numeric defaults and custom string IDs."""
    if stored_id == target_id:
        return True
    try:
        return int(stored_id) == int(target_id)
    except (ValueError, TypeError):
        return str(stored_id) == str(target_id)


def _apply_overlay(default_template, overlay):
    """
    Merge an overlay record onto a default template.

    - Adds is_enabled=true to all default items
    - Sets is_enabled=false for items in overlay["disabled_items"]
    - Appends overlay["custom_items"] with is_enabled from disabled_custom_items
    - Returns a new dict (does not mutate inputs)
    """
    result = copy.deepcopy(default_template)

    disabled_set = set()
    for item_id in overlay.get("disabled_items", []):
        disabled_set.add(_normalize_id(item_id))

    disabled_custom_set = {str(cid) for cid in overlay.get("disabled_custom_items", [])}
    custom_items = overlay.get("custom_items", [])

    # Mark is_enabled on all default items
    for category in result.get("categories", []):
        for item in category.get("items", []):
            item["is_enabled"] = _normalize_id(item.get("id")) not in disabled_set

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
            custom_id = str(custom_copy.get("id", ""))
            custom_copy["is_enabled"] = custom_id not in disabled_custom_set
            custom_copy["is_custom"] = True
            if "title" not in custom_copy:
                custom_copy["title"] = "Custom Field"

            target_cat_id = custom_copy.pop("category_id", None)
            if target_cat_id is not None and _normalize_id(target_cat_id) in cat_map:
                cat_map[_normalize_id(target_cat_id)]["items"].append(custom_copy)
            else:
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
    Also drops categories that have no items left.
    Used by inspection endpoints (mobile app should only see enabled items).
    Returns a new dict.
    """
    result = copy.deepcopy(template)
    filtered_categories = []

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

        has_items = bool(category.get("items"))
        has_sub_items = any(sub.get("items") for sub in category.get("sub_sections", []))
        if has_items or has_sub_items:
            filtered_categories.append(category)

    result["categories"] = filtered_categories
    return result


def _merge_stored_item(template_item, stored_item):
    """Merge saved answers/evidence from a stored inspection onto a template item."""
    merged = copy.deepcopy(template_item)
    if not stored_item:
        return merged

    for field in (
        "answer", "finding", "action_item", "responsible", "due_date",
        "blocked_by_wrong_image",
    ):
        val = stored_item.get(field)
        if val not in (None, ""):
            merged[field] = val

    if stored_item.get("evidence"):
        merged["evidence"] = copy.deepcopy(stored_item.get("evidence", []))

    stored_title = str(stored_item.get("title", "")).strip()
    if stored_title and merged.get("title") == "Custom Field":
        merged["title"] = stored_title

    return merged


def sync_inspection_with_template(stored_inspection, checklist_type, company_key):
    """
    Reconcile a stored inspection with the latest company checklist template.
    Preserves answers/evidence for items still present; drops disabled/removed items;
    adds new custom items from the template.
    """
    if not stored_inspection or not company_key or company_key == "default":
        return stored_inspection

    template = load_checklist(checklist_type, company_key, force_refresh=True)
    if template is None:
        return stored_inspection

    filtered = filter_disabled_items(template)

    stored_lookup = {}
    for cat in stored_inspection.get("categories", []):
        for item in cat.get("items", []):
            stored_lookup[_normalize_id(item.get("id"))] = item
        for sub in cat.get("sub_sections", []):
            for item in sub.get("items", []):
                stored_lookup[_normalize_id(item.get("id"))] = item

    new_categories = []
    for cat in filtered.get("categories", []):
        new_cat = copy.deepcopy(cat)
        new_cat["items"] = [
            _merge_stored_item(item, stored_lookup.get(_normalize_id(item.get("id"))))
            for item in cat.get("items", [])
        ]

        if cat.get("sub_sections"):
            new_sub_sections = []
            for sub in cat.get("sub_sections", []):
                new_sub = copy.deepcopy(sub)
                new_sub["items"] = [
                    _merge_stored_item(item, stored_lookup.get(_normalize_id(item.get("id"))))
                    for item in sub.get("items", [])
                ]
                if new_sub.get("items"):
                    new_sub_sections.append(new_sub)
            new_cat["sub_sections"] = new_sub_sections

        if new_cat.get("items") or any(s.get("items") for s in new_cat.get("sub_sections", [])):
            new_categories.append(new_cat)

    result = copy.deepcopy(stored_inspection)
    result["categories"] = new_categories
    return result


def load_checklist(checklist_type: str, company_key: str = "default", force_refresh: bool = False) -> dict:
    """
    Fetch a checklist template from DynamoDB with company overlay merge.

    Pass force_refresh=True to bypass cache (used by mobile/admin after mutations).
    Returns None if default template not found (caller should fall back to hardcoded).
    """
    cache_key = f"{company_key}:{checklist_type}"
    if not force_refresh and cache_key in _CACHE:
        return copy.deepcopy(_CACHE[cache_key])

    try:
        table = _get_table()
        default_item = _fetch_item(table, "default", checklist_type)
        if not default_item:
            return None

        for key in ("tenant_id", "checklist_type", "created_at", "updated_at"):
            default_item.pop(key, None)

        if company_key == "default":
            result = _mark_all_enabled(default_item)
        else:
            overlay = _fetch_item(table, company_key, checklist_type)
            if overlay and any(
                k in overlay for k in ("disabled_items", "custom_items", "disabled_custom_items")
            ):
                result = _apply_overlay(default_item, overlay)
            else:
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
    """Load the raw company overlay record from DynamoDB."""
    if company_key == "default":
        return None

    try:
        table = _get_table()
        item = _fetch_item(table, company_key, checklist_type)
        if item and any(
            k in item for k in ("disabled_items", "custom_items", "disabled_custom_items")
        ):
            return item
    except Exception as e:
        logger.error(
            "Failed to load overlay for '%s' company '%s': %s",
            checklist_type, company_key, e,
        )
    return None


def get_default_item_ids(checklist_type: str) -> set:
    """Get the set of all valid item IDs from the default template."""
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


def get_custom_item_ids(checklist_type: str, company_key: str) -> set:
    """Get custom item IDs defined in a company's overlay."""
    overlay = load_company_overlay(checklist_type, company_key)
    if not overlay:
        return set()
    return {str(ci.get("id")) for ci in overlay.get("custom_items", []) if ci.get("id")}


_CONFIG_CACHE = {}   # company_key → config dict

COMPANY_CONFIG_SK = "__company_config__"

VALID_BLOCKED_LABELS = {"need_review", "fail"}


def get_company_config(company_key: str) -> dict:
    """
    Get company-level configuration from DynamoDB.
    Always reads fresh from DynamoDB (no cache) to avoid stale config across Lambdas.
    """
    default_config = {"blocked_verdict_label": "need_review"}
    if not company_key or company_key == "default":
        return default_config

    try:
        table = _get_table()
        item = _fetch_item(table, company_key, COMPANY_CONFIG_SK)
        if item:
            return {
                "blocked_verdict_label": item.get("blocked_verdict_label", "need_review"),
            }
    except Exception as e:
        logger.warning("Failed to load company config for '%s': %s", company_key, e)

    return default_config


def save_company_config(company_key: str, blocked_verdict_label: str) -> dict:
    """Save company-level configuration to DynamoDB."""
    from datetime import datetime, timezone
    table = _get_table()
    now = datetime.now(timezone.utc).isoformat()

    item = {
        "tenant_id": company_key,
        "checklist_type": COMPANY_CONFIG_SK,
        "blocked_verdict_label": blocked_verdict_label,
        "updated_at": now,
    }
    table.put_item(Item=item)

    config = {"blocked_verdict_label": blocked_verdict_label}
    _CONFIG_CACHE[company_key] = config

    return {"blocked_verdict_label": blocked_verdict_label, "updated_at": now}


def clear_cache():
    """Clear the in-memory cache. Useful after CRUD updates or for testing."""
    _CACHE.clear()
    _CONFIG_CACHE.clear()
