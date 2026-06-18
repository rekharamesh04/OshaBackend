"""
checklist_loader.py — Shared Checklist Template Loader

Fetches inspection checklist templates from the 'osha-checklist-templates'
DynamoDB table with tenant-specific fallback:

  1. Try (tenant_id, checklist_type)
  2. If not found, fall back to ("default", checklist_type)
  3. Cache in Lambda memory for warm invocation reuse
  4. Return deep copy so callers can mutate freely

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


def load_checklist(checklist_type: str, tenant_id: str = "default") -> dict:
    """
    Fetch a checklist template from DynamoDB with tenant fallback.

    1. Try (tenant_id, checklist_type) — company-specific
    2. If not found and tenant_id != "default", try ("default", checklist_type)
    3. Cache result in memory for warm Lambda reuse
    4. Return deep copy so callers can mutate (add answers, evidence, etc.)

    Returns None if not found in DynamoDB (caller should fall back to hardcoded).
    """
    cache_key = f"{tenant_id}:{checklist_type}"
    if cache_key in _CACHE:
        return copy.deepcopy(_CACHE[cache_key])

    try:
        table = _get_table()

        # Try tenant-specific record first
        resp = table.get_item(Key={
            "tenant_id": tenant_id,
            "checklist_type": checklist_type,
        })
        item = resp.get("Item")

        # Fallback to default if tenant-specific not found
        if not item and tenant_id != "default":
            resp = table.get_item(Key={
                "tenant_id": "default",
                "checklist_type": checklist_type,
            })
            item = resp.get("Item")

        if item:
            # Remove DynamoDB key / metadata fields — they aren't part of the template
            item.pop("tenant_id", None)
            item.pop("checklist_type", None)
            item.pop("created_at", None)
            item.pop("updated_at", None)
            item = _convert_decimals(item)
            _CACHE[cache_key] = item
            return copy.deepcopy(item)

    except Exception as e:
        logger.error(
            "Failed to load checklist '%s' for tenant '%s': %s",
            checklist_type, tenant_id, e,
        )

    return None


def clear_cache():
    """Clear the in-memory cache. Useful after CRUD updates or for testing."""
    _CACHE.clear()
