"""
seed_checklists.py — One-time Migration Script
Extracts hardcoded checklist definitions from each Lambda's source file
and uploads them to the 'osha-checklist-templates' DynamoDB table.

Usage:
  python scripts/seed_checklists.py --region ap-south-1
  python scripts/seed_checklists.py --region ap-south-1 --dry-run
  python scripts/seed_checklists.py --region ap-south-1 --table my-custom-table

Requirements:
  - boto3 installed (pip install boto3)
  - AWS credentials configured (aws configure or IAM role)
  - DynamoDB table 'osha-checklist-templates' must already exist with:
      Partition Key: tenant_id (String)
      Sort Key:      checklist_type (String)
"""

import argparse
import importlib.util
import json
import os
import sys
import types
import unittest.mock
from datetime import datetime, timezone
from decimal import Decimal

# ─────────────────────────────────────────────
# Save real boto3 before mocking (needed for DynamoDB writes later)
# ─────────────────────────────────────────────
import boto3 as _real_boto3


# ─────────────────────────────────────────────
# Lambda source files and their checklist variable names
# ─────────────────────────────────────────────
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CHECKLIST_SOURCES = [
    {
        "checklist_type": "fire-extinguisher",
        "module_path": os.path.join(REPO_ROOT, "Osha-Fire-Extinguisher-Lambda", "lambda_function.py"),
        "variable_name": "FIRE_EXTINGUISHER_CHECKLIST",
    },
    {
        "checklist_type": "eyewash",
        "module_path": os.path.join(REPO_ROOT, "Osha-EyeWash-lambda", "lambda_function.py"),
        "variable_name": "EYEWASH_CHECKLIST",
    },
    {
        "checklist_type": "exit-door",
        "module_path": os.path.join(REPO_ROOT, "Osha-Exitdoor", "lambda_function.py"),
        "variable_name": "EXIT_DOOR_CHECKLIST",
    },
    {
        "checklist_type": "racking",
        "module_path": os.path.join(REPO_ROOT, "Osha-Monthly-Racking-Lambda", "lambda_function.py"),
        "variable_name": "RACKING_CHECKLIST",
    },
    {
        "checklist_type": "hra",
        "module_path": os.path.join(REPO_ROOT, "Osha-HRA-Lambda", "lambda_function.py"),
        "variable_name": "HRA_CHECKLIST",
    },
    {
        "checklist_type": "recordkeeping",
        "module_path": os.path.join(REPO_ROOT, "osha-checklist-handler", "lambda_function.py"),
        "variable_name": "OSHA_CHECKLIST",
    },
]


def convert_floats_to_decimal(obj):
    """Convert float values to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    return obj


def mock_aws_modules():
    """
    Mock AWS SDK and optional modules so Lambda files can be imported
    without real AWS connections or missing dependencies.
    """
    modules_to_mock = [
        "boto3",
        "botocore",
        "botocore.exceptions",
        "PIL",
        "PIL.Image",
    ]
    for mod_name in modules_to_mock:
        sys.modules[mod_name] = unittest.mock.MagicMock()


def restore_boto3():
    """Restore real boto3 after mocking."""
    sys.modules["boto3"] = _real_boto3


def load_module_from_file(filepath, module_name):
    """Dynamically load a Python file as a module."""
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    if spec is None:
        raise ImportError(f"Cannot create module spec from: {filepath}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract_checklist(source):
    """
    Extract a checklist dict from a Lambda source file.
    Returns the checklist dict or None on failure.
    """
    filepath = source["module_path"]
    var_name = source["variable_name"]
    checklist_type = source["checklist_type"]

    if not os.path.exists(filepath):
        print(f"  ✗ File not found: {filepath}")
        return None

    try:
        module_name = f"lambda_{checklist_type.replace('-', '_')}"
        module = load_module_from_file(filepath, module_name)
        checklist = getattr(module, var_name, None)

        if checklist is None:
            print(f"  ✗ Variable '{var_name}' not found in {filepath}")
            return None

        # Convert to plain dict (in case it's a custom object)
        checklist = json.loads(json.dumps(checklist, default=str))

        # Count items
        item_count = 0
        cat_count = 0
        for cat in checklist.get("categories", []):
            cat_count += 1
            item_count += len(cat.get("items", []))
            for sub in cat.get("sub_sections", []):
                item_count += len(sub.get("items", []))

        print(f"  ✓ {checklist_type}: {item_count} items across {cat_count} categories")
        return checklist

    except Exception as e:
        print(f"  ✗ Error loading {checklist_type}: {e}")
        return None


def seed_to_dynamodb(table, tenant_id, checklist_type, checklist, dry_run=False):
    """Write a checklist to the DynamoDB table."""
    now = datetime.now(timezone.utc).isoformat()

    item = {
        "tenant_id": tenant_id,
        "checklist_type": checklist_type,
        **checklist,
        "created_at": now,
        "updated_at": now,
    }

    # Convert floats to Decimal for DynamoDB
    item = convert_floats_to_decimal(item)

    if dry_run:
        print(f"  [DRY RUN] Would write: tenant_id={tenant_id}, checklist_type={checklist_type}")
        return True

    try:
        table.put_item(Item=item)
        print(f"  ✓ Written: tenant_id={tenant_id}, checklist_type={checklist_type}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to write {checklist_type}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Seed checklist templates to DynamoDB")
    parser.add_argument("--region", default="ap-south-1", help="AWS region (default: ap-south-1)")
    parser.add_argument("--table", default="osha-checklist-templates", help="DynamoDB table name")
    parser.add_argument("--tenant-id", default="default", help="Tenant ID (default: 'default')")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DynamoDB")
    args = parser.parse_args()

    print("=" * 60)
    print("OSHA Checklist Template Seeder")
    print("=" * 60)
    print(f"Region:    {args.region}")
    print(f"Table:     {args.table}")
    print(f"Tenant ID: {args.tenant_id}")
    print(f"Dry Run:   {args.dry_run}")
    print()

    # ── Step 1: Extract checklists from Lambda source files ──
    print("Step 1: Extracting checklists from Lambda source files...")
    print("-" * 60)

    mock_aws_modules()

    checklists = {}
    for source in CHECKLIST_SOURCES:
        checklist = extract_checklist(source)
        if checklist:
            checklists[source["checklist_type"]] = checklist

    # Restore real boto3 for DynamoDB writes
    restore_boto3()

    print()
    print(f"Extracted {len(checklists)}/{len(CHECKLIST_SOURCES)} checklists successfully.")
    print()

    if not checklists:
        print("No checklists extracted. Nothing to seed.")
        sys.exit(1)

    # ── Step 2: Write to DynamoDB ──
    print("Step 2: Writing to DynamoDB...")
    print("-" * 60)

    if not args.dry_run:
        dynamodb = _real_boto3.resource("dynamodb", region_name=args.region)
        table = dynamodb.Table(args.table)
    else:
        table = None

    success_count = 0
    for checklist_type, checklist in checklists.items():
        if seed_to_dynamodb(table, args.tenant_id, checklist_type, checklist, args.dry_run):
            success_count += 1

    print()
    print("=" * 60)
    print(f"DONE: {success_count}/{len(checklists)} checklists seeded.")
    if args.dry_run:
        print("(Dry run — no data was written. Remove --dry-run to execute.)")
    print("=" * 60)


if __name__ == "__main__":
    main()
