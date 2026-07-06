"""Generate OSHA Safety Hub Postman collection."""
import json
import os

DEFAULT_BASE_URL = "https://1nm4udlaif.execute-api.ap-south-1.amazonaws.com/dev"

# Checklist types and their mobile API path prefixes
CHECKLIST_TYPES = [
    {
        "type": "fire-extinguisher",
        "label": "Fire Extinguisher",
        "checklist_get": "/fire-extinguisher-inspection/checklist",
        "inspection_get": "/fire-extinguisher-inspection/{{inspection_id}}",
        "patch_item": "/fire-extinguisher/session/{{inspection_id}}/items/{{custom_item_id}}",
        "analyze": "/fire-extinguisher/analyze",
    },
    {
        "type": "eyewash",
        "label": "EyeWash",
        "checklist_get": "/eyewash-inspection/checklist",
        "inspection_get": "/eyewash-inspection/{{inspection_id}}",
        "patch_item": "/eyewash/session/{{inspection_id}}/items/{{custom_item_id}}",
        "analyze": "/eyewash/analyze",
    },
    {
        "type": "exit-door",
        "label": "Exit Door",
        "checklist_get": "/exit-door-inspection/checklist",
        "inspection_get": "/exit-door-inspection/{{inspection_id}}",
        "patch_item": "/exit-door/session/{{inspection_id}}/items/{{custom_item_id}}",
        "analyze": "/exit-door/analyze",
    },
    {
        "type": "hra",
        "label": "HRA",
        "checklist_get": "/hra-inspection/checklist",
        "inspection_get": "/hra-inspection/{{inspection_id}}",
        "patch_item": None,
        "analyze": None,
    },
    {
        "type": "racking",
        "label": "Racking",
        "checklist_get": "/racking-inspection/checklist",
        "inspection_get": "/racking-inspection/{{inspection_id}}",
        "patch_item": "/racking/session/{{inspection_id}}/items/{{custom_item_id}}",
        "analyze": "/racking/analyze",
    },
    {
        "type": "recordkeeping",
        "label": "Recordkeeping",
        "checklist_get": "/inspection/checklist",
        "inspection_get": "/inspection/{{inspection_id}}",
        "patch_item": None,
        "analyze": None,
    },
]

COLLECTION_VARS = [
    {"key": "base_url", "value": DEFAULT_BASE_URL},
    {"key": "api_key", "value": "YOUR_API_KEY_HERE"},
    {"key": "company_key", "value": "cigroupusa"},
    {"key": "checklist_type", "value": "fire-extinguisher"},
    {"key": "inspection_id", "value": ""},
    {"key": "session_id", "value": ""},
    {"key": "item_id", "value": "1"},
    {"key": "custom_item_id", "value": ""},
    {"key": "job_id", "value": ""},
    {"key": "reseller_key", "value": ""},
    {"key": "location_key", "value": ""},
    {"key": "station_id", "value": ""},
]


def make_request(name, method, path, body=None, query=None, description=""):
    path = "/" + path.strip("/")
    raw = "{{base_url}}" + path
    if query:
        raw += "?" + "&".join(f"{k}={v}" for k, v in query.items())
    item = {
        "name": name,
        "request": {
            "method": method,
            "header": [
                {"key": "x-api-key", "value": "{{api_key}}", "type": "text"},
                {"key": "Content-Type", "value": "application/json", "type": "text"},
            ],
            "url": raw,
            "description": description,
        },
    }
    if body is not None:
        item["request"]["body"] = {
            "mode": "raw",
            "raw": json.dumps(body, indent=2),
            "options": {"raw": {"language": "json"}},
        }
    return item


def folder(name, items, description=""):
    return {"name": name, "description": description, "item": items}


def custom_crud_flow_for_type(ct):
    """Seven-step custom question CRUD flow for one checklist type."""
    t = ct["type"]
    label = ct["label"]
    return folder(label, [
        make_request("1. Add Custom Item", "POST", f"/checklist-template/{t}/custom-item", {
            "company_key": "{{company_key}}",
            "category_id": 1,
            "description": f"Custom {label} test question",
            "requires_evidence": True,
        }, description="Save `id` from response into `custom_item_id` variable."),
        make_request("2. Get Admin Template", "GET", f"/checklist-template/{t}",
                     query={"company_key": "{{company_key}}"},
                     description="Custom item should appear with is_enabled=true and is_custom=true."),
        make_request(f"3. Get Mobile Checklist", "GET", ct["checklist_get"],
                     query={"company_key": "{{company_key}}"},
                     description="Custom item should appear in the correct category."),
        make_request("4. Toggle Custom OFF", "PUT", f"/checklist-template/{t}/toggle-item", {
            "company_key": "{{company_key}}",
            "item_id": "{{custom_item_id}}",
            "enabled": False,
        }),
        make_request("5. Get Mobile Checklist (hidden)", "GET", ct["checklist_get"],
                     query={"company_key": "{{company_key}}"},
                     description="Custom item should NOT appear."),
        make_request("6. Toggle Custom ON", "PUT", f"/checklist-template/{t}/toggle-item", {
            "company_key": "{{company_key}}",
            "item_id": "{{custom_item_id}}",
            "enabled": True,
        }),
        make_request("7. Delete Custom Item", "DELETE",
                     f"/checklist-template/{t}/custom-item/{{custom_item_id}}",
                     query={"company_key": "{{company_key}}"},
                     description="Removes custom item from overlay."),
    ])


def template_admin_for_type(ct):
    t = ct["type"]
    label = ct["label"]
    return folder(label, [
        make_request("Get Merged Template", "GET", f"/checklist-template/{t}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Get Raw Overlay", "GET", f"/checklist-template/{t}/config",
                     query={"company_key": "{{company_key}}"}),
        make_request("Toggle Default Item", "PUT", f"/checklist-template/{t}/toggle-item", {
            "company_key": "{{company_key}}", "item_id": 1, "enabled": False,
        }),
        make_request("Toggle Custom Item", "PUT", f"/checklist-template/{t}/toggle-item", {
            "company_key": "{{company_key}}", "item_id": "{{custom_item_id}}", "enabled": False,
        }),
        make_request("Add Custom Item", "POST", f"/checklist-template/{t}/custom-item", {
            "company_key": "{{company_key}}", "category_id": 1,
            "description": f"Custom {label} question", "requires_evidence": True,
        }),
        make_request("Delete Custom Item", "DELETE",
                     f"/checklist-template/{t}/custom-item/{{custom_item_id}}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Delete Company Overlay", "DELETE", f"/checklist-template/{t}",
                     query={"company_key": "{{company_key}}"}),
    ])


# ═══════════════════════════════════════════════════════════════
# NEW FEATURES — Custom Checklist (test first)
# ═══════════════════════════════════════════════════════════════
new_features = folder("NEW Features — Custom Checklist", [
    folder("00 — Quick Setup", [
        make_request("Health Check — List Templates", "GET", "/checklist-templates",
                     query={"company_key": "{{company_key}}"},
                     description="Confirms base_url and api_key are correct. Expect 200."),
        make_request("Variables Reference", "GET", "/checklist-template/{{checklist_type}}",
                     query={"company_key": "{{company_key}}"},
                     description=(
                         "Collection variables to set before testing:\n"
                         "- api_key\n- company_key\n- checklist_type\n"
                         "- custom_item_id (from Add Custom Item response)\n"
                         "- inspection_id / session_id (for mobile evidence tests)"
                     )),
    ]),
    folder("01 — Custom Question CRUD",
           [custom_crud_flow_for_type(ct) for ct in CHECKLIST_TYPES],
           description="Run steps 1→7 in order for each checklist type. Works for ALL types."),
    folder("02 — Default Item Toggle", [
        make_request("Toggle Default Item OFF", "PUT",
                     "/checklist-template/{{checklist_type}}/toggle-item", {
            "company_key": "{{company_key}}", "item_id": 1, "enabled": False,
        }, description="Set checklist_type variable first (e.g. fire-extinguisher)."),
        make_request("Get Mobile Checklist (item hidden)", "GET",
                     "/fire-extinguisher-inspection/checklist",
                     query={"company_key": "{{company_key}}"},
                     description="Change URL path to match checklist_type if not fire-extinguisher."),
        make_request("Toggle Default Item ON", "PUT",
                     "/checklist-template/{{checklist_type}}/toggle-item", {
            "company_key": "{{company_key}}", "item_id": 1, "enabled": True,
        }),
        make_request("Get Mobile Checklist (item visible)", "GET",
                     "/fire-extinguisher-inspection/checklist",
                     query={"company_key": "{{company_key}}"}),
    ], description="Verifies disabled default items are filtered from mobile checklist."),
    folder("03 — Custom Question Evidence", [
        make_request("Get Evidence Upload URL", "GET", "/evidence/upload-url", query={
            "filename": "custom_evidence.jpg",
            "contentType": "image/jpeg",
            "inspectionType": "{{checklist_type}}",
        }, description="Upload file to returned URL, then use file_key in PATCH/analyze."),
    ] + [
        req for ct in CHECKLIST_TYPES if ct["patch_item"]
        for req in [
            make_request(
                f"PATCH Custom Item — {ct['label']}", "PATCH", ct["patch_item"], {
                    "answer": "Yes",
                    "finding": "Custom item verified",
                    "action_item": "",
                    "evidence": [{"file_key": "evidence/{{checklist_type}}/sample.jpg", "analyzed_at": ""}],
                },
                description=f"Requires inspection_id and custom_item_id. Path: {ct['patch_item']}",
            ),
        ]
    ] + [
        req for ct in CHECKLIST_TYPES if ct["analyze"]
        for req in [
            make_request(
                f"Analyze Custom Item — {ct['label']}", "POST", ct["analyze"], {
                    "inspection_id": "{{inspection_id}}",
                    "item_id": "{{custom_item_id}}",
                    "file_key": "evidence/{{checklist_type}}/sample.jpg",
                    "company_key": "{{company_key}}",
                },
                description="AI analysis using item description for custom questions.",
            ),
        ]
    ]),
    folder("04 — Inspection Sync (Resume Bug)", [
        make_request("Get Inspection with company_key — Fire Extinguisher", "GET",
                     "/fire-extinguisher-inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"},
                     description="Syncs stored inspection with latest template overlay."),
        make_request("Get Inspection with company_key — EyeWash", "GET",
                     "/eyewash-inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Get Inspection with company_key — Exit Door", "GET",
                     "/exit-door-inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Get Inspection with company_key — HRA", "GET",
                     "/hra-inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Get Inspection with company_key — Racking", "GET",
                     "/racking-inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"}),
        make_request("Get Inspection with company_key — Recordkeeping", "GET",
                     "/inspection/{{inspection_id}}",
                     query={"company_key": "{{company_key}}"}),
    ], description="Tests resume flow: disabled/custom items sync into stored inspections."),
], description="Test new custom checklist APIs here first. Then use Web/Mobile folders for full coverage.")

# ═══════════════════════════════════════════════════════════════
# WEB DASHBOARD
# ═══════════════════════════════════════════════════════════════
web_dashboard = folder("Web Dashboard", [
    folder("Organization", [
        folder("Resellers", [
            make_request("List Resellers", "GET", "/api/resellers"),
            make_request("Create Reseller", "POST", "/api/resellers", {"name": "Test Reseller"}),
            make_request("Update Reseller", "PUT", "/api/resellers/{{reseller_key}}", {"name": "Updated Reseller"}),
            make_request("Delete Reseller", "DELETE", "/api/resellers/{{reseller_key}}"),
        ]),
        folder("Companies & Locations", [
            make_request("List Companies", "GET", "/api/companies"),
            make_request("Create Company", "POST", "/api/companies", {
                "name": "Test Company", "company_key": "{{company_key}}",
            }),
            make_request("Update Company", "PUT", "/api/companies/{{company_key}}", {"name": "Updated Company"}),
            make_request("Delete Company", "DELETE", "/api/companies/{{company_key}}"),
            make_request("Create Location", "POST", "/api/companies/{{company_key}}/locations", {
                "name": "Main Plant", "location_key": "{{location_key}}",
            }),
            make_request("Update Location", "PUT", "/api/companies/{{company_key}}/locations/{{location_key}}",
                         {"name": "Updated Location"}),
            make_request("Get Location Inspection Categories", "GET",
                         "/api/companies/{{company_key}}/locations/{{location_key}}/inspection-categories"),
            make_request("Toggle Category OFF", "PUT",
                         "/api/companies/{{company_key}}/locations/{{location_key}}/toggle-category",
                         {"category_key": "eyewash", "enabled": False}),
            make_request("Toggle Category ON", "PUT",
                         "/api/companies/{{company_key}}/locations/{{location_key}}/toggle-category",
                         {"category_key": "eyewash", "enabled": True}),
            make_request("Delete Location", "DELETE",
                         "/api/companies/{{company_key}}/locations/{{location_key}}"),
            make_request("Create Station", "POST",
                         "/api/companies/{{company_key}}/locations/{{location_key}}/stations", {
                "station_type": "fire-extinguisher", "name": "FE Station 1",
            }),
            make_request("Update Station", "PUT", "/api/stations/{{station_id}}", {"status": "ok", "notes": ""}),
            make_request("Delete Station", "DELETE", "/api/stations/{{station_id}}"),
        ]),
    ]),
    folder("Alerts & Admin Inspections", [
        make_request("Get Alerts", "GET", "/api/alerts"),
        make_request("Admin List Inspections", "GET", "/api/inspections",
                     query={"company_key": "{{company_key}}"}),
    ]),
    folder("Checklist Template Admin", [
        folder("Global", [
            make_request("List All Templates", "GET", "/checklist-templates",
                         query={"company_key": "{{company_key}}"}),
        ]),
    ] + [template_admin_for_type(ct) for ct in CHECKLIST_TYPES]),
    folder("Company Config", [
        make_request("Get Company Config", "GET", "/company-config/{{company_key}}"),
        make_request("Put Company Config", "PUT", "/company-config/{{company_key}}", {
            "blocked_verdict_label": "need_review",
        }),
    ]),
])

# ═══════════════════════════════════════════════════════════════
# MOBILE APP — helper builders per inspection type
# ═══════════════════════════════════════════════════════════════
def mobile_type_folder(ct):
    t = ct["type"]
    label = ct["label"]
    items = [
        folder("Checklist & Inspection", [
            make_request("Get Checklist", "GET", ct["checklist_get"],
                         query={"company_key": "{{company_key}}"}),
            make_request("Get Inspection", "GET", ct["inspection_get"],
                         query={"company_key": "{{company_key}}"}),
            make_request("Delete Inspection", "DELETE", ct["inspection_get"]),
        ]),
    ]

    if t == "fire-extinguisher":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/fire-extinguisher-inspection", {
            "session_id": "{{session_id}}", "categories": [], "general_results": [], "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/fire-extinguisher-inspections"))
        items.append(folder("Session & Items", [
            make_request("Update Item", "PATCH",
                         "/fire-extinguisher/session/{{inspection_id}}/items/{{item_id}}",
                         {"answer": "Yes", "finding": "", "action_item": ""}),
            make_request("Add Item Note", "PATCH",
                         "/fire-extinguisher/session/{{inspection_id}}/items/{{item_id}}/note",
                         {"note": "Test note"}),
            make_request("Get Report", "GET", "/fire-extinguisher/session/{{inspection_id}}/report"),
            make_request("Pause Session", "POST", "/fire-extinguisher/session/{{session_id}}/pause", {}),
            make_request("Resume Session", "GET", "/fire-extinguisher/session/{{session_id}}/resume"),
            make_request("Delete Session", "DELETE", "/fire-extinguisher/session/{{session_id}}"),
        ]))
        items.append(folder("AI & Voice", [
            make_request("Analyze Image", "POST", "/fire-extinguisher/analyze", {
                "inspection_id": "{{inspection_id}}", "item_id": "{{item_id}}",
                "file_key": "evidence/fire-extinguisher/sample.jpg", "company_key": "{{company_key}}",
            }),
            make_request("Get Analyze Job Status", "GET", "/fire-extinguisher/analyze/status/{{job_id}}"),
            make_request("Voice Command", "POST", "/fire-extinguisher/voice", {
                "inspection_id": "{{inspection_id}}", "current_item_id": "{{item_id}}", "transcript": "next",
            }),
            make_request("Generate QR", "POST", "/fire-extinguisher/qr/generate", {
                "station_id": "{{station_id}}", "height_cm": 120,
            }),
        ]))
    elif t == "eyewash":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/eyewash-inspection", {
            "session_id": "{{session_id}}", "categories": [], "general_results": [], "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/eyewash-inspections"))
        items[0]["item"].append(make_request("Get Completion Readiness", "GET",
                         "/eyewash-inspection/{{inspection_id}}/completion-readiness"))
        items.append(folder("Session & Items", [
            make_request("Update Item", "PATCH", "/eyewash/session/{{inspection_id}}/items/{{item_id}}",
                         {"answer": "Yes"}),
            make_request("Add Item Note", "PATCH",
                         "/eyewash/session/{{inspection_id}}/items/{{item_id}}/note", {"note": "Test note"}),
            make_request("Confirm Manual Answer", "POST",
                         "/eyewash-inspection/{{inspection_id}}/items/{{item_id}}/confirm", {"answer": "Yes"}),
            make_request("Pause Session", "POST", "/eyewash/session/{{session_id}}/pause", {}),
            make_request("Resume Session", "GET", "/eyewash/session/{{session_id}}/resume"),
            make_request("Delete Session", "DELETE", "/eyewash/session/{{session_id}}"),
        ]))
        items.append(folder("AI & Voice", [
            make_request("Analyze Image", "POST", "/eyewash/analyze", {
                "inspection_id": "{{inspection_id}}", "item_id": "{{item_id}}",
                "file_key": "evidence/eyewash/sample.jpg", "company_key": "{{company_key}}",
            }),
            make_request("Batch Analyze", "POST", "/eyewash/analyze-batch", {
                "inspection_id": "{{inspection_id}}", "items": [],
            }),
            make_request("Voice Command", "POST", "/eyewash/voice", {
                "inspection_id": "{{inspection_id}}", "current_item_id": "{{item_id}}", "transcript": "next",
            }),
        ]))
    elif t == "exit-door":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/exit-door-inspection", {
            "session_id": "{{session_id}}", "categories": [], "general_results": [], "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/exit-door-inspections"))
        items.append(folder("Session & Items", [
            make_request("Update Item", "PATCH", "/exit-door/session/{{inspection_id}}/items/{{item_id}}",
                         {"answer": "Yes"}),
            make_request("Add Item Note", "PATCH",
                         "/exit-door/session/{{inspection_id}}/items/{{item_id}}/note", {"note": "Test note"}),
            make_request("Get Report", "GET", "/exit-door/session/{{inspection_id}}/report"),
            make_request("Pause Session", "POST", "/exit-door/session/{{session_id}}/pause", {}),
            make_request("Resume Session", "GET", "/exit-door/session/{{session_id}}/resume"),
            make_request("Delete Session", "DELETE", "/exit-door/session/{{session_id}}"),
        ]))
        items.append(folder("AI & Voice", [
            make_request("Analyze Image", "POST", "/exit-door/analyze", {
                "inspection_id": "{{inspection_id}}", "item_id": "{{item_id}}",
                "file_key": "evidence/exit-door/sample.jpg", "company_key": "{{company_key}}",
            }),
            make_request("Batch Analyze", "POST", "/exit-door/analyze-batch", {
                "inspection_id": "{{inspection_id}}", "items": [],
            }),
            make_request("Get Analyze Job Status", "GET", "/exit-door/analyze/status/{{job_id}}"),
            make_request("Voice Command", "POST", "/exit-door/voice", {
                "inspection_id": "{{inspection_id}}", "current_item_id": "{{item_id}}", "transcript": "next",
            }),
        ]))
    elif t == "hra":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/hra-inspection", {
            "session_id": "{{session_id}}", "categories": [], "general_results": [], "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/hra-inspections"))
    elif t == "racking":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/racking-inspection", {
            "session_id": "{{session_id}}", "categories": [], "general_results": [], "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/racking-inspections"))
        items[0]["item"].append(make_request("Get AI Enablement", "GET", "/racking/ai-enablement"))
        items.append(folder("Session & Items", [
            make_request("Update Item", "PATCH", "/racking/session/{{inspection_id}}/items/{{item_id}}",
                         {"answer": "In compliance"}),
            make_request("Add Item Note", "PATCH",
                         "/racking/session/{{inspection_id}}/items/{{item_id}}/note", {"note": "Test note"}),
            make_request("Pause Session", "POST", "/racking/session/{{session_id}}/pause", {}),
            make_request("Resume Session", "GET", "/racking/session/{{session_id}}/resume"),
            make_request("Delete Session", "DELETE", "/racking/session/{{session_id}}"),
        ]))
        items.append(folder("AI & Voice", [
            make_request("Analyze Image", "POST", "/racking/analyze", {
                "inspection_id": "{{inspection_id}}", "item_id": "{{item_id}}",
                "file_key": "evidence/racking/sample.jpg", "company_key": "{{company_key}}",
            }),
            make_request("Batch Analyze", "POST", "/racking/analyze-batch", {
                "inspection_id": "{{inspection_id}}", "items": [],
            }),
            make_request("Voice Command", "POST", "/racking/voice", {
                "inspection_id": "{{inspection_id}}", "current_item_id": "{{item_id}}", "transcript": "next",
            }),
        ]))
    elif t == "recordkeeping":
        items[0]["item"].insert(1, make_request("Create Inspection", "POST", "/inspection", {
            "session_id": "{{session_id}}", "categories": [],
            "general_results": [{"finding": "", "action_item": "", "responsible": "", "due_date": ""}],
            "notes": "",
        }))
        items[0]["item"].insert(2, make_request("List Inspections", "GET", "/inspections"))

    return folder(label, items)


mobile_app = folder("Mobile App", [
    folder("Shared", [
        make_request("Create Session", "POST", "/inspection-session", {
            "auditor_name": "Test Auditor",
            "facility_area": "Warehouse A",
            "date_of_audit": "2026-07-01",
            "location": "Main Plant",
            "station": "Station 1",
            "station_id": "{{station_id}}",
            "company_key": "{{company_key}}",
        }),
        make_request("Get Evidence Upload URL", "GET", "/evidence/upload-url", query={
            "filename": "photo.jpg", "contentType": "image/jpeg", "inspectionType": "fire-extinguisher",
        }),
        make_request("Get Evidence Download URL", "GET", "/evidence/download-url", query={
            "file_key": "evidence/fire-extinguisher/2026-07-01/sample_photo.jpg",
        }),
        make_request("Mobile Inspection Status", "GET", "/api/mobile/inspection-status", query={
            "company_key": "{{company_key}}", "location_key": "{{location_key}}",
        }),
    ]),
] + [mobile_type_folder(ct) for ct in CHECKLIST_TYPES])

collection = {
    "info": {
        "_postman_id": "osha-safety-hub-api-collection-v2",
        "name": "OSHA Safety Hub API",
        "description": (
            "OSHA Safety Hub backend.\n\n"
            "**Start here:** NEW Features — Custom Checklist\n\n"
            "**Variables:** base_url, api_key, company_key, custom_item_id, inspection_id\n\n"
            f"**base_url:** {DEFAULT_BASE_URL}"
        ),
        "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
    },
    "auth": {
        "type": "apikey",
        "apikey": [
            {"key": "value", "value": "{{api_key}}", "type": "string"},
            {"key": "key", "value": "x-api-key", "type": "string"},
            {"key": "in", "value": "header", "type": "string"},
        ],
    },
    "variable": COLLECTION_VARS,
    "item": [new_features, web_dashboard, mobile_app],
}

out_path = os.path.join(os.path.dirname(__file__), "OSHA-Safety-Hub.postman_collection.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(collection, f, indent=2)

# Print folder tree summary
def print_tree(items, indent=0):
    for i in items:
        print("  " * indent + "- " + i["name"])
        if "item" in i:
            print_tree(i["item"], indent + 1)

print(f"Written {out_path}\n")
print("Folder structure:")
print_tree(collection["item"])
