"""
Quarterly Hazard Risk Assessment (HRA) - Lambda Handler
Single Lambda function handling all 5 API routes:
  POST /hra-inspection              → Create new HRA inspection (linked to session)
  GET  /hra-inspections             → List all HRA inspections (summary)
  GET  /hra-inspection/{id}         → Get full HRA inspection by ID
  GET  /hra-inspection/checklist    → Get checklist template
  DELETE /hra-inspection/{id}       → Delete an HRA inspection by ID
"""

import copy
import json
import logging
import os
import uuid

import boto3
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger(__name__)

try:
    from checklist_loader import load_checklist, clear_cache, filter_disabled_items, get_company_config, sync_inspection_with_template, build_mobile_checklist_response
except ImportError:
    load_checklist = None
    clear_cache = None
    filter_disabled_items = None
    get_company_config = None
    sync_inspection_with_template = None
    build_mobile_checklist_response = None

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("osha-hra-inspections")
sessions_table = dynamodb.Table("osha-inspection-sessions")

# API Key Authentication
EXPECTED_API_KEY = os.getenv("API_KEY", "").strip()


# ─────────────────────────────────────────────
# Checklist Definition — Fallback (used when DynamoDB is unreachable)
# ─────────────────────────────────────────────
_FALLBACK_CHECKLIST = {
    "inspection_type": "Quarterly Hazard Risk Assessment (HRA)",
    "general_information": {
        "location": "",
        "start_date": "",
        "checklist": "Quarterly Hazard Risk Assessment (HRA)",
        "leader": "",
        "team": [],
    },
    "available_answers": ["Yes", "No", "N/A"],
    "categories": [
        {
            "id": 1,
            "name": "Recordkeeping",
            "items": [
                {"id": 1, "description": "The ‘OSHA 300A Summary of Work-Related Injuries and Illnesses’ is signed, dated and posted in a conspicuous place where notices to employees are customarily posted.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 2, "description": "OSHA 300A Logs for the previous 5 years are on-file in the OSHA Recordkeeping Binder and certified with Branch manager’s signature and date.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 3, "description": "IF filed last year, the Branch manager is able to produce a signed and dated copy of the previous year’s Tier II report upon request. This is referring to the Emergency Planning and Community Right-to-Know Act (EPCRA) sections 311-312.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 4, "description": "The location has a SDS station that allows employees to access it all times while they are in the facility and the SDS contains current sheets for all chemicals used. In addition, an archive file is maintained for all discontinued SDS sheets for 30 years.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 5, "description": "Department of Labor posters are current and posted in a conspicuous area with Workers Comp and Payroll Info completed. (Check the QR code at the bottom of the postings to validate poster is current. Email HR for Workers Comp/Payroll info if needed.)", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 6, "description": "Medcor posters are on display throughout the facility.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 7, "description": "IF your facility is required to participate in a Stormwater Pollution Prevention Plan (SWPPP), monthly monitoring and sampling is being recorded by assigned personnel.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 2,
            "name": "Fleet",
            "items": [
                {"id": 8, "description": "The Branch Manager is able to produce a current and valid copy of the vehicle insurance card required to be carried in all company owned and leased vehicles.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 9, "description": "The Branch manager is able to produce a current and valid copy of the ‘Hazardous Materials Certificate of Registration’ issued by the US Department of Transportation.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 10, "description": "The Branch Manager is able to produce DOT Hazardous Materials training records that documents training of all “HazMat” employees. Training must be renewed every 3 years.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 11, "description": "The Branch manager is able to produce a current and valid copy of their DOT Hazardous Materials Security Plan.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 12, "description": "All employees that drive for company business have a current ride along on file (renewed every 3 years).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 13, "description": "CDL/ HOS medical cards are current for all CDL / HOS drivers.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 14, "description": "IF operating in a state that requires additional HazMat registrations, the branch manager is able to produce a current and valid copy of that HazMat Registration.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 15, "description": "All vehicles are labeled with their payload capacities using the Payload SOP linked in the EHS Resources folder under Fleet.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 16, "description": "All company vehicles have a current \"In the event of a collision\" checklist.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 17, "description": "All company vehicles are equipped with a Fleet Kit including required documents, emergency response tools and spill kits. 1 spill kit & 6ml poly bag PER vehicle.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 18, "description": "All company vehicles are clean and in good condition. DOT numbers and tags are current.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 19, "description": "All vehicles are set up for a static preventative maintenance schedule with a qualified vendor. Vendor inspection records are on file at facility. PMs are current as of the time of the period pertaining to this HRA.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 20, "description": "Vehicle pre-trip and post-trip inspections are being performed daily. Proof of inspections can be provided at time of request either digitally or by hard copy.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 21, "description": "IF applicable, all DOT regulated fleet vehicles have current annual periodic inspections at time of this HRA completion. Quarterly CVSA inspections are highly recommended.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 3,
            "name": "Facility",
            "items": [
                {"id": 22, "description": "Parking lot is well maintained with adequate lighting. Every exit is illuminated.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 23, "description": "NFPA signs are posted at warehouse entrances where required by local municipalities.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 24, "description": "Containers are stored, stacked, blocked, and limited in height so they are stable and secure.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 25, "description": "Fixed jacks are placed under the nose of trailers when not attached to a tractor.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 26, "description": "Absorptive floor mats in good repair and available at entryways reducing the risk of trip and slip hazards. \"Caution: Wet Floor.\" signage is available where needed.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 27, "description": "Restrooms are maintained as clean and sanitary.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 28, "description": "\"Employees Must Wash Hands\" signage is posted in restrooms.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 29, "description": "Break Area is maintained as clean and sanitary.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 30, "description": "\"Employees Must Wash Hands Before Eating or Drinking\" signage is posted in the break area.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 31, "description": "Individual lockers and changing area is kept clean and orderly. Soiled uniforms are segregated and placed in the appropriate bin.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 32, "description": "Hallways and warehouse aisle widths are maintained in good condition and free of obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 33, "description": "Aisles and passageways are properly illuminated.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 34, "description": "There is safe clearance for equipment through aisles and doorways.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 35, "description": "\"NO Food or Drink\" signs are displayed in the warehouse. Employees are given opportunities to hydrate in a hygienic space.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 36, "description": "All pallets in use are in good repair; damaged pallets are segregated and identified until repaired/ removed.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 37, "description": "\"NO SMOKING\" signs are displayed throughout the warehouse and around the charging area.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 38, "description": "Any bowed beams, bent beams, and/or damaged legs are identified for repair and the section is not used for storage.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 39, "description": "IF storing, handling or transporting Class 9 Lithium chemistry, OSHA required Right to Know Hazard Training has been executed and recorded for ALL personnel at the location.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 40, "description": "Used lithium and spent automotive fluids are not accumulating. Items are moved out of the facility promptly. Where a third party is supporting with disposal and reclamation, a copy of the uniform hazardous waste manifest must be obtained and maintained at the branch.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 41, "description": "All overhead doors, mechanical dock levelers, and automotive post lifts are inspected and serviced annually. Records are retained for four years.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 42, "description": "Trucks and trailers are secured from movement during loading and unloading operations by wheel chocks or a dock lock.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 43, "description": "Flammable products are stored away from potential ignition sources, preferably in a flammable safety cabinet.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 44, "description": "Combustible rags/ scrap are stored in an approved metal container; combustible waste is discarded promptly.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 45, "description": "Battery charging installations are located in areas designated for that purpose.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 46, "description": "Precautions are taken to prevent open flames, sparks, or electric arcs in the charging area (i.e. welding, short circuits, exposed wiring, damaged battery chargers, etc.).", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 47, "description": "Spill kits are readily available to contain and neutralize PH of battery acid. Number of spill kits in warehouse dependent on volume.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 48, "description": "Fall hazards over 30\" including stairs, second levels and open dock doors must be protected by 2 chains or other means designed to withstand a lateral force of 200 lbs; one at least 42” high and the other at mid-height.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 49, "description": "Housekeeping is acceptable throughout the facility.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 50, "description": "All visitors are made aware of the CBS Visitor Safety Policy. Signage displayed denoting restricted areas within the facility. Visitors granted access to restricted areas, document their presence in the Visitor Log Book and are escorted at all times.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 4,
            "name": "Electrical",
            "items": [
                {"id": 51, "description": "The purpose of each circuit breaker is clearly identified in all breaker panels. (Breakers must be numbered and correspond to the panel detail sheet.)", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 52, "description": "The purpose of each electrical disconnect switch (throw switch) is clearly identified.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 53, "description": "Circuit breaker panels and disconnect switches are free of obstructions. 36\" clear space in front and 18\" clear space to the sides.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 54, "description": "Main circuit breaker panels are protected from damage by forklifts and other equipment by barrier protection or other means.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 55, "description": "All electrical boxes are properly covered; no open knockouts or breaker panels.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 56, "description": "All electrical cords are in good condition. Insulation around wires are intact. Damaged cords are tagged out of service until they can be professionally repaired or until the cord is discarded.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 57, "description": "Electrical outlets are not overloaded. GFCI outlets are installed near faucets.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 5,
            "name": "Emergency Action Plan",
            "items": [
                {"id": 58, "description": "Fire extinguishers are properly mounted, marked with identifying signs, fully charged, and a clear of any obstructions.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 59, "description": "A current tag is attached to each fire extinguisher indicating that an annual maintenance check has been performed.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 60, "description": "Portable fire extinguishers and/or hoses are visually inspected monthly. (Tag must be initialed and dated.)", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 61, "description": "The fire alarm and sprinkler system are tested annually.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 62, "description": "Static inspection program is in place supporting preventative maintenance requirements for Compartmental Fire Rated Emergency doors. Annual drop test is performed by responsible party.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 63, "description": "Emergency eye wash facilities are within the immediate work area where employees are exposed to injurious corrosive materials (within 10 seconds distance unobstructed). Shower facilities required for any rebuilding locations or heavy charging locations.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 64, "description": "Emergency eye wash stations and shower facilities are inspected weekly. (Tag initialed and dated; wiped down.) (Dates checked on eye wash solution and hard plumbed eye wash/shower activated.)", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 65, "description": "Exit doors have a simple opening mechanism such as a panic bar or lever handle that unlocks from the inside. (Deadbolts or keyed locks do not meet this standard.) (Doors cannot be chained or locked from the inside in any manner.)", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 66, "description": "All emergency exits are unobstructed and the minimum width of any way of exit access at least 28 inches.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 67, "description": "All exits discharge directly to the street, or to a yard, court, or other open space that gives safe access to a public way.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 68, "description": "Any door, passage, or stairway that could be mistaken for an exit or a way of exit access, is identified by a sign reading “Not an Exit” or similar designation, or identified by a sign indicating its actual character, such as “To Basement,” “Storeroom,” “Linen Closet,” or the like.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 69, "description": "Exits are marked by an internally lighted and readily visible \"EXIT\" sign and emergency lighting is provided to light the pathway to the EXIT.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 70, "description": "A functional test is conducted monthly on each emergency light for a duration of 30 seconds and is documented.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 71, "description": "First-aid kits are easily accessible to each work area, with necessary supplies available and are periodically inspected and replenished as needed.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 72, "description": "There are at least two employees trained and certified to render first aid/CPR for each facility. One trained team member must remain on-site during facility operations at all times.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 73, "description": "The facility is equipped with a Grab & Go Kit and all employees are aware of its location.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 74, "description": "An Emergency Exit site map is current and displayed in common areas throughout the facility.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 75, "description": "Supporting emergency preparedness, the facility Emergency Response Team is recording and executing emergency planning mock drills quarterly.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 6,
            "name": "Material Handling Equipment",
            "items": [
                {"id": 76, "description": "All forklifts are equipped with seat belts and they are worn by all employees when operating the equipment.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 77, "description": "Powered MHE (forktrucks/lifttrucks, motorized pallet jacks, personnel lifts) operators have completed required knowledge training through our company LMS prior to equipment operation. Certification is issued after scoring 100% on the LMS course and passing the company practical evaluation. Operator cards must be on their person or posted in the facility after certification.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 78, "description": "Stunt driving and horseplay are prohibited.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 79, "description": "A safety cage with a rated tie off point, full body harness, and retractable lanyard are available IF the location is utilizing the forklift to elevate personnel to overhead areas.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 80, "description": "Forklifts and other powered industrial trucks are inspected prior to use at least daily (required before each shift if multiple shifts) using an inspection form.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 81, "description": "All material handling equipment is set up for a static preventative maintenance schedule with a qualified vendor. Vendor inspection records are on file at facility. PMs are current as of the time of the period pertaining to this HRA.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 82, "description": "When a powered industrial truck is left unattended (defined as an operator more than 25ft. away or not within clear view), forks are fully lowered, controls neutralized, power shut off, and brakes set. Wheels blocked if the truck is parked on an incline.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 83, "description": "Forklifts are clearly marked with and only loads within the rated capacity of the truck are handled. Data plate is legible.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 84, "description": "Signage posted within facility alerting operators approaching intersections and awareness of pedestrian pathways. Drivers slow down and sound the horn at cross aisles and other locations where vision is obstructed. If the load being carried obstructs forward view, the driver travels with the load trailing.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 85, "description": "Drivers look in the direction of, and keep a clear view of the path of travel.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 86, "description": "All grades are ascended or descended slowly; and grades in excess of 10 percent are driven with the load upgrade.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 87, "description": "All ladders in use are in good repair. Damaged ladders are segregated and labeled as \"Out of Service\" until repaired/ removed. Portable ladders are properly stored when not in use.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 88, "description": "Personnel using powered equipment (Pallet wrappers, powered conveyor, powered tools), have completed manufacturer specific operator training recorded at the branch level.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 89, "description": "All personnel have been trained and adhere to PIT distancing requirements.\nTWO forktrucks distance: truck to floor personnel\nTHREE forktrucks distance: truck to truck\nFOUR forktrucks distance: truck to escorted visitors", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 7,
            "name": "EHS Training",
            "items": [
                {"id": 90, "description": "All team members know where to access the most current EHS Supplement to the Employee Handbook.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 91, "description": "All team members know where to access the current year's EHS Committee Minutes.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 92, "description": "The outstanding LMS training for all team members is within a period of less than two months.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 93, "description": "Stretching poster is on display and being utilized during morning meetings.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 94, "description": "All team members know where to physically access the most current SDS book.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 95, "description": "All chemicals are properly labeled and there are no unlabeled secondary containers in the facility.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 96, "description": "Employees are sample tested for knowledge on safe lifting. Team lift stickers are used. Team lifts are pre-coordinated for deliveries.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 97, "description": "All employees have been issued and are using approved safety knives for cutting cardboard, shrink wrap, or any other material.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 98, "description": "Training has been provided to familiarize employees with the general principles of fire extinguisher use and the hazards involved with incipient stage fire fighting when first hired and annually thereafter.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 99, "description": "Employees are sample tested for knowledge on incident reporting. Awareness around the requirement to call Medcor at time of occurrence and to notify their supervisor immediately.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
            ],
        },
        {
            "id": 8,
            "name": "Personal Protective Equipment (PPE)",
            "items": [
                {"id": 100, "description": "Visitors are wearing approved safety glasses at all times while inside the warehouse. Visitors who wear prescription eye glasses will be offered goggles. Visitors should not be near charging areas.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 101, "description": "All employees are wearing ASTM approved and company authorized safety boots meeting the minimum requirement of ASTM F2413-18 I/75, C/75, M/75 rating when performing work inside the warehouse or while handling batteries?. This includes truck drivers delivering batteries or picking up used batteries at a customer location.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 102, "description": "All employees are wearing the proper PPE when exposed to potential battery acid splash(goggles, face shield, aprons, gloves). Signage present near charging station displaying the requirement.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
                {"id": 103, "description": "All employees are wearing proper, clean and neat uniforms. Personnel have proper safety grooming.", "answer": "", "finding": "", "action_item": "", "responsible": "", "due_date": "", "evidence": []},
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


# ─────────────────────────────────────────────
# Helper: Build HTTP Response with CORS headers
# ─────────────────────────────────────────────
def _normalized_headers(event):
    """Normalize header keys to lowercase for case-insensitive lookup."""
    headers = event.get("headers") or {}
    return {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in headers.items()}


def require_api_key(event):
    """Validate the x-api-key header or query param. Returns None if valid, or an error response."""
    if not EXPECTED_API_KEY:
        return build_response(500, {"error": "Server API_KEY env var is not configured"})
    headers = _normalized_headers(event)
    provided = (headers.get("x-api-key") or headers.get("x_api_key") or headers.get("apikey") or "").strip()
    # Fallback: also check query string parameters (for browser URL testing)
    if not provided:
        qsp = event.get("queryStringParameters") or {}
        provided = (qsp.get("x-api-key") or qsp.get("api_key") or qsp.get("apikey") or "").strip()
    if not provided or provided != EXPECTED_API_KEY:
        return build_response(403, {"error": "Forbidden", "message": "Invalid or missing API key"})
    return None


def build_response(status_code, body):
    """Builds a standardized API Gateway response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        },
        "body": json.dumps(body, default=str),
    }


# ─────────────────────────────────────────────
# Helper: Get checklist template (DynamoDB → fallback)
# ─────────────────────────────────────────────
def get_checklist_template(company_key="default", force_refresh=False):
    """Load checklist from DynamoDB with company overlay fallback. Falls back to hardcoded."""
    if load_checklist is not None:
        template = load_checklist("hra", company_key, force_refresh=force_refresh)
        if template is not None:
            return template
    return copy.deepcopy(_FALLBACK_CHECKLIST)


# ─────────────────────────────────────────────
# Helper: Build item description lookup from checklist template
# ─────────────────────────────────────────────
def build_description_lookup(company_key="default"):
    """Creates a dict mapping item_id → description from the checklist template."""
    checklist = get_checklist_template(company_key)
    lookup = {}
    for category in checklist.get("categories", []):
        for item in category.get("items", []):
            lookup[item["id"]] = item["description"]
    return lookup


# ─────────────────────────────────────────────
# Helper: Convert Decimal types from DynamoDB
# ─────────────────────────────────────────────
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


# ─────────────────────────────────────────────
# API 1: GET /hra-inspection/checklist — Get Checklist Template
# ─────────────────────────────────────────────
def get_checklist(event):
    """
    Returns the HRA checklist template for mobile inspection.
    Disabled items are filtered out so the mobile app only sees enabled questions.
    Supports optional ?company_key= query parameter for company-specific checklists.
    """
    params = event.get("queryStringParameters") or {}
    company_key = params.get("company_key", params.get("tenant_id", "default")).strip() or "default"
    template = get_checklist_template(company_key, force_refresh=True)
    if build_mobile_checklist_response is not None and template is not None:
        template = build_mobile_checklist_response(template, "hra", company_key)
    elif filter_disabled_items is not None:
        template = filter_disabled_items(template)
    return build_response(200, template)


# ─────────────────────────────────────────────
# API 2: POST /hra-inspection — Create New HRA Inspection
# ─────────────────────────────────────────────
def create_inspection(event):
    """
    Creates a new HRA inspection record linked to an existing session.
    Session is created via POST /inspection-session (in osha-checklist Lambda).

    Expects JSON body:
    {
        "session_id": "string",
        "team": ["string"],                            // optional
        "categories": [
            {
                "id": 1,
                "name": "Recordkeeping",
                "items": [
                    { "id": 1, "answer": "Yes", "finding": "", "action_item": "", "responsible": "", "due_date": "" }
                ]
            }
        ],
        "general_results": [
            { "finding": "", "action_item": "", "responsible": "", "due_date": "" }
        ],
        "notes": "string"                              // optional (max 5000 chars)
    }
    """
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return build_response(400, {"error": "Invalid JSON in request body"})

    # Validate required fields
    session_id = body.get("session_id", "").strip()
    team = body.get("team", [])
    general_results = body.get("general_results", [])
    notes = body.get("notes", "").strip() if isinstance(body.get("notes", ""), str) else ""
    categories = body.get("categories", [])

    if not session_id:
        return build_response(400, {"error": "session_id is required"})
    if len(notes) > 5000:
        return build_response(400, {"error": "notes must be under 5000 characters"})
    if not isinstance(general_results, list):
        return build_response(400, {"error": "general_results must be a list"})
    if not categories or not isinstance(categories, list):
        return build_response(400, {"error": "categories must be a non-empty list"})

    # Look up the session to get auditor details
    session_result = sessions_table.get_item(Key={"session_id": session_id})
    session = session_result.get("Item")

    if not session:
        return build_response(404, {"error": "Session not found. Create a session first via POST /inspection-session"})

    # Validate each category and its items
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

    # Generate unique ID and timestamp
    inspection_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    # Compute final status from submitted answers
    def _hra_compute_status(cats):
        """Returns 'completed' if all items answered, 'in_progress' otherwise."""
        for cat in (cats or []):
            if not isinstance(cat, dict):
                continue
            for it in cat.get("items", []):
                if not isinstance(it, dict):
                    continue
                if str(it.get("answer", "")).strip() == "":
                    return "in_progress"
        return "completed" if cats else "in_progress"

    derived_status = _hra_compute_status(categories)

    # Build the item to save (merge session info + checklist data)
    item = {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "company_key": str(body.get("company_key", "")).strip(),
        "auditor_name": session.get("auditor_name", ""),
        "location": session.get("location", ""),
        "facility_area": session.get("facility_area", ""),
        "station": session.get("station", ""),
        "station_id": session.get("station_id", ""),
        "date_of_audit": session.get("date_of_audit", ""),
        "team": team if team else [],
        "categories": categories,
        "general_results": general_results if general_results else [],
        "notes": notes,
        "status": derived_status,
        "created_at": created_at,
        "updated_at": created_at,
    }

    # Stamp completed_at when all items were answered on first submit
    if derived_status == "completed":
        item["completed_at"] = created_at
        logger.info(f"[SUBMIT] HRA inspection {inspection_id} marked completed at {created_at}")

    # Save to DynamoDB
    table.put_item(Item=item)

    # Stamp shared session so Dashboard autosave can resolve the inspection table.
    try:
        sessions_table.update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET inspection_id = :iid, inspection_type = :itype, updated_at = :u",
            ExpressionAttributeValues={
                ":iid": inspection_id,
                ":itype": "hra",
                ":u": created_at,
            },
        )
    except Exception:
        logger.exception(
            "Failed to link session %s to inspection %s (hra)",
            session_id, inspection_id,
        )

    # Return the generated ID
    return build_response(201, {
        "inspection_id": inspection_id,
        "session_id": session_id,
        "created_at": created_at,
        "status": derived_status,
        "message": "HRA inspection created.",
    })


# ─────────────────────────────────────────────
# API 3: GET /hra-inspections — List All HRA Inspections
# ─────────────────────────────────────────────
def list_inspections(event):
    """
    Returns a summary list of all HRA inspections.
    Does NOT include the full categories array (keeps it lightweight).
    Sorted by created_at (newest first).
    """
    result = table.scan()
    items = result.get("Items", [])

    # Handle pagination if table has more than 1MB of data
    while "LastEvaluatedKey" in result:
        result = table.scan(ExclusiveStartKey=result["LastEvaluatedKey"])
        items.extend(result.get("Items", []))

    # Convert Decimal types
    items = convert_decimals(items)

    # Return summary only (remove categories to keep payload small)
    summary_list = []
    for item in items:
        summary_list.append({
            "inspection_id": item.get("inspection_id"),
            "session_id": item.get("session_id"),
            "auditor_name": item.get("auditor_name"),
            "location": item.get("location"),
            "facility_area": item.get("facility_area"),
            "station": item.get("station"),
            "date_of_audit": item.get("date_of_audit"),
            "team": item.get("team", []),
            "created_at": item.get("created_at"),
        })

    # Sort by created_at (newest first)
    summary_list.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return build_response(200, summary_list)


# ─────────────────────────────────────────────
# API 4: GET /hra-inspection/{id} — Get Full HRA Inspection
# ─────────────────────────────────────────────
def get_inspection(event):
    """
    Returns the full HRA inspection object including all categories and responses.
    Extracts inspection_id from the URL path parameters.
    """
    path_params = event.get("pathParameters", {}) or {}
    inspection_id = path_params.get("inspection_id", "")

    if not inspection_id:
        return build_response(400, {"error": "inspection_id is required in the URL path"})

    params = event.get("queryStringParameters") or {}
    company_key = str(params.get("company_key", params.get("tenant_id", ""))).strip()

    # Fetch from DynamoDB
    result = table.get_item(Key={"inspection_id": inspection_id}, ConsistentRead=True)
    item = result.get("Item")

    if not item:
        return build_response(404, {"error": "Inspection not found"})

    # Convert Decimal types
    item = convert_decimals(item)

    if company_key and company_key != "default" and sync_inspection_with_template is not None:
        synced = sync_inspection_with_template(item, "hra", company_key)
        if synced.get("categories") != item.get("categories"):
            item = synced
            item["updated_at"] = datetime.now(timezone.utc).isoformat()
            table.put_item(Item=item)

    # Enrich items with descriptions from checklist template
    description_lookup = build_description_lookup(company_key or "default")
    categories = item.get("categories", [])
    for category in categories:
        for checklist_item in category.get("items", []):
            item_id = checklist_item.get("id")
            if item_id in description_lookup:
                checklist_item["description"] = description_lookup[item_id]

    # Build ordered response so JSON keys are in a logical order
    ordered_item = {
        "inspection_id": item.get("inspection_id"),
        "session_id": item.get("session_id"),
        "auditor_name": item.get("auditor_name"),
        "location": item.get("location"),
        "facility_area": item.get("facility_area"),
        "station": item.get("station"),
        "date_of_audit": item.get("date_of_audit"),
        "team": item.get("team", []),
        "categories": categories,
        "general_results": item.get("general_results", []),
        "notes": item.get("notes", ""),
        "created_at": item.get("created_at"),
    }

    return build_response(200, ordered_item)


# ─────────────────────────────────────────────
# API 5: DELETE /hra-inspection/{id} — Delete HRA Inspection
# ─────────────────────────────────────────────
def delete_inspection(event):
    """
    Deletes an HRA inspection record by inspection_id.
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
        print(f"Error deleting inspection: {str(e)}")
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
            print(f"Warning: Failed to delete linked session {session_id}: {str(e)}")

    return build_response(200, {
        "message": "Inspection deleted successfully",
        "inspection_id": inspection_id,
        "session_id": session_id,
        "session_deleted": deleted_session,
    })


# ─────────────────────────────────────────────
# Main Handler — Routes to correct function
# ─────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Main entry point. Routes the request based on HTTP method and path.

    Routes:
        GET  /hra-inspection/checklist          → get_checklist
        POST /hra-inspection                    → create_inspection
        GET  /hra-inspections                   → list_inspections
        GET  /hra-inspection/{inspection_id}    → get_inspection
        DELETE /hra-inspection/{inspection_id}  → delete_inspection
        OPTIONS (any)                           → CORS preflight
    """
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

    http_method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method", "")
    resource = event.get("resource") or event.get("routeKey", "")
    path = event.get("path") or event.get("rawPath", "")

    print(f"Received: {http_method} {resource} (path: {path})")  # CloudWatch logging

    # CORS preflight
    if http_method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # API Key validation
    auth_error = require_api_key(event)
    if auth_error:
        return auth_error

    # Route to the correct handler
    if http_method == "GET" and resource == "/hra-inspection/checklist":
        return get_checklist(event)

    elif http_method == "POST" and resource == "/hra-inspection":
        return create_inspection(event)

    elif http_method == "GET" and resource == "/hra-inspections":
        return list_inspections(event)

    elif http_method == "GET" and resource == "/hra-inspection/{inspection_id}":
        return get_inspection(event)

    elif http_method == "DELETE" and resource == "/hra-inspection/{inspection_id}":
        return delete_inspection(event)

    else:
        return build_response(404, {"error": f"Route not found: {http_method} {resource}"})
