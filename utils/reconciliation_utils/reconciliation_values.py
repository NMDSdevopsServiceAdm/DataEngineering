from dataclasses import dataclass


@dataclass
class ReconciliationColumns:
    parent_sub_or_single: str = "parent_sub_or_single"
    ownership: str = "ownership"
    potentials: str = "potentials"
    ever_existed: str = "ever_existed"
    subject: str = "Subject"
    nmds: str = "nmdsid AS NMDS"
    name: str = "Name"
    description: str = "Description"
    requester_name: str = "`Requester Name`"
    requester_name_2: str = "`Requester Name 2`"
    sector: str = "Sector"
    status: str = "Status"
    technician: str = "Technician"
    sfc_region: str = "`SfC Region`"
    manual_call_log: str = "`Manual Call Log`"
    mode: str = "Mode"
    priority: str = "Priority"
    category: str = "Category"
    sub_category: str = "Subcategory"
    is_requester_named: str = "`Is requester named user on account?`"
    security_question: str = "`Correct answer to security question received`"
    website: str = "Website"
    item: str = "Item"
    phone: str = "Phone"
    workplace_id: str = "nmdsid AS `Workplace Id`"


@dataclass
class ReconciliationValues:
    parent: str = "parent"
    subsidiary: str = "subsidiary"
    single: str = "single"
    workplace: str = "workplace"
    singles_and_subs: str = "singles_and_subs"
    parents: str = "parents"


@dataclass
class ReconciliationDict:
    establishment_type_dict = {
        "1": "Local authority (adult services)",
        "3": "Local authority (generic/other)",
        "6": "Private sector",
        "7": "Voluntary/Charity",
        "8": "Other",
    }

    region_id_dict = {
        "4": "B - North East",
        "2": "C - East Midlands",
        "7": "D - South West",
        "8": "E - West Midlands",
        "5": "F - North West",
        "3": "G - London",
        "6": "H - South East",
        "1": "I - Eastern",
        "9": "J - Yorkshire Humber",
    }
