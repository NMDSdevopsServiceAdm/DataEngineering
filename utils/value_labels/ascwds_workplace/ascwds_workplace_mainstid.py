from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceValueLabelsMainstid:
    """The possible values of the mainstid column in ascwds workplace data"""

    column_name: str = AWP.main_service_id

    labels_dict = {
        "1": "Care home services with nursing - CHN",
        "2": "Care home services without nursing - CHS",
        "3": "Adult placement home",
        "4": "Sheltered housing",
        "5": "Other adult residential care service",
        "6": "Day care and day services",
        "7": "Other adult day care services",
        "8": "Domiciliary care services (Adults) - DCC",
        "9": "Home nursing care for a person aged 18 or over",
        "10": "Domestic services and home help",
        "11": "Meals on wheels",
        "12": "Other adult domiciliary care service",
        "13": "Carers support",
        "14": "Short breaks / respite care",
        "15": "Community support and outreach",
        "16": "Social work and care management",
        "17": "Shared lives - SHL",
        "18": "Disability adaptations / assistive technology services",
        "19": "Occupational / employment-related services",
        "20": "Information and advice services",
        "21": "Other adult community care service",
        "22": "Care home / hostel",
        "23": "Family centre (residential)",
        "25": "Other childrens residential care service",
        "26": "Full day care, e.g. day nursery",
        "27": "Sessional day care e.g. play group / preschool",
        "29": "Holiday club",
        "32": "Other childrens day care services",
        "33": "Domiciliary care services (Childrens) - DCC",
        "34": "Fostering or adoption service / agency",
        "37": "Social work and care management",
        "38": "Family support",
        "41": "Other childrens community care service",
        "42": "NHS Primary Care Trust",
        "43": "Social Care NHS Trust",
        "48": "Independent acute or mental health hospital",
        "51": "Other independent healthcare setting",
        "52": "Any other Services",
        "53": "Sheltered housing",
        "54": "Extra care housing services - EXC",
        "55": "Supported living services - SLS",
        "56": "Childrens homes",
        "60": "Specialist college services - SPC",
        "61": "Community based services for people with a learning disability - LDC",
        "62": "Community based services for people with mental health needs - MHC",
        "63": "Community based services for people who misuse substances - SMC",
        "64": "Community healthcare services - CHC",
        "65": "Acute services - ACS",
        "66": "Hospice services - HPS",
        "67": "Long term conditions services - LTC",
        "68": "Hospital services for people with mental health needs, learning disabilities and/or problems with substance misuse - MLS",
        "69": "Rehabilitation services - RHS",
        "70": "Residential substance misuse treatment/rehabilitation services - RSM",
        "71": "Other healthcare service",
        "72": "Head office services",
        "73": "Live-in Care (can only be used as Other Service) - CQC Regulated",
        "74": "Nurses Agency",
        "75": "Any childrens/young peoples service",
    }
