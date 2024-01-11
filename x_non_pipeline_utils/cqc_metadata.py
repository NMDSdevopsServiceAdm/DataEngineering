from dataclasses import dataclass
from pathlib import Path


@dataclass
class CqcCategories:
    services_dict = {
        "Service type - Shared Lives": "CQC Shared Lives",
        "Service type - Care home service with nursing": "CQC Care home with nursing",
        "Service type - Care home service without nursing": "CQC Care only home",
        "Service type - Domiciliary care service": "CQC Non residential",
        "Service type - Community health care services - Nurses Agency only": "CQC Non residential",
        "Service type - Supported living service": "CQC Non residential",
        "Service type - Extra Care housing services": "CQC Non residential",
        "Service type - Residential substance misuse treatment and/or rehabilitation service": "CQC Other Residential",
        "Service type - Hospice services": "CQC Other Residential",
        "Service type - Acute services with overnight beds": "CQC Other Residential",
        "Service type - Rehabilitation services": "CQC Other non-res",
        "Service type - Community based services for people with a learning disability": "CQC Other non-res",
        "Service type - Community based services for people who misuse substances": "CQC Other non-res",
        "Service type - Community healthcare service": "CQC Other non-res",
        "Service type - Diagnostic and/or screening service": "CQC Other non-res",
        "Service type - Community based services for people with mental health needs": "CQC Other non-res",
        "Service type - Long term conditions services": "CQC Other non-res",
        "Service type - Hospital services for people with mental health needs, learning disabilities and problems with substance misuse": "CQC Other non-res",
        "Service type - Doctors consultation service": "CQC Other non-res",
        "Service type - Doctors treatment service": "CQC Other non-res",
        "Service type - Dental service": "CQC Other non-res",
        "Service type - Urgent care services": "CQC Other non-res",
        "Service type - Mobile doctors service": "CQC Other non-res",
        "Service type - Remote clinical advice service": "CQC Other non-res",
        "Service type - Acute services without overnight beds / listed acute services with or without overnight beds": "CQC Other non-res",
        "Service type - Ambulance service": "CQC Other non-res",
        "Service type - Blood and Transplant service": "CQC Other non-res",
        "Service type - Diagnostic and/or screening service - single handed sessional providers": "CQC Other non-res",
        "Service type - Hospice services at home": "CQC Other non-res",
        "Service type - Hyperbaric Chamber": "CQC Other non-res",
        "Service type - Prison Healthcare Services": "CQC Other non-res",
        "Service type - Specialist college service": "Exclude",
    }
    la_keywords = "Department of Community Services|Social & Community Services|SCC Adult Social Care|Cheshire West and Chester Reablement Service|Council|Social Services|MBC|MDC|London Borough|Royal Borough|Borough of"
    non_la_keywords = "The Council of St Monica Trust"


@dataclass
class CqcConfig:
    directory = Path(
        "F:/ASC-WDS Copy Files/Research & Analysis Team Folders/Analysis Team/b. Data Sources/02. Data sets and databases/CQC Registered Providers lists/"
    )
    old_file_name = Path("2024/01. CQC 050124.xlsx")
    new_file_name = Path("2024/01. CQC 050124 inc sector and service.xlsx")
    sheet_name = "HSCA_Active_Locations"
    blank_rows = 6  # CQC data file starts on row 7
    columns_to_export = ["Location ID", "Sector", "Main Service"]
    new_sheet_name = "CQC sector service"


@dataclass
class ColumnNames:
    sector = "sector"
    provider_name = "Provider Name"
    location_type = "Location Type/Sector"
    main_service = "Main Service"
    main_service_group = "Main Service Group"


@dataclass
class ColumnValues:
    local_authority = "Local authority"
    independent = "Independent"
    yes = "Y"
    service_not_found = "_not found_"
    social_care_org = "Social Care Org"
