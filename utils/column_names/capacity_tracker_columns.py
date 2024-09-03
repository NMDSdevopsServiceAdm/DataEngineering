from dataclasses import dataclass


@dataclass
class CapacityTrackerCareHomeColumns:
    admission_status: str = "admissionstatus"
    agency_care_workers_employed: str = "agencycareworkersemployed"
    agency_non_care_workers_employed: str = "agencynoncareworkersemployed"
    agency_nurses_employed: str = "agencynursesemployed"
    care_workers_absent_covid: str = "careworkersabsentcovid"
    care_workers_absent_general: str = "careworkersabsentgeneral"
    care_workers_employed: str = "careworkersemployed"
    ccg: str = "ccg"
    covid19_count: str = "covid19count"
    covid_residents_total: str = "covidresidentstotal"
    cqc_id: str = "cqcid"
    days_absence: str = "daysabsence"
    declared_vacancies: str = "declaredvacancies"
    hours_absence: str = "hoursabsence"
    hours_agency: str = "hoursagency"
    hours_overtime: str = "hoursovertime"
    hours_paid: str = "hourspaid"
    icb: str = "icb"
    is_accepting_admissions: str = "isacceptingadmissions"
    last_updated_bst: str = "lastupdatedbst"
    last_updated_utc: str = "lastupdatedutc"
    local_authority: str = "local_authority"
    localauthority: str = "localauthority"
    location: str = "location"
    lrf: str = "lrf"
    max_capacity: str = "maxcapacity"
    non_care_workers_absent_covid: str = "noncareworkersabsentcovid"
    non_care_workers_absent_general: str = "noncareworkersabsentgeneral"
    non_care_workers_employed: str = "noncareworkersemployed"
    nurses_absent_covid: str = "nursesabsentcovid"
    nurses_absent_general: str = "nursesabsentgeneral"
    nurses_employed: str = "nursesemployed"
    ods_code: str = "odscode"
    overall_status: str = "overallstatus"
    parent_organisation: str = "parentorganisation"
    region: str = "region"
    stp: str = "stp"
    sub_icb: str = "subicb"
    used_beds: str = "usedbeds"
    workforce_status: str = "workforcestatus"


@dataclass
class CapacityTrackerCareHomeCleanColumns:
    capacity_tracker_import_date: str = "capacity_tracker_import_date"
