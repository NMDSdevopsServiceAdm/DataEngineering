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
    agency_total_employed: str = "agency_total_employed"
    non_agency_total_employed: str = "non_agency_total_employed"
    agency_and_non_agency_total_employed: str = "agency_and_non_agency_total_employed"


@dataclass
class CapacityTrackerNonResColumns:
    can_provider_more_hours: str = "can_provider_more_hours"
    confirmed_save: str = "confirmed_save"
    covid_confirmed_and_suspected: str = "covid_confirmed_and_suspected"
    covid_notes: str = "covid_notes"
    covid_vaccination_full_course: str = "covid_vaccination_full_course"
    cqc_care_workers_absent: str = "cqc_care_workers_absent"
    cqc_care_workers_employed: str = "cqc_care_workers_employed"
    cqc_id: str = CapacityTrackerCareHomeColumns.cqc_id
    cqc_survey_last_updated_bst: str = "cqc_survey_last_updated_bst"
    cqc_survey_last_updated_utc: str = "cqc_survey_last_updated_utc"
    cqc_survey_last_updated_utc_formatted: str = "cqc_survey_last_updated_utc_formatted"
    days_absence_dom_care: str = "days_absence_dom_care"
    extra_hours_count: str = "extra_hours_count"
    has_cqc_ppe_issues: str = "has_cqc_ppe_issues"
    hours_absence_dom_care: str = "hours_absence_dom_care"
    hours_agency_dom_care: str = "hours_agency_dom_care"
    hours_overtime_dom_care: str = "hours_overtime_dom_care"
    hours_paid_dom_care: str = "hours_paid_dom_care"
    icb: str = CapacityTrackerCareHomeColumns.icb
    la_region_name: str = "la_region_name"
    legacy_covid_confirmed: str = "legacy_covid_confirmed"
    legacy_covid_suspected: str = "legacy_covid_suspected"
    local_authority: str = CapacityTrackerCareHomeColumns.local_authority
    localauthority: str = CapacityTrackerCareHomeColumns.localauthority
    location: str = CapacityTrackerCareHomeColumns.location
    lrf: str = CapacityTrackerCareHomeColumns.lrf
    ods_code: str = CapacityTrackerCareHomeColumns.ods_code
    region: str = CapacityTrackerCareHomeColumns.region
    return_poc_percent: str = "return_poc_percent"
    service_user_count: str = "service_user_count"
    sub_icb: str = CapacityTrackerCareHomeColumns.sub_icb
    users_nhs_la: str = "users_nhs_la"
    users_self_funded: str = "users_self_funded"


@dataclass
class CapacityTrackerNonResCleanColumns:
    capacity_tracker_import_date: str = (
        CapacityTrackerCareHomeCleanColumns.capacity_tracker_import_date
    )
