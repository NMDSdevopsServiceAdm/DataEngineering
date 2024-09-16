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
    can_provider_more_hours: str = "canprovidermorehours"
    confirmed_save: str = "confirmedsave"
    covid_vaccination_autumn_23: str = "covid_vaccination_autumn_23"
    covid_vaccination_full_course: str = "covid_vaccination_full_course"
    cqc_care_workers_absent: str = "cqccareworkersabsent"
    cqc_care_workers_employed: str = "cqccareworkersemployed"
    cqc_id: str = "cqcid"
    cqc_survey_last_updated_bst: str = "cqcsurveylastupdatedbst"
    cqc_survey_last_updated_utc: str = "cqcsurveylastupdatedutc"
    days_absence_dom_care: str = "daysabsencedomcare"
    extra_hours_count: str = "extrahourscount"
    flu_vaccination_autumn_23: str = "flu_vaccination_autumn_23"
    hours_absence_dom_care: str = "hoursabsencedomcare"
    hours_agency_dom_care: str = "hoursagencydomcare"
    hours_overtime_dom_care: str = "hoursovertimedomcare"
    hours_paid_dom_care: str = "hourspaiddomcare"
    icb_name: str = "icbname"
    la_name: str = "laname"
    la_region_name: str = "laregionname"
    legacy_covid_confirmed: str = "legacycovidconfirmed"
    legacy_covid_suspected: str = "legacycovidsuspected"
    local_authority: str = "local_authority"
    location: str = "location"
    lrf_name: str = "lrfname"
    ods_code: str = "odscode"
    region_name: str = "regionname"
    returned_poc_percent: str = "returnedpocpercent"
    service_user_count: str = "serviceusercount"
    sub_icb_name: str = "subicbname"
    users_nhs_la: str = "usersnhsla"
    users_self_funded: str = "usersselffunded"


@dataclass
class CapacityTrackerNonResCleanColumns:
    capacity_tracker_import_date: str = (
        CapacityTrackerCareHomeCleanColumns.capacity_tracker_import_date
    )
