from dataclasses import dataclass

from pyspark.sql.types import StringType, StructField, StructType

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
    CapacityTrackerNonResColumns as CTNR,
)
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)


@dataclass
class IngestASCWDSData:
    raise_mainjrid_error_when_mainjrid_not_in_df_schema = StructType(
        [
            StructField(AWK.establishment_id, StringType(), False),
            StructField(AWK.location_id, StringType(), True),
        ]
    )
    raise_mainjrid_error_when_mainjrid_in_df_schema = StructType(
        [
            *raise_mainjrid_error_when_mainjrid_not_in_df_schema,
            StructField(AWK.main_job_role_id, StringType(), True),
        ]
    )

    fix_nmdssc_dates_schema = StructType(
        [
            StructField(AWK.establishment_id, StringType(), False),
            StructField(AWK.created_date, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.updated_date, StringType(), True),
        ]
    )

    fix_nmdssc_dates_with_last_logged_in_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), False),
            StructField(AWP.master_update_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWP.last_logged_in, StringType(), True),
        ]
    )


@dataclass
class CapacityTrackerCareHomeSchema:
    sample_schema = StructType(
        [
            StructField(CTCH.local_authority, StringType(), True),
            StructField(CTCH.location, StringType(), True),
            StructField(CTCH.parent_organisation, StringType(), True),
            StructField(CTCH.lrf, StringType(), True),
            StructField(CTCH.localauthority, StringType(), True),
            StructField(CTCH.region, StringType(), True),
            StructField(CTCH.icb, StringType(), True),
            StructField(CTCH.sub_icb, StringType(), True),
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.ods_code, StringType(), True),
            StructField(CTCH.covid_residents_total, StringType(), True),
            StructField(CTCH.is_accepting_admissions, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.nurses_absent_general, StringType(), True),
            StructField(CTCH.nurses_absent_covid, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.care_workers_absent_general, StringType(), True),
            StructField(CTCH.care_workers_absent_covid, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_absent_general, StringType(), True),
            StructField(CTCH.non_care_workers_absent_covid, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
            StructField(CTCH.hours_paid, StringType(), True),
            StructField(CTCH.hours_overtime, StringType(), True),
            StructField(CTCH.hours_agency, StringType(), True),
            StructField(CTCH.hours_absence, StringType(), True),
            StructField(CTCH.days_absence, StringType(), True),
            StructField(CTCH.last_updated_utc, StringType(), True),
            StructField(CTCH.last_updated_bst, StringType(), True),
        ]
    )


@dataclass
class CapacityTrackerNonResSchema:
    sample_schema = StructType(
        [
            StructField(CTNR.local_authority, StringType(), True),
            StructField(CTNR.sub_icb_name, StringType(), True),
            StructField(CTNR.icb_name, StringType(), True),
            StructField(CTNR.region_name, StringType(), True),
            StructField(CTNR.la_name, StringType(), True),
            StructField(CTNR.lrf_name, StringType(), True),
            StructField(CTNR.la_region_name, StringType(), True),
            StructField(CTNR.location, StringType(), True),
            StructField(CTNR.cqc_id, StringType(), True),
            StructField(CTNR.ods_code, StringType(), True),
            StructField(CTNR.cqc_survey_last_updated_utc, StringType(), True),
            StructField(CTNR.cqc_survey_last_updated_bst, StringType(), True),
            StructField(CTNR.service_user_count, StringType(), True),
            StructField(CTNR.legacy_covid_confirmed, StringType(), True),
            StructField(CTNR.legacy_covid_suspected, StringType(), True),
            StructField(CTNR.cqc_care_workers_employed, StringType(), True),
            StructField(CTNR.cqc_care_workers_absent, StringType(), True),
            StructField(CTNR.can_provider_more_hours, StringType(), True),
            StructField(CTNR.extra_hours_count, StringType(), True),
            StructField(CTNR.covid_vaccination_full_course, StringType(), True),
            StructField(CTNR.covid_vaccination_autumn_23, StringType(), True),
            StructField(CTNR.flu_vaccination_autumn_23, StringType(), True),
            StructField(CTNR.confirmed_save, StringType(), True),
            StructField(CTNR.hours_paid_dom_care, StringType(), True),
            StructField(CTNR.hours_overtime_dom_care, StringType(), True),
            StructField(CTNR.hours_agency_dom_care, StringType(), True),
            StructField(CTNR.hours_absence_dom_care, StringType(), True),
            StructField(CTNR.daysa_bsence_dom_care, StringType(), True),
            StructField(CTNR.users_nhs_la, StringType(), True),
            StructField(CTNR.users_self_funded, StringType(), True),
            StructField(CTNR.returned_poc_percent, StringType(), True),
        ]
    )

    remove_invalid_characters_from_column_names_schema = StructType(
        [
            StructField(CTNR.cqc_id, StringType(), True),
            StructField("column with spaces", StringType(), True),
            StructField("column_without_spaces", StringType(), True),
            StructField("column_with_brackets()", StringType(), True),
            StructField("column_with_brackets and spaces()", StringType(), True),
        ]
    )


@dataclass
class ONSSchemas:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )
