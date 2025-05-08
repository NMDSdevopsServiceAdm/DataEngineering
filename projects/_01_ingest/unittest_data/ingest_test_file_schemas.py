from dataclasses import dataclass

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

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
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


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
class ASCWDSWorkplaceSchemas:
    workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.total_staff, StringType(), True),
            StructField(AWP.worker_records, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
            StructField(AWP.is_parent, StringType(), True),
            StructField(AWP.parent_id, StringType(), True),
            StructField(AWP.last_logged_in, StringType(), True),
            StructField(AWP.nmds_id, StringType(), True),
        ]
    )

    filter_test_account_when_orgid_present_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
        ]
    )
    filter_test_account_when_orgid_not_present_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
        ]
    )

    remove_white_space_from_nmdsid_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.nmds_id, StringType(), True),
        ]
    )

    location_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
        ]
    )

    mupddate_for_org_schema = StructType(
        [
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
        ]
    )
    expected_mupddate_for_org_schema = StructType(
        [
            *mupddate_for_org_schema,
            StructField(AWPClean.master_update_date_org, DateType(), True),
        ]
    )

    add_purge_data_col_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.is_parent, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
            StructField(AWPClean.master_update_date_org, DateType(), True),
        ]
    )
    expected_add_purge_data_col_schema = StructType(
        [
            *add_purge_data_col_schema,
            StructField(AWPClean.data_last_amended_date, DateType(), True),
        ]
    )

    add_workplace_last_active_date_col_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWPClean.data_last_amended_date, DateType(), True),
            StructField(AWPClean.last_logged_in_date, DateType(), True),
        ]
    )
    expected_add_workplace_last_active_date_col_schema = StructType(
        [
            *add_workplace_last_active_date_col_schema,
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
        ]
    )

    date_col_for_purging_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
        ]
    )
    expected_date_col_for_purging_schema = StructType(
        [
            *date_col_for_purging_schema,
            StructField(AWPClean.purge_date, DateType(), True),
        ]
    )

    workplace_last_active_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), True),
            StructField("last_active", DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
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
class ONSData:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkplaceRawData:
    raw_ascwds_workplace_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkerRawData:
    raw_ascwds_worker_schema = StructType(
        [
            StructField(AWKClean.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.main_job_role_id, StringType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkplaceCleanedData:
    cleaned_ascwds_workplace_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.total_staff_bounded, IntegerType(), True),
            StructField(AWPClean.worker_records_bounded, IntegerType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkerCleanedData:
    cleaned_ascwds_worker_schema = StructType(
        [
            StructField(AWKClean.establishment_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.main_job_role_clean, StringType(), True),
            StructField(AWKClean.main_job_role_clean_labelled, StringType(), True),
        ]
    )


@dataclass
class PostcodeMatcherSchema:
    locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.postal_code, StringType(), True),
        ]
    )
    postcodes_schema = StructType(
        [
            StructField(ONS.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONS.cssr, StringType(), True),
        ]
    )

    clean_postcode_column_schema = StructType(
        [
            StructField(CQCLClean.postcode, StringType(), True),
            StructField(CQCLClean.cssr, StringType(), True),
        ]
    )
    expected_clean_postcode_column_when_col_not_dropped_schema = StructType(
        [
            StructField(CQCLClean.postcode, StringType(), True),
            StructField(CQCLClean.cssr, StringType(), True),
            StructField(CQCLClean.postcode_cleaned, StringType(), True),
        ]
    )
    expected_clean_postcode_column_when_col_is_dropped_schema = StructType(
        [
            StructField(CQCLClean.cssr, StringType(), True),
            StructField(CQCLClean.postcode_cleaned, StringType(), True),
        ]
    )
