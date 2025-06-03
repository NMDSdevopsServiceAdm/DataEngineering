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
from utils.column_names.raw_data_files.cqc_pir_columns import (
    CqcPirColumns as CQCPIR,
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
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
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
class ASCWDSWorkerSchemas:
    worker_schema = StructType(
        [
            StructField(AWK.location_id, StringType(), True),
            StructField(AWK.establishment_id, StringType(), True),
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
            StructField(AWK.year, StringType(), True),
            StructField(AWK.month, StringType(), True),
            StructField(AWK.day, StringType(), True),
        ]
    )

    create_clean_main_job_role_column_schema = StructType(
        [
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.main_job_role_id, StringType(), True),
        ]
    )
    expected_create_clean_main_job_role_column_schema = StructType(
        [
            *create_clean_main_job_role_column_schema,
            StructField(AWKClean.main_job_role_clean, StringType(), True),
            StructField(AWKClean.main_job_role_clean_labelled, StringType(), True),
        ]
    )

    replace_care_navigator_with_care_coordinator_schema = StructType(
        [
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.main_job_role_clean, StringType(), True),
        ]
    )

    impute_not_known_job_roles_schema = StructType(
        [
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.main_job_role_clean, StringType(), True),
        ]
    )

    remove_workers_with_not_known_job_role_schema = StructType(
        [
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.main_job_role_clean, StringType(), True),
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
class IngestONSData:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )


@dataclass
class ValidatePostcodeDirectoryRawData:
    raw_postcode_directory_schema = StructType(
        [
            StructField(Keys.import_date, StringType(), True),
            StructField(ONS.postcode, StringType(), True),
            StructField(ONS.cssr, StringType(), True),
            StructField(ONS.region, StringType(), True),
            StructField(ONS.rural_urban_indicator_2011, StringType(), True),
        ]
    )


@dataclass
class CleanONSData:
    full_schema = StructType(
        [
            StructField(ONS.postcode, StringType(), True),
            StructField(ONS.cssr, StringType(), True),
            StructField(ONS.region, StringType(), True),
            StructField(ONS.sub_icb, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.icb_region, StringType(), True),
            StructField(ONS.ccg, StringType(), True),
            StructField(ONS.latitude, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
            StructField(ONS.imd_score, StringType(), True),
            StructField(ONS.lower_super_output_area_2011, StringType(), True),
            StructField(ONS.middle_super_output_area_2011, StringType(), True),
            StructField(ONS.rural_urban_indicator_2011, StringType(), True),
            StructField(ONS.lower_super_output_area_2021, StringType(), True),
            StructField(ONS.middle_super_output_area_2021, StringType(), True),
            StructField(ONS.parliamentary_constituency, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_refactored_contemporary_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.contemporary_sub_icb, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
            StructField(ONSClean.contemporary_icb_region, StringType(), True),
            StructField(ONSClean.contemporary_ccg, StringType(), True),
            StructField(ONSClean.contemporary_latitude, StringType(), True),
            StructField(ONSClean.contemporary_longitude, StringType(), True),
            StructField(ONSClean.contemporary_imd_score, StringType(), True),
            StructField(ONSClean.contemporary_lsoa11, StringType(), True),
            StructField(ONSClean.contemporary_msoa11, StringType(), True),
            StructField(ONSClean.contemporary_rural_urban_ind_11, StringType(), True),
            StructField(ONSClean.contemporary_lsoa21, StringType(), True),
            StructField(ONSClean.contemporary_msoa21, StringType(), True),
            StructField(ONSClean.contemporary_constituency, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_refactored_current_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(ONSClean.current_sub_icb, StringType(), True),
            StructField(ONSClean.current_icb, StringType(), True),
            StructField(ONSClean.current_icb_region, StringType(), True),
            StructField(ONSClean.current_ccg, StringType(), True),
            StructField(ONSClean.current_latitude, StringType(), True),
            StructField(ONSClean.current_longitude, StringType(), True),
            StructField(ONSClean.current_imd_score, StringType(), True),
            StructField(ONSClean.current_lsoa11, StringType(), True),
            StructField(ONSClean.current_msoa11, StringType(), True),
            StructField(ONSClean.current_rural_urban_ind_11, StringType(), True),
            StructField(ONSClean.current_lsoa21, StringType(), True),
            StructField(ONSClean.current_msoa21, StringType(), True),
            StructField(ONSClean.current_constituency, StringType(), True),
        ]
    )


@dataclass
class ValidatePostcodeDirectoryCleanedData:
    raw_postcode_directory_schema = StructType(
        [
            StructField(ONS.import_date, StringType(), True),
            StructField(ONS.postcode, StringType(), True),
        ]
    )
    cleaned_postcode_directory_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(ONSClean.current_rural_urban_ind_11, StringType(), True),
        ]
    )

    calculate_expected_size_schema = raw_postcode_directory_schema


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
class CleanCQCPIRSchema:
    sample_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), False),
            StructField(CQCPIR.location_name, StringType(), False),
            StructField(CQCPIR.pir_type, StringType(), False),
            StructField(CQCPIR.pir_submission_date, StringType(), False),
            StructField(CQCPIR.pir_people_directly_employed, IntegerType(), True),
            StructField(CQCPIR.staff_leavers, IntegerType(), True),
            StructField(CQCPIR.staff_vacancies, IntegerType(), True),
            StructField(CQCPIR.shared_lives_leavers, IntegerType(), True),
            StructField(CQCPIR.shared_lives_vacancies, IntegerType(), True),
            StructField(CQCPIR.primary_inspection_category, StringType(), False),
            StructField(CQCPIR.region, StringType(), False),
            StructField(CQCPIR.local_authority, StringType(), False),
            StructField(CQCPIR.number_of_beds, IntegerType(), False),
            StructField(CQCPIR.domiciliary_care, StringType(), True),
            StructField(CQCPIR.location_status, StringType(), False),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    add_care_home_column_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(CQCPIR.pir_type, StringType(), True),
        ]
    )
    expected_care_home_column_schema = StructType(
        [
            *add_care_home_column_schema,
            StructField(CQCPIRClean.care_home, StringType(), True),
        ]
    )

    remove_rows_missing_pir_people_directly_employed_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(CQCPIR.pir_people_directly_employed, IntegerType(), True),
        ]
    )

    remove_unused_pir_types_schema = add_care_home_column_schema

    filter_latest_submission_date_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_submission_date_as_date, DateType(), True),
        ]
    )


@dataclass
class NullPeopleDirectlyEmployedSchema:
    null_people_directly_employed_outliers_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIR.pir_people_directly_employed, IntegerType(), True),
        ]
    )
    expected_null_people_directly_employed_outliers_schema = StructType(
        [
            *null_people_directly_employed_outliers_schema,
            StructField(
                CQCPIRClean.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
        ]
    )

    null_large_single_submission_locations_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(
                CQCPIRClean.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
        ]
    )


@dataclass
class ValidatePIRRawData:
    raw_cqc_pir_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCPIR.pir_people_directly_employed, StringType(), True),
        ]
    )


@dataclass
class ValidatePIRCleanedData:
    cleaned_cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_people_directly_employed, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
        ]
    )


@dataclass
class PostcodeMatcherSchema:
    locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), False),
            StructField(CQCLClean.cqc_location_import_date, DateType(), False),
            StructField(CQCL.postal_code, StringType(), False),
        ]
    )
    postcodes_schema = StructType(
        [
            StructField(ONS.postcode, StringType(), False),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), False),
            StructField(ONS.cssr, StringType(), False),
        ]
    )

    clean_postcode_column_schema = StructType(
        [
            StructField(CQCL.postal_code, StringType(), False),
            StructField(CQCLClean.cssr, StringType(), False),
        ]
    )
    expected_clean_postcode_column_when_col_not_dropped_schema = StructType(
        [
            *clean_postcode_column_schema,
            StructField(CQCLClean.postcode_cleaned, StringType(), False),
        ]
    )
    expected_clean_postcode_column_when_col_is_dropped_schema = StructType(
        [
            StructField(CQCLClean.cssr, StringType(), False),
            StructField(CQCLClean.postcode_cleaned, StringType(), False),
        ]
    )

    join_postcode_data_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), False),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), False),
            StructField(CQCLClean.postcode_cleaned, StringType(), False),
        ]
    )
    join_postcode_data_postcodes_schema = StructType(
        [
            StructField(CQCLClean.postcode_cleaned, StringType(), False),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), False),
            StructField(ONSClean.contemporary_cssr, StringType(), False),
        ]
    )
    expected_join_postcode_data_matched_schema = StructType(
        [
            *join_postcode_data_locations_schema,
            StructField(ONSClean.contemporary_cssr, StringType(), False),
        ]
    )
    expected_join_postcode_data_unmatched_schema = join_postcode_data_locations_schema


@dataclass
class ValidateLocationsAPIRawData:
    raw_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.type, StringType(), True),
        ]
    )


@dataclass
class ValidateProvidersAPIRawData:
    raw_cqc_providers_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCPClean.name, StringType(), True),
        ]
    )
