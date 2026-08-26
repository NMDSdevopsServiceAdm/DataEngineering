from dataclasses import dataclass

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as CQCPIR


@dataclass
class ASCWDSWorkerSchemas:
    workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
        ]
    )

    worker_schema = StructType(
        [
            StructField(AWK.location_id, StringType(), True),
            StructField(AWK.establishment_id, StringType(), True),
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
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

    remap_mainjrid_codes_schema = StructType(
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


@dataclass
class IngestCapacityTrackerCareHomeSchema:
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
class IngestCapacityTrackerNonResSchema:
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
class CleanCapacityTrackerCareHomeSchema:
    capacity_tracker_care_home_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )
    remove_matching_agency_and_non_agency_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
        ]
    )
    create_new_columns_with_totals_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, IntegerType(), True),
            StructField(CTCH.care_workers_employed, IntegerType(), True),
            StructField(CTCH.non_care_workers_employed, IntegerType(), True),
            StructField(CTCH.agency_nurses_employed, IntegerType(), True),
            StructField(CTCH.agency_care_workers_employed, IntegerType(), True),
            StructField(CTCH.agency_non_care_workers_employed, IntegerType(), True),
        ]
    )
    expected_create_new_columns_with_totals_schema = StructType(
        [
            *create_new_columns_with_totals_schema,
            StructField(CTCHClean.non_agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.ct_care_home_total_employed, IntegerType(), True),
        ]
    )


@dataclass
class CleanCapacityTrackerNonResSchema:
    capacity_tracker_non_res_schema = StructType(
        [
            StructField(CTNR.cqc_id, StringType(), True),
            StructField(CTNR.cqc_care_workers_employed, StringType(), True),
            StructField(CTNR.service_user_count, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )


@dataclass
class ValidateCleanedCapacityTrackerCareHomeData:
    ct_care_home_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    cleaned_ct_care_home_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
            StructField(CTCHClean.ct_care_home_import_date, DateType(), True),
            StructField(CTCHClean.non_agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.ct_care_home_total_employed, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = ct_care_home_schema


@dataclass
class ValidateCleanedCapacityTrackerNonResData:
    ct_non_res_schema = StructType(
        [
            StructField(CTNR.cqc_id, StringType(), True),
            StructField(CTNR.cqc_care_workers_employed, StringType(), True),
            StructField(CTNR.service_user_count, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    cleaned_ct_non_res_schema = StructType(
        [
            StructField(CTNRClean.cqc_id, StringType(), True),
            StructField(CTNRClean.cqc_care_workers_employed, StringType(), True),
            StructField(CTNRClean.service_user_count, StringType(), True),
            StructField(CTNRClean.ct_non_res_import_date, DateType(), True),
        ]
    )
    calculate_expected_size_schema = ct_non_res_schema


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
class ValidatePIRCleanedData:
    cleaned_cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_people_directly_employed, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
        ]
    )
