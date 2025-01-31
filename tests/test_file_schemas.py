from dataclasses import dataclass

from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    DateType,
    DoubleType,
    BooleanType,
    MapType,
)

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
    CapacityTrackerNonResColumns as CTNR,
    CapacityTrackerNonResCleanColumns as CTNRClean,
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
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import (
    CQCRatingsColumns as CQCRatings,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    ArchivePartitionKeys as ArchiveKeys,
    IndCqcColumns as IndCQC,
    PrimaryServiceRollingAverageColumns as RA_TempCol,
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
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_names.validation_table_columns import Validation
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class CalculatePaRatioSchemas:
    total_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.TOTAL_STAFF_RECODED,
                FloatType(),
                True,
            ),
        ]
    )
    average_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.AVERAGE_STAFF,
                FloatType(),
                True,
            ),
        ]
    )
    reduce_year_by_one_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField("other column", StringType(), True),
        ]
    )


@dataclass
class IngestASCWDSData:
    raise_mainjrid_error_when_mainjrid_not_in_df_schema = StructType(
        [
            StructField(AWK.establishment_id, StringType(), True),
            StructField(AWK.location_id, StringType(), True),
        ]
    )
    raise_mainjrid_error_when_mainjrid_in_df_schema = StructType(
        [
            *raise_mainjrid_error_when_mainjrid_not_in_df_schema,
            StructField(AWK.main_job_role_id, StringType(), True),
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
class ONSData:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )

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
            StructField(
                ONS.westminster_parliamentary_consitituency, StringType(), True
            ),
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
            StructField(
                ONSClean.contemporary_constituancy,
                StringType(),
                True,
            ),
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
            StructField(
                ONSClean.current_constituancy,
                StringType(),
                True,
            ),
        ]
    )


@dataclass
class PAFilledPostsByIcbAreaSchema:
    sample_ons_contemporary_with_duplicates_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
        ]
    )

    sample_ons_contemporary_schema = sample_ons_contemporary_with_duplicates_schema

    expected_postcode_count_per_la_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
        ]
    )

    expected_postcode_count_per_la_icb_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    sample_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    expected_ratio_between_hybrid_area_and_la_area_postcodes_schema = StructType(
        [
            *sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    full_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    sample_pa_filled_posts_schema = StructType(
        [
            StructField(DP.LA_AREA, StringType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    expected_create_date_column_from_year_in_pa_estimates_schema = StructType(
        [
            *sample_pa_filled_posts_schema,
            StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
        ]
    )

    sample_postcode_proportions_before_joining_pa_filled_posts_schema = (
        expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema
    )

    sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_schema = (
        StructType(
            [
                StructField(DP.LA_AREA, StringType(), True),
                StructField(
                    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
                    DoubleType(),
                    True,
                ),
                StructField(DP.YEAR, StringType(), True),
                StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
            ]
        )
    )

    expected_postcode_proportions_after_joining_pa_filled_posts_schema = StructType(
        [
            *sample_postcode_proportions_before_joining_pa_filled_posts_schema,
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    sample_proportions_and_pa_filled_posts_schema = StructType(
        [
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
        ]
    )

    expected_pa_filled_posts_after_applying_proportions_schema = StructType(
        [
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA,
                DoubleType(),
                True,
            ),
        ]
    )

    sample_la_name_schema = StructType([StructField(DP.LA_AREA, StringType(), True)])

    expected_la_names_with_correct_spelling_schema = sample_la_name_schema


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

    capacity_tracker_care_home_schema = StructType(
        [
            StructField(CTCH.cqc_id, StringType(), True),
            StructField(CTCH.nurses_employed, StringType(), True),
            StructField(CTCH.care_workers_employed, StringType(), True),
            StructField(CTCH.non_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_nurses_employed, StringType(), True),
            StructField(CTCH.agency_care_workers_employed, StringType(), True),
            StructField(CTCH.agency_non_care_workers_employed, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
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
            StructField(
                CTCHClean.agency_and_non_agency_total_employed, IntegerType(), True
            ),
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
    capacity_tracker_non_res_schema = StructType(
        [
            StructField(CTNR.cqc_id, StringType(), True),
            StructField(CTNR.cqc_care_workers_employed, StringType(), True),
            StructField(CTNR.service_user_count, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField("other column", StringType(), True),
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
class CQCLocationsSchema:
    detailed_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCL.organisation_type, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.registration_date, StringType(), True),
            StructField(CQCL.deregistration_date, StringType(), True),
            StructField(CQCL.dormancy, StringType(), True),
            StructField(CQCL.number_of_beds, IntegerType(), True),
            StructField(CQCL.website, StringType(), True),
            StructField(CQCL.postal_address_line1, StringType(), True),
            StructField(CQCL.postal_address_town_city, StringType(), True),
            StructField(CQCL.postal_address_county, StringType(), True),
            StructField(CQCL.region, StringType(), True),
            StructField(CQCL.postal_code, StringType(), True),
            StructField(CQCL.onspd_latitude, StringType(), True),
            StructField(CQCL.onspd_longitude, StringType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCL.inspection_directorate, StringType(), True),
            StructField(CQCL.main_phone_number, StringType(), True),
            StructField(CQCL.local_authority, StringType(), True),
            StructField(
                CQCL.regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.code, StringType(), True),
                            StructField(
                                CQCL.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                CQCL.person_family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_roles,
                                                ArrayType(StringType(), True),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_title, StringType(), True
                                            ),
                                        ]
                                    )
                                ),
                                True,
                            ),
                        ]
                    )
                ),
            ),
            StructField(
                CQCL.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
            StructField(
                CQCL.specialisms,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(
                CQCL.relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    impute_historic_relationships_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_impute_historic_relationships_schema = StructType(
        [
            *impute_historic_relationships_schema,
            StructField(
                CQCLClean.imputed_relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    get_relationships_where_type_is_predecessor_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCLClean.first_known_relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_get_relationships_where_type_is_predecessor_schema = StructType(
        [
            *get_relationships_where_type_is_predecessor_schema,
            StructField(
                CQCLClean.relationships_predecessors_only,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    impute_missing_gac_service_types_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(
                CQCL.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    expected_impute_missing_gac_service_types_schema = StructType(
        [
            *impute_missing_gac_service_types_schema,
            StructField(
                CQCLClean.imputed_gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )

    primary_service_type_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(
                CQCLClean.imputed_gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    expected_primary_service_type_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(
                CQCLClean.imputed_gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
            StructField(CQCLClean.primary_service_type, StringType(), True),
        ]
    )

    realign_carehome_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCLClean.primary_service_type, StringType(), True),
        ]
    )

    small_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    join_provider_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )

    expected_joined_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCLClean.provider_name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )

    invalid_postcode_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.postal_code, StringType(), True),
        ]
    )

    registration_status_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    social_care_org_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.type, StringType(), True),
        ]
    )

    locations_for_ons_join_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.provider_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    ons_postcode_directory_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
        ]
    )

    expected_ons_join_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(CQCL.postal_code, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    expected_split_registered_schema = StructType(
        [
            *expected_ons_join_schema,
        ]
    )

    expected_services_offered_schema = StructType(
        [
            *primary_service_type_schema,
            StructField(
                CQCLClean.services_offered,
                ArrayType(
                    StringType(),
                ),
            ),
        ]
    )

    clean_registration_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_date, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_clean_registration_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_date, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCLClean.imputed_registration_date, StringType(), True),
        ]
    )
    remove_late_registration_dates_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCLClean.imputed_registration_date, StringType(), True),
        ]
    )
    clean_provider_id_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    impute_missing_data_from_provider_dataset_schema = StructType(
        [
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    remove_specialist_colleges_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(
                CQCLClean.services_offered,
                ArrayType(
                    StringType(),
                ),
            ),
        ]
    )
    add_column_related_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(
                CQCLClean.imputed_relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.related_location_id, StringType(), True),
                            StructField(CQCL.related_location_name, StringType(), True),
                            StructField(CQCL.type, StringType(), True),
                            StructField(CQCL.reason, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    expected_add_column_related_location_schema = StructType(
        [
            *add_column_related_location_schema,
            StructField(CQCLClean.related_location, StringType(), True),
        ]
    )


@dataclass
class UtilsSchema:
    filter_to_max_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("date_type_column", DateType(), True),
            StructField("import_date_style_col", StringType(), True),
        ]
    )

    select_rows_with_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value_to_filter_on", StringType(), True),
        ]
    )

    select_rows_with_non_null_values_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("column_with_nulls", FloatType(), True),
        ]
    )


@dataclass
class CleaningUtilsSchemas:
    worker_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
        ]
    )

    expected_schema_with_new_columns = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
            StructField("gender_labels", StringType(), True),
            StructField("nationality_labels", StringType(), True),
        ]
    )

    scale_schema = StructType(
        [
            StructField("int", IntegerType(), True),
            StructField("float", FloatType(), True),
            StructField("non_scale", StringType(), True),
        ]
    )

    expected_scale_schema = StructType(
        [
            *scale_schema,
            StructField("bound_int", IntegerType(), True),
            StructField("bound_float", FloatType(), True),
        ]
    )

    sample_col_to_date_schema = StructType(
        [
            StructField("input_string", StringType(), True),
            StructField("expected_value", DateType(), True),
        ]
    )

    align_dates_primary_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
        ]
    )

    align_dates_secondary_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
        ]
    )

    primary_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
        ]
    )

    secondary_dates_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_aligned_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_merged_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
        ]
    )

    reduce_dataset_to_earliest_file_per_month_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.import_date, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )

    cast_to_int_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, StringType(), True),
            StructField(AWP.worker_records, StringType(), True),
        ]
    )

    cast_to_int_expected_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, IntegerType(), True),
            StructField(AWP.worker_records, IntegerType(), True),
        ]
    )

    filled_posts_per_bed_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.care_home, StringType(), True),
        ]
    )
    expected_filled_posts_per_bed_ratio_schema = StructType(
        [
            *filled_posts_per_bed_ratio_schema,
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    filled_posts_from_beds_and_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    expected_filled_posts_from_beds_and_ratio_schema = StructType(
        [
            *filled_posts_from_beds_and_ratio_schema,
            StructField(IndCQC.care_home_model, DoubleType(), True),
        ]
    )

    remove_duplicate_locationids_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
        ]
    )


@dataclass
class CQCProviderSchema:
    rows_without_cqc_sector_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField("some_data", StringType(), True),
        ]
    )
    expected_rows_with_cqc_sector_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField("some_data", StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
        ]
    )

    full_schema = StructType(
        fields=[
            StructField(CQCP.provider_id, StringType(), True),
            StructField(
                CQCP.location_ids,
                ArrayType(
                    StringType(),
                ),
            ),
            StructField(CQCP.organisation_type, StringType(), True),
            StructField(CQCP.ownership_type, StringType(), True),
            StructField(CQCP.type, StringType(), True),
            StructField(CQCP.uprn, StringType(), True),
            StructField(CQCP.name, StringType(), True),
            StructField(CQCP.registration_status, StringType(), True),
            StructField(CQCP.registration_date, StringType(), True),
            StructField(CQCP.deregistration_date, StringType(), True),
            StructField(CQCP.postal_address_line1, StringType(), True),
            StructField(CQCP.postal_address_town_city, StringType(), True),
            StructField(CQCP.postal_address_county, StringType(), True),
            StructField(CQCP.region, StringType(), True),
            StructField(CQCP.postal_code, StringType(), True),
            StructField(CQCP.onspd_latitude, FloatType(), True),
            StructField(CQCP.onspd_longitude, FloatType(), True),
            StructField(CQCP.main_phone_number, StringType(), True),
            StructField(CQCP.companies_house_number, StringType(), True),
            StructField(CQCP.inspection_directorate, StringType(), True),
            StructField(CQCP.constituency, StringType(), True),
            StructField(CQCP.local_authority, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class CQCPIRSchema:
    sample_schema = StructType(
        fields=[
            StructField(CQCPIR.location_id, StringType(), False),
            StructField(CQCPIR.location_name, StringType(), False),
            StructField(CQCPIR.pir_type, StringType(), False),
            StructField(CQCPIR.pir_submission_date, StringType(), False),
            StructField(
                CQCPIR.pir_people_directly_employed,
                IntegerType(),
                True,
            ),
            StructField(
                CQCPIR.staff_leavers,
                IntegerType(),
                True,
            ),
            StructField(CQCPIR.staff_vacancies, IntegerType(), True),
            StructField(
                CQCPIR.shared_lives_leavers,
                IntegerType(),
                True,
            ),
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


@dataclass
class CQCPIRCleanSchema:
    clean_subset_for_grouping_by = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_submission_date_as_date, DateType(), True),
        ]
    )


@dataclass
class FilterCleanedValuesSchema:
    sample_schema = StructType(
        [
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("import_date", StringType(), True),
        ]
    )


@dataclass
class MergeIndCQCData:
    clean_cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), False),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_people_directly_employed, IntegerType(), True),
        ]
    )

    clean_cqc_location_for_merge_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )

    clean_ascwds_workplace_for_merge_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    expected_cqc_and_pir_merged_schema = StructType(
        [
            *clean_cqc_location_for_merge_schema,
            StructField(CQCPIRClean.pir_people_directly_employed, IntegerType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
        ]
    )

    expected_cqc_and_ascwds_merged_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    cqc_sector_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class MergeCoverageData:
    clean_cqc_location_for_merge_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )

    clean_ascwds_workplace_for_merge_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    expected_cqc_and_ascwds_merged_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    sample_in_ascwds_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
        ]
    )

    expected_in_ascwds_schema = StructType(
        [
            *sample_in_ascwds_schema,
            StructField(CoverageColumns.in_ascwds, IntegerType(), True),
        ]
    )

    sample_cqc_locations_schema = StructType(
        [StructField(AWPClean.location_id, StringType(), True)]
    )

    sample_cqc_ratings_for_merge_schema = StructType(
        [
            StructField(AWPClean.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    expected_cqc_locations_and_latest_cqc_rating_schema = StructType(
        [
            *sample_cqc_locations_schema,
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
        ]
    )


@dataclass
class LmEngagementUtilsSchemas:
    add_columns_for_locality_manager_dashboard_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(CoverageColumns.in_ascwds, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
        ]
    )
    expected_add_columns_for_locality_manager_dashboard_schema = StructType(
        [
            *add_columns_for_locality_manager_dashboard_schema,
            StructField(CoverageColumns.la_monthly_coverage, FloatType(), True),
            StructField(CoverageColumns.coverage_monthly_change, FloatType(), True),
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_monthly, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_ytd, IntegerType(), True),
        ]
    )

    expected_calculate_la_coverage_monthly_schema = StructType(
        [
            *add_columns_for_locality_manager_dashboard_schema,
            StructField(CoverageColumns.la_monthly_coverage, FloatType(), True),
        ]
    )
    calculate_coverage_monthly_change_schema = (
        expected_calculate_la_coverage_monthly_schema
    )

    expected_calculate_coverage_monthly_change_schema = StructType(
        [
            *expected_calculate_la_coverage_monthly_schema,
            StructField(CoverageColumns.coverage_monthly_change, FloatType(), True),
        ]
    )

    calculate_locations_monthly_change_schema = (
        expected_calculate_coverage_monthly_change_schema
    )
    expected_calculate_locations_monthly_change_schema = StructType(
        [
            *expected_calculate_coverage_monthly_change_schema,
            StructField(CoverageColumns.in_ascwds_last_month, IntegerType(), True),
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
        ]
    )

    calculate_new_registrations_schema = (
        expected_calculate_locations_monthly_change_schema
    )
    expected_calculate_new_registrations_schema = StructType(
        [
            *expected_calculate_coverage_monthly_change_schema,
            StructField(CoverageColumns.locations_monthly_change, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_monthly, IntegerType(), True),
            StructField(CoverageColumns.new_registrations_ytd, IntegerType(), True),
        ]
    )


@dataclass
class IndCQCDataUtils:
    input_schema_for_adding_estimate_filled_posts_and_source = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("model_name_1", FloatType(), True),
            StructField("model_name_2", FloatType(), True),
            StructField("model_name_3", FloatType(), True),
        ]
    )

    expected_schema_with_estimate_filled_posts_and_source = StructType(
        [
            *input_schema_for_adding_estimate_filled_posts_and_source,
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    estimated_source_description_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    get_selected_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.rolling_average_model, FloatType(), True),
        ]
    )
    expected_get_selected_value_schema = StructType(
        [
            *get_selected_value_schema,
            StructField("new_column", FloatType(), True),
        ]
    )


@dataclass
class CleanIndCQCData:
    merged_schema_for_cleaning_job = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.import_date, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(CQCLClean.current_cssr, StringType(), True),
            StructField(CQCLClean.current_rural_urban_ind_11, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(CQCPIRClean.pir_people_directly_employed, IntegerType(), True),
            StructField(AWPClean.total_staff_bounded, IntegerType(), True),
            StructField(AWPClean.worker_records_bounded, IntegerType(), True),
            StructField(CQCLClean.primary_service_type, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.postcode, StringType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )

    remove_cqc_duplicates_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.postcode, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(AWPClean.total_staff_bounded, IntegerType(), True),
            StructField(AWPClean.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
        ]
    )

    repeated_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("integer_column", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_without_repeated_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("integer_column", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField("integer_column_deduplicated", IntegerType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsDifferenceInRangeSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class ReconciliationSchema:
    input_ascwds_workplace_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )
    input_cqc_location_api_schema = StructType(
        [
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, StringType(), True),
        ]
    )


@dataclass
class CleanAscwdsFilledPostOutliersSchema:
    unfiltered_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
        ]
    )


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersSchema:
    ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )

    filter_df_to_care_homes_with_known_beds_and_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )

    calculate_standardised_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.expected_filled_posts, DoubleType(), True),
        ]
    )
    expected_calculate_standardised_residuals_schema = StructType(
        [
            *calculate_standardised_residuals_schema,
            StructField(IndCQC.standardised_residual, DoubleType(), True),
        ]
    )

    standardised_residual_percentile_cutoff_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.standardised_residual, DoubleType(), True),
        ]
    )

    expected_standardised_residual_percentile_cutoff_with_percentiles_schema = (
        StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.primary_service_type, StringType(), True),
                StructField(IndCQC.standardised_residual, DoubleType(), True),
                StructField(IndCQC.lower_percentile, DoubleType(), True),
                StructField(IndCQC.upper_percentile, DoubleType(), True),
            ]
        )
    )

    duplicate_ratios_within_standardised_residual_cutoff_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.standardised_residual, DoubleType(), True),
            StructField(IndCQC.lower_percentile, DoubleType(), True),
            StructField(IndCQC.upper_percentile, DoubleType(), True),
        ]
    )

    expected_duplicate_ratios_within_standardised_residual_cutoff_schema = StructType(
        [
            *duplicate_ratios_within_standardised_residual_cutoff_schema,
            StructField(
                IndCQC.filled_posts_per_bed_ratio_within_std_resids, DoubleType(), True
            ),
        ]
    )

    min_and_max_permitted_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.filled_posts_per_bed_ratio_within_std_resids, DoubleType(), True
            ),
            StructField(IndCQC.number_of_beds_banded, FloatType(), True),
        ]
    )
    expected_min_and_max_permitted_ratios_schema = StructType(
        [
            *min_and_max_permitted_ratios_schema,
            StructField(IndCQC.min_filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.max_filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    set_minimum_permitted_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    winsorize_outliers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.min_filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.max_filled_posts_per_bed_ratio, FloatType(), True),
        ]
    )

    combine_dataframes_care_home_schema = StructType(
        [
            *ind_cqc_schema,
            StructField("additional column", DoubleType(), True),
        ]
    )

    combine_dataframes_non_care_home_schema = ind_cqc_schema

    expected_combined_dataframes_schema = combine_dataframes_non_care_home_schema


@dataclass
class NonResAscwdsFeaturesSchema(object):
    basic_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(
                IndCQC.regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(IndCQC.name, StringType(), True),
                            StructField(IndCQC.code, StringType(), True),
                            StructField(
                                IndCQC.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                IndCQC.person_family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                IndCQC.person_given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                IndCQC.person_roles,
                                                ArrayType(StringType(), True),
                                                True,
                                            ),
                                            StructField(
                                                IndCQC.person_title, StringType(), True
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(
                IndCQC.specialisms,
                ArrayType(
                    StructType([StructField(IndCQC.name, StringType(), True)]), True
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.rolling_rate_of_change_model, DoubleType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class CareHomeFeaturesSchema:
    clean_merged_data_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.rolling_rate_of_change_model, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class NonResPirFeaturesSchema:
    features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(
                IndCQC.imputed_non_res_pir_people_directly_employed, FloatType(), True
            ),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class EstimateIndCQCFilledPostsSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(
                IndCQC.contemporary_rural_urban_indicator_2011, StringType(), True
            ),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
        ]
    )


@dataclass
class EstimateIndCQCFilledPostsByJobRoleSchemas:
    estimated_ind_cqc_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
        ]
    )
    cleaned_ascwds_worker_schema = StructType(
        [
            StructField(AWKClean.establishment_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.main_job_role_clean_labelled, StringType(), True),
        ]
    )

    # TODO: Temp test data to check outputs, consider removing or tidying at the end
    expected_estimated_ind_cqc_filled_posts_by_job_role_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(AWKClean.main_job_role_clean_labelled, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField("ascwds_num_of_jobs", IntegerType(), True),
            StructField("estimated_num_of_jobs", DoubleType(), True),
            StructField("ascwds_num_of_jobs_rebased", DoubleType(), True),
            StructField("estimate_filled_posts_by_job_role", DoubleType(), True),
        ]
    )


@dataclass
class EstimateMissingAscwdsFilledPostsSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, FloatType(), True),
        ]
    )


@dataclass
class ModelPrimaryServiceRollingAverage:
    primary_service_rolling_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )
    expected_primary_service_rolling_average_schema = StructType(
        [
            *primary_service_rolling_average_schema,
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.rolling_rate_of_change_model, DoubleType(), True),
        ]
    )

    single_column_to_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )
    expected_single_column_to_average_schema = StructType(
        [
            *single_column_to_average_schema,
            StructField(RA_TempCol.column_to_average, DoubleType(), True),
        ]
    )

    clean_column_to_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RA_TempCol.column_to_average, DoubleType(), True),
        ]
    )
    expected_clean_column_to_average_schema = StructType(
        [
            *clean_column_to_average_schema,
            StructField(RA_TempCol.care_home_status_count, IntegerType(), True),
            StructField(RA_TempCol.submission_count, IntegerType(), True),
        ]
    )

    calculate_care_home_status_count_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
        ]
    )
    expected_calculate_care_home_status_count_schema = StructType(
        [
            *calculate_care_home_status_count_schema,
            StructField(RA_TempCol.care_home_status_count, IntegerType(), True),
        ]
    )

    calculate_submission_count_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RA_TempCol.column_to_average, DoubleType(), True),
        ]
    )
    expected_calculate_submission_count_schema = StructType(
        [
            *calculate_submission_count_schema,
            StructField(RA_TempCol.submission_count, IntegerType(), True),
        ]
    )

    interpolate_column_to_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.column_to_average, DoubleType(), True),
        ]
    )
    expected_interpolate_column_to_average_schema = StructType(
        [
            *interpolate_column_to_average_schema,
            StructField(RA_TempCol.column_to_average_interpolated, DoubleType(), True),
        ]
    )

    calculate_rolling_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(RA_TempCol.column_to_average_interpolated, DoubleType(), True),
        ]
    )
    expected_calculate_rolling_average_schema = StructType(
        [
            *calculate_rolling_average_schema,
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
        ]
    )

    calculate_rolling_rate_of_change_schema = calculate_rolling_average_schema

    add_previous_value_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.column_to_average_interpolated, DoubleType(), True),
        ]
    )
    expected_add_previous_value_column_schema = StructType(
        [
            *add_previous_value_column_schema,
            StructField(
                RA_TempCol.previous_column_to_average_interpolated, DoubleType(), True
            ),
        ]
    )

    add_rolling_sum_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.column_to_average_interpolated, DoubleType(), True),
            StructField(
                RA_TempCol.previous_column_to_average_interpolated, DoubleType(), True
            ),
        ]
    )
    expected_add_rolling_sum_schema = StructType(
        [
            *add_rolling_sum_schema,
            StructField(RA_TempCol.rolling_current_period_sum, DoubleType(), True),
        ]
    )

    single_period_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(RA_TempCol.rolling_current_period_sum, DoubleType(), True),
            StructField(RA_TempCol.rolling_previous_period_sum, DoubleType(), True),
        ]
    )
    expected_single_period_rate_of_change_schema = StructType(
        [
            *single_period_rate_of_change_schema,
            StructField(RA_TempCol.single_period_rate_of_change, DoubleType(), True),
        ]
    )

    deduplicate_dataframe_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.single_period_rate_of_change, DoubleType(), True),
            StructField("another_col", DoubleType(), True),
        ]
    )
    expected_deduplicate_dataframe_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.single_period_rate_of_change, DoubleType(), True),
        ]
    )

    cumulative_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RA_TempCol.single_period_rate_of_change, DoubleType(), True),
        ]
    )
    expected_cumulative_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.rolling_rate_of_change_model, DoubleType(), True),
        ]
    )


@dataclass
class ModelImputationWithExtrapolationAndInterpolationSchemas:
    column_with_null_values: str = "null_values"

    imputation_with_extrapolation_and_interpolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(column_with_null_values, DoubleType(), True),
            StructField("trend_model", DoubleType(), True),
        ]
    )

    split_dataset_for_imputation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.has_non_null_value, BooleanType(), True),
        ]
    )

    non_null_submission_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(column_with_null_values, DoubleType(), True),
        ]
    )
    expected_non_null_submission_schema = StructType(
        [
            *non_null_submission_schema,
            StructField(IndCQC.has_non_null_value, BooleanType(), True),
        ]
    )

    imputation_model_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(column_with_null_values, DoubleType(), True),
            StructField(IndCQC.extrapolation_model, DoubleType(), True),
            StructField(IndCQC.interpolation_model, DoubleType(), True),
        ]
    )
    expected_imputation_model_schema = StructType(
        [
            *imputation_model_schema,
            StructField("imputation_model", DoubleType(), True),
        ]
    )


@dataclass
class ModelExtrapolation:
    extrapolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), False),
        ]
    )

    first_and_final_submission_dates_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
        ]
    )
    expected_first_and_final_submission_dates_schema = StructType(
        [
            *first_and_final_submission_dates_schema,
            StructField(IndCQC.first_submission_time, IntegerType(), True),
            StructField(IndCQC.final_submission_time, IntegerType(), True),
        ]
    )

    extrapolation_forwards_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.rolling_average_model, FloatType(), False),
        ]
    )
    expected_extrapolation_forwards_schema = StructType(
        [
            *extrapolation_forwards_schema,
            StructField(IndCQC.extrapolation_backwards, FloatType(), True),
        ]
    )
    extrapolation_forwards_mock_schema = StructType(
        [
            *extrapolation_forwards_schema,
            StructField(IndCQC.previous_non_null_value, FloatType(), True),
            StructField(IndCQC.previous_model_value, FloatType(), False),
        ]
    )

    extrapolation_backwards_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.first_submission_time, IntegerType(), True),
            StructField(IndCQC.final_submission_time, IntegerType(), True),
            StructField(IndCQC.rolling_average_model, FloatType(), False),
        ]
    )
    expected_extrapolation_backwards_schema = StructType(
        [
            *extrapolation_backwards_schema,
            StructField(IndCQC.extrapolation_backwards, FloatType(), True),
        ]
    )
    extrapolation_backwards_mock_schema = StructType(
        [
            *extrapolation_backwards_schema,
            StructField(IndCQC.first_non_null_value, FloatType(), True),
            StructField(IndCQC.first_model_value, FloatType(), False),
        ]
    )

    combine_extrapolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.first_submission_time, IntegerType(), True),
            StructField(IndCQC.final_submission_time, IntegerType(), True),
            StructField(IndCQC.extrapolation_forwards, FloatType(), True),
            StructField(IndCQC.extrapolation_backwards, FloatType(), True),
        ]
    )
    expected_combine_extrapolation_schema = StructType(
        [
            *combine_extrapolation_schema,
            StructField(IndCQC.extrapolation_model, FloatType(), True),
        ]
    )


@dataclass
class ModelInterpolation:
    interpolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.extrapolation_forwards, DoubleType(), True),
        ]
    )

    calculate_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.extrapolation_forwards, DoubleType(), True),
        ]
    )
    expected_calculate_residual_schema = StructType(
        [
            *calculate_residual_schema,
            StructField(IndCQC.residual, DoubleType(), True),
        ]
    )

    time_between_submissions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
        ]
    )
    expected_time_between_submissions_schema = StructType(
        [
            *time_between_submissions_schema,
            StructField(
                IndCQC.proportion_of_time_between_submissions, DoubleType(), True
            ),
        ]
    )
    time_between_submissions_mock_schema = StructType(
        [
            *time_between_submissions_schema,
            StructField(IndCQC.previous_submission_time, IntegerType(), True),
            StructField(IndCQC.next_submission_time, IntegerType(), False),
        ]
    )

    calculate_interpolated_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.previous_non_null_value, DoubleType(), True),
            StructField(IndCQC.residual, DoubleType(), True),
            StructField(
                IndCQC.proportion_of_time_between_submissions, DoubleType(), True
            ),
        ]
    )
    expected_calculate_interpolated_values_schema = StructType(
        [
            *calculate_interpolated_values_schema,
            StructField(IndCQC.interpolation_model, DoubleType(), True),
        ]
    )


@dataclass
class ModelFeatures:
    vectorise_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("col_1", FloatType(), True),
            StructField("col_2", IntegerType(), True),
            StructField("col_3", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    expected_vectorised_feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )
    add_time_registered_schema = StructType(
        [
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    expected_add_time_registered_schema = StructType(
        [
            *add_time_registered_schema,
            StructField(IndCQC.time_registered, IntegerType(), True),
        ]
    )


@dataclass
class ModelCareHomes:
    care_homes_cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    care_homes_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
        ]
    )


@dataclass
class ModelNonResWithDormancy:
    non_res_with_dormancy_cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    non_res_with_dormancy_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
        ]
    )


@dataclass
class ModelNonResWithoutDormancy:
    non_res_without_dormancy_cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    non_res_without_dormancy_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
        ]
    )


@dataclass
class ModelNonResPirLinearRegressionSchemas:
    non_res_pir_cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(
                IndCQC.imputed_non_res_pir_people_directly_employed, FloatType(), True
            ),
        ]
    )
    non_res_pir_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(
                IndCQC.imputed_non_res_pir_people_directly_employed, FloatType(), True
            ),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )
    expected_non_res_pir_prediction_schema = StructType(
        [
            *non_res_pir_cleaned_ind_cqc_schema,
            StructField(IndCQC.non_res_pir_linear_regression_model, FloatType(), True),
        ]
    )


@dataclass
class InsertPredictionsIntoLocations:
    cleaned_cqc_schema = ModelCareHomes.care_homes_cleaned_ind_cqc_schema
    care_home_features_schema = ModelCareHomes.care_homes_features_schema

    predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )


@dataclass
class MLModelMetrics:
    ind_cqc_with_predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    r2_metric_schema = predictions_schema


@dataclass
class ValidateMergedIndCqcData:
    cqc_locations_schema = MergeIndCQCData.clean_cqc_location_for_merge_schema
    merged_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.provider_name, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.organisation_id, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class ValidateMergedCoverageData:
    cqc_locations_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )
    merged_coverage_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
        ]
    )

    calculate_expected_size_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.postal_code, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )


@dataclass
class FlattenCQCRatings:
    test_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    test_ascwds_workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    filter_to_first_import_of_most_recent_month_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    flatten_current_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    flatten_historic_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
                                                                StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_flatten_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )

    add_current_or_historic_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
        ]
    )
    expected_add_current_or_historic_schema = StructType(
        [
            *add_current_or_historic_schema,
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    remove_blank_rows_schema = expected_flatten_ratings_schema

    add_rating_sequence_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
        ]
    )
    expected_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
        ]
    )
    expected_reversed_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )

    add_latest_rating_flag_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )
    expected_add_latest_rating_flag_schema = StructType(
        [
            *add_latest_rating_flag_schema,
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
        ]
    )

    create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )

    expected_create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )

    select_ratings_for_benchmarks_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    add_good_and_outstanding_flag_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
        ]
    )

    expected_add_good_and_outstanding_flag_column_schema = StructType(
        [
            *add_good_and_outstanding_flag_column_schema,
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
        ]
    )

    ratings_join_establishment_ids_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("other_ratings_column", StringType(), True),
        ]
    )
    ascwds_join_establishment_ids_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
        ]
    )
    expected_join_establishment_ids_schema = StructType(
        [
            *ratings_join_establishment_ids_schema,
            StructField(AWP.establishment_id, StringType(), True),
        ]
    )
    create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCRatings.benchmarks_location_id, StringType(), True),
            StructField(CQCRatings.benchmarks_establishment_id, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.benchmarks_overall_rating, StringType(), True),
            StructField(CQCRatings.inspection_date, StringType(), True),
        ]
    )

    add_numerical_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )
    expected_add_numerical_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.safe_rating_value, IntegerType(), True),
            StructField(CQCRatings.well_led_rating_value, IntegerType(), True),
            StructField(CQCRatings.caring_rating_value, IntegerType(), True),
            StructField(CQCRatings.responsive_rating_value, IntegerType(), True),
            StructField(CQCRatings.effective_rating_value, IntegerType(), True),
            StructField(CQCRatings.total_rating_value, IntegerType(), True),
        ]
    )

    location_id_hash_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
        ]
    )

    expected_location_id_hash_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.location_id_hash, StringType(), True),
        ]
    )


@dataclass
class ValidationUtils:
    validation_schema = StructType(
        [
            StructField(Validation.check, StringType(), True),
            StructField(Validation.check_level, StringType(), True),
            StructField(Validation.check_status, StringType(), True),
            StructField(Validation.constraint, StringType(), True),
            StructField(Validation.constraint_status, StringType(), True),
            StructField(Validation.constraint_message, StringType(), True),
        ]
    )

    size_of_dataset_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
        ]
    )

    index_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    min_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    min_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
        ]
    )

    max_values_schema = min_values_schema
    max_values_multiple_columns_schema = min_values_multiple_columns_schema

    one_column_schema = size_of_dataset_schema
    two_column_schema = index_column_schema
    multiple_rules_schema = index_column_schema

    categorical_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
        ]
    )
    add_column_with_length_of_string_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
        ]
    )
    expected_add_column_with_length_of_string_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(Validation.location_id_length, IntegerType(), True),
        ]
    )
    custom_type_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
        ]
    )


@dataclass
class ValidateLocationsAPICleanedData:
    raw_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    cleaned_cqc_locations_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.cqc_provider_import_date, DateType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.provider_id, StringType(), True),
            StructField(CQCLClean.provider_name, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.registration_status, StringType(), True),
            StructField(CQCLClean.imputed_registration_date, DateType(), True),
            StructField(CQCLClean.dormancy, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(CQCLClean.primary_service_type, StringType(), True),
            StructField(CQCLClean.contemporary_ons_import_date, DateType(), True),
            StructField(CQCLClean.contemporary_cssr, StringType(), True),
            StructField(CQCLClean.contemporary_region, StringType(), True),
            StructField(CQCLClean.current_ons_import_date, DateType(), True),
            StructField(CQCLClean.current_cssr, StringType(), True),
            StructField(CQCLClean.current_region, StringType(), True),
            StructField(CQCLClean.current_rural_urban_ind_11, StringType(), True),
            StructField(
                CQCLClean.services_offered,
                ArrayType(
                    StringType(),
                ),
            ),
        ]
    )
    calculate_expected_size_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )


@dataclass
class ValidateProvidersAPICleanedData:
    raw_cqc_providers_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    cleaned_cqc_providers_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
        ]
    )
    calculate_expected_size_schema = raw_cqc_providers_schema


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
class ValidateCleanedIndCqcData:
    merged_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.postcode, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.provider_name, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.organisation_id, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )
    calculate_expected_size_schema = merged_ind_cqc_schema


@dataclass
class ValidateEstimatedMissingAscwdsFilledPostsData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_missing_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.provider_name, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.organisation_id, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateCareHomeIndCqcFeaturesData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
        ]
    )
    care_home_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.features, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateNonResASCWDSIndCqcFeaturesSchema:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(
                IndCQC.imputed_gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    non_res_ascwds_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateNonResPirIndCqcFeaturesSchema:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(
                IndCQC.imputed_non_res_pir_people_directly_employed, FloatType(), True
            ),
        ]
    )
    non_res_pir_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateEstimatedIndCqcFilledPostsData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_ind_cqc_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed, IntegerType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.unix_time, IntegerType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.care_home_model, DoubleType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateEstimatedIndCqcFilledPostsByJobRoleSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_ind_cqc_filled_posts_by_job_role_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


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
class ValidateLocationsAPIRawData:
    raw_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.registration_date, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.number_of_beds, IntegerType(), True),
            StructField(CQCL.dormancy, StringType(), True),
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
class RawDataAdjustments:
    worker_data_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
            StructField(AWK.establishment_id, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )

    pir_data_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCPIR.pir_type, StringType(), True),
            StructField(CQCPIR.pir_submission_date, StringType(), True),
            StructField(CQCPIR.domiciliary_care, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )

    locations_data_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )


@dataclass
class DiagnosticsOnKnownFilledPostsSchemas:
    estimate_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.rolling_average_model, FloatType(), True),
            StructField(IndCQC.care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_pir_linear_regression_model, FloatType(), True),
            StructField(
                IndCQC.imputed_posts_non_res_with_dormancy_model, FloatType(), True
            ),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class DiagnosticsOnCapacityTrackerSchemas:
    estimate_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.rolling_average_model, FloatType(), True),
            StructField(IndCQC.care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_pir_linear_regression_model, FloatType(), True),
            StructField(
                IndCQC.imputed_posts_non_res_with_dormancy_model, FloatType(), True
            ),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.unix_time, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    join_estimates_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    capacity_tracker_care_home_schema = StructType(
        [
            StructField(CTCHClean.cqc_id, StringType(), False),
            StructField(CTCHClean.capacity_tracker_import_date, DateType(), False),
            StructField(CTCHClean.non_agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.agency_total_employed, IntegerType(), True),
            StructField(
                CTCHClean.agency_and_non_agency_total_employed, IntegerType(), True
            ),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    capacity_tracker_non_res_schema = StructType(
        [
            StructField(CTNRClean.cqc_id, StringType(), False),
            StructField(CTNRClean.capacity_tracker_import_date, DateType(), False),
            StructField(CTNRClean.cqc_care_workers_employed, IntegerType(), True),
            StructField(
                CTNRClean.cqc_care_workers_employed_rolling_avg, FloatType(), True
            ),
            StructField(CTNRClean.service_user_count, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_joined_care_home_schema = StructType(
        [
            *join_estimates_schema,
            StructField(CTCHClean.capacity_tracker_import_date, DateType(), True),
            StructField(CTCHClean.non_agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.agency_total_employed, IntegerType(), True),
            StructField(
                CTCHClean.agency_and_non_agency_total_employed, IntegerType(), True
            ),
        ]
    )
    expected_joined_non_res_schema = StructType(
        [
            *estimate_filled_posts_schema,
            StructField(CTCHClean.capacity_tracker_import_date, DateType(), True),
            StructField(CTNRClean.cqc_care_workers_employed, IntegerType(), True),
            StructField(
                CTNRClean.cqc_care_workers_employed_rolling_avg, FloatType(), True
            ),
            StructField(CTNRClean.service_user_count, IntegerType(), True),
        ]
    )
    convert_to_all_posts_using_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(CTNRClean.cqc_care_workers_employed_imputed, FloatType(), True),
        ]
    )
    expected_convert_to_all_posts_using_ratio_schema = StructType(
        [
            *convert_to_all_posts_using_ratio_schema,
            StructField(
                CTNRClean.capacity_tracker_filled_post_estimate, FloatType(), True
            ),
        ]
    )
    calculate_care_worker_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(CTNRClean.cqc_care_workers_employed_imputed, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
        ]
    )


@dataclass
class DiagnosticsUtilsSchemas:
    filter_to_known_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(
                "other_column",
                FloatType(),
                True,
            ),
        ]
    )
    restructure_dataframe_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField("model_type_one", FloatType(), True),
            StructField("model_type_two", FloatType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    expected_restructure_dataframe_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    calculate_distribution_metrics_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
        ]
    )
    expected_calculate_mean_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.distribution_mean, FloatType(), True),
        ]
    )
    expected_calculate_standard_deviation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.distribution_standard_deviation, FloatType(), True),
        ]
    )
    expected_calculate_kurtosis_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.distribution_kurtosis, FloatType(), True),
        ]
    )
    expected_calculate_skewness_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.distribution_skewness, FloatType(), True),
        ]
    )

    expected_calculate_distribution_metrics_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.distribution_mean, FloatType(), True),
            StructField(IndCQC.distribution_standard_deviation, FloatType(), True),
            StructField(IndCQC.distribution_kurtosis, FloatType(), True),
            StructField(IndCQC.distribution_skewness, FloatType(), True),
        ]
    )

    calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
        ]
    )
    expected_calculate_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
        ]
    )
    expected_calculate_absolute_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.absolute_residual, FloatType(), True),
        ]
    )
    expected_calculate_percentage_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.percentage_residual, FloatType(), True),
        ]
    )
    expected_calculate_standardised_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.standardised_residual, FloatType(), True),
        ]
    )
    expected_calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.absolute_residual, FloatType(), True),
            StructField(IndCQC.percentage_residual, FloatType(), True),
            StructField(IndCQC.standardised_residual, FloatType(), True),
        ]
    )

    calculate_aggregate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.absolute_residual, FloatType(), True),
            StructField(IndCQC.percentage_residual, FloatType(), True),
            StructField(IndCQC.standardised_residual, FloatType(), True),
        ]
    )
    expected_calculate_aggregate_residuals_schema = StructType(
        [
            *calculate_aggregate_residuals_schema,
            StructField(IndCQC.average_absolute_residual, FloatType(), True),
            StructField(IndCQC.average_percentage_residual, FloatType(), True),
            StructField(IndCQC.max_residual, FloatType(), True),
            StructField(IndCQC.min_residual, FloatType(), True),
            StructField(
                IndCQC.percentage_of_residuals_within_absolute_value, FloatType(), True
            ),
            StructField(
                IndCQC.percentage_of_residuals_within_percentage_value,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.percentage_of_residuals_within_absolute_or_percentage_values,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.percentage_of_standardised_residuals_within_limit,
                FloatType(),
                True,
            ),
        ]
    )

    create_summary_dataframe_schema = StructType(
        [
            *expected_restructure_dataframe_schema,
            StructField(IndCQC.distribution_mean, FloatType(), True),
            StructField(IndCQC.distribution_standard_deviation, FloatType(), True),
            StructField(IndCQC.distribution_kurtosis, FloatType(), True),
            StructField(IndCQC.distribution_skewness, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.absolute_residual, FloatType(), True),
            StructField(IndCQC.percentage_residual, FloatType(), True),
            StructField(IndCQC.average_absolute_residual, FloatType(), True),
            StructField(IndCQC.average_percentage_residual, FloatType(), True),
            StructField(IndCQC.max_residual, FloatType(), True),
            StructField(IndCQC.min_residual, FloatType(), True),
            StructField(
                IndCQC.percentage_of_residuals_within_absolute_value, FloatType(), True
            ),
            StructField(
                IndCQC.percentage_of_residuals_within_percentage_value,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.percentage_of_standardised_residuals_within_limit,
                FloatType(),
                True,
            ),
        ]
    )
    expected_create_summary_dataframe_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_source, StringType(), True),
            StructField(IndCQC.distribution_mean, FloatType(), True),
            StructField(IndCQC.distribution_standard_deviation, FloatType(), True),
            StructField(IndCQC.distribution_kurtosis, FloatType(), True),
            StructField(IndCQC.distribution_skewness, FloatType(), True),
            StructField(IndCQC.average_absolute_residual, FloatType(), True),
            StructField(IndCQC.average_percentage_residual, FloatType(), True),
            StructField(IndCQC.max_residual, FloatType(), True),
            StructField(IndCQC.min_residual, FloatType(), True),
            StructField(
                IndCQC.percentage_of_residuals_within_absolute_value, FloatType(), True
            ),
            StructField(
                IndCQC.percentage_of_residuals_within_percentage_value,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.percentage_of_standardised_residuals_within_limit,
                FloatType(),
                True,
            ),
        ]
    )


@dataclass
class ASCWDSFilteringUtilsSchemas:
    add_filtering_column_schema = StructType(
        [
            StructField(
                IndCQC.location_id,
                StringType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
        ]
    )
    expected_add_filtering_column_schema = StructType(
        [
            *add_filtering_column_schema,
            StructField(
                IndCQC.ascwds_filtering_rule,
                StringType(),
                True,
            ),
        ]
    )
    update_filtering_rule_schema = StructType(
        [
            StructField(
                IndCQC.location_id,
                StringType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filtering_rule,
                StringType(),
                True,
            ),
        ]
    )


@dataclass
class NullFilledPostsUsingInvalidMissingDataCodeSchema:
    null_filled_posts_using_invalid_missing_data_code_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )


@dataclass
class NullGroupedProvidersSchema:
    null_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )

    calculate_data_for_grouped_provider_identification_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    expected_calculate_data_for_grouped_provider_identification_schema = StructType(
        [
            *calculate_data_for_grouped_provider_identification_schema,
            StructField(IndCQC.locations_at_provider_count, IntegerType(), True),
            StructField(
                IndCQC.locations_in_ascwds_at_provider_count, IntegerType(), True
            ),
            StructField(
                IndCQC.locations_in_ascwds_with_data_at_provider_count,
                IntegerType(),
                True,
            ),
            StructField(IndCQC.number_of_beds_at_provider, IntegerType(), True),
        ]
    )

    identify_potential_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.locations_at_provider_count, IntegerType(), True),
            StructField(
                IndCQC.locations_in_ascwds_at_provider_count, IntegerType(), True
            ),
            StructField(
                IndCQC.locations_in_ascwds_with_data_at_provider_count,
                IntegerType(),
                True,
            ),
        ]
    )
    expected_identify_potential_grouped_providers_schema = StructType(
        [
            *identify_potential_grouped_providers_schema,
            StructField(
                IndCQC.locations_in_ascwds_with_data_at_provider_count,
                BooleanType(),
                True,
            ),
        ]
    )

    null_care_home_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.number_of_beds_at_provider, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.potential_grouped_provider, BooleanType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )


@dataclass
class ArchiveFilledPostsEstimates:
    filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    select_import_dates_to_archive_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    create_archive_date_partitions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_create_archive_date_partitions_schema = StructType(
        [
            *create_archive_date_partitions_schema,
            StructField(ArchiveKeys.archive_day, StringType(), True),
            StructField(ArchiveKeys.archive_month, StringType(), True),
            StructField(ArchiveKeys.archive_year, StringType(), True),
            StructField(ArchiveKeys.archive_timestamp, StringType(), True),
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
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )
    cleaned_ct_care_home_schema = StructType(
        [
            *ct_care_home_schema,
            StructField(CTCHClean.capacity_tracker_import_date, DateType(), True),
            StructField(CTCHClean.non_agency_total_employed, IntegerType(), True),
            StructField(CTCHClean.agency_total_employed, IntegerType(), True),
            StructField(
                CTCHClean.agency_and_non_agency_total_employed, IntegerType(), True
            ),
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
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )
    cleaned_ct_non_res_schema = StructType(
        [
            StructField(CTNRClean.cqc_id, StringType(), True),
            StructField(CTNRClean.cqc_care_workers_employed, StringType(), True),
            StructField(CTNRClean.service_user_count, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(CTNRClean.capacity_tracker_import_date, DateType(), True),
        ]
    )
    calculate_expected_size_schema = ct_non_res_schema


@dataclass
class ReconciliationUtilsSchema:
    input_ascwds_workplace_schema = ReconciliationSchema.input_ascwds_workplace_schema
    input_cqc_location_api_schema = ReconciliationSchema.input_cqc_location_api_schema

    expected_prepared_most_recent_cqc_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )
    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    regtype_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
        ]
    )

    remove_head_office_accounts_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
        ]
    )

    filter_to_relevant_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.registration_status, StringType(), True),
            StructField(CQCLClean.deregistration_date, DateType(), True),
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    add_singles_and_subs_description_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
        ]
    )

    expected_singles_and_subs_description_schema = StructType(
        [
            *add_singles_and_subs_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
        ]
    )

    expected_create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
        ]
    )

    final_column_selection_schema = StructType(
        [
            StructField("extra_column", StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
        ]
    )

    expected_final_column_selection_schema = StructType(
        [
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
        ]
    )

    add_subject_column_schema = StructType(
        [
            StructField("id", StringType(), True),
        ]
    )

    expected_add_subject_column_schema = StructType(
        [
            *add_subject_column_schema,
            StructField(ReconColumn.subject, StringType(), True),
        ]
    )

    new_issues_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )
    unique_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_join_array_of_nmdsids_schema = StructType(
        [
            *unique_schema,
            StructField("new_column", StringType(), True),
        ]
    )

    create_parents_description_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(ReconColumn.new_potential_subs, StringType(), True),
            StructField(ReconColumn.old_potential_subs, StringType(), True),
            StructField(
                ReconColumn.missing_or_incorrect_potential_subs, StringType(), True
            ),
        ]
    )

    expected_create_parents_description_schema = StructType(
        [
            *create_parents_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )

    cqc_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
        ]
    )
    ascwds_data_for_join_schema = StructType(
        [
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
        ]
    )
    expected_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
        ]
    )


@dataclass
class BlendAscwdsPirData:
    blend_pir_and_ascwds_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, IntegerType(), True),
        ]
    )

    create_repeated_ascwds_clean_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, IntegerType(), True),
        ]
    )
    expected_create_repeated_ascwds_clean_column_schema = StructType(
        [
            *create_repeated_ascwds_clean_column_schema,
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, IntegerType(), True
            ),
        ]
    )
    create_pir_people_directly_employed_dedup_modelled_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    expected_create_pir_people_directly_employed_dedup_modelled_column_schema = (
        StructType(
            [
                *create_pir_people_directly_employed_dedup_modelled_column_schema,
                StructField(
                    IndCQC.pir_people_directly_employed_filled_posts, FloatType(), True
                ),
            ]
        )
    )
    create_last_submission_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, IntegerType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    expected_create_last_submission_columns_schema = StructType(
        [
            *create_last_submission_columns_schema,
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
        ]
    )
    blend_modelled_pir_ands_ascwds_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, IntegerType(), True
            ),
            StructField(
                IndCQC.pir_people_directly_employed_filled_posts, FloatType(), True
            ),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
        ]
    )
    expected_blend_modelled_pir_ands_ascwds_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, IntegerType(), True
            ),
            StructField(
                IndCQC.pir_people_directly_employed_filled_posts, FloatType(), True
            ),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
        ]
    )

    drop_temporary_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, IntegerType(), True
            ),
            StructField(
                IndCQC.pir_people_directly_employed_filled_posts, FloatType(), True
            ),
        ]
    )


@dataclass
class AscwdsJobroleCountSchema:
    worker_schema = StructType(
        [
            StructField(AWKClean.establishment_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.main_job_role_clean_labelled, StringType(), True),
        ]
    )

    worker_with_jobrole_count_schema = StructType(
        [
            *worker_schema,
            StructField("ascwds_main_job_role_counts", IntegerType(), True),
        ]
    )
