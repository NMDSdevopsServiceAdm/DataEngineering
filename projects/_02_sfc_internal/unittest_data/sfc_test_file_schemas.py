from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
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
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)


@dataclass
class ReconciliationUtilsSchema:
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
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
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
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
        ]
    )

    sample_in_ascwds_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.removed_by_purge_date_filter, BooleanType(), True),
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
    sample_cqc_providers_for_merge_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )
    sample_merged_coverage_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCPClean.provider_id, StringType(), True),
        ]
    )
    expected_merged_covergae_and_provider_name_joined_schema = StructType(
        [
            *sample_merged_coverage_schema,
            StructField(CQCLClean.provider_name, StringType(), True),
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
            StructField("_year", IntegerType(), True),
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
