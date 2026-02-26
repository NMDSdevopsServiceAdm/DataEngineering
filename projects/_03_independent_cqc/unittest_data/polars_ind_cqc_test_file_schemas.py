from dataclasses import dataclass

import polars as pl

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
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
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    NullGroupedProviderColumns as NGPcol,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


@dataclass
class PrepareJobRoleCountsUtilsSchemas:
    aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )
    expected_aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
            (Keys.import_date, pl.String()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.UInt32()),
        ]
    )


@dataclass
class FeaturesEngineeringUtilsSchemas:
    add_array_column_count_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.services_offered, pl.List(pl.String())),
        ]
    )
    expected_add_array_column_count_schema = pl.Schema(
        list(add_array_column_count_schema.items())
        + [(IndCQC.service_count, pl.UInt32())]
    )

    add_date_index_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
        ]
    )
    expected_add_date_index_column_schema = pl.Schema(
        list(add_date_index_column_schema.items())
        + [(IndCQC.cqc_location_import_date_indexed, pl.UInt32())]
    )

    cap_integer_at_max_value_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.service_count, pl.Int32()),
        ]
    )
    expected_cap_integer_at_max_value_schema = pl.Schema(
        list(cap_integer_at_max_value_schema.items())
        + [(IndCQC.service_count_capped, pl.Int32())]
    )

    col_with_categories: str = "categories"

    expand_encode_and_extract_features_when_not_array_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (col_with_categories, pl.String()),
        ]
    )
    expected_expand_encode_and_extract_features_when_not_array_schema = pl.Schema(
        list(expand_encode_and_extract_features_when_not_array_schema.items())
        + [
            ("has_A", pl.Int8()),
            ("has_B", pl.Int8()),
            ("has_C", pl.Int8()),
        ]
    )

    expand_encode_and_extract_features_when_is_array_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (col_with_categories, pl.List(pl.String())),
        ]
    )
    expected_expand_encode_and_extract_features_when_is_array_schema = pl.Schema(
        list(expand_encode_and_extract_features_when_is_array_schema.items())
        + [
            ("has_A", pl.Int8()),
            ("has_B", pl.Int8()),
            ("has_C", pl.Int8()),
        ]
    )

    group_rural_urban_sparse_categories_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.current_rural_urban_indicator_2011, pl.String()),
        ]
    )
    expected_group_rural_urban_sparse_categories_schema = pl.Schema(
        list(group_rural_urban_sparse_categories_schema.items())
        + [(IndCQC.current_rural_urban_indicator_2011_for_non_res_model, pl.String())]
    )

    add_power_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("values", pl.UInt32()),
        ]
    )
    expected_add_power_column_schema = pl.Schema(
        list(add_power_column_schema.items()) + [("squared_values", pl.UInt32())]
    )

    select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("import_date", pl.String()),
            ("other_col", pl.String()),
            ("feature_1", pl.UInt32()),
            ("feature_2", pl.UInt32()),
            ("feature_3", pl.UInt32()),
            ("dependent", pl.Float64()),
            (IndCQC.care_home_status_count, pl.Int32()),
        ]
    )
    expected_select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            (IndCQC.care_home_status_count, pl.Int32()),
            ("dependent", pl.Float64()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
            ("feature_3", pl.Int32()),
            ("import_date", pl.String()),
        ]
    )


@dataclass
class ModelTrainingUtilsSchemas:
    split_train_test_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts, pl.Float32()),
        ]
    )

    multiple_feature_cols = ["feature_1", "feature_2"]
    single_feature_col = ["feature_1"]
    dependent_col = "dependent"
    convert_dataframe_to_numpy_basic_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
            ("dependent", pl.Float64()),
        ]
    )


@dataclass
class ModelUtilsSchemas:
    features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
        ]
    )

    expected_predictions_dataframe_schema = pl.Schema(
        list(features_schema.items())
        + [
            (IndCQC.prediction, pl.Float64()),
            (IndCQC.prediction_run_id, pl.String()),
        ]
    )


@dataclass
class ValidateModelsSchemas:
    validate_model_feature_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.dormancy, pl.String()),
            (IndCQC.regulated_activities_offered, pl.List(pl.String())),
            (IndCQC.posts_rolling_average_model, pl.Float32()),
            (IndCQC.services_offered, pl.List(pl.String())),
            (IndCQC.specialisms_offered, pl.List(pl.String())),
            (IndCQC.current_rural_urban_indicator_2011, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.related_location, pl.String()),
            (IndCQC.time_registered, pl.Int32()),
            (IndCQC.time_since_dormant, pl.Int32),
        ]
    )


@dataclass
class ValidateModel01FeaturesSchemas:
    validation_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.dormancy, pl.String()),
            (Keys.import_date, pl.String()),
            ("feature 1", pl.String()),
            ("feature 2", pl.String()),
            (IndCQC.imputed_filled_post_model, pl.Float32),
        ]
    )


@dataclass
class EstimateIndCqcFilledPostsByJobRoleUtilsSchemas:
    estimates_df_before_join_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
        ]
    )
    worker_df_before_join_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.Int64()),
        ]
    )
    expected_join_worker_to_estimates_dataframe_schema = pl.Schema(
        list(estimates_df_before_join_schema.items())
        + [
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.Int64()),
        ]
    )


@dataclass
class MergeIndCQCSchemas:
    cqc_location_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
        ]
    )
    cqc_pir_schema = pl.Schema(
        [
            (CQCPIRClean.location_id, pl.String()),
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("pir_col", pl.String()),
        ]
    )
    ascwds_workplace_schema = pl.Schema(
        [
            (AWPClean.location_id, pl.String()),
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            ("ascwds_col", pl.String()),
        ]
    )
    ct_non_res_schema = pl.Schema(
        [
            (CTNRClean.cqc_id, pl.String()),
            (CTNRClean.ct_non_res_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("ct_non_res_col", pl.String()),
        ]
    )
    ct_care_home_schema = pl.Schema(
        [
            (CTCHClean.cqc_id, pl.String()),
            (CTCHClean.ct_care_home_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("ct_care_home_col", pl.String()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.cqc_sector, pl.String()),
            (IndCQC.cqc_pir_import_date, pl.Date()),
            ("pir_col", pl.String()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
            ("ascwds_col", pl.String()),
            (IndCQC.ct_non_res_import_date, pl.Date()),
            ("ct_non_res_col", pl.String()),
            (IndCQC.ct_care_home_import_date, pl.Date()),
            ("ct_care_home_col", pl.String()),
        ]
    )


@dataclass
class MergeUtilsSchemas:
    clean_cqc_location_for_merge_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.cqc_sector, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int64()),
        ]
    )

    data_to_merge_without_care_home_col_schema = pl.Schema(
        [
            (AWPClean.location_id, pl.String()),
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.establishment_id, pl.String()),
            (AWPClean.total_staff, pl.Int64()),
        ]
    )

    expected_merged_without_care_home_col_schema = pl.Schema(
        list(clean_cqc_location_for_merge_schema.items())
        + [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.establishment_id, pl.String()),
            (AWPClean.total_staff, pl.Int64()),
        ]
    )

    data_to_merge_with_care_home_col_schema = pl.Schema(
        [
            (CQCPIRClean.location_id, pl.String()),
            (CQCPIRClean.care_home, pl.String()),
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
        ]
    )

    expected_merged_with_care_home_col_schema = pl.Schema(
        list(clean_cqc_location_for_merge_schema.items())
        + [
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
        ]
    )


@dataclass
class ValidateMergeIndCQCSchemas:
    merged_ind_cqc_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
            (IndCQC.cqc_pir_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.name, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.cqc_sector, pl.String()),
            (IndCQC.imputed_registration_date, pl.Date()),
            (IndCQC.dormancy, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.services_offered, pl.List(pl.String())),
            (IndCQC.primary_service_type, pl.String()),
            (IndCQC.contemporary_ons_import_date, pl.Date()),
            (IndCQC.contemporary_cssr, pl.String()),
            (IndCQC.contemporary_region, pl.String()),
            (IndCQC.current_ons_import_date, pl.Date()),
            (IndCQC.current_cssr, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.current_rural_urban_indicator_2011, pl.String()),
            (IndCQC.current_lsoa21, pl.String()),
            (IndCQC.current_msoa21, pl.String()),
            (IndCQC.pir_people_directly_employed_cleaned, pl.Int64()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.organisation_id, pl.String()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.related_location, pl.String()),
        ]
    )

    cqc_locations_cleaned_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int64()),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsSchemas:
    calculate_ascwds_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsDifferenceInRangeSchemas:
    test_difference_within_range_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsSchemas:
    calculate_ascwds_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsUtilsSchemas:
    estimated_source_description_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.estimate_filled_posts, pl.Float64()),
            (IndCQC.estimate_filled_posts_source, pl.String()),
        ]
    )

    common_checks_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
        ]
    )


@dataclass
class CleanIndCQCSchema:
    replace_zero_beds_with_null_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
        ]
    )

    populate_missing_care_home_number_of_beds_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
        ]
    )

    filter_to_care_homes_with_known_beds_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
        ]
    )

    average_beds_per_location_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
        ]
    )

    expected_average_beds_per_location_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("avg_beds", pl.Int64()),
        ]
    )

    replace_null_beds_with_average_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.Utf8),
            (IndCQC.number_of_beds, pl.Int64),
            ("avg_beds", pl.Int64),
        ]
    )

    expected_replace_null_beds_with_average_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.Utf8),
            (IndCQC.number_of_beds, pl.Int64),
        ]
    )

    calculate_time_registered_for_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.imputed_registration_date, pl.Date()),
        ]
    )

    expected_calculate_time_registered_for_schema = pl.Schema(
        list(calculate_time_registered_for_schema.items())
        + [
            (IndCQC.time_registered, pl.UInt32()),
        ]
    )

    calculate_time_since_dormant_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.dormancy, pl.String()),
        ]
    )
    expected_calculate_time_since_dormant_schema = pl.Schema(
        list(calculate_time_since_dormant_schema.items())
        + [
            (IndCQC.time_since_dormant, pl.Int64()),
        ]
    )

    remove_cqc_dual_registrations_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.name, pl.String()),
            (IndCQC.postcode, pl.String()),
            (IndCQC.care_home, pl.String()),
            (AWPClean.total_staff_bounded, pl.Int64()),
            (AWPClean.worker_records_bounded, pl.Int64()),
            (IndCQC.imputed_registration_date, pl.Date()),
        ]
    )


@dataclass
class ArchiveFilledPostsEstimates:
    estimate_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    expected_add_latest_annual_estimate_date_schema = (
        list(estimate_filled_posts_schema.items())
    ) + ["most_recent_annual_estimate_date"]

    expected_create_archive_date_partitions_schema = pl.Schema(
        list(estimate_filled_posts_schema.items())
        + [
            (ArchiveKeys.archive_day, pl.String()),
            (ArchiveKeys.archive_month, pl.String()),
            (ArchiveKeys.archive_year, pl.String()),
            (ArchiveKeys.archive_timestamp, pl.String()),
        ]
    )


@dataclass
class CleanFilteringUtilsSchemas:
    add_filtering_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
        ]
    )
    expected_add_filtering_column_schema = pl.Schema(
        list(add_filtering_column_schema.items())
        + [
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )
    update_filtering_rule_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )

    aggregate_values_to_provider_level_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.ct_care_home_total_employed_cleaned, pl.Int64()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )
    expected_aggregate_values_to_provider_level_schema = pl.Schema(
        list(aggregate_values_to_provider_level_schema.items())
        + [
            (IndCQC.ct_care_home_total_employed_cleaned_provider_sum, pl.Int64()),
        ]
    )


@dataclass
class CleanUtilsSchemas:
    locations_with_repeated_value_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("integer_column", pl.Int64()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    expected_locations_without_repeated_values_schema = pl.Schema(
        list(locations_with_repeated_value_schema.items())
        + [
            ("integer_column_deduplicated", pl.Int64()),
        ]
    )
    providers_with_repeated_value_schema = pl.Schema(
        [
            (IndCQC.provider_id, pl.String()),
            ("integer_column", pl.Int64()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    expected_providers_without_repeated_values_schema = pl.Schema(
        list(providers_with_repeated_value_schema.items())
        + [
            ("integer_column_deduplicated", pl.Int64()),
        ]
    )


@dataclass
class ForwardFillLatestKnownValue:
    col_to_forward_fill: str = "col_to_forward_fill"
    days_to_forward_fill: str = "days_to_forward_fill"
    last_known_date: str = "last_known_date"
    last_known_value: str = "last_known_value"

    input_return_last_known_value_locations_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (col_to_forward_fill, pl.Int64()),
        ]
    )
    expected_return_last_known_value_locations_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (last_known_date, pl.Date()),
            (last_known_value, pl.Int64()),
        ]
    )
    forward_fill_locations_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (col_to_forward_fill, pl.Int64()),
            (last_known_date, pl.Date()),
            (last_known_value, pl.Int64()),
            (days_to_forward_fill, pl.Int64()),
        ]
    )
    forward_fill_latest_known_value_locations_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (col_to_forward_fill, pl.Int64()),
        ]
    )
    size_based_forward_fill_days_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (col_to_forward_fill, pl.Int64()),
        ]
    )
    expected_size_based_forward_fill_days_schema = pl.Schema(
        list(size_based_forward_fill_days_schema.items())
        + [(days_to_forward_fill, pl.Int32())]
    )


@dataclass
class NullFilledPostsUsingInvalidMissingDataCodeSchema:
    null_filled_posts_using_invalid_missing_data_code_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )


@dataclass
class NullGroupedProvidersSchema:
    null_grouped_providers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
            (IndCQC.pir_people_directly_employed_dedup, pl.Float64()),
            # Remove folloiwng when calculate_data_for_grouped_provider is converted to polars
            (NGPcol.location_pir_average, pl.Float64()),
            (NGPcol.count_of_cqc_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_with_data_in_provider, pl.Int64()),
            (NGPcol.number_of_beds_at_provider, pl.Int64()),
            (NGPcol.provider_pir_count, pl.Int64()),
            (NGPcol.provider_pir_sum, pl.Float64()),
        ]
    )

    calculate_data_for_grouped_provider_identification_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.pir_people_directly_employed_dedup, pl.Float64()),
        ]
    )
    expected_calculate_data_for_grouped_provider_identification_schema = pl.Schema(
        list(calculate_data_for_grouped_provider_identification_schema.items())
        + [
            (NGPcol.location_pir_average, pl.Float64()),
            (NGPcol.count_of_cqc_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_with_data_in_provider, pl.Int64()),
            (NGPcol.number_of_beds_at_provider, pl.Int64()),
            (NGPcol.provider_pir_count, pl.Int64()),
            (NGPcol.provider_pir_sum, pl.Float64()),
        ]
    )

    identify_potential_grouped_providers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (NGPcol.count_of_cqc_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_in_provider, pl.Int64()),
            (NGPcol.count_of_awcwds_locations_with_data_in_provider, pl.Int64()),
        ]
    )
    expected_identify_potential_grouped_providers_schema = pl.Schema(
        list(identify_potential_grouped_providers_schema.items())
        + [
            (NGPcol.potential_grouped_provider, pl.Boolean()),
        ]
    )

    null_care_home_grouped_providers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.number_of_beds, pl.Int64()),
            (NGPcol.number_of_beds_at_provider, pl.Int64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (NGPcol.potential_grouped_provider, pl.Boolean()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )

    null_non_res_grouped_providers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (NGPcol.potential_grouped_provider, pl.Boolean()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (NGPcol.location_pir_average, pl.Float64()),
            (NGPcol.provider_pir_count, pl.Int64()),
            (NGPcol.provider_pir_sum, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )


@dataclass
class CleanAscwdsFilledPostOutliersSchema:
    unfiltered_ind_cqc_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
        ]
    )


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersSchema:
    ind_cqc_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.number_of_beds_banded, pl.Float64()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )

    filter_df_to_care_homes_with_known_beds_and_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
        ]
    )

    select_data_not_in_subset_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("other_col", pl.String()),
        ]
    )

    calculate_average_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.number_of_beds_banded, pl.Float64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    expected_calculate_average_filled_posts_schema = pl.Schema(
        [
            (IndCQC.number_of_beds_banded, pl.Float64()),
            (IndCQC.avg_filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    calculate_expected_filled_posts_base_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.number_of_beds_banded, pl.Float64()),
        ]
    )

    calculate_expected_filled_posts_join_schema = pl.Schema(
        [
            (IndCQC.number_of_beds_banded, pl.Float64()),
            (IndCQC.avg_filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    expected_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.number_of_beds_banded, pl.Float64()),
            (IndCQC.avg_filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.expected_filled_posts, pl.Float64()),
        ]
    )

    calculate_standardised_residuals_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.expected_filled_posts, pl.Float64()),
        ]
    )
    expected_calculate_standardised_residuals_schema = pl.Schema(
        list(calculate_standardised_residuals_schema.items())
        + [
            (IndCQC.standardised_residual, pl.Float64()),
        ]
    )

    standardised_residual_percentile_cutoff_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (IndCQC.standardised_residual, pl.Float64()),
        ]
    )

    expected_standardised_residual_percentile_cutoff_with_percentiles_schema = (
        pl.Schema(
            [
                (IndCQC.location_id, pl.String()),
                (IndCQC.primary_service_type, pl.String()),
                (IndCQC.standardised_residual, pl.Float64()),
                (IndCQC.lower_percentile, pl.Float64()),
                (IndCQC.upper_percentile, pl.Float64()),
            ]
        )
    )

    duplicate_ratios_within_standardised_residual_cutoff_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.standardised_residual, pl.Float64()),
            (IndCQC.lower_percentile, pl.Float64()),
            (IndCQC.upper_percentile, pl.Float64()),
        ]
    )

    expected_duplicate_ratios_within_standardised_residual_cutoff_schema = pl.Schema(
        list(duplicate_ratios_within_standardised_residual_cutoff_schema.items())
        + [
            (IndCQC.filled_posts_per_bed_ratio_within_std_resids, pl.Float64()),
        ]
    )

    min_and_max_permitted_ratios_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.filled_posts_per_bed_ratio_within_std_resids, pl.Float64()),
            (IndCQC.number_of_beds_banded, pl.Float64()),
        ]
    )
    expected_min_and_max_permitted_ratios_schema = pl.Schema(
        list(min_and_max_permitted_ratios_schema.items())
        + [
            (IndCQC.min_filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.max_filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    set_minimum_permitted_ratio_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    winsorize_outliers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.min_filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.max_filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    combine_dataframes_care_home_schema = pl.Schema(
        list(ind_cqc_schema.items())
        + [
            ("additional column", pl.Float64()),
        ]
    )

    combine_dataframes_non_care_home_schema = ind_cqc_schema

    expected_combined_dataframes_schema = combine_dataframes_non_care_home_schema
