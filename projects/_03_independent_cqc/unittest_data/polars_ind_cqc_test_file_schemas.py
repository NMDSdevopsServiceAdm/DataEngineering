from dataclasses import dataclass
from typing import Final

import polars as pl

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
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
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    NonResWithAndWithoutDormancyCombinedColumns as NRModel_TempCol,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    NullGroupedProviderColumns as NGPcol,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

EXPANDED_ID: Final[str] = "expanded_id"
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


@dataclass
class PrepareJobRoleCountsUtilsSchemas:
    aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
        ]
    )
    expected_aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.UInt32()),
        ]
    )

    filter_to_cqc_locations_schema = pl.Schema(
        [
            (AWKClean.location_id, pl.String()),
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
class ValidateCleanIndCQCSchemas:
    clean_ind_cqc_schema = pl.Schema(
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
            (IndCQC.ascwds_filtering_rule, pl.String()),
            (IndCQC.specialism_dementia, pl.String()),
            (IndCQC.specialism_learning_disabilities, pl.String()),
            (IndCQC.specialism_mental_health, pl.String()),
            (IndCQC.time_registered, pl.Int32()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
        ]
    )

    merged_ind_cqc_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int64()),
        ]
    )

    expected_size_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.name, pl.String()),
            (IndCQC.postcode, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.imputed_registration_date, pl.Date()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
        ]
    )


@dataclass
class ValidateImputedIndCqcAscwdsAndPir:
    cleaned_ind_cqc_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    imputed_ind_cqc_ascwds_and_pir_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
            (IndCQC.cqc_pir_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.provider_id, pl.String()),
            (IndCQC.cqc_sector, pl.String()),
            (IndCQC.imputed_registration_date, pl.Date()),
            (IndCQC.dormancy, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
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
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.pir_people_directly_employed_dedup, pl.Int64()),
            (IndCQC.unix_time, pl.Int64()),
            (IndCQC.pir_people_directly_employed_cleaned, pl.Int64()),
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )


@dataclass
class ValidateEstimatedIndCQCFilledPostsSchemas:
    imputed_ind_cqc_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    estimated_ind_cqc_filled_posts_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.cqc_sector, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.primary_service_type, pl.String()),
            (IndCQC.primary_service_type_second_level, pl.String()),
            (IndCQC.current_ons_import_date, pl.Date()),
            (IndCQC.current_cssr, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.pir_people_directly_employed_cleaned, pl.Int64()),
            (IndCQC.total_staff_bounded, pl.Int64()),
            (IndCQC.worker_records_bounded, pl.Int64()),
            (IndCQC.ascwds_filled_posts_source, pl.String()),
            (IndCQC.ascwds_filled_posts, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.pir_people_directly_employed_dedup, pl.Int64()),
            (IndCQC.ascwds_pir_merged, pl.Float64()),
            (IndCQC.unix_time, pl.Int64()),
            (IndCQC.estimate_filled_posts, pl.Float64()),
            (IndCQC.estimate_filled_posts_source, pl.String()),
            (IndCQC.posts_rolling_average_model, pl.Float64()),
            (IndCQC.care_home_model, pl.Float64()),
            (IndCQC.imputed_posts_non_res_combined_model, pl.Float64()),
            (IndCQC.non_res_with_dormancy_model, pl.Float64()),
            (IndCQC.non_res_without_dormancy_model, pl.Float64()),
            (IndCQC.imputed_pir_filled_posts_model, pl.Float64()),
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

    repeated_value_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.provider_id, pl.String()),
            ("integer_column", pl.Int64()),
            (IndCQC.cqc_location_import_date, pl.Date()),
        ]
    )

    expected_without_repeated_values_schema = pl.Schema(
        [
            *repeated_value_schema.items(),
            ("integer_column_deduplicated", pl.Int64()),
        ]
    )

    calculate_care_home_status_count_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
        ]
    )
    expected_calculate_care_home_status_count_schema = pl.Schema(
        [
            *calculate_care_home_status_count_schema.items(),
            (IndCQC.care_home_status_count, pl.UInt32()),
        ]
    )
    merged_schema_for_cleaning_job = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (ONSClean.current_region, pl.String()),
            (CQCLClean.current_cssr, pl.String()),
            (CQCLClean.current_rural_urban_ind_11, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int64()),
            (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
            (AWPClean.total_staff_bounded, pl.Int64()),
            (AWPClean.worker_records_bounded, pl.Int64()),
            (CQCLClean.primary_service_type, pl.String()),
            (IndCQC.name, pl.String()),
            (IndCQC.postcode, pl.String()),
            (IndCQC.imputed_registration_date, pl.Date()),
        ]
    )


@dataclass
class ImputeIndCqcAscwdsAndPirSchema:
    expected_rolling_average_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.posts_rolling_average_model, pl.Float64()),
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
        ]
    )

    grouped_provider_schema = pl.Schema(
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
    expected_grouped_provider_schema = pl.Schema(
        list(grouped_provider_schema.items())
        + [
            (NGPcol.location_pir_average, pl.Float64()),
            (NGPcol.count_of_cqc_locations_in_provider, pl.UInt32()),
            (NGPcol.count_of_awcwds_locations_in_provider, pl.UInt32()),
            (NGPcol.count_of_awcwds_locations_with_data_in_provider, pl.UInt32()),
            (NGPcol.number_of_beds_at_provider, pl.Int64()),
            (NGPcol.provider_pir_count, pl.UInt32()),
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


@dataclass
class ConvertPirPeopleToFilledPostsSchema:
    input_schema = pl.Schema(
        [
            (IndCQC.care_home, pl.String()),
            (IndCQC.pir_people_directly_employed_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
        ]
    )
    output_schema = pl.Schema(
        list(input_schema.items()) + [(IndCQC.pir_filled_posts_model, pl.Float64())]
    )


@dataclass
class NullCtPostsToBedsOutliers:
    null_ct_posts_to_beds_outliers_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ct_care_home_total_employed, pl.Int64()),
            (IndCQC.ct_care_home_posts_per_bed_ratio, pl.Float64()),
            (IndCQC.ct_care_home_total_employed_cleaned, pl.Int64()),
            (IndCQC.ct_care_home_filtering_rule, pl.String()),
        ]
    )


@dataclass
class NullValuesExceedingRepetitionLimitSchema:
    input_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ct_non_res_care_workers_employed_cleaned: pl.Int64,
        IndCQC.ct_non_res_filtering_rule: pl.String,
    }


@dataclass
class NullLongitudinalOutliersSchema:
    input_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.ct_non_res_care_workers_employed_cleaned: pl.Int64,
            IndCQC.ct_non_res_filtering_rule: pl.String,
        }
    )


@dataclass
class EstimateFilledPostsModelsUtils:
    enrich_model_ind_cqc_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.care_home: pl.String,
            IndCQC.number_of_beds: pl.Int64,
        }
    )

    test_non_res_model_name: str = "non_res_model"
    enrich_model_predictions_non_res_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.service_count: pl.Int64,
            IndCQC.prediction: pl.Float64,
            IndCQC.prediction_run_id: pl.String,
        }
    )
    expected_enrich_model_ind_cqc_non_res_schema = pl.Schema(
        {
            **enrich_model_ind_cqc_schema,
            test_non_res_model_name: pl.Float64,
            f"{test_non_res_model_name}_run_id": pl.String,
        }
    )

    test_care_home_model_name: str = IndCQC.care_home_model
    enrich_model_predictions_care_home_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.number_of_beds: pl.Int64,
            IndCQC.prediction: pl.Float64,
            IndCQC.prediction_run_id: pl.String,
        }
    )
    expected_enrich_model_ind_cqc_care_home_schema = pl.Schema(
        {
            **enrich_model_ind_cqc_schema,
            test_care_home_model_name: pl.Float64,
            f"{test_care_home_model_name}_run_id": pl.String,
        }
    )

    join_test_model: str = IndCQC.care_home_model
    join_ind_cqc_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.current_region: pl.String,
            IndCQC.number_of_beds: pl.Int64,
            IndCQC.cqc_location_import_date: pl.Date,
        }
    )
    join_prediction_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.number_of_beds: pl.Int64,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.prediction: pl.Float64,
            IndCQC.prediction_run_id: pl.String,
        }
    )
    expected_join_without_run_id_schema = pl.Schema(
        {
            **join_ind_cqc_schema,
            join_test_model: pl.Float64,
        }
    )
    expected_join_with_run_id_schema = pl.Schema(
        {
            **join_ind_cqc_schema,
            join_test_model: pl.Float64,
            f"{join_test_model}_run_id": pl.String,
        }
    )


@dataclass
class ModelNonResWithAndWithoutDormancyCombinedSchemas:
    estimated_posts_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.care_home: pl.String,
            IndCQC.related_location: pl.String,
            IndCQC.time_registered: pl.Int64,
            IndCQC.non_res_without_dormancy_model: pl.Float64,
            IndCQC.non_res_with_dormancy_model: pl.Float64,
            IndCQC.non_res_combined_model: pl.Float64,
        }
    )

    group_time_registered_to_six_month_bands_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.time_registered: pl.Int64,
            NRModel_TempCol.time_registered_banded_and_capped: pl.Float64,
        }
    )

    calculate_and_apply_model_ratios_schema = pl.Schema(
        {
            IndCQC.related_location: pl.String,
            NRModel_TempCol.time_registered_banded_and_capped: pl.Int64,
            IndCQC.non_res_without_dormancy_model: pl.Float64,
            IndCQC.non_res_with_dormancy_model: pl.Float64,
            NRModel_TempCol.avg_with_dormancy: pl.Float64,
            NRModel_TempCol.avg_without_dormancy: pl.Float64,
            NRModel_TempCol.adjustment_ratio: pl.Float64,
            NRModel_TempCol.non_res_without_dormancy_model_adjusted: pl.Float64,
        }
    )

    calculate_and_apply_residuals_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.non_res_with_dormancy_model: pl.Float64,
            NRModel_TempCol.non_res_without_dormancy_model_adjusted: pl.Float64,
            NRModel_TempCol.residual_at_overlap: pl.Float64,
            NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied: pl.Float64,
        }
    )


@dataclass
class EstimateNonResCapacityTrackerFilledPostsSchemas:
    expected_estimate_non_res_capacity_tracker_filled_posts_schema = pl.Schema(
        {
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.care_home: pl.String,
            IndCQC.ct_non_res_care_workers_employed_imputed: pl.Float32,
            IndCQC.estimate_filled_posts: pl.Float32,
            IndCQC.ct_non_res_all_posts: pl.Float32,
            IndCQC.ct_non_res_filled_post_estimate: pl.Float32,
            IndCQC.ct_non_res_filled_post_estimate_source: pl.String,
        }
    )


@dataclass
class TestJoinEstimatesToAscwds:
    TEST_ROLES = ["role_a", "role_b"]
    estimates_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.ascwds_workplace_import_date: pl.String,
            IndCQC.establishment_id: pl.String,
        }
    )
    ascwds_schema = pl.Schema(
        {
            IndCQC.ascwds_workplace_import_date: pl.String,
            IndCQC.establishment_id: pl.String,
            IndCQC.main_job_role_clean_labelled: pl.Enum(TEST_ROLES),
            "value": pl.Float64,
        }
    )
    expected_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(TEST_ROLES),
            "value": pl.Float64,
        }
    )


@dataclass
class ImputeJobRoleSchemas:

    create_imputed_ascwds_job_role_counts_expected_schema = {
        EXPANDED_ID: pl.UInt32,
        IndCQC.location_id: pl.String,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_job_role_counts: pl.Int64,
        IndCQC.estimate_filled_posts: pl.Float32,
        IndCQC.ascwds_job_role_ratios: pl.Float32,  # extra col
        IndCQC.imputed_ascwds_job_role_ratios: pl.Float32,  # extra col
        IndCQC.imputed_ascwds_job_role_counts: pl.Float32,  # extra col
    }

    create_ascwds_job_role_rolling_ratio_expected_schema = {
        EXPANDED_ID: pl.UInt16,
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.primary_service_type: pl.String,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.imputed_ascwds_job_role_counts: pl.Float32,
        IndCQC.ascwds_job_role_rolling_sum: pl.Float32,
        IndCQC.ascwds_job_role_rolling_ratio: pl.Float32,
    }

    managerial_adjustment_core_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.registered_manager_names: pl.List,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.estimate_filled_posts_by_job_role: pl.Float64,
    }

    managerial_adjustment_expected_schema = managerial_adjustment_core_schema | {
        # These output columns will be used by different tests.
        "diff": pl.Float64,
        "proportions": pl.Float64,
        "adjusted_estimates": pl.Float64,
    }


@dataclass
class EstimateFilledPostsByJobRoleEstimateUtilsSchemas:
    calculate_estimated_filled_posts_by_job_role_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.estimate_filled_posts: pl.Float32,
            IndCQC.ascwds_job_role_ratios: pl.Float32,
            IndCQC.imputed_ascwds_job_role_ratios: pl.Float32,
            IndCQC.ascwds_job_role_rolling_ratio: pl.Float32,
            IndCQC.ascwds_job_role_ratios_merged_source: pl.String,
            IndCQC.ascwds_job_role_ratios_merged: pl.Float32,
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
        }
    )

    has_rm_in_cqc_rm_name_list_flag_schema = pl.Schema(
        {
            IndCQC.registered_manager_names: pl.List(pl.String),
            IndCQC.registered_manager_count: pl.UInt32,
        }
    )

    adjust_managerial_roles_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
            IndCQC.registered_manager_count: pl.Float32,
        }
    )
    expected_adjust_managerial_roles_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
        }
    )

    expected_calculate_reg_man_difference_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
            IndCQC.registered_manager_count: pl.Float32,
            IndCQC.difference_between_estimate_and_cqc_registered_managers: pl.Float32,
        }
    )

    expected_calculate_non_rm_managerial_distribution_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role: pl.Float32,
        }
    )

    expected_distribute_rm_difference_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role: pl.Float32,
            IndCQC.registered_manager_count: pl.Int32,
            IndCQC.difference_between_estimate_and_cqc_registered_managers: pl.Float32,
            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role: pl.Float32,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
        }
    )

    expected_calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_schema = pl.Schema(
        {
            "id": pl.Int32,
            IndCQC.estimate_filled_posts: pl.Float32,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
            IndCQC.estimate_filled_posts_from_all_job_roles: pl.Float32,
            IndCQC.difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles: pl.Float32,
        }
    )
