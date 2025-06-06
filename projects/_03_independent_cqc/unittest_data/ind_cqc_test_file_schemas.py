from dataclasses import dataclass

from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    LongType,
    IntegerType,
    FloatType,
    DoubleType,
    DateType,
    ArrayType,
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
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class MergeIndCQCData:
    clean_cqc_location_for_merge_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )

    data_to_merge_without_care_home_col_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )
    expected_merged_without_care_home_col_schema = StructType(
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

    data_to_merge_with_care_home_col_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), False),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(
                CQCPIRClean.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
        ]
    )
    expected_merged_with_care_home_col_schema = StructType(
        [
            *clean_cqc_location_for_merge_schema,
            StructField(
                CQCPIRClean.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
        ]
    )


@dataclass
class ValidateMergedIndCqcData:
    cqc_locations_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )
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
            StructField(
                IndCQC.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
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
class ImputeIndCqcAscwdsAndPirSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, FloatType(), True),
        ]
    )


@dataclass
class ModelAndMergePirData:
    model_pir_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    expected_model_pir_filled_posts_schema = StructType(
        [
            *model_pir_filled_posts_schema,
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
        ]
    )

    blend_pir_and_ascwds_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
        ]
    )

    create_repeated_ascwds_clean_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
        ]
    )
    expected_create_repeated_ascwds_clean_column_schema = StructType(
        [
            *create_repeated_ascwds_clean_column_schema,
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, FloatType(), True
            ),
        ]
    )

    create_last_submission_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
        ]
    )
    expected_create_last_submission_columns_schema = StructType(
        [
            *create_last_submission_columns_schema,
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
        ]
    )

    create_ascwds_pir_merged_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, FloatType(), True
            ),
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
        ]
    )
    expected_create_ascwds_pir_merged_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, FloatType(), True
            ),
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
        ]
    )

    include_pir_if_never_submitted_ascwds_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.pir_filled_posts_model, FloatType(), True),
        ]
    )

    drop_temporary_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.last_ascwds_submission, DateType(), True),
            StructField(IndCQC.last_pir_submission, DateType(), True),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean_repeated, FloatType(), True
            ),
        ]
    )
    expected_drop_temporary_columns = [IndCQC.location_id]


@dataclass
class ValidateImputedIndCqcAscwdsAndPir:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    imputed_ind_cqc_ascwds_and_pir_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.imputed_registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class TrainLinearRegressionModelSchema:
    feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class ModelMetrics:
    model_metrics_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )

    calculate_residual_non_res_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
        ]
    )
    expected_calculate_residual_non_res_schema = StructType(
        [
            *calculate_residual_non_res_schema,
            StructField(IndCQC.residual, FloatType(), True),
        ]
    )
    calculate_residual_care_home_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.care_home_model, FloatType(), True),
        ]
    )
    expected_calculate_residual_care_home_schema = StructType(
        [
            *calculate_residual_care_home_schema,
            StructField(IndCQC.residual, FloatType(), True),
        ]
    )

    generate_metric_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    generate_proportion_of_predictions_within_range_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.residual, FloatType(), True),
        ]
    )

    combine_metrics_current_schema = StructType(
        [
            StructField(IndCQC.model_name, StringType(), True),
            StructField(IndCQC.model_version, StringType(), True),
            StructField(IndCQC.run_number, StringType(), True),
            StructField(IndCQC.r2, FloatType(), True),
            StructField(IndCQC.rmse, FloatType(), True),
            StructField(IndCQC.prediction_within_10_posts, FloatType(), True),
            StructField(IndCQC.prediction_within_25_posts, FloatType(), True),
        ]
    )
    combine_metrics_previous_schema = StructType(
        [
            StructField(IndCQC.model_name, StringType(), True),
            StructField(IndCQC.model_version, StringType(), True),
            StructField(IndCQC.r2, FloatType(), True),
        ]
    )
    expected_combined_metrics_schema = combine_metrics_current_schema


@dataclass
class RunLinearRegressionModelSchema:
    feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
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
class EstimateIndCQCFilledPostsByJobRoleSchemas:
    estimated_ind_cqc_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(
                IndCQC.registered_manager_names, ArrayType(StringType(), True), True
            ),
        ]
    )
    cleaned_ascwds_worker_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(IndCQC.main_job_role_clean_labelled, StringType(), True),
        ]
    )


class EstimateIndCQCFilledPostsByJobRoleUtilsSchemas:
    test_map_column: str = "test_map_column"
    create_map_column_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(MainJobRoleLabels.care_worker, IntegerType(), True),
            StructField(MainJobRoleLabels.registered_nurse, IntegerType(), True),
            StructField(MainJobRoleLabels.senior_care_worker, IntegerType(), True),
            StructField(MainJobRoleLabels.senior_management, IntegerType(), True),
        ]
    )
    expected_create_map_column_schema = StructType(
        [
            *create_map_column_schema,
            StructField(
                test_map_column,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )
    expected_create_map_column_when_drop_columns_is_true_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(
                test_map_column,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    aggregate_ascwds_worker_with_additional_column_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.main_job_role_clean_labelled, StringType(), True),
        ]
    )

    aggregate_ascwds_worker_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(IndCQC.main_job_role_clean_labelled, StringType(), True),
        ]
    )
    expected_aggregate_ascwds_worker_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    estimated_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
        ]
    )
    aggregated_job_role_breakdown_df = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )
    merged_job_role_estimate_schema = StructType(
        [
            *estimated_filled_posts_schema,
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    create_total_from_values_in_map_column_when_counts_are_longs_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), LongType()),
                True,
            ),
        ]
    )
    expected_create_total_from_values_in_map_column_when_counts_are_longs_schema = (
        StructType(
            [
                *create_total_from_values_in_map_column_when_counts_are_longs_schema,
                StructField("temp_total_count_of_worker_records", LongType(), True),
            ]
        )
    )

    create_total_from_values_in_map_column_when_counts_are_doubles_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), DoubleType()),
                True,
            ),
        ]
    )
    expected_create_total_from_values_in_map_column_when_counts_are_doubles_schema = (
        StructType(
            [
                *create_total_from_values_in_map_column_when_counts_are_doubles_schema,
                StructField("temp_total_count_of_worker_records", DoubleType(), True),
            ]
        )
    )

    create_ratios_from_counts_when_counts_are_longs_schema = (
        expected_create_total_from_values_in_map_column_when_counts_are_longs_schema
    )
    expected_create_ratios_from_counts_when_counts_are_longs_schema = StructType(
        [
            *create_ratios_from_counts_when_counts_are_longs_schema,
            StructField(
                IndCQC.ascwds_job_role_ratios, MapType(StringType(), DoubleType()), True
            ),
        ]
    )

    create_ratios_from_counts_when_counts_are_doubles_schema = (
        expected_create_total_from_values_in_map_column_when_counts_are_doubles_schema
    )
    expected_create_ratios_from_counts_when_counts_are_doubles_schema = StructType(
        [
            *create_ratios_from_counts_when_counts_are_doubles_schema,
            StructField(
                IndCQC.ascwds_job_role_ratios,
                MapType(StringType(), DoubleType()),
                True,
            ),
        ]
    )

    create_estimate_filled_posts_by_job_role_map_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(
                IndCQC.ascwds_job_role_ratios_merged,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_create_estimate_filled_posts_by_job_role_map_column_schema = StructType(
        [
            *create_estimate_filled_posts_by_job_role_map_column_schema,
            StructField(
                IndCQC.estimate_filled_posts_by_job_role,
                MapType(StringType(), DoubleType()),
                True,
            ),
        ]
    )

    remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_schema = (
        StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
                StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
                StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
                StructField(
                    IndCQC.ascwds_job_role_counts,
                    MapType(StringType(), IntegerType()),
                    True,
                ),
            ]
        )
    )

    count_registered_manager_names_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(
                IndCQC.registered_manager_names, ArrayType(StringType(), True), True
            ),
        ]
    )
    expected_count_registered_manager_names_schema = StructType(
        [
            *count_registered_manager_names_schema,
            StructField(IndCQC.registered_manager_count, IntegerType(), True),
        ]
    )

    sum_job_role_split_by_service_schema = StructType(
        [
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_worker_import_date, DateType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
        ]
    )
    expected_sum_job_role_split_by_service_schema = StructType(
        [
            *sum_job_role_split_by_service_schema,
            StructField(
                IndCQC.ascwds_job_role_rolling_sum,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    interpolate_job_role_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_job_role_ratios_filtered,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_interpolate_job_role_ratios_schema = StructType(
        [
            *interpolate_job_role_ratios_schema,
            StructField(
                IndCQC.ascwds_job_role_ratios_interpolated,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )

    extrapolate_job_role_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_job_role_ratios_filtered,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_extrapolate_job_role_ratios_schema = StructType(
        [
            *extrapolate_job_role_ratios_schema.fields,
            StructField(
                IndCQC.ascwds_job_role_ratios_extrapolated,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )

    pivot_job_role_column_schema = StructType(
        [
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.main_job_role_clean_labelled, StringType(), False),
            StructField(IndCQC.ascwds_job_role_ratios_interpolated, FloatType(), True),
        ]
    )
    expected_pivot_job_role_column_schema = StructType(
        [
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(MainJobRoleLabels.care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.registered_nurse, FloatType(), True),
            StructField(MainJobRoleLabels.senior_care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.senior_management, FloatType(), True),
        ]
    )
    expected_pivot_job_role_column_two_job_roles_schema = StructType(
        [
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(MainJobRoleLabels.care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.registered_nurse, FloatType(), True),
        ]
    )

    convert_map_with_all_null_values_to_null_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, StringType(), False),
            StructField(
                IndCQC.ascwds_job_role_ratios_interpolated,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_convert_map_with_all_null_values_to_null_schema = StructType(
        [
            *convert_map_with_all_null_values_to_null_schema,
        ]
    )

    unpacked_mapped_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(
                IndCQC.estimate_filled_posts_by_job_role,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_unpacked_mapped_column_schema = StructType(
        [
            *unpacked_mapped_column_schema,
            StructField(MainJobRoleLabels.care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.registered_nurse, FloatType(), True),
            StructField(MainJobRoleLabels.senior_care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.senior_management, FloatType(), True),
        ]
    )

    non_rm_managerial_estimate_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(MainJobRoleLabels.senior_management, FloatType(), True),
            StructField(MainJobRoleLabels.middle_management, FloatType(), True),
            StructField(MainJobRoleLabels.first_line_manager, FloatType(), True),
            StructField(MainJobRoleLabels.supervisor, FloatType(), True),
            StructField(MainJobRoleLabels.other_managerial_staff, FloatType(), True),
            StructField(MainJobRoleLabels.deputy_manager, FloatType(), True),
            StructField(MainJobRoleLabels.team_leader, FloatType(), True),
            StructField(MainJobRoleLabels.data_governance_manager, FloatType(), True),
            StructField(MainJobRoleLabels.it_manager, FloatType(), True),
            StructField(MainJobRoleLabels.it_service_desk_manager, FloatType(), True),
        ]
    )
    expected_non_rm_managerial_estimate_filled_posts_schema = StructType(
        [
            *non_rm_managerial_estimate_filled_posts_schema,
            StructField(
                IndCQC.sum_non_rm_managerial_estimated_filled_posts, FloatType(), True
            ),
            StructField(
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )

    estimate_and_cqc_registered_manager_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(MainJobRoleLabels.registered_manager, FloatType(), True),
            StructField(IndCQC.registered_manager_count, IntegerType(), True),
        ]
    )
    expected_estimate_and_cqc_registered_manager_schema = StructType(
        [
            *estimate_and_cqc_registered_manager_schema,
            StructField(
                IndCQC.difference_between_estimate_and_cqc_registered_managers,
                FloatType(),
                True,
            ),
        ]
    )

    sum_job_group_counts_from_job_role_count_map_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )
    expected_sum_job_group_counts_from_job_role_count_map_schema = StructType(
        [
            *sum_job_group_counts_from_job_role_count_map_schema,
            StructField(
                IndCQC.ascwds_job_group_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )
    sum_job_group_counts_from_job_role_count_map_for_patching_create_map_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_job_group_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    filter_ascwds_job_role_map_when_dc_or_manregprof_1_or_more_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(
                IndCQC.ascwds_job_role_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_group_counts,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )
    expected_filter_ascwds_job_role_map_when_dc_or_manregprof_1_or_more_schema = (
        StructType(
            [
                *filter_ascwds_job_role_map_when_dc_or_manregprof_1_or_more_schema,
                StructField(
                    IndCQC.ascwds_job_role_counts_filtered,
                    MapType(StringType(), IntegerType()),
                    True,
                ),
            ]
        )
    )

    filter_ascwds_job_role_count_map_when_job_group_ratios_outside_percentile_boundaries_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(
                IndCQC.ascwds_job_group_ratios,
                MapType(StringType(), FloatType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_counts_filtered,
                MapType(StringType(), IntegerType()),
                True,
            ),
        ]
    )

    transform_imputed_job_role_ratios_to_counts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(
                IndCQC.imputed_ascwds_job_role_ratios,
                MapType(StringType(), FloatType()),
            ),
        ]
    )
    expected_transform_imputed_job_role_ratios_to_counts_schema = StructType(
        [
            *transform_imputed_job_role_ratios_to_counts_schema,
            StructField(
                IndCQC.imputed_ascwds_job_role_counts,
                MapType(StringType(), FloatType()),
            ),
        ]
    )

    recalculate_managerial_filled_posts_non_rm_col_list = [
        "managerial_role_1",
        "managerial_role_2",
        "managerial_role_3",
        "managerial_role_4",
    ]
    recalculate_managerial_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField("managerial_role_1", FloatType(), True),
            StructField("managerial_role_2", FloatType(), True),
            StructField("managerial_role_3", FloatType(), True),
            StructField("managerial_role_4", FloatType(), True),
            StructField(
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role,
                MapType(StringType(), FloatType()),
                True,
            ),
            StructField(
                IndCQC.difference_between_estimate_and_cqc_registered_managers,
                FloatType(),
                True,
            ),
        ]
    )

    recalculate_total_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(MainJobRoleLabels.care_worker, FloatType(), False),
            StructField(MainJobRoleLabels.registered_nurse, FloatType(), False),
            StructField(MainJobRoleLabels.senior_care_worker, FloatType(), False),
            StructField(MainJobRoleLabels.senior_management, FloatType(), False),
        ]
    )
    expected_recalculate_total_filled_posts_schema = StructType(
        [
            *recalculate_total_filled_posts_schema,
            StructField(
                IndCQC.estimate_filled_posts_from_all_job_roles, FloatType(), False
            ),
        ]
    )

    overwrite_registered_manager_estimate_with_cqc_count_schema = StructType(
        [
            StructField(MainJobRoleLabels.registered_manager, FloatType(), False),
            StructField(IndCQC.registered_manager_count, IntegerType(), False),
        ]
    )

    combine_interpolated_and_extrapolated_job_role_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.ascwds_job_role_ratios_filtered,
                MapType(StringType(), FloatType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_ratios_interpolated,
                MapType(StringType(), FloatType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_ratios_extrapolated,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_combine_interpolated_and_extrapolated_job_role_ratios_schema = StructType(
        [
            *combine_interpolated_and_extrapolated_job_role_ratios_schema,
            StructField(
                IndCQC.imputed_ascwds_job_role_ratios,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )

    calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_schema = StructType(
        [
            StructField(IndCQC.estimate_filled_posts, FloatType(), False),
            StructField(
                IndCQC.estimate_filled_posts_from_all_job_roles, FloatType(), False
            ),
        ]
    )
    expected_calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_schema = StructType(
        [
            *calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_schema,
            StructField(
                IndCQC.difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles,
                FloatType(),
                False,
            ),
        ]
    )


@dataclass
class EstimateJobRolesPrimaryServiceRollingSumSchemas:
    add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_schema = StructType(
        [
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.main_job_role_clean_labelled, StringType(), False),
            StructField(IndCQC.ascwds_job_role_counts_exploded, FloatType(), True),
        ]
    )
    expected_add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_schema = StructType(
        [
            *add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_schema,
            StructField(IndCQC.ascwds_job_role_rolling_sum, FloatType(), True),
        ]
    )

    primary_service_rolling_sum_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.imputed_ascwds_job_role_counts,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )
    expected_primary_service_rolling_sum_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.imputed_ascwds_job_role_counts,
                MapType(StringType(), FloatType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_rolling_sum,
                MapType(StringType(), FloatType()),
                True,
            ),
        ]
    )


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
            StructField(
                CQCPIRClean.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
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
            StructField(
                IndCQC.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
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
