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
    BooleanType,
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
    NonResWithAndWithoutDormancyCombinedColumns as NRModel_TempCol,
    PartitionKeys as Keys,
    PrimaryServiceRateOfChangeColumns as RoC_TempCol,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
    JobGroupLabels,
)


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

    create_estimate_filled_posts_job_group_columns_schema = StructType(
        [
            StructField(MainJobRoleLabels.care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.senior_care_worker, FloatType(), True),
            StructField(MainJobRoleLabels.senior_management, FloatType(), True),
            StructField(MainJobRoleLabels.middle_management, FloatType(), True),
            StructField(MainJobRoleLabels.registered_nurse, FloatType(), True),
            StructField(MainJobRoleLabels.social_worker, FloatType(), True),
            StructField(MainJobRoleLabels.admin_staff, FloatType(), True),
            StructField(MainJobRoleLabels.ancillary_staff, FloatType(), True),
        ]
    )
    expected_create_estimate_filled_posts_job_group_columns_schema = StructType(
        [
            *create_estimate_filled_posts_job_group_columns_schema,
            StructField(JobGroupLabels.direct_care, FloatType(), True),
            StructField(JobGroupLabels.managers, FloatType(), True),
            StructField(JobGroupLabels.regulated_professions, FloatType(), True),
            StructField(JobGroupLabels.other, FloatType(), True),
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


@dataclass
class CareHomeFeaturesSchema:
    clean_merged_data_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(IndCQC.specialisms_offered, ArrayType(StringType()), True),
            StructField(
                CQCLClean.imputed_regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.code, StringType(), True),
                        ]
                    )
                ),
            ),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(
                IndCQC.banded_bed_ratio_rolling_average_model, DoubleType(), True
            ),
            StructField(
                IndCQC.ascwds_rate_of_change_trendline_model, DoubleType(), True
            ),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class NonResAscwdsFeaturesSchema(object):
    basic_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.imputed_registration_date, DateType(), False),
            StructField(IndCQC.time_registered, IntegerType(), False),
            StructField(IndCQC.time_since_dormant, IntegerType(), True),
            StructField(IndCQC.current_region, StringType(), False),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(
                IndCQC.imputed_regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(IndCQC.name, StringType(), True),
                            StructField(IndCQC.code, StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
            StructField(IndCQC.specialisms_offered, ArrayType(StringType()), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.ascwds_pir_merged, DoubleType(), True),
            StructField(IndCQC.imputed_filled_post_model, DoubleType(), True),
            StructField(IndCQC.posts_rolling_average_model, DoubleType(), True),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), False),
            StructField(IndCQC.related_location, StringType(), True),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
            StructField(Keys.import_date, StringType(), False),
        ]
    )


@dataclass
class ValidateCareHomeIndCqcFeaturesData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(
                IndCQC.imputed_specialisms,
                ArrayType(
                    StructType([StructField(IndCQC.name, StringType(), True)]), True
                ),
                True,
            ),
        ]
    )
    care_home_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.features, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateFeaturesNonResASCWDSWithDormancyIndCqcSchema:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
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
            StructField(
                IndCQC.imputed_specialisms,
                ArrayType(
                    StructType([StructField(IndCQC.name, StringType(), True)]), True
                ),
                True,
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
class ValidateFeaturesNonResASCWDSWithoutDormancyIndCqcSchema:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
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
            StructField(
                IndCQC.imputed_specialisms,
                ArrayType(
                    StructType([StructField(IndCQC.name, StringType(), True)]), True
                ),
                True,
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
class ModelFeatures:
    vectorise_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField("col_1", FloatType(), True),
            StructField("col_2", IntegerType(), True),
            StructField("col_3", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    expected_vectorised_feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )

    expand_encode_and_extract_features_when_not_array_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField("categories", StringType(), True),
        ]
    )
    expected_expand_encode_and_extract_features_when_not_array_schema = StructType(
        [
            *expand_encode_and_extract_features_when_not_array_schema,
            StructField("has_A", IntegerType(), True),
            StructField("has_B", IntegerType(), True),
            StructField("has_C", IntegerType(), True),
        ]
    )

    expand_encode_and_extract_features_when_is_array_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField("categories", ArrayType(StringType()), True),
        ]
    )
    expected_expand_encode_and_extract_features_when_is_array_schema = StructType(
        [
            *expand_encode_and_extract_features_when_is_array_schema,
            StructField("has_A", IntegerType(), True),
            StructField("has_B", IntegerType(), True),
            StructField("has_C", IntegerType(), True),
        ]
    )

    cap_integer_at_max_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.service_count, IntegerType(), True),
        ]
    )
    expected_cap_integer_at_max_value_schema = StructType(
        [
            *cap_integer_at_max_value_schema,
            StructField(IndCQC.service_count_capped, IntegerType(), True),
        ]
    )

    add_array_column_count_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                IndCQC.gac_service_types,
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
    expected_add_array_column_count_schema = StructType(
        [
            *add_array_column_count_schema,
            StructField(IndCQC.service_count, IntegerType(), True),
        ]
    )

    add_date_index_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
        ]
    )
    expected_add_date_index_column_schema = StructType(
        [
            *add_date_index_column_schema,
            StructField(IndCQC.cqc_location_import_date_indexed, IntegerType(), False),
        ]
    )

    group_rural_urban_sparse_categories_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
        ]
    )
    expected_group_rural_urban_sparse_categories_schema = StructType(
        [
            *group_rural_urban_sparse_categories_schema,
            StructField(
                IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
                StringType(),
                True,
            ),
        ]
    )

    filter_without_dormancy_features_to_pre_2025_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
        ]
    )

    add_squared_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date_indexed, DoubleType(), True),
        ]
    )
    expected_add_squared_column_schema = StructType(
        [
            *add_squared_column_schema,
            StructField(
                IndCQC.cqc_location_import_date_indexed_squared, DoubleType(), True
            ),
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
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class EstimateFilledPostsModelsUtils:
    cleaned_cqc_schema = ModelCareHomes.care_homes_cleaned_ind_cqc_schema

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

    set_min_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    combine_care_home_ratios_and_non_res_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )
    expected_combine_care_home_ratios_and_non_res_posts_schema = StructType(
        [
            *combine_care_home_ratios_and_non_res_posts_schema,
            StructField(IndCQC.combined_ratio_and_filled_posts, DoubleType(), True),
        ]
    )

    convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.banded_bed_ratio_rolling_average_model, DoubleType(), True
            ),
            StructField(IndCQC.posts_rolling_average_model, DoubleType(), True),
        ]
    )

    create_test_and_train_datasets_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )

    train_lasso_regression_model_schema = StructType(
        [
            StructField(IndCQC.features, VectorUDT(), True),
            StructField(IndCQC.imputed_filled_post_model, DoubleType(), True),
        ]
    )


@dataclass
class ModelPrimaryServiceRateOfChange:
    primary_service_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.combined_ratio_and_filled_posts, DoubleType(), True),
        ]
    )
    expected_primary_service_rate_of_change_schema = StructType(
        [
            *primary_service_rate_of_change_schema,
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )

    clean_column_with_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RoC_TempCol.column_with_values, DoubleType(), True),
        ]
    )
    expected_clean_column_with_values_schema = StructType(
        [
            *clean_column_with_values_schema,
            StructField(RoC_TempCol.care_home_status_count, IntegerType(), True),
            StructField(RoC_TempCol.submission_count, IntegerType(), True),
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
            StructField(RoC_TempCol.care_home_status_count, IntegerType(), True),
        ]
    )

    calculate_submission_count_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RoC_TempCol.column_with_values, DoubleType(), True),
        ]
    )
    expected_calculate_submission_count_schema = StructType(
        [
            *calculate_submission_count_schema,
            StructField(RoC_TempCol.submission_count, IntegerType(), True),
        ]
    )

    interpolate_column_with_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RoC_TempCol.column_with_values, DoubleType(), True),
        ]
    )
    expected_interpolate_column_with_values_schema = StructType(
        [
            *interpolate_column_with_values_schema,
            StructField(
                RoC_TempCol.column_with_values_interpolated, DoubleType(), True
            ),
        ]
    )

    add_previous_value_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                RoC_TempCol.column_with_values_interpolated, DoubleType(), True
            ),
        ]
    )
    expected_add_previous_value_column_schema = StructType(
        [
            *add_previous_value_column_schema,
            StructField(
                RoC_TempCol.previous_column_with_values_interpolated, DoubleType(), True
            ),
        ]
    )

    add_rolling_sum_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                RoC_TempCol.column_with_values_interpolated, DoubleType(), True
            ),
            StructField(
                RoC_TempCol.previous_column_with_values_interpolated, DoubleType(), True
            ),
        ]
    )
    expected_add_rolling_sum_columns_schema = StructType(
        [
            *add_rolling_sum_columns_schema,
            StructField(RoC_TempCol.rolling_current_period_sum, DoubleType(), True),
            StructField(RoC_TempCol.rolling_previous_period_sum, DoubleType(), True),
        ]
    )

    calculate_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(RoC_TempCol.rolling_current_period_sum, DoubleType(), True),
            StructField(RoC_TempCol.rolling_previous_period_sum, DoubleType(), True),
        ]
    )
    expected_calculate_rate_of_change_schema = StructType(
        [
            *calculate_rate_of_change_schema,
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )


@dataclass
class ModelPrimaryServiceRateOfChangeTrendlineSchemas:
    primary_service_rate_of_change_trendline_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.combined_ratio_and_filled_posts, DoubleType(), True),
        ]
    )
    expected_primary_service_rate_of_change_trendline_schema = StructType(
        [
            *primary_service_rate_of_change_trendline_schema,
            StructField(
                IndCQC.ascwds_rate_of_change_trendline_model, DoubleType(), True
            ),
        ]
    )
    calculate_rate_of_change_trendline_mock_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_rate_of_change_trendline_model, DoubleType(), True
            ),
        ]
    )

    deduplicate_dataframe_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
            StructField("another_col", DoubleType(), True),
        ]
    )
    expected_deduplicate_dataframe_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )

    calculate_rate_of_change_trendline_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )
    expected_calculate_rate_of_change_trendline_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(
                IndCQC.number_of_beds_banded_for_rate_of_change, DoubleType(), True
            ),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(
                IndCQC.ascwds_rate_of_change_trendline_model, DoubleType(), True
            ),
        ]
    )


@dataclass
class ModelRollingAverageSchemas:
    rolling_average_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
        ]
    )
    expected_rolling_average_schema = StructType(
        [
            *rolling_average_schema,
            StructField(IndCQC.posts_rolling_average_model, FloatType(), True),
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
            StructField(IndCQC.posts_rolling_average_model, DoubleType(), False),
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
            StructField(IndCQC.posts_rolling_average_model, FloatType(), False),
        ]
    )
    expected_extrapolation_forwards_schema = StructType(
        [
            *extrapolation_forwards_schema,
            StructField(IndCQC.extrapolation_forwards, FloatType(), True),
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
            StructField(IndCQC.posts_rolling_average_model, FloatType(), False),
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
            StructField(IndCQC.time_between_submissions, IntegerType(), True),
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
            StructField(IndCQC.time_between_submissions, IntegerType(), True),
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
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
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
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class ModelNonResWithAndWithoutDormancyCombinedSchemas:
    estimated_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.related_location, StringType(), True),
            StructField(IndCQC.time_registered, IntegerType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
        ]
    )

    group_time_registered_to_six_month_bands_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.time_registered, IntegerType(), False),
        ]
    )
    expected_group_time_registered_to_six_month_bands_schema = StructType(
        [
            *group_time_registered_to_six_month_bands_schema,
            StructField(
                NRModel_TempCol.time_registered_banded_and_capped, IntegerType(), False
            ),
        ]
    )

    calculate_and_apply_model_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.related_location, StringType(), True),
            StructField(
                NRModel_TempCol.time_registered_banded_and_capped, IntegerType(), True
            ),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
        ]
    )
    expected_calculate_and_apply_model_ratios_schema = StructType(
        [
            *calculate_and_apply_model_ratios_schema,
            StructField(NRModel_TempCol.avg_with_dormancy, FloatType(), True),
            StructField(NRModel_TempCol.avg_without_dormancy, FloatType(), True),
            StructField(NRModel_TempCol.adjustment_ratio, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
        ]
    )

    average_models_by_related_location_and_time_registered_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.related_location, StringType(), True),
            StructField(
                NRModel_TempCol.time_registered_banded_and_capped, IntegerType(), True
            ),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
        ]
    )
    expected_average_models_by_related_location_and_time_registered_schema = StructType(
        [
            StructField(IndCQC.related_location, StringType(), True),
            StructField(
                NRModel_TempCol.time_registered_banded_and_capped, IntegerType(), True
            ),
            StructField(NRModel_TempCol.avg_with_dormancy, FloatType(), True),
            StructField(NRModel_TempCol.avg_without_dormancy, FloatType(), True),
        ]
    )

    calculate_adjustment_ratios_schema = StructType(
        [
            StructField(IndCQC.related_location, StringType(), True),
            StructField(
                NRModel_TempCol.time_registered_banded_and_capped, IntegerType(), True
            ),
            StructField(NRModel_TempCol.avg_with_dormancy, FloatType(), True),
            StructField(NRModel_TempCol.avg_without_dormancy, FloatType(), True),
        ]
    )
    expected_calculate_adjustment_ratios_schema = StructType(
        [
            *calculate_adjustment_ratios_schema,
            StructField(NRModel_TempCol.adjustment_ratio, FloatType(), True),
        ]
    )

    apply_model_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(NRModel_TempCol.adjustment_ratio, FloatType(), True),
        ]
    )
    expected_apply_model_ratios_schema = StructType(
        [
            *apply_model_ratios_schema,
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
        ]
    )

    calculate_and_apply_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
        ]
    )
    expected_calculate_and_apply_residuals_schema = StructType(
        [
            *calculate_and_apply_residuals_schema,
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )

    calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(NRModel_TempCol.first_overlap_date, DateType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
        ]
    )
    expected_calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
        ]
    )

    apply_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
        ]
    )
    expected_apply_residuals_schema = StructType(
        [
            *apply_residuals_schema,
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )

    combine_model_predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )
    expected_combine_model_predictions_schema = StructType(
        [
            *combine_model_predictions_schema,
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )
    expected_calculate_and_apply_residuals_schema = StructType(
        [
            *calculate_and_apply_residuals_schema,
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )

    calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(NRModel_TempCol.first_overlap_date, DateType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
        ]
    )
    expected_calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
        ]
    )

    apply_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted,
                FloatType(),
                True,
            ),
            StructField(NRModel_TempCol.residual_at_overlap, FloatType(), True),
        ]
    )
    expected_apply_residuals_schema = StructType(
        [
            *apply_residuals_schema,
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )

    combine_model_predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(
                NRModel_TempCol.non_res_without_dormancy_model_adjusted_and_residual_applied,
                FloatType(),
                True,
            ),
        ]
    )
    expected_combine_model_predictions_schema = StructType(
        [
            *combine_model_predictions_schema,
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
class DiagnosticsOnKnownFilledPostsSchemas:
    estimate_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.ascwds_pir_merged, FloatType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.posts_rolling_average_model, FloatType(), True),
            StructField(IndCQC.care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_combined_model, FloatType(), True),
            StructField(IndCQC.imputed_pir_filled_posts_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_non_res_combined_model, FloatType(), True),
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
            StructField(IndCQC.posts_rolling_average_model, FloatType(), True),
            StructField(IndCQC.care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_care_home_model, FloatType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.non_res_with_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_without_dormancy_model, FloatType(), True),
            StructField(IndCQC.non_res_combined_model, FloatType(), True),
            StructField(IndCQC.imputed_pir_filled_posts_model, FloatType(), True),
            StructField(IndCQC.imputed_posts_non_res_combined_model, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.number_of_beds_banded, DoubleType(), True),
            StructField(IndCQC.unix_time, IntegerType(), True),
            StructField(IndCQC.ct_care_home_import_date, DateType(), True),
            StructField(IndCQC.ct_care_home_total_employed, IntegerType(), True),
            StructField(IndCQC.ct_non_res_import_date, DateType(), True),
            StructField(IndCQC.ct_non_res_care_workers_employed, IntegerType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    convert_to_all_posts_using_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ct_non_res_care_workers_employed_imputed, FloatType(), True
            ),
        ]
    )
    expected_convert_to_all_posts_using_ratio_schema = StructType(
        [
            *convert_to_all_posts_using_ratio_schema,
            StructField(IndCQC.ct_non_res_filled_post_estimate, FloatType(), True),
        ]
    )
    calculate_care_worker_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ct_non_res_care_workers_employed_imputed, FloatType(), True
            ),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
        ]
    )


@dataclass
class DiagnosticsUtilsSchemas:
    filter_to_known_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField("other_column", FloatType(), True),
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
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
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
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
        ]
    )
    expected_calculate_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
        ]
    )
    expected_calculate_absolute_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.absolute_residual, FloatType(), True),
        ]
    )
    expected_calculate_percentage_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.percentage_residual, FloatType(), True),
        ]
    )
    expected_calculate_standardised_residual_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
            StructField(IndCQC.residual, FloatType(), True),
            StructField(IndCQC.standardised_residual, FloatType(), True),
        ]
    )
    expected_calculate_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
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
