from dataclasses import dataclass

from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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
            StructField(IndCQC.ct_care_home_total_employed_imputed, DoubleType(), True),
            StructField(IndCQC.ct_non_res_filled_post_estimate, DoubleType(), True),
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
