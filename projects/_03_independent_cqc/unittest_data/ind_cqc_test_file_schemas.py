from dataclasses import dataclass

from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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


@dataclass
class RunLinearRegressionModelSchema:
    feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )
