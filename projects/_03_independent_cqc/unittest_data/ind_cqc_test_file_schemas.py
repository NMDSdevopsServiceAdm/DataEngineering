from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    NonResWithAndWithoutDormancyCombinedColumns as NRModel_TempCol,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as RoC_TempCol,
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
    expected_convert_pir_to_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
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
class ImputeUtilsSchema:
    convert_care_home_ratios_to_posts_schema = StructType(
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

    combine_care_home_and_non_res_values_into_single_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )
    expected_combine_care_home_and_non_res_values_into_single_column_schema = (
        StructType(
            [
                *combine_care_home_and_non_res_values_into_single_column_schema,
                StructField(IndCQC.combined_ratio_and_filled_posts, DoubleType(), True),
            ]
        )
    )


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
            StructField(IndCQC.current_lsoa21, StringType(), True),
            StructField(IndCQC.current_msoa21, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class EstimateFilledPostsModelsUtils:
    enrich_model_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )

    test_non_res_model_name: str = "non_res_model"
    enrich_model_predictions_non_res_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.service_count, IntegerType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
            StructField(IndCQC.prediction_run_id, StringType(), True),
        ]
    )
    expected_enrich_model_ind_cqc_non_res_schema = StructType(
        [
            *enrich_model_ind_cqc_schema,
            StructField(test_non_res_model_name, FloatType(), True),
            StructField(f"{test_non_res_model_name}_run_id", StringType(), True),
        ]
    )

    test_care_home_model_name: str = IndCQC.care_home_model
    enrich_model_predictions_care_home_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
            StructField(IndCQC.prediction_run_id, StringType(), True),
        ]
    )
    expected_enrich_model_ind_cqc_care_home_schema = StructType(
        [
            *enrich_model_ind_cqc_schema,
            StructField(test_care_home_model_name, FloatType(), True),
            StructField(f"{test_care_home_model_name}_run_id", StringType(), True),
        ]
    )

    set_min_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    join_test_model: str = IndCQC.care_home_model
    join_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    join_prediction_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
            StructField(IndCQC.prediction_run_id, StringType(), True),
        ]
    )
    expected_join_without_run_id_schema = StructType(
        [
            *join_ind_cqc_schema,
            StructField(join_test_model, FloatType(), True),
        ]
    )
    expected_join_with_run_id_schema = StructType(
        [
            *join_ind_cqc_schema,
            StructField(join_test_model, FloatType(), True),
            StructField(f"{join_test_model}_run_id", StringType(), True),
        ]
    )


@dataclass
class EstimateNonResCTFilledPostsSchemas:
    estimates_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(
                IndCQC.ct_non_res_care_workers_employed_imputed, DoubleType(), True
            ),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
        ]
    )

    convert_to_all_posts_using_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(
                IndCQC.ct_non_res_care_workers_employed_imputed, DoubleType(), True
            ),
        ]
    )
    expected_convert_to_all_posts_using_ratio_schema = StructType(
        [
            *convert_to_all_posts_using_ratio_schema,
            StructField(IndCQC.ct_non_res_filled_post_estimate, DoubleType(), True),
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
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
            StructField(IndCQC.combined_ratio_and_filled_posts, DoubleType(), True),
            StructField(IndCQC.care_home_status_count, IntegerType(), True),
        ]
    )
    expected_primary_service_rate_of_change_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )

    remove_ineligible_locations_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(IndCQC.care_home_status_count, IntegerType(), True),
            StructField(RoC_TempCol.current_period, DoubleType(), True),
            StructField(RoC_TempCol.submission_count, IntegerType(), True),
        ]
    )

    calculate_submission_count_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RoC_TempCol.current_period, DoubleType(), True),
        ]
    )
    expected_calculate_submission_count_schema = StructType(
        [
            *calculate_submission_count_schema,
            StructField(RoC_TempCol.submission_count, IntegerType(), True),
        ]
    )

    interpolate_current_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RoC_TempCol.current_period, DoubleType(), True),
        ]
    )
    expected_interpolate_current_values_schema = StructType(
        [
            *interpolate_current_values_schema,
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
        ]
    )

    add_previous_value_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
        ]
    )
    expected_add_previous_value_column_schema = StructType(
        [
            *add_previous_value_column_schema,
            StructField(RoC_TempCol.previous_period_interpolated, DoubleType(), True),
        ]
    )

    calculate_primary_service_rolling_sums_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RoC_TempCol.current_period_cleaned, DoubleType(), True),
            StructField(RoC_TempCol.previous_period_cleaned, DoubleType(), True),
        ]
    )
    expected_calculate_primary_service_rolling_sums_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(RoC_TempCol.rolling_current_sum, DoubleType(), True),
            StructField(RoC_TempCol.rolling_previous_sum, DoubleType(), True),
        ]
    )


@dataclass
class ModelPrimaryServiceRateOfChangeCleaningSchemas:
    calculate_absolute_and_percentage_change_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(RoC_TempCol.previous_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.abs_change, DoubleType(), True),
            StructField(RoC_TempCol.perc_change, DoubleType(), True),
        ]
    )

    compute_non_res_threshold_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RoC_TempCol.previous_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.abs_change, DoubleType(), True),
            StructField(RoC_TempCol.perc_change, DoubleType(), True),
        ]
    )

    build_keep_condition_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.care_home, StringType(), False),
            StructField(RoC_TempCol.previous_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.abs_change, DoubleType(), True),
            StructField(RoC_TempCol.perc_change, DoubleType(), True),
            StructField("keep", BooleanType(), False),
        ]
    )

    apply_rate_of_change_cleaning_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(RoC_TempCol.previous_period_interpolated, DoubleType(), True),
            StructField(RoC_TempCol.current_period_interpolated, DoubleType(), True),
            StructField("keep", BooleanType(), False),
            StructField(RoC_TempCol.previous_period_cleaned, DoubleType(), True),
            StructField(RoC_TempCol.current_period_cleaned, DoubleType(), True),
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
            StructField(IndCQC.care_home_status_count, IntegerType(), True),
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

    calculate_rate_of_change_trendline_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.single_period_rate_of_change, DoubleType(), True),
        ]
    )
    expected_calculate_rate_of_change_trendline_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.number_of_beds_banded_roc, DoubleType(), True),
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


@dataclass
class IndCQCDataUtils:
    merge_columns_in_order_when_double_type_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("model_name_1", DoubleType(), True),
            StructField("model_name_2", DoubleType(), True),
            StructField("model_name_3", DoubleType(), True),
        ]
    )
    expected_merge_columns_in_order_when_double_type_schema = StructType(
        [
            *merge_columns_in_order_when_double_type_schema,
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    merge_columns_in_order_when_map_type_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ascwds_job_role_ratios, MapType(StringType(), DoubleType()), True
            ),
            StructField(
                IndCQC.ascwds_job_role_rolling_ratio,
                MapType(StringType(), DoubleType()),
                True,
            ),
        ]
    )
    expected_merge_columns_in_order_when_map_type_schema = StructType(
        [
            *merge_columns_in_order_when_map_type_schema,
            StructField(
                IndCQC.ascwds_job_role_ratios_merged,
                MapType(StringType(), DoubleType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_ratios_merged_source, StringType(), True
            ),
        ]
    )

    get_selected_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.posts_rolling_average_model, FloatType(), True),
        ]
    )
    expected_get_selected_value_schema = StructType(
        [
            *get_selected_value_schema,
            StructField("new_column", FloatType(), True),
        ]
    )

    nullify_ct_values_previous_to_first_submission_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField("a", DoubleType(), True),
            StructField("b", DoubleType(), True),
            StructField("c", StringType(), True),
        ]
    )


# converted to polars -> projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas.ForwardFillLatestKnownValue
@dataclass
class ForwardFillLatestKnownValue:
    col_to_forward_fill: str = "col_to_forward_fill"
    days_to_forward_fill: str = "days_to_forward_fill"
    last_known_date: str = "last_known_date"
    last_known_value: str = "last_known_value"

    size_based_forward_fill_days_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(col_to_forward_fill, IntegerType(), True),
        ]
    )
    expected_size_based_forward_fill_days_schema = StructType(
        [
            *size_based_forward_fill_days_schema,
            StructField(days_to_forward_fill, IntegerType(), True),
        ]
    )

    input_return_last_known_value_locations_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(col_to_forward_fill, IntegerType(), True),
        ]
    )
    expected_return_last_known_value_locations_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(last_known_date, DateType(), True),
            StructField(last_known_value, IntegerType(), True),
        ]
    )

    forward_fill_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(col_to_forward_fill, IntegerType(), True),
            StructField(last_known_date, DateType(), True),
            StructField(last_known_value, IntegerType(), True),
            StructField(days_to_forward_fill, IntegerType(), True),
        ]
    )
