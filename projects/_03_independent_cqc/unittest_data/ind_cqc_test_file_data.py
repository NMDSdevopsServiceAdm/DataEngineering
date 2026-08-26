from dataclasses import dataclass
from datetime import date

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    EstimateFilledPostsSource,
    PrimaryServiceType,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)


@dataclass
class DiagnosticsOnKnownFilledPostsData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            10.0,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
        ),
    ]


@dataclass
class DiagnosticsOnCapacityTrackerData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            11.0,
            None,
        ),
        (
            "loc 1",
            date(2024, 2, 2),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            11.0,
            None,
        ),
        (
            "loc 2",
            date(2024, 1, 1),
            CareHome.not_care_home,
            PrimaryServiceType.non_residential,
            10.0,
            None,
            None,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            10.0,
        ),
    ]


@dataclass
class DiagnosticsUtilsData:
    filter_to_known_values_rows = [
        ("loc 1", 1.0, 1.0),
        ("loc 2", None, 1.0),
        ("loc 3", 2.0, None),
    ]

    expected_filter_to_known_values_rows = [
        ("loc 1", 1.0, 1.0),
        ("loc 3", 2.0, None),
    ]
    list_of_models = ["model_type_one", "model_type_two"]
    expected_list_of_models = [
        *CatValues.estimate_filled_posts_source_column_values.categorical_values,
        IndCQC.estimate_filled_posts,
    ]
    restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            PrimaryServiceType.care_home_only,
            13.0,
            12.0,
        ),
    ]
    expected_restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_one",
            13.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_two",
            12.0,
        ),
    ]
    # fmt: off
    calculate_distribution_metrics_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0),
    ]
    expected_calculate_distribution_mean_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0),
    ]
    expected_calculate_distribution_standard_deviation_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 7.0710678118654755),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 7.0710678118654755),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 7.0710678118654755),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 7.0710678118654755),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 7.0710678118654755),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 7.0710678118654755),
    ]
    expected_calculate_distribution_kurtosis_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, -2.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, -2.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, -2.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, -2.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, -2.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, -2.0),
    ]
    expected_calculate_distribution_skewness_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 0.0),
    ]
    expected_calculate_distribution_metrics_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0, 7.0710678118654755, -2.0, 0.0),
    ]
    # fmt: on

    calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0),
        ("loc 2", 5.0, 10.0),
    ]
    expected_calculate_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0),
        ("loc 2", 5.0, 10.0, 5.0),
    ]
    expected_calculate_absolute_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0),
        ("loc 2", 5.0, 10.0, 5.0, 5.0),
    ]
    expected_calculate_percentage_residual_rows = [
        ("loc 1", 10.0, 5.0, -1.0),
        ("loc 2", 5.0, 10.0, 0.5),
    ]
    expected_calculate_standardised_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 2.23606798),
    ]
    expected_calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0, -1.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 5.0, 0.5, 2.23606798),
    ]
    # fmt: off
    calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4),
    ]
    expected_calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1, 35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2,  35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
    ]
    # fmt: on

    # fmt: off
    create_summary_dataframe_rows = [
        ("loc 1", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 2", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 4", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 5", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
        ("loc 6", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    expected_create_summary_dataframe_rows = [
        (PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 1.0, 2.0, 3.0, 4.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 3.0, 4.0, 5.0, 6.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    # fmt: on
