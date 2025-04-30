from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType


@dataclass
class ImputeIndCqcAscwdsAndPirData:
    cleaned_ind_cqc_rows = [
        (
            "1-001",
            date(2025, 1, 1),
            9,
            CareHome.care_home,
            PrimaryServiceType.care_home_with_nursing,
            25.0,
            2.5,
            10.0,
        ),
    ]


@dataclass
class ModelAndMergePirData:
    model_pir_filled_posts_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10),
        ("loc 2", date(2024, 1, 1), CareHome.not_care_home, None),
        ("loc 3", date(2024, 1, 1), CareHome.care_home, 10),
        ("loc 4", date(2024, 1, 1), CareHome.care_home, None),
    ]
    expected_model_pir_filled_posts_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10, 10.64384),
        ("loc 2", date(2024, 1, 1), CareHome.not_care_home, None, None),
        ("loc 3", date(2024, 1, 1), CareHome.care_home, 10, None),
        ("loc 4", date(2024, 1, 1), CareHome.care_home, None, None),
    ]

    blend_pir_and_ascwds_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10.0, 20.0),
    ]
    create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows = [
        ("loc 1", date(2024, 1, 1), None),
        ("loc 1", date(2024, 2, 1), 100.0),
        ("loc 1", date(2024, 3, 1), None),
    ]
    expected_create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows = [
        ("loc 1", date(2024, 1, 1), None, None),
        ("loc 1", date(2024, 2, 1), 100.0, 100.0),
        ("loc 1", date(2024, 3, 1), None, 100.0),
    ]

    create_repeated_ascwds_clean_column_when_missing_middle_data_rows = [
        ("loc 3", date(2024, 1, 1), 40.0),
        ("loc 3", date(2024, 2, 1), None),
        ("loc 3", date(2024, 3, 1), 60.0),
    ]
    expected_create_repeated_ascwds_clean_column_when_missing_middle_data_rows = [
        ("loc 3", date(2024, 1, 1), 40.0, 40.0),
        ("loc 3", date(2024, 2, 1), None, 40.0),
        ("loc 3", date(2024, 3, 1), 60.0, 60.0),
    ]

    create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows = [
        ("loc 1", date(2024, 1, 1), 100.0),
        ("loc 1", date(2024, 2, 1), None),
        ("loc 2", date(2024, 1, 1), 50.0),
        ("loc 2", date(2024, 2, 1), None),
    ]
    expected_create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows = [
        ("loc 1", date(2024, 1, 1), 100.0, 100.0),
        ("loc 1", date(2024, 2, 1), None, 100.0),
        ("loc 2", date(2024, 1, 1), 50.0, 50.0),
        ("loc 2", date(2024, 2, 1), None, 50.0),
    ]

    create_last_submission_columns_rows = [
        ("loc 1", date(2024, 1, 1), 10.0, None),
        ("loc 1", date(2024, 2, 1), None, 20.0),
        ("loc 2", date(2024, 1, 1), None, 30.0),
        ("loc 2", date(2024, 2, 1), 40.0, None),
        ("loc 3", date(2024, 1, 1), None, None),
        ("loc 3", date(2024, 2, 1), None, None),
        ("loc 4", date(2024, 1, 1), 50.0, None),
        ("loc 4", date(2024, 2, 1), None, None),
        ("loc 4", date(2024, 3, 1), 60.0, None),
        ("loc 4", date(2024, 4, 1), None, 70.0),
    ]
    expected_create_last_submission_columns_rows = [
        ("loc 1", date(2024, 1, 1), 10.0, None, date(2024, 1, 1), date(2024, 2, 1)),
        ("loc 1", date(2024, 2, 1), None, 20.0, date(2024, 1, 1), date(2024, 2, 1)),
        ("loc 2", date(2024, 1, 1), None, 30.0, date(2024, 2, 1), date(2024, 1, 1)),
        ("loc 2", date(2024, 2, 1), 40.0, None, date(2024, 2, 1), date(2024, 1, 1)),
        ("loc 3", date(2024, 1, 1), None, None, None, None),
        ("loc 3", date(2024, 2, 1), None, None, None, None),
        ("loc 4", date(2024, 1, 1), 50.0, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 2, 1), None, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 3, 1), 60.0, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 4, 1), None, 70.0, date(2024, 3, 1), date(2024, 4, 1)),
    ]

    # fmt: off
    create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 200.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 200.0, None, 200.0),
    ]
    create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, 200.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, 200.0, None, None),
    ]
    create_ascwds_pir_merged_column_when_asc_after_pir_rows = [
        ("loc 1", date(2020, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, 200.0, None),
        ("loc 1", date(2021, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None),
        ("loc 1", date(2022, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None),
        ("loc 1", date(2023, 1, 1), date(2023, 1, 1), date(2020, 1, 1), 10.0, None, 10.0),
    ]
    expected_create_ascwds_pir_merged_column_when_asc_after_pir_rows = [
        ("loc 1", date(2020, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, 200.0, None, None),
        ("loc 1", date(2021, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2023, 1, 1), date(2020, 1, 1), 10.0, None, 10.0, 10.0),
    ]
    create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 110.0, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, 110.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, 10.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 110.0, None, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, 110.0, 110.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, 10.0, None, None),
    ]
    create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 5.0, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, 5.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, 10.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 5.0, None, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, 5.0, 5.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, 10.0, None, None),
    ]
    # fmt: on


@dataclass
class TrainLinearRegressionModelData:
    feature_rows = [
        ("1-001", Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None),
    ]


@dataclass
class ModelMetrics:
    model_metrics_rows = [
        ("1-001", None, 50.0, Vectors.dense([10.0, 1.0, 0.0])),
        ("1-002", 37, 40.0, Vectors.dense([20.0, 0.0, 1.0])),
    ]

    calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8),
        ("1-002", None, 10.0, 43.2),
    ]
    expected_calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8, 3.2),
        ("1-002", None, 10.0, 43.2, -33.2),
    ]

    calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1),
        ("1-002", 2, 5.0, 6.0),
    ]
    expected_calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1, 5.0),
        ("1-002", 2, 5.0, 6.0, -7.0),
    ]

    generate_metric_rows = [
        ("1-001", 50.0, 46.8),
        ("1-002", 10.0, 12.2),
    ]


@dataclass
class RunLinearRegressionModelData:
    feature_rows = [
        ("1-001", 10, Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", 40, Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None, None),
    ]
