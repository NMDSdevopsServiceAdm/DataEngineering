from dataclasses import dataclass
from typing import Any

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_values.categorical_column_values import PrimaryServiceType

# Existing share columns stand in for actuals and model predictions.
ACTUAL_SHARES = [EmpStatus.bank_or_pool_percentage, EmpStatus.agency_percentage]
PREDICTED_SHARES = [EmpStatus.permanent_percentage, EmpStatus.temporary_percentage]
NON_RES = PrimaryServiceType.non_residential
CARE_HOME = PrimaryServiceType.care_home_only


@dataclass
class ModelMetricsUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class TestModelMetricsUtilsData:
    cell_shares_are_worker_weighted_test_cases = [
        # Unweighted, non-res would be 0.4 for both.
        ModelMetricsUtilsTestCase(
            id="weights_each_row_by_its_workers",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.3, 0.5, 0.7],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.8],
                IndCQC.estimate_filled_posts_by_job_role: [3.0, 1.0, 2.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.35, 0.7],
                ACTUAL_SHARES[0]: [0.3, 0.8],
                ShareModel.cell_weight: [4.0, 2.0],
            },
        ),
    ]

    unknown_rows_excluded_from_cells_test_cases = [
        ModelMetricsUtilsTestCase(
            id="unknown_row_left_out_of_its_cells_shares_and_weight",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES],
                PREDICTED_SHARES[0]: [0.5, 0.9],
                ACTUAL_SHARES[0]: [0.4, None],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.cell_weight: [2.0],
            },
        ),
        # Keeping the row's workers without its prediction would give 0.1.
        ModelMetricsUtilsTestCase(
            id="row_without_a_prediction_left_out_of_its_cells_shares_and_weight",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES],
                PREDICTED_SHARES[0]: [0.5, None],
                ACTUAL_SHARES[0]: [0.4, 0.9],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.cell_weight: [2.0],
            },
        ),
        ModelMetricsUtilsTestCase(
            id="cell_without_any_known_rows_is_left_out",
            input_data={
                IndCQC.primary_service_type: [NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.5, 0.9],
                ACTUAL_SHARES[0]: [0.4, None],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.cell_weight: [2.0],
            },
        ),
    ]

    perfect_predictions_test_cases = [
        ModelMetricsUtilsTestCase(
            id="every_share_predicted_exactly",
            input_data={
                PREDICTED_SHARES[0]: [0.2, 0.5, 0.7],
                PREDICTED_SHARES[1]: [0.3, 0.1, 0.2],
                ACTUAL_SHARES[0]: [0.2, 0.5, 0.7],
                ACTUAL_SHARES[1]: [0.3, 0.1, 0.2],
                ShareModel.cell_weight: [1.0, 2.0, 3.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES,
                IndCQC.r2: [1.0, 1.0],
                ShareModel.mean_absolute_error: [0.0, 0.0],
            },
        ),
    ]

    # One exact cell and one 20 point miss: unweighted, that's R² 0.5 and error 10.
    larger_cells_weigh_more_test_cases = [
        ModelMetricsUtilsTestCase(
            id="miss_in_the_smaller_cell_counts_for_less",
            input_data={
                PREDICTED_SHARES[0]: [0.5, 0.7],
                ACTUAL_SHARES[0]: [0.5, 0.9],
                ShareModel.cell_weight: [3.0, 1.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [2 / 3],
                ShareModel.mean_absolute_error: [5.0],
            },
        ),
        ModelMetricsUtilsTestCase(
            id="miss_in_the_larger_cell_counts_for_more",
            input_data={
                PREDICTED_SHARES[0]: [0.5, 0.7],
                ACTUAL_SHARES[0]: [0.5, 0.9],
                ShareModel.cell_weight: [1.0, 3.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [0.0],
                ShareModel.mean_absolute_error: [15.0],
            },
        ),
    ]

    # Keeping the last cell's weight without its error would give R² 0.94 and error 5.
    cells_missing_a_share_test_cases = [
        ModelMetricsUtilsTestCase(
            id="cell_without_a_prediction_left_out",
            input_data={
                PREDICTED_SHARES[0]: [0.3, 0.5, None],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.9],
                ShareModel.cell_weight: [1.0, 1.0, 2.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [0.75],
                ShareModel.mean_absolute_error: [10.0],
            },
        ),
    ]

    scores_split_by_fold_test_cases = [
        ModelMetricsUtilsTestCase(
            id="each_fold_scored_on_its_own_cells",
            input_data={
                ModelEvaluation.fold: [0, 0, 1, 1],
                PREDICTED_SHARES[0]: [0.2, 0.6, 0.3, 0.5],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.2, 0.6],
                ShareModel.cell_weight: [1.0, 1.0, 1.0, 1.0],
            },
            expected_data={
                ModelEvaluation.fold: [0, 1],
                ShareModel.share: ACTUAL_SHARES[:1] * 2,
                IndCQC.r2: [1.0, 0.75],
                ShareModel.mean_absolute_error: [0.0, 10.0],
            },
        ),
    ]
