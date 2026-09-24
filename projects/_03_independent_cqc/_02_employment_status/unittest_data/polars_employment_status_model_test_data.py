from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
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
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    RUI,
    CareHome,
    CQCLatestRating,
    CQCRatingsValues,
    CurrentCSSR,
    ImputationRowKind,
    PrimaryServiceType,
    PublishedJobRoleLabels,
    Region,
    Services,
    Specialisms,
)

KNOWN_SHARE = EmpStatus.permanent_percentage_clean
# Existing columns stand in for the imputed and rolling average shares.
IMPUTED_SHARE = EmpStatus.permanent_percentage
ROLLING_AVERAGE_SHARE = IndCQC.posts_rolling_average_model

CARE_WORKER = PublishedJobRoleLabels.care_worker
REGISTERED_NURSE = PublishedJobRoleLabels.registered_nurse
KNOWN = ImputationRowKind.known
INTERPOLATED = ImputationRowKind.interpolated
CARRIED = ImputationRowKind.carried

JAN_TO_JUN = [date(2024, month, 1) for month in range(1, 7)]
JAN_TO_MAR = JAN_TO_JUN[:3]

LATEST = CQCLatestRating.is_latest_rating
NOT_LATEST = CQCLatestRating.not_latest_rating


@dataclass
class ModelUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class AddLatestOverallRatingTestCase:
    id: str
    input_data: dict[str, Any]
    ratings_data: dict[str, Any]
    expected_data: dict[str, Any]


def one_location_rating_case(
    id: str,
    ratings: list[tuple[str | None, str, str | None, int]],
    expected_rating: str,
) -> AddLatestOverallRatingTestCase:
    """One location, dated after its ratings: (rating, date, assessment date, latest flag)."""
    overall_ratings, rating_dates, assessment_dates, flags = map(list, zip(*ratings))
    location = {
        IndCQC.location_id: ["loc1"],
        IndCQC.cqc_location_import_date: [date(2025, 1, 1)],
    }
    return AddLatestOverallRatingTestCase(
        id=id,
        input_data=location,
        ratings_data={
            IndCQC.location_id: ["loc1"] * len(ratings),
            CQCRatings.overall_rating: overall_ratings,
            CQCRatings.date: rating_dates,
            CQCL.assessment_date: assessment_dates,
            CQCRatings.latest_rating_flag: flags,
        },
        expected_data={**location, ShareModel.latest_overall_rating: [expected_rating]},
    )


@dataclass
class FoldSafeRollingAverageTestCase:
    id: str
    n_folds: int
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


def fold_safe_case(
    id: str,
    folds: list[int],
    shares: list[float | None],
    expected_averages: list[float],
    n_folds: int = 2,
    existing_averages: list[float] | None = None,
) -> FoldSafeRollingAverageTestCase:
    """One location per row, in one service and date, so averages come from other folds."""
    rows = {
        IndCQC.location_id: [f"loc{i}" for i in range(len(folds))],
        IndCQC.published_job_role_label: [CARE_WORKER] * len(folds),
        IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * len(folds),
        IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * len(folds),
        ModelEvaluation.fold: folds,
        IMPUTED_SHARE: shares,
    }
    existing = (
        {} if existing_averages is None else {ROLLING_AVERAGE_SHARE: existing_averages}
    )
    return FoldSafeRollingAverageTestCase(
        id=id,
        n_folds=n_folds,
        input_data={**rows, **existing},
        expected_data={**rows, ROLLING_AVERAGE_SHARE: expected_averages},
    )


@dataclass
class TestModelUtilsData:
    add_elapsed_months_test_cases = [
        ModelUtilsTestCase(
            id="quarterly_step_counts_as_three_months",
            input_data={
                IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 4, 1)],
            },
            expected_data={
                IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 4, 1)],
                ShareModel.elapsed_months: [0, 3],
            },
        ),
        ModelUtilsTestCase(
            id="counts_across_the_year_end",
            input_data={
                IndCQC.cqc_location_import_date: [date(2023, 11, 1), date(2024, 2, 1)],
            },
            expected_data={
                IndCQC.cqc_location_import_date: [date(2023, 11, 1), date(2024, 2, 1)],
                ShareModel.elapsed_months: [0, 3],
            },
        ),
        ModelUtilsTestCase(
            id="counts_from_the_earliest_date_across_all_locations",
            input_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 3, 1), date(2024, 1, 1)],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 3, 1), date(2024, 1, 1)],
                ShareModel.elapsed_months: [2, 0],
            },
        ),
    ]

    add_imputation_row_kind_test_cases = [
        ModelUtilsTestCase(
            id="labels_every_kind_along_a_location_roles_dates",
            input_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 6,
                IndCQC.cqc_location_import_date: JAN_TO_JUN,
                KNOWN_SHARE: [None, 0.5, None, 0.7, None, None],
                IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.7, 0.7, None],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 6,
                IndCQC.cqc_location_import_date: JAN_TO_JUN,
                KNOWN_SHARE: [None, 0.5, None, 0.7, None, None],
                IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.7, 0.7, None],
                ShareModel.imputation_row_kind: [
                    CARRIED,
                    KNOWN,
                    INTERPOLATED,
                    KNOWN,
                    CARRIED,
                    None,
                ],
            },
        ),
        ModelUtilsTestCase(
            id="uses_each_location_roles_own_known_dates",
            input_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3
                + [REGISTERED_NURSE] * 3,
                IndCQC.cqc_location_import_date: JAN_TO_MAR * 2,
                KNOWN_SHARE: [0.5, None, 0.7, None, 0.4, None],
                IMPUTED_SHARE: [0.5, 0.6, 0.7, 0.4, 0.4, 0.4],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3
                + [REGISTERED_NURSE] * 3,
                IndCQC.cqc_location_import_date: JAN_TO_MAR * 2,
                KNOWN_SHARE: [0.5, None, 0.7, None, 0.4, None],
                IMPUTED_SHARE: [0.5, 0.6, 0.7, 0.4, 0.4, 0.4],
                ShareModel.imputation_row_kind: [
                    KNOWN,
                    INTERPOLATED,
                    KNOWN,
                    CARRIED,
                    KNOWN,
                    CARRIED,
                ],
            },
        ),
    ]

    add_provider_location_count_test_cases = [
        ModelUtilsTestCase(
            id="counts_a_location_once_across_its_job_roles",
            input_data={
                IndCQC.provider_id: ["prov1"] * 3,
                IndCQC.location_id: ["loc1", "loc1", "loc2"],
                IndCQC.published_job_role_label: [
                    CARE_WORKER,
                    REGISTERED_NURSE,
                    CARE_WORKER,
                ],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 3,
            },
            expected_data={
                IndCQC.provider_id: ["prov1"] * 3,
                IndCQC.location_id: ["loc1", "loc1", "loc2"],
                IndCQC.published_job_role_label: [
                    CARE_WORKER,
                    REGISTERED_NURSE,
                    CARE_WORKER,
                ],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 3,
                ShareModel.provider_location_count: [2, 2, 2],
            },
        ),
        ModelUtilsTestCase(
            id="counts_each_provider_and_date_separately",
            input_data={
                IndCQC.provider_id: ["prov1", "prov1", "prov1", "prov2"],
                IndCQC.location_id: ["loc1", "loc2", "loc1", "loc3"],
                IndCQC.published_job_role_label: [CARE_WORKER] * 4,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                ],
            },
            expected_data={
                IndCQC.provider_id: ["prov1", "prov1", "prov1", "prov2"],
                IndCQC.location_id: ["loc1", "loc2", "loc1", "loc3"],
                IndCQC.published_job_role_label: [CARE_WORKER] * 4,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                ],
                ShareModel.provider_location_count: [2, 2, 1, 1],
            },
        ),
    ]

    latest_rating_joined_per_location_test_cases = [
        AddLatestOverallRatingTestCase(
            id="uses_the_latest_rating_not_older_ones",
            input_data={
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 2, 1)],
            },
            ratings_data={
                IndCQC.location_id: ["loc1", "loc1"],
                CQCRatings.overall_rating: [
                    CQCRatingsValues.requires_improvement,
                    CQCRatingsValues.good,
                ],
                CQCRatings.date: ["2023-01-01", "2024-01-01"],
                CQCL.assessment_date: [None, None],
                CQCRatings.latest_rating_flag: [NOT_LATEST, LATEST],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 2, 1)],
                ShareModel.latest_overall_rating: [CQCRatingsValues.good] * 2,
            },
        ),
        AddLatestOverallRatingTestCase(
            id="each_location_gets_its_own_rating",
            input_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 2,
            },
            ratings_data={
                IndCQC.location_id: ["loc1", "loc2"],
                CQCRatings.overall_rating: [
                    CQCRatingsValues.good,
                    CQCRatingsValues.outstanding,
                ],
                CQCRatings.date: ["2024-01-01", "2024-01-01"],
                CQCL.assessment_date: [None, None],
                CQCRatings.latest_rating_flag: [LATEST, LATEST],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 2,
                ShareModel.latest_overall_rating: [
                    CQCRatingsValues.good,
                    CQCRatingsValues.outstanding,
                ],
            },
        ),
    ]

    # A rating counts from its own date, so the 2023-01-01 row gets it.
    rating_as_of_import_date_test_cases = [
        AddLatestOverallRatingTestCase(
            id="uses_the_rating_in_place_on_each_date",
            input_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2022, 6, 1),
                    date(2023, 1, 1),
                    date(2024, 6, 1),
                ],
            },
            ratings_data={
                IndCQC.location_id: ["loc1", "loc1"],
                CQCRatings.overall_rating: [
                    CQCRatingsValues.requires_improvement,
                    CQCRatingsValues.good,
                ],
                CQCRatings.date: ["2023-01-01", "2024-01-01"],
                CQCL.assessment_date: [None, None],
                CQCRatings.latest_rating_flag: [NOT_LATEST, LATEST],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2022, 6, 1),
                    date(2023, 1, 1),
                    date(2024, 6, 1),
                ],
                ShareModel.latest_overall_rating: [
                    CQCRatingsValues.not_yet_rated,
                    CQCRatingsValues.requires_improvement,
                    CQCRatingsValues.good,
                ],
            },
        ),
    ]

    blank_latest_rating_test_cases = [
        one_location_rating_case(
            id="uses_the_most_recent_real_rating",
            ratings=[
                (CQCRatingsValues.requires_improvement, "2022-01-01", None, NOT_LATEST),
                (CQCRatingsValues.good, "2023-01-01", None, NOT_LATEST),
                (None, "2024-01-01", None, LATEST),
            ],
            expected_rating=CQCRatingsValues.good,
        ),
    ]

    # The expected rating is listed second, so these fail if row order decides.
    same_date_ratings_test_cases = [
        one_location_rating_case(
            id="later_assessment_date_comes_first",
            ratings=[
                (CQCRatingsValues.good, "2024-06-01", "2024-03-01", NOT_LATEST),
                (CQCRatingsValues.outstanding, "2024-06-01", "2024-04-01", NOT_LATEST),
                (None, "2024-07-01", None, LATEST),
            ],
            expected_rating=CQCRatingsValues.outstanding,
        ),
        one_location_rating_case(
            id="latest_rating_flag_breaks_a_tie",
            ratings=[
                (CQCRatingsValues.good, "2024-06-01", "2024-04-01", NOT_LATEST),
                (CQCRatingsValues.outstanding, "2024-06-01", "2024-04-01", LATEST),
            ],
            expected_rating=CQCRatingsValues.outstanding,
        ),
    ]

    unrated_location_test_cases = [
        AddLatestOverallRatingTestCase(
            id="location_missing_from_the_ratings",
            input_data={
                IndCQC.location_id: ["loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            },
            ratings_data={
                IndCQC.location_id: ["loc2"],
                CQCRatings.overall_rating: [CQCRatingsValues.good],
                CQCRatings.date: ["2024-01-01"],
                CQCL.assessment_date: [None],
                CQCRatings.latest_rating_flag: [LATEST],
            },
            expected_data={
                IndCQC.location_id: ["loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                ShareModel.latest_overall_rating: [CQCRatingsValues.not_yet_rated],
            },
        ),
        one_location_rating_case(
            id="location_with_only_blank_ratings",
            ratings=[
                (None, "2023-01-01", None, NOT_LATEST),
                (None, "2024-01-01", None, LATEST),
            ],
            expected_rating=CQCRatingsValues.not_yet_rated,
        ),
    ]

    existing_rating_replaced_test_case = AddLatestOverallRatingTestCase(
        id="rating_from_an_earlier_run",
        input_data={
            IndCQC.location_id: ["loc1"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            ShareModel.latest_overall_rating: [CQCRatingsValues.inadequate],
        },
        ratings_data={
            IndCQC.location_id: ["loc1"],
            CQCRatings.overall_rating: [CQCRatingsValues.good],
            CQCRatings.date: ["2024-01-01"],
            CQCL.assessment_date: [None],
            CQCRatings.latest_rating_flag: [LATEST],
        },
        expected_data={
            IndCQC.location_id: ["loc1"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            ShareModel.latest_overall_rating: [CQCRatingsValues.good],
        },
    )

    # loc1 has two roles, two dates and two ratings, so a bad join would duplicate its rows.
    build_modelling_dataset_shares_data = {
        IndCQC.location_id: ["loc1"] * 4 + ["loc2"] * 2,
        IndCQC.provider_id: ["prov1"] * 6,
        IndCQC.cqc_location_import_date: JAN_TO_MAR[:2] * 3,
        IndCQC.published_job_role_label: [CARE_WORKER] * 2
        + [REGISTERED_NURSE] * 2
        + [CARE_WORKER] * 2,
        IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 6,
        IndCQC.care_home: [CareHome.not_care_home] * 6,
        IndCQC.current_region: [Region.london] * 6,
        IndCQC.services_offered: [[Services.domiciliary_care_service]] * 6,
        IndCQC.estimate_filled_posts_by_job_role: [8.0, 9.0, 2.0, 2.0, 5.0, 5.0],
        KNOWN_SHARE: [0.5, None, 0.6, None, None, None],
        IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.6, None, None],
        ROLLING_AVERAGE_SHARE: [0.55] * 6,
    }
    build_modelling_dataset_estimates_data = {
        IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
        IndCQC.cqc_location_import_date: JAN_TO_MAR[:2] * 2,
        IndCQC.specialisms_offered: [[Specialisms.dementia]] * 4,
        IndCQC.regulated_activities_offered: [["Personal care"]] * 4,
        IndCQC.current_rural_urban_indicator_2011: [RUI.urban_city] * 4,
        IndCQC.current_cssr: [CurrentCSSR.leeds] * 4,
        IndCQC.time_registered: [12, 13, 40, 41],
        IndCQC.estimate_filled_posts: [10.0, 11.0, 5.0, 5.0],
    }
    build_modelling_dataset_ratings_data = {
        IndCQC.location_id: ["loc1", "loc1", "loc2"],
        CQCRatings.overall_rating: [
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.outstanding,
        ],
        CQCRatings.date: ["2023-01-01", "2024-01-01", "2024-01-01"],
        CQCL.assessment_date: [None, None, None],
        CQCRatings.latest_rating_flag: [NOT_LATEST, LATEST, LATEST],
    }

    # Averaging all 3 shares would give 0.5 on every row.
    tested_fold_excluded_test_cases = [
        fold_safe_case(
            id="leaves_out_only_the_tested_fold",
            folds=[0, 1, 2],
            shares=[0.2, 0.4, 0.9],
            expected_averages=[0.65, 0.55, 0.3],
            n_folds=3,
        ),
        fold_safe_case(
            id="leaves_out_every_location_in_the_tested_fold",
            folds=[0, 1, 1],
            shares=[0.2, 0.4, 0.9],
            expected_averages=[0.65, 0.2, 0.2],
        ),
    ]

    tested_fold_gets_group_value_test_cases = [
        fold_safe_case(
            id="location_without_its_own_share_gets_its_groups_value",
            folds=[0, 1, 1],
            shares=[0.2, 0.6, None],
            expected_averages=[0.6, 0.2, 0.2],
        ),
    ]

    existing_averages_replaced_test_case = fold_safe_case(
        id="averages_from_all_folds_are_replaced",
        folds=[0, 1, 1],
        shares=[0.2, 0.4, 0.9],
        expected_averages=[0.65, 0.2, 0.2],
        existing_averages=[0.5, 0.5, 0.5],
    )


ACTUAL_SHARES = [
    EmpStatus.permanent_percentage_clean,
    EmpStatus.temporary_percentage_clean,
]
# Existing share columns stand in for model predictions.
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
