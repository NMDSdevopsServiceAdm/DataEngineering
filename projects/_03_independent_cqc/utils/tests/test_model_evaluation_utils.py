import numpy as np
import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.model_evaluation_utils as job
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    FOLD_SEED,
)
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    TestModelEvaluationUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)


ROW_COUNT_SCHEMA = {ModelEvaluation.number_of_rows: pl.UInt32}


class TestAssignLocationFolds:
    @staticmethod
    def assign_folds(location_ids: list[str], n_folds: int) -> pl.LazyFrame:
        return job.assign_location_folds(
            pl.LazyFrame({IndCQC.location_id: location_ids}),
            location_column=IndCQC.location_id,
            n_folds=n_folds,
            seed=FOLD_SEED,
        )

    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in Data.location_rows_share_a_fold_test_cases],
    )
    def test_location_rows_share_a_fold(self, case):
        returned_df = self.assign_folds(case.location_ids, case.n_folds).collect()

        folds_per_location = returned_df.group_by(IndCQC.location_id).agg(
            pl.col(ModelEvaluation.fold).n_unique()
        )
        # No rows gained or lost: the fold join mustn't fan out or drop rows.
        assert returned_df.height == len(case.location_ids)
        # Every row got a fold: each location found a match in the join.
        assert returned_df[ModelEvaluation.fold].null_count() == 0
        # Each location sits in one fold, so it's never in both training and test.
        assert (folds_per_location[ModelEvaluation.fold] == 1).all()

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.folds_are_balanced_test_cases]
    )
    def test_folds_are_balanced(self, case):
        locations_per_fold = (
            self.assign_folds(case.location_ids, case.n_folds)
            .group_by(ModelEvaluation.fold)
            .agg(pl.col(IndCQC.location_id).n_unique())
            .collect()[IndCQC.location_id]
        )

        # Every fold is used, so none is empty.
        assert locations_per_fold.len() == case.n_folds
        # Location counts per fold differ by at most one.
        assert locations_per_fold.max() - locations_per_fold.min() <= 1

    def test_folds_repeat_for_same_seed(self):
        case = Data.folds_repeat_for_same_seed_test_case

        first_lf = self.assign_folds(case.location_ids, case.n_folds)
        reordered_lf = self.assign_folds(case.location_ids[::-1], case.n_folds)

        pl_testing.assert_frame_equal(
            first_lf.unique(), reordered_lf.unique(), check_row_order=False
        )

    def test_existing_folds_are_replaced(self):
        case = Data.existing_folds_replaced_test_case
        # No new assignment gives fold n_folds, as folds are numbered from 0.
        earlier_folds_lf = pl.LazyFrame(
            {
                IndCQC.location_id: case.location_ids,
                ModelEvaluation.fold: [case.n_folds] * len(case.location_ids),
            },
            schema_overrides={ModelEvaluation.fold: pl.UInt8},
        )

        returned_lf = job.assign_location_folds(
            earlier_folds_lf,
            location_column=IndCQC.location_id,
            n_folds=case.n_folds,
            seed=FOLD_SEED,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            self.assign_folds(case.location_ids, case.n_folds),
            check_row_order=False,
        )


class TestAddNeverSubmittedFlag:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.never_submitted_test_cases]
    )
    def test_never_submitted_only_when_no_row_is_ever_known(self, case):
        returned_lf = job.add_never_submitted_flag(
            pl.LazyFrame(case.input_data),
            known_column=IndCQC.ascwds_filled_posts_dedup_clean,
            location_column=IndCQC.location_id,
        )

        pl_testing.assert_frame_equal(returned_lf, pl.LazyFrame(case.expected_data))


class TestMeanPeriodToPeriodChange:
    @staticmethod
    def mean_change(case) -> pl.LazyFrame:
        return job.mean_period_to_period_change(
            pl.LazyFrame(case.input_data),
            value_columns=case.columns,
            partition_columns=case.partition_columns,
            date_column=IndCQC.cqc_location_import_date,
        )

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.steady_predictions_test_cases]
    )
    def test_steady_predictions_have_zero_change(self, case):
        pl_testing.assert_frame_equal(
            self.mean_change(case), pl.LazyFrame(case.expected_data)
        )

    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in Data.change_within_partition_test_cases],
    )
    def test_change_measured_within_partition(self, case):
        pl_testing.assert_frame_equal(
            self.mean_change(case), pl.LazyFrame(case.expected_data)
        )

    def test_change_measured_in_date_order(self):
        case = Data.unordered_dates_test_case

        pl_testing.assert_frame_equal(
            self.mean_change(case), pl.LazyFrame(case.expected_data)
        )


class TestAggregateTotalsByGroup:
    @staticmethod
    def aggregate(case) -> pl.LazyFrame:
        return job.aggregate_totals_by_group(
            pl.LazyFrame(case.input_data),
            predicted_column=IndCQC.estimate_filled_posts,
            actual_column=IndCQC.ascwds_filled_posts_dedup_clean,
            grouping_columns=[IndCQC.primary_service_type],
        )

    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in Data.group_totals_sum_predicted_and_known_posts_test_cases
        ],
    )
    def test_group_totals_sum_predicted_and_known_posts(self, case):
        pl_testing.assert_frame_equal(
            self.aggregate(case),
            pl.LazyFrame(case.expected_data, schema_overrides=ROW_COUNT_SCHEMA),
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in Data.rows_without_known_posts_excluded_from_totals_test_cases
        ],
    )
    def test_rows_without_known_posts_excluded_from_totals(self, case):
        pl_testing.assert_frame_equal(
            self.aggregate(case),
            pl.LazyFrame(case.expected_data, schema_overrides=ROW_COUNT_SCHEMA),
            check_row_order=False,
        )


class TestScoreGroupTotals:
    @staticmethod
    def score(case) -> pl.LazyFrame:
        return job.score_group_totals(
            pl.LazyFrame(case.input_data),
            predicted_column=IndCQC.estimate_filled_posts,
            actual_column=IndCQC.ascwds_filled_posts_dedup_clean,
            by_columns=case.by_columns,
        )

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.perfect_totals_test_cases]
    )
    def test_perfect_totals_score_one_and_zero(self, case):
        pl_testing.assert_frame_equal(
            self.score(case), pl.LazyFrame(case.expected_data)
        )

    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in Data.total_scores_split_by_fold_test_cases],
    )
    def test_total_scores_split_by_fold(self, case):
        pl_testing.assert_frame_equal(
            self.score(case), pl.LazyFrame(case.expected_data), check_row_order=False
        )


class TestAddFinancialYear:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.financial_year_test_cases]
    )
    def test_financial_year_is_the_year_it_started_in(self, case):
        input_lf = pl.LazyFrame({IndCQC.cqc_location_import_date: case.dates})

        returned_lf = job.add_financial_year(input_lf, IndCQC.cqc_location_import_date)

        assert returned_lf.collect()[ModelEvaluation.financial_year].to_list() == (
            case.expected_years
        )


class TestScoreRows:
    @staticmethod
    def score(case) -> pl.LazyFrame:
        return job.score_rows(
            pl.LazyFrame(case.input_data),
            predicted_column=IndCQC.estimate_filled_posts,
            actual_column=IndCQC.ascwds_filled_posts_dedup_clean,
            by_columns=case.by_columns,
        )

    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in Data.score_rows_test_cases
            + Data.rows_without_known_posts_not_scored_test_cases
            + Data.scores_split_by_fold_test_cases
        ],
    )
    def test_rows_scored_in_posts(self, case):
        pl_testing.assert_frame_equal(
            self.score(case),
            pl.LazyFrame(case.expected_data, schema_overrides=ROW_COUNT_SCHEMA),
            check_row_order=False,
        )


class TestCalculatePeriodBias:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.period_bias_test_cases]
    )
    def test_bias_measured_in_each_period_and_group(self, case):
        returned_lf = job.calculate_period_bias(
            pl.LazyFrame(case.input_data),
            predicted_column=IndCQC.estimate_filled_posts,
            actual_column=IndCQC.ascwds_filled_posts_dedup_clean,
            period_column=IndCQC.cqc_location_import_date,
            by_columns=[IndCQC.primary_service_type],
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            pl.LazyFrame(case.expected_data, schema_overrides=ROW_COUNT_SCHEMA),
            check_row_order=False,
        )


class TestFitBiasSlopePerYear:
    @staticmethod
    def fit(period_bias_lf: pl.LazyFrame) -> pl.LazyFrame:
        return job.fit_bias_slope_per_year(
            period_bias_lf,
            actual_column=IndCQC.ascwds_filled_posts_dedup_clean,
            period_column=IndCQC.cqc_location_import_date,
            by_columns=[IndCQC.primary_service_type],
        )

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.bias_slope_test_cases]
    )
    def test_slope_and_period_counts_reported_per_group(self, case):
        returned_lf = self.fit(
            pl.LazyFrame(
                case.input_data,
                schema_overrides=ROW_COUNT_SCHEMA,
            )
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            pl.LazyFrame(
                case.expected_data,
                schema_overrides={
                    ModelEvaluation.number_of_periods: pl.UInt32,
                    ModelEvaluation.minimum_rows_in_period: pl.UInt32,
                },
            ),
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in Data.weighted_bias_slope_test_cases]
    )
    def test_slope_weighted_by_known_total(self, case):
        input_lf = pl.LazyFrame(
            {
                IndCQC.primary_service_type: ["group"] * len(case.dates),
                IndCQC.cqc_location_import_date: case.dates,
                ModelEvaluation.bias: case.biases,
                IndCQC.ascwds_filled_posts_dedup_clean: case.known_totals,
                ModelEvaluation.number_of_rows: [1] * len(case.dates),
            }
        )
        years = [(d - case.dates[0]).days / 365.25 for d in case.dates]
        # polyfit weights multiply the residuals, so weighting by total needs the square root.
        expected_slope = np.polyfit(
            years, case.biases, deg=1, w=np.sqrt(case.known_totals)
        )[0]

        returned_slope = self.fit(input_lf).collect()[
            ModelEvaluation.bias_slope_per_year
        ][0]

        assert returned_slope == pytest.approx(expected_slope)
