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
        assert returned_df.height == len(case.location_ids)
        assert returned_df[ModelEvaluation.fold].null_count() == 0
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

        assert locations_per_fold.len() == case.n_folds
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
        # Folds are numbered from 0, so n_folds is a fold number no new assignment gives.
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
            columns=case.columns,
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
