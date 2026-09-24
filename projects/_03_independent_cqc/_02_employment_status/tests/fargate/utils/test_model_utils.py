from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.model_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    FOLD_SEED,
    IMPUTED_SHARE,
    KNOWN_SHARE,
    ROLLING_AVERAGE_SHARE,
)
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    TestModelUtilsData as Data,
)
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import ImputationRowKind

LOCATION_ROLE_COLUMNS = [IndCQC.location_id, IndCQC.published_job_role_label]
LOCATION_ROLE_DATE_COLUMNS = [*LOCATION_ROLE_COLUMNS, IndCQC.cqc_location_import_date]
AS_LOCATION_TYPE = pl.col(IndCQC.location_id).cast(CatColType.LocationCatType)

SCHEMA_OVERRIDES = {
    KNOWN_SHARE: pl.Float32,
    IMPUTED_SHARE: pl.Float32,
    ROLLING_AVERAGE_SHARE: pl.Float32,
    ShareModel.fold: pl.UInt8,
    ShareModel.elapsed_months: pl.Int32,
    ShareModel.provider_location_count: pl.UInt32,
    ShareModel.latest_overall_rating: pl.Categorical,
    ShareModel.imputation_row_kind: pl.Enum(
        [
            ImputationRowKind.known,
            ImputationRowKind.interpolated,
            ImputationRowKind.carried,
        ]
    ),
}


def to_lf(data: dict[str, Any]) -> pl.LazyFrame:
    """Build a LazyFrame, typing the columns the functions create or expect."""
    return pl.LazyFrame(
        data,
        schema_overrides={
            col: dtype for col, dtype in SCHEMA_OVERRIDES.items() if col in data
        },
    )


def to_ratings_lf(data: dict[str, Any]) -> pl.LazyFrame:
    """Build a ratings LazyFrame, with its text columns typed as strings even when blank."""
    return pl.LazyFrame(
        data,
        schema_overrides=dict.fromkeys(
            [CQCRatings.overall_rating, CQCRatings.date, CQCL.assessment_date],
            pl.String,
        ),
    )


class TestAddElapsedMonths:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_elapsed_months_test_cases],
    )
    def test_elapsed_months_follow_calendar_gaps(self, case):
        returned_lf = job.add_elapsed_months(
            to_lf(case.input_data), date_column=IndCQC.cqc_location_import_date
        )

        pl_testing.assert_frame_equal(returned_lf, to_lf(case.expected_data))


class TestAddImputationRowKind:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_imputation_row_kind_test_cases
        ],
    )
    def test_row_kind_labels_known_interpolated_and_carried(self, case):
        returned_lf = job.add_imputation_row_kind(
            to_lf(case.input_data),
            known_column=KNOWN_SHARE,
            imputed_column=IMPUTED_SHARE,
            partition_columns=LOCATION_ROLE_COLUMNS,
            date_column=IndCQC.cqc_location_import_date,
        )

        pl_testing.assert_frame_equal(returned_lf, to_lf(case.expected_data))


class TestAddNeverSubmittedFlag:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_never_submitted_flag_test_cases
        ],
    )
    def test_never_submitted_only_when_no_role_is_ever_known(self, case):
        returned_lf = job.add_never_submitted_flag(
            to_lf(case.input_data),
            known_column=KNOWN_SHARE,
            location_column=IndCQC.location_id,
        )

        pl_testing.assert_frame_equal(returned_lf, to_lf(case.expected_data))


class TestAddProviderLocationCount:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_provider_location_count_test_cases
        ],
    )
    def test_provider_size_counts_locations_not_rows(self, case):
        returned_lf = job.add_provider_location_count(
            to_lf(case.input_data),
            provider_column=IndCQC.provider_id,
            location_column=IndCQC.location_id,
            date_column=IndCQC.cqc_location_import_date,
        )

        pl_testing.assert_frame_equal(returned_lf, to_lf(case.expected_data))


class TestAddLatestOverallRating:
    @staticmethod
    def returned_and_expected_lfs(case) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """
        Run the function with the location ID typed as in the pipeline: categorical in the
        dataset and a plain string in the ratings.
        """
        returned_lf = job.add_latest_overall_rating(
            to_lf(case.input_data).with_columns(AS_LOCATION_TYPE),
            to_ratings_lf(case.ratings_data),
            location_column=IndCQC.location_id,
            date_column=IndCQC.cqc_location_import_date,
        )

        return returned_lf, to_lf(case.expected_data).with_columns(AS_LOCATION_TYPE)

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.latest_rating_joined_per_location_test_cases
        ],
    )
    def test_latest_rating_joined_per_location(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.rating_as_of_import_date_test_cases
        ],
    )
    def test_rating_as_of_each_import_date(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.blank_latest_rating_test_cases
        ],
    )
    def test_blank_latest_rating_uses_latest_real_rating(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.same_date_ratings_test_cases],
    )
    def test_same_date_ratings_ordered_like_the_ratings_job(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.unrated_location_test_cases],
    )
    def test_unrated_location_gets_not_yet_rated(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_existing_rating_is_replaced(self):
        returned_lf, expected_lf = self.returned_and_expected_lfs(
            Data.existing_rating_replaced_test_case
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestBuildModellingDataset:
    def test_builder_does_not_duplicate_rows(self):
        # Location IDs are typed as in the pipeline: categorical in the shares dataset and plain
        # strings in the estimates and ratings.
        shares_lf = to_lf(Data.build_modelling_dataset_shares_data).with_columns(
            AS_LOCATION_TYPE
        )

        returned_lf = job.build_modelling_dataset(
            shares_lf,
            pl.LazyFrame(Data.build_modelling_dataset_estimates_data),
            to_ratings_lf(Data.build_modelling_dataset_ratings_data),
            known_share_columns=[KNOWN_SHARE],
            imputed_share_columns=[IMPUTED_SHARE],
            rolling_average_columns=[ROLLING_AVERAGE_SHARE],
        )

        pl_testing.assert_frame_equal(
            returned_lf.select(LOCATION_ROLE_DATE_COLUMNS),
            shares_lf.select(LOCATION_ROLE_DATE_COLUMNS),
            check_row_order=False,
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
        [
            pytest.param(case, id=case.id)
            for case in Data.location_rows_share_a_fold_test_cases
        ],
    )
    def test_location_rows_share_a_fold(self, case):
        returned_df = self.assign_folds(case.location_ids, case.n_folds).collect()

        folds_per_location = returned_df.group_by(IndCQC.location_id).agg(
            pl.col(ShareModel.fold).n_unique()
        )
        assert returned_df.height == len(case.location_ids)
        assert returned_df[ShareModel.fold].null_count() == 0
        assert (folds_per_location[ShareModel.fold] == 1).all()

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.folds_are_balanced_test_cases],
    )
    def test_folds_are_balanced(self, case):
        locations_per_fold = (
            self.assign_folds(case.location_ids, case.n_folds)
            .group_by(ShareModel.fold)
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
                ShareModel.fold: [case.n_folds] * len(case.location_ids),
            },
            schema_overrides={ShareModel.fold: pl.UInt8},
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


def mean_share_by_service_and_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Stand in for a rolling average: the mean share per service and date, joined back onto
    every row.
    """
    group_columns = [IndCQC.primary_service_type, IndCQC.cqc_location_import_date]
    means_lf = lf.group_by(group_columns).agg(
        pl.col(IMPUTED_SHARE).mean().alias(ROLLING_AVERAGE_SHARE)
    )
    return lf.join(means_lf, on=group_columns, how="left")


class TestAddFoldSafeRollingAverage:
    @staticmethod
    def add_fold_safe_average(case) -> pl.LazyFrame:
        return job.add_fold_safe_rolling_average(
            to_lf(case.input_data),
            rolling_average_function=mean_share_by_service_and_date,
            input_columns=[IMPUTED_SHARE],
            output_columns=[ROLLING_AVERAGE_SHARE],
            fold_column=ShareModel.fold,
            n_folds=case.n_folds,
            key_columns=LOCATION_ROLE_DATE_COLUMNS,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.tested_fold_excluded_test_cases
        ],
    )
    def test_tested_fold_excluded_from_its_rolling_average(self, case):
        returned_lf = self.add_fold_safe_average(case)

        averages = [*LOCATION_ROLE_DATE_COLUMNS, ROLLING_AVERAGE_SHARE]
        pl_testing.assert_frame_equal(
            returned_lf.select(averages),
            to_lf(case.expected_data).select(averages),
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.tested_fold_gets_group_value_test_cases
        ],
    )
    def test_tested_fold_still_gets_group_value(self, case):
        returned_lf = self.add_fold_safe_average(case)

        pl_testing.assert_frame_equal(
            returned_lf, to_lf(case.expected_data), check_row_order=False
        )

    def test_existing_output_columns_are_replaced(self):
        case = Data.existing_averages_replaced_test_case

        returned_lf = self.add_fold_safe_average(case)

        pl_testing.assert_frame_equal(
            returned_lf, to_lf(case.expected_data), check_row_order=False
        )
