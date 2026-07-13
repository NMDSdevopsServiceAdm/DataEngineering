from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_impute.fargate.utils.combine_ascwds_and_pir as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CombineASCWDSAndPIRData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CombineASCWDSAndPIRSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._03_impute.fargate.utils.combine_ascwds_and_pir"
)


class TestCombineASCWDSAndPIR:
    test_lf = pl.LazyFrame(
        Data.blend_pir_and_ascwds_rows,
        Schemas.blend_pir_and_ascwds_schema,
        orient="row",
    )

    @patch(f"{PATCH_PATH}.include_pir_if_never_submitted_ascwds")
    @patch(f"{PATCH_PATH}.drop_temporary_columns")
    @patch(f"{PATCH_PATH}.create_ascwds_pir_merged_column")
    @patch(f"{PATCH_PATH}.create_repeated_ascwds_clean_column")
    @patch(f"{PATCH_PATH}.split_dataset_for_merging")
    @patch(f"{PATCH_PATH}.create_last_submission_columns")
    def test_merge_ascwds_and_pir_filled_post_submissions_calls_correct_functions(
        self,
        create_last_submission_columns_mock: Mock,
        split_dataset_for_merging_mock: Mock,
        create_repeated_ascwds_clean_column_mock: Mock,
        create_ascwds_pir_merged_column_mock: Mock,
        drop_temporary_columns_mock: Mock,
        include_pir_if_never_submitted_ascwds_mock: Mock,
    ):
        split_dataset_for_merging_mock.return_value = (self.test_lf, self.test_lf)
        drop_temporary_columns_mock.return_value = self.test_lf
        include_pir_if_never_submitted_ascwds_mock.return_value = self.test_lf

        job.merge_ascwds_and_pir_filled_post_submissions(self.test_lf)

        create_last_submission_columns_mock.assert_called_once()
        split_dataset_for_merging_mock.assert_called_once()
        create_repeated_ascwds_clean_column_mock.assert_called_once()
        create_ascwds_pir_merged_column_mock.assert_called_once()
        drop_temporary_columns_mock.assert_called_once()
        include_pir_if_never_submitted_ascwds_mock.assert_called_once()

    def test_merge_ascwds_and_pir_filled_post_submissions_completes(self):
        returned_lf = job.merge_ascwds_and_pir_filled_post_submissions(self.test_lf)
        assert isinstance(returned_lf, pl.LazyFrame)


class TestCreateLastSubmissionColumns:
    expected_lf = pl.LazyFrame(
        Data.expected_create_last_submission_columns_rows,
        Schemas.expected_create_last_submission_columns_schema,
        orient="row",
    )
    test_lf = expected_lf.drop(
        IndCQC.last_ascwds_submission, IndCQC.last_pir_submission
    )
    returned_lf = job.create_last_submission_columns(test_lf)

    def test_function_returns_correct_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf, self.expected_lf, check_row_order=False
        )


class TestSplitDatasetForMerging:
    test_lf = pl.LazyFrame(
        Data.input_split_dataset_for_merging_rows,
        Schemas.split_dataset_for_merging_rows,
        orient="row",
    )
    expected_for_merging_lf = pl.LazyFrame(
        Data.expected_for_merging_rows,
        Schemas.split_dataset_for_merging_rows,
        orient="row",
    )
    expected_not_merging_lf = pl.LazyFrame(
        Data.expected_not_merging_rows,
        Schemas.split_dataset_for_merging_rows,
        orient="row",
    )

    returned_for_merging_lf, returned_not_merging_lf = job.split_dataset_for_merging(
        test_lf
    )

    def test_function_returns_correct_rows(self):
        pl_testing.assert_frame_equal(
            self.returned_for_merging_lf, self.expected_for_merging_lf
        )
        pl_testing.assert_frame_equal(
            self.returned_not_merging_lf, self.expected_not_merging_lf
        )


class TestCreateRepeatedAscwdsCleanColumn:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.create_repeated_ascwds_clean_column_test_cases
        ],
    )
    def test_function_returns_correct_values(self, case):

        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.expected_create_repeated_ascwds_clean_column_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
        returned_lf = job.create_repeated_ascwds_clean_column(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestCreateAscwdsPirMergedColumn:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.create_ascwds_pir_merged_column_test_cases
        ],
    )
    def test_function_returns_correct_values(self, case):

        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.ascwds_pir_merged)
        returned_lf = job.create_ascwds_pir_merged_column(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestIncludePirIfNeverSubmittedAscwds:
    expected_lf = pl.LazyFrame(
        Data.expected_include_pir_if_never_submitted_ascwds_rows,
        Schemas.expected_include_pir_if_never_submitted_ascwds_schema,
        orient="row",
    )
    test_lf = expected_lf.drop(IndCQC.ascwds_pir_merged)
    returned_lf = job.include_pir_if_never_submitted_ascwds(test_lf)

    def test_function_returns_correct_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf, self.expected_lf, check_row_order=False
        )


class TestDropTemporaryColumns:
    test_lf = pl.LazyFrame([], Schemas.drop_temporary_columns_schema)
    expected_columns = Schemas.expected_drop_temporary_columns
    returned_columns = job.drop_temporary_columns(test_lf).collect_schema().names()

    def test_function_removes_temporary_columns(self):
        assert self.returned_columns == self.expected_columns
