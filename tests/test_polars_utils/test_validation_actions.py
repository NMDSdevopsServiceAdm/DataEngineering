import unittest
from unittest.mock import ANY, patch

import pointblank as pb
import polars as pl
import polars.testing as pl_testing
import pytest

import polars_utils.validation.actions as job
from polars_utils.validation import actions as vl
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys

SRC_PATH = "polars_utils.validation.actions"


class TestValidate(unittest.TestCase):
    def setUp(self) -> None:
        self.df = pl.DataFrame(
            [
                ("1-00001", "20240101", "a"),
                ("1-00002", "20240101", "b"),
                ("1-00001", "20240201", "b"),
                ("1-00002", "20240201", "c"),
                ("1-00002", "20240201", None),
            ],
            schema=pl.Schema(
                [
                    ("someId", pl.String),
                    ("my_date", pl.String),
                    ("name", pl.String),
                ]
            ),
            orient="row",
        )


class TestWriteReports(TestValidate):
    @patch(f"{SRC_PATH}._report_on_fail")
    @patch("boto3.client", autospec=True)
    def test_write_reports(self, mock_s3_client, mock_report_on_fail):
        # Given
        validation = (
            pb.Validate(self.df, thresholds=pb.Thresholds(error=1))
            .rows_distinct(["someId", "my_date"])
            .col_vals_not_null(["someId", "my_date", "name"])
            .interrogate()
        )
        # When
        with self.assertRaises(AssertionError) as context:
            vl.write_reports(validation, "bucket", "reports")

        # Then
        self.assertIn(
            "Expect entirely distinct rows across `someId`, `my_date`.",
            str(context.exception),
        )
        self.assertIn(
            "Expect that all values in `name` should not be Null.",
            str(context.exception),
        )
        mock_s3_client.return_value.put_object.assert_called_once_with(
            Body=ANY,
            Bucket="bucket",
            Key="reports/summary.html",
        )

        # calls include null check for each column
        self.assertEqual(mock_report_on_fail.call_count, 4)

        calls = mock_report_on_fail.call_args_list
        rows_distinct_step = calls[0][0][0]
        col_vals_not_null_someId_step = calls[1][0][0]
        col_vals_not_null_my_date_step = calls[2][0][0]
        col_vals_not_null_name_step = calls[3][0][0]

        self.assertEqual(
            rows_distinct_step,
            rows_distinct_step
            | {
                "i": 1,
                "assertion_type": "rows_distinct",
                "column": ["someId", "my_date"],
                "all_passed": False,
            },
        )

        self.assertEqual(
            col_vals_not_null_someId_step,
            col_vals_not_null_someId_step
            | {
                "i": 2,
                "assertion_type": "col_vals_not_null",
                "column": "someId",
                "all_passed": True,
            },
        )
        self.assertEqual(
            col_vals_not_null_my_date_step,
            col_vals_not_null_my_date_step
            | {
                "i": 3,
                "assertion_type": "col_vals_not_null",
                "column": "my_date",
                "all_passed": True,
            },
        )
        self.assertEqual(
            col_vals_not_null_name_step,
            col_vals_not_null_name_step
            | {
                "i": 4,
                "assertion_type": "col_vals_not_null",
                "column": "name",
                "all_passed": False,
            },
        )


class TestReportOnFail(TestValidate):
    @patch("polars_utils.utils.write_to_parquet", autospec=True)
    @patch("pointblank.Validate")
    def test_report_when_fail(self, mock_validate, mock_write_parquet):
        # Given
        step = {
            "i": 1,
            "assertion_type": "rows_distinct",
            "column": ["someId", "my_date"],
            "all_passed": False,
        }
        mock_df = pl.DataFrame(
            {
                "_row_num_": [4, 5],
                "someId": ["1-00002", "1-00002"],
                "my_date": ["20240201", "20240201"],
            },
            schema={"_row_num_": pl.UInt32, "someId": pl.String, "my_date": pl.String},
        )
        mock_validate.return_value.get_data_extracts.return_value = mock_df
        # When
        vl._report_on_fail(step, pb.Validate(ANY), "bucket", "path")
        # Then
        mock_validate.return_value.get_data_extracts.assert_called_once_with(
            1, frame=True
        )
        mock_write_parquet.assert_called_once_with(
            mock_df,
            "s3://bucket/path/failed_step_1_rows_distinct_someId_my_date.parquet",
            append=False,
        )

    @patch("pointblank.Validate")
    def test_report_when_succeed(self, mock_validate):
        # Given
        step = {
            "i": 1,
            "assertion_type": "rows_distinct",
            "column": ["someId", "my_date"],
            "all_passed": True,
        }
        # When
        vl._report_on_fail(step, pb.Validate(ANY), "bucket", "path")
        # Then
        self.assertEqual(
            mock_validate.return_value.get_data_extracts.call_args_list, []
        )


class TestIsUniqueCount(TestValidate):
    def test_is_unique_count_equal_true(self):
        # Given
        func = vl.is_unique_count_equal("someId", 2)
        # When
        result = func(self.df)
        # Then
        self.assertTrue(result)

    def is_unique_count_equal_false(self):
        # Given
        func = vl.is_unique_count_equal("someId", 3)
        # When
        result = func(self.df)
        # Then
        self.assertFalse(result)


class TestAddListColumnValidationCheckFlags(unittest.TestCase):

    def test_add_list_column_validation_check_flags(self):
        df_test = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "my_list": [[1, 2], [], None, [3, None]],
                "another_list": [[None], [5], [], None],
                "other_col": ["A", "B", "C", "D"],
            }
        )

        df_expected = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "other_col": ["A", "B", "C", "D"],
                "my_list_has_no_empty_or_null": [1, 0, 1, 0],
                "my_list_is_not_null": [1, 1, 0, 1],
                "another_list_has_no_empty_or_null": [0, 1, 0, 1],
                "another_list_is_not_null": [1, 1, 1, 0],
            }
        )
        df_result = job.add_list_column_validation_check_flags(
            df_test, ["my_list", "another_list"]
        )
        pl_testing.assert_frame_equal(df_result, df_expected)


class TestMakeColHasFewerNullsValidator(TestValidate):
    col_a = "col_a"
    col_b = "col_b"

    def test_make_col_has_fewer_nulls_validator(self):
        # Given

        df = pl.DataFrame(
            {
                self.col_a: [1, None, 3, None],
                self.col_b: [None, None, 3, None],
            }
        )
        validator = vl.make_col_has_fewer_nulls_validator(self.col_a, self.col_b)
        # When
        result = validator(df)
        # Then
        self.assertTrue(result)

    def test_make_col_has_fewer_nulls_validator_fails(self):
        df_fail = pl.DataFrame(
            {
                self.col_a: [1, None, 3, None],
                self.col_b: [1, 2, 3, None],
            }
        )
        validator_fail = vl.make_col_has_fewer_nulls_validator(self.col_a, self.col_b)
        # When
        result_fail = validator_fail(df_fail)
        # Then
        self.assertFalse(result_fail)


class TestMakeConvertColToIntegersPreprocessor:
    def test_make_convert_col_to_integers_preprocessor(self):
        preprocessor = job.make_convert_col_to_integers_preprocessor(PartitionKeys.year)
        df = pl.DataFrame({PartitionKeys.year: ["2020", "2021", "2022"]})
        result = preprocessor(df)
        expected = pl.DataFrame({PartitionKeys.year: [2020, 2021, 2022]})
        pl_testing.assert_frame_equal(result, expected)

    def test_convert_col_to_integers_with_non_numeric_values(self):
        df = pl.DataFrame({PartitionKeys.year: ["2020", "invalid", "2022"]})
        with pytest.raises(pl.exceptions.InvalidOperationError):
            job.make_convert_col_to_integers_preprocessor(PartitionKeys.year)(df)


class TestCreateSlvSchema:
    returned_schema = job.create_slv_schema([9, 10])

    def test_raises_value_error_when_given_empty_list(self):
        with pytest.raises(ValueError) as context:
            job.create_slv_schema([])

        assert str(context.value) == "Given job role list be populated. Got []"

    def test_returns_a_polars_schema(self):
        assert type(self.returned_schema) == pl.Schema

    def test_returns_schema_for_requested_job_roles(self):
        expected_schema = pl.Schema(
            {
                AWPClean.job_role_09_employees: pl.Int32,
                AWPClean.job_role_09_starters: pl.Int32,
                AWPClean.job_role_09_leavers: pl.Int32,
                AWPClean.job_role_09_vacancies: pl.Int32,
                AWPClean.job_role_10_employees: pl.Int32,
                AWPClean.job_role_10_starters: pl.Int32,
                AWPClean.job_role_10_leavers: pl.Int32,
                AWPClean.job_role_10_vacancies: pl.Int32,
            }
        )

        assert self.returned_schema == expected_schema


if __name__ == "__main__":
    unittest.main(warnings="ignore")
