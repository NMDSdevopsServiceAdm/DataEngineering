import unittest
from pathlib import Path
from unittest.mock import ANY, patch

import pointblank as pb
import polars as pl

from polars_utils import validate as vl

SRC_PATH = "polars_utils.validate"
SIMPLE_MOCK_CONFIG = {
    "datasets": {
        "my_dataset": {
            "dataset": "dataset_name_in_s3",
            "domain": "cqc_or_other",
            "version": "x.x.x",
            "report_name": "data_quality_report",
        }
    }
}
TEMP_FILE = Path(__file__).parent / "test.parquet"


class ValidateTests(unittest.TestCase):
    def setUp(self) -> None:
        types_df = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
                "a_list": [
                    [[1, 2], [1], None],
                    [[1, 2], [2], None],
                    [[1, 2], [3], None],
                ],
            }
        ).with_columns(pl.struct(pl.all()).alias("a_struct"))
        types_df.write_parquet(TEMP_FILE)

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

    def tearDown(self) -> None:
        try:
            TEMP_FILE.unlink()
        except FileNotFoundError:
            pass
        return super().tearDown()

    def test_read_parquet_keep_all(self):
        # Given
        expected = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
                "a_list": [
                    [[1, 2], [1], None],
                    [[1, 2], [2], None],
                    [[1, 2], [3], None],
                ],
                "a_struct": [
                    {"foo": 1, "bar": None, "a_list": [[1, 2], [1], None]},
                    {"foo": 2, "bar": "bak", "a_list": [[1, 2], [2], None]},
                    {"foo": 3, "bar": "baz", "a_list": [[1, 2], [3], None]},
                ],
            }
        )
        # When
        result = vl.read_parquet(TEMP_FILE)
        # Then
        self.assertTrue(result.equals(expected))

    def test_read_parquet_exclude_complex(self):
        # Given
        expected = pl.DataFrame(
            {
                "foo": [1, 2, 3],
                "bar": [None, "bak", "baz"],
            }
        )
        # When
        result = vl.read_parquet(TEMP_FILE, exclude_complex_types=True)
        # Then
        self.assertTrue(result.equals(expected))

    @patch(f"{SRC_PATH}._report_on_fail")
    @patch("boto3.client", autospec=True)
    def test_write_reports(self, mock_s3_client, mock_report_on_fail):
        # Given
        validation = (
            pb.Validate(self.df, thresholds=pb.Thresholds(warning=1))
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
            Key="reports/index.html",
        )

        # calls include null check for each column
        self.assertEquals(mock_report_on_fail.call_count, 4)

        calls = mock_report_on_fail.call_args_list
        rows_distinct_step = calls[0][0][0]
        col_vals_not_null_someId_step = calls[1][0][0]
        col_vals_not_null_my_date_step = calls[2][0][0]
        col_vals_not_null_name_step = calls[3][0][0]

        self.assertDictContainsSubset(
            {
                "i": 1,
                "assertion_type": "rows_distinct",
                "column": ["someId", "my_date"],
                "all_passed": False,
            },
            rows_distinct_step,
        )
        self.assertDictContainsSubset(
            {
                "i": 2,
                "assertion_type": "col_vals_not_null",
                "column": "someId",
                "all_passed": True,
            },
            col_vals_not_null_someId_step,
        )
        self.assertDictContainsSubset(
            {
                "i": 3,
                "assertion_type": "col_vals_not_null",
                "column": "my_date",
                "all_passed": True,
            },
            col_vals_not_null_my_date_step,
        )
        self.assertDictContainsSubset(
            {
                "i": 4,
                "assertion_type": "col_vals_not_null",
                "column": "name",
                "all_passed": False,
            },
            col_vals_not_null_name_step,
        )

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
        self.assertEquals(
            mock_validate.return_value.get_data_extracts.call_args_list, []
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
