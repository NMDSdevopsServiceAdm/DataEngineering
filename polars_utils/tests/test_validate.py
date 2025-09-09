import unittest
from pathlib import Path
from unittest.mock import ANY, call, patch

import pointblank as pb
import polars as pl
import yaml

from polars_utils import validate as utils

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
TEST_CONFIG_PATH = Path(__file__).parent.resolve() / "config"


class ValidateDatasetsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_path = "some/directory"
        self.destination = "some/other/other/directory"
        self.raw_df = pl.DataFrame(
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
        self.yaml = yaml.safe_load(
            """
            tbl: null
            tbl_name: "delta_something_api"
            label: "Basic data quality checks"
            brief: 
            thresholds:
                warning: 1
            actions:
                warning: "{LEVEL}: {type} validation failed with {n_failed} records for column {col}."
            steps:
            - rows_distinct:
                columns_subset: [someId, my_date]
                brief: "Ensure all {col} values are distinct"
            - col_vals_not_null:
                columns: [someId, my_date, name]
                brief: "Ensure {col} columns are fully populated"
        """
        )

    def error_on_not_in_config(self):
        # When
        with self.assertRaises(ValueError) as context:
            utils.validate_dataset("bucket", "not_in_config")
        # Then
        self.assertIn(
            "Dataset not_in_config not found in config file", str(context.exception)
        )

    @patch(f"{SRC_PATH}.CONFIG", SIMPLE_MOCK_CONFIG)
    def error_on_missing_file(self):
        # When
        with self.assertRaises(FileNotFoundError) as context:
            utils.validate_dataset("bucket", "my_dataset")
        # Then
        self.assertIn(
            "Rules file config/my_dataset.yml not found", str(context.exception)
        )

    @patch(f"{SRC_PATH}.CONFIG_PATH", TEST_CONFIG_PATH)
    @patch(f"{SRC_PATH}.CONFIG", SIMPLE_MOCK_CONFIG)
    @patch(f"{SRC_PATH}.report_on_fail")
    @patch("boto3.client", autospec=True)
    @patch("pointblank.yaml.YAMLValidator.load_config", autospec=True)
    @patch("polars.scan_parquet", autospec=True)
    def test_validation_failures(
        self, mock_scan, mock_yaml, mock_s3_client, mock_report_on_fail
    ):
        # Given
        mock_scan.return_value.collect.return_value = self.raw_df
        mock_yaml.return_value = self.yaml
        # When
        with self.assertRaises(AssertionError) as context:
            utils.validate_dataset("bucket", "my_dataset")

        # Then
        self.assertIn(
            "Expect entirely distinct rows across `someId`, `my_date`.",
            str(context.exception),
        )
        mock_scan.assert_called_once_with(
            "s3://bucket/domain=cqc_or_other/dataset=dataset_name_in_s3/version=x.x.x/",
            cast_options=ANY,
            extra_columns=ANY,
        )
        mock_s3_client.return_value.put_object.assert_called_once_with(
            Body=ANY,
            Bucket="bucket",
            Key="domain=data_validation_reports/dataset=data_quality_report/index.html",
        )
        mock_report_on_fail.assert_has_calls(
            [
                call(
                    ANY,
                    ANY,
                    "bucket",
                    "domain=data_validation_reports/dataset=data_quality_report",
                ),
                call(
                    ANY,
                    ANY,
                    "bucket",
                    "domain=data_validation_reports/dataset=data_quality_report",
                ),
            ]
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
        utils.report_on_fail(step, pb.Validate(ANY), "bucket", "path")
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
        utils.report_on_fail(step, pb.Validate(ANY), "bucket", "path")
        # Then
        self.assertEquals(
            mock_validate.return_value.get_data_extracts.call_args_list, []
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
