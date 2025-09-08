import unittest
from unittest.mock import ANY, patch

import polars as pl
import yaml

from polars_utils.validate_datasets import main

SRC_PATH = "polars_utils.validate_datasets"
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

    @patch(f"{SRC_PATH}.CONFIG", SIMPLE_MOCK_CONFIG)
    @patch("polars_utils.utils.write_to_parquet", autospec=True)
    @patch("boto3.client", autospec=True)
    @patch("pointblank.yaml.YAMLValidator.load_config", autospec=True)
    @patch("polars.scan_parquet", autospec=True)
    def test_validation_failures(
        self, mock_scan, mock_yaml, mock_s3_client, mock_parquet
    ):
        # Given
        mock_scan.return_value.collect.return_value = self.raw_df
        mock_yaml.return_value = self.yaml
        failed_rows_distinct = pl.DataFrame(
            {
                "_row_num_": [4, 5],
                "someId": ["1-00002", "1-00002"],
                "my_date": ["20240201", "20240201"],
            },
            schema={"_row_num_": pl.UInt32, "someId": pl.String, "my_date": pl.String},
        )
        failed_rows_null = pl.DataFrame(
            {
                "_row_num_": [5],
                "someId": ["1-00002"],
                "my_date": ["20240201"],
                "name": [None],
            },
            schema={
                "_row_num_": pl.UInt32,
                "someId": pl.String,
                "my_date": pl.String,
                "name": pl.String,
            },
        )

        # When
        with self.assertRaises(AssertionError) as context:
            main("bucket", "my_dataset")

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
        mock_parquet_calls = mock_parquet.call_args_list

        # Comparing equality of Dataframes not possible as tuple
        self.assertTrue(mock_parquet_calls[0][0][0].equals(failed_rows_distinct))
        self.assertEquals(
            mock_parquet_calls[0][0][1],
            "s3://bucket/domain=data_validation_reports/dataset=data_quality_report/failed_step_1_rows_distinct_someId_my_date.parquet",
        )
        self.assertTrue(mock_parquet_calls[1][0][0].equals(failed_rows_null))
        self.assertEquals(
            mock_parquet_calls[1][0][1],
            "s3://bucket/domain=data_validation_reports/dataset=data_quality_report/failed_step_4_col_vals_not_null_name.parquet",
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
