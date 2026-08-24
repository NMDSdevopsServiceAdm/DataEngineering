import json
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.cqc_pir.fargate.validate_cqc_pir_raw_data as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols

PATCH_PATH = "projects._01_ingest.cqc_pir.fargate.validate_cqc_pir_raw_data"


class TestMain:
    source_df = pl.DataFrame(
        {
            Keys.import_date: ["20250101"],
            PIRCols.location_id: ["1-000000001"],
            PIRCols.pir_people_directly_employed: [10],
        }
    )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {"col_vals_not_null", "col_vals_between"}
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"
