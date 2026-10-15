import json
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_05_estimate_counts as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_05_estimate_counts"


class TestMain:
    def setup_method(self) -> None:
        source_schema = {
            IndCqcColumns.location_id: pl.String,
        }
        source_rows = [
            ("1-001"),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select([IndCqcColumns.location_id])

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_reads_source_and_compare_datasets(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        assert read_parquet_mock.call_count == 2
        read_parquet_mock.assert_has_calls(
            [
                call(source="s3://bucket/my/source/"),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.COMPARE_COLS_TO_IMPORT,
                ),
            ]
        )
        write_reports_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_row_count_match(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert "row_count_match" in assertion_types_present
