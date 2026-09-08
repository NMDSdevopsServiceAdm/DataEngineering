import json
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare_worker as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare_worker"


class TestMain:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.source_df = pl.DataFrame({"worker_id": ["1", "2", "3"]})
        self.compare_df = pl.DataFrame({"worker_id": ["1", "2", "3"]})

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        assert mock_read_parquet.call_count == 2
        mock_read_parquet.assert_has_calls(
            [
                call(source="s3://bucket/my/source/"),
                call(source="s3://bucket/my/compare/"),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_row_count_match(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert "row_count_match" in assertion_types_present
