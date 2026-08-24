import json
from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc._09_archive_estimates.fargate.validate_archive_job_role_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.validate_archive_job_role_estimates"

BUCKET_NAME = "bucket"
SOURCE_PATH = "my/source/"
REPORTS_PATH = "my/reports/"


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_main_reads_source_and_writes_reports_with_row_count_check(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = pl.DataFrame(
            {
                IndCQC.location_id: ["1-001"],
                IndCQC.estimate_filled_posts_by_job_role: [10.0],
            }
        )

        job.main(BUCKET_NAME, SOURCE_PATH, REPORTS_PATH)

        mock_read_parquet.assert_called_once_with(
            source=f"s3://{BUCKET_NAME}/{SOURCE_PATH}"
        )
        mock_write_reports.assert_called_once()

        validation_arg, bucket_name_arg, reports_path_arg = (
            mock_write_reports.call_args[0]
        )
        assert bucket_name_arg == BUCKET_NAME
        assert reports_path_arg == REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert "row_count_match" in assertion_types_present
