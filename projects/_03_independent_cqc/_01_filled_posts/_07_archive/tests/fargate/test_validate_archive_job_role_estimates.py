import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._03_independent_cqc._01_filled_posts._07_archive.fargate.validate_archive_job_role_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._07_archive.fargate.validate_archive_job_role_estimates"

BUCKET_NAME = "bucket"
SOURCE_PATH = "my/source/"
COMPARE_PATH = "my/compare/"
OTHER_OUTPUT_PATH = "my/other/"
REPORTS_PATH = "my/reports/"


class TestMain:
    @patch(f"{PATCH_PATH}.job_role_metadata_validation")
    @patch(f"{PATCH_PATH}.job_role_estimates_validation")
    def test_main_calls_estimates_validation_when_dataset_is_estimates(
        self,
        mock_estimates_validation: Mock,
        mock_metadata_validation: Mock,
    ):
        job.main(
            BUCKET_NAME,
            "estimates",
            SOURCE_PATH,
            COMPARE_PATH,
            OTHER_OUTPUT_PATH,
            REPORTS_PATH,
        )

        mock_estimates_validation.assert_called_once_with(
            BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
        )
        mock_metadata_validation.assert_not_called()

    @patch(f"{PATCH_PATH}.job_role_metadata_validation")
    @patch(f"{PATCH_PATH}.job_role_estimates_validation")
    def test_main_calls_metadata_validation_when_dataset_is_metadata(
        self,
        mock_estimates_validation: Mock,
        mock_metadata_validation: Mock,
    ):
        job.main(
            BUCKET_NAME,
            "metadata",
            SOURCE_PATH,
            COMPARE_PATH,
            OTHER_OUTPUT_PATH,
            REPORTS_PATH,
        )

        mock_metadata_validation.assert_called_once_with(
            BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
        )
        mock_estimates_validation.assert_not_called()

    def test_main_raises_on_unknown_dataset(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            job.main(
                BUCKET_NAME,
                "something_else",
                SOURCE_PATH,
                COMPARE_PATH,
                OTHER_OUTPUT_PATH,
                REPORTS_PATH,
            )


class TestJobRoleEstimatesValidation:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    def test_job_role_estimates_validation_runs(
        self,
        mock_get_run_number: Mock,
        mock_scan_parquet: Mock,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_get_run_number.return_value = 3

        latest_partition_df = pl.DataFrame(
            {
                IndCQC.location_id: ["1-001", "1-002"],
                IndCQC.cqc_location_import_date: [date(2026, 1, 1), date(2026, 1, 1)],
                IndCQC.main_job_role_clean_labelled: ["care_worker", "support_worker"],
                IndCQC.estimate_filled_posts: [10.0, 20.0],
                ArchiveKeys.archive_date: [date(2026, 9, 22), date(2026, 9, 22)],
                ArchiveKeys.run_number: [3, 3],
            }
        )
        mock_scanned_lf = Mock()
        mock_filtered_lf = Mock()
        mock_scanned_lf.filter.return_value = mock_filtered_lf
        mock_filtered_lf.collect.return_value = latest_partition_df
        mock_scan_parquet.return_value = mock_scanned_lf

        mock_read_parquet.return_value = pl.DataFrame({"dummy": [1, 2]})

        job.job_role_estimates_validation(
            BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
        )

        mock_get_run_number.assert_any_call([f"s3://{BUCKET_NAME}/{SOURCE_PATH}"])
        mock_get_run_number.assert_any_call(
            [
                f"s3://{BUCKET_NAME}/{SOURCE_PATH}",
                f"s3://{BUCKET_NAME}/{OTHER_OUTPUT_PATH}",
            ]
        )
        assert mock_get_run_number.call_count == 2

        mock_scan_parquet.assert_called_once_with(f"s3://{BUCKET_NAME}/{SOURCE_PATH}")
        mock_scanned_lf.filter.assert_called_once()
        mock_read_parquet.assert_called_once_with(
            source=f"s3://{BUCKET_NAME}/{COMPARE_PATH}"
        )

        mock_write_reports.assert_called_once()
        validation_arg, bucket_name_arg, reports_path_arg = (
            mock_write_reports.call_args[0]
        )
        assert bucket_name_arg == BUCKET_NAME
        assert reports_path_arg == REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}
        expected_assertions = {
            "col_schema_match",
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "specially",
        }
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"


class TestJobRoleMetadataValidation:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    def test_job_role_metadata_validation_runs(
        self,
        mock_get_run_number: Mock,
        mock_scan_parquet: Mock,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_get_run_number.return_value = 3

        latest_partition_df = pl.DataFrame(
            {
                IndCQC.id_per_locationid_import_date: [1, 2],
                ArchiveKeys.archive_date: [date(2026, 9, 22), date(2026, 9, 22)],
                ArchiveKeys.run_number: [3, 3],
            }
        )
        mock_scanned_lf = Mock()
        mock_filtered_lf = Mock()
        mock_scanned_lf.filter.return_value = mock_filtered_lf
        mock_filtered_lf.collect.return_value = latest_partition_df
        mock_scan_parquet.return_value = mock_scanned_lf

        mock_read_parquet.return_value = pl.DataFrame({"dummy": [1, 2]})

        job.job_role_metadata_validation(
            BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
        )

        mock_get_run_number.assert_any_call([f"s3://{BUCKET_NAME}/{SOURCE_PATH}"])
        mock_get_run_number.assert_any_call(
            [
                f"s3://{BUCKET_NAME}/{SOURCE_PATH}",
                f"s3://{BUCKET_NAME}/{OTHER_OUTPUT_PATH}",
            ]
        )
        assert mock_get_run_number.call_count == 2

        mock_scan_parquet.assert_called_once_with(f"s3://{BUCKET_NAME}/{SOURCE_PATH}")
        mock_scanned_lf.filter.assert_called_once()
        mock_read_parquet.assert_called_once_with(
            source=f"s3://{BUCKET_NAME}/{COMPARE_PATH}"
        )

        mock_write_reports.assert_called_once()
        validation_arg, bucket_name_arg, reports_path_arg = (
            mock_write_reports.call_args[0]
        )
        assert bucket_name_arg == BUCKET_NAME
        assert reports_path_arg == REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}
        expected_assertions = {
            "col_schema_match",
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "specially",
        }
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"
