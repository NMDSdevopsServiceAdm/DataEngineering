import json
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.validate_ascwds_worker_raw_data as job
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerRawCategoricalValues as CatValues,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_ascwds_worker_raw_data"


def build_source_df(main_job_role_ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            AWK.establishment_id: [f"estab_{i}" for i in range(len(main_job_role_ids))],
            AWK.worker_id: [f"worker_{i}" for i in range(len(main_job_role_ids))],
            AWK.main_job_role_id: main_job_role_ids,
            AWK.import_date: ["20260101"] * len(main_job_role_ids),
        }
    )


def get_step(report_json: list[dict], assertion_type: str) -> dict:
    return next(
        item for item in report_json if item["assertion_type"] == assertion_type
    )


class TestMain:
    known_values = CatValues.main_job_role_id_column_values.categorical_values

    source_df = build_source_df([known_values[0], "-1"])

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/",
            selected_columns=[
                AWK.establishment_id,
                AWK.worker_id,
                AWK.main_job_role_id,
                AWK.import_date,
            ],
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {"col_vals_not_null", "col_vals_in_set", "specially"}
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_categorical_and_distinct_checks_pass_when_every_known_value_and_negative_one_are_present(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        # Historical raw data already contains legitimate "-1" rows, so both the
        # categorical and distinct-count checks must still pass with "-1" present,
        # even though ingest now hard-fails on new occurrences of it.
        mock_read_parquet.return_value = build_source_df([*self.known_values, "-1"])

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assert get_step(report_json, "col_vals_in_set")["all_passed"] is True
        assert get_step(report_json, "specially")["all_passed"] is True

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_categorical_check_fails_for_a_value_outside_the_known_set_and_negative_one(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = build_source_df([self.known_values[0], "999"])

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assert get_step(report_json, "col_vals_in_set")["all_passed"] is False

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_distinct_check_fails_when_a_known_job_role_is_missing(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        # Every known value plus "-1" is required to make the exact count in
        # is_unique_count_equal; dropping one known value should fail it.
        mock_read_parquet.return_value = build_source_df([*self.known_values[1:], "-1"])

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assert get_step(report_json, "specially")["all_passed"] is False
