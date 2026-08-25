import json
from unittest.mock import Mock, patch

import pointblank as pb
import polars as pl

import projects._01_ingest.ascwds.fargate.validate_ascwds_worker_raw_data as job
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerRawCategoricalValues as CatValues,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_ascwds_worker_raw_data"


class TestMain:
    known_values = CatValues.main_job_role_id_column_values.categorical_values

    source_df = pl.DataFrame(
        {
            AWK.establishment_id: ["estab_1", "estab_2"],
            AWK.worker_id: ["worker_1", "worker_2"],
            AWK.main_job_role_id: [known_values[0], "-1"],
            AWK.import_date: ["20260101", "20260101"],
        }
    )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
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

    def test_unknown_value_negative_one_is_allowed_by_the_categorical_check(self):
        # Historical raw data already contains legitimate "-1" rows, so the
        # categorical/distinct-value checks must still pass with "-1" present,
        # even though ingest now hard-fails on new occurrences of it.
        validation = (
            pb.Validate(
                data=self.source_df,
                thresholds=GLOBAL_THRESHOLDS,
                actions=GLOBAL_ACTIONS,
            )
            .col_vals_in_set(AWK.main_job_role_id, [*self.known_values, "-1"])
            .interrogate()
        )

        report_json = json.loads(validation.get_json_report())
        categorical_step = next(
            item for item in report_json if item["assertion_type"] == "col_vals_in_set"
        )

        assert categorical_step["all_passed"] is True
