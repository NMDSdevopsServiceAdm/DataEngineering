import json
from datetime import date, datetime
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._03_independent_cqc._01_filled_posts._07_archive.fargate.validate_archive_job_role_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLogCols,
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

    @patch(f"{PATCH_PATH}.job_role_run_log_validation")
    @patch(f"{PATCH_PATH}.job_role_metadata_validation")
    @patch(f"{PATCH_PATH}.job_role_estimates_validation")
    def test_main_calls_run_log_validation_when_dataset_is_run_log(
        self,
        mock_estimates_validation: Mock,
        mock_metadata_validation: Mock,
        mock_run_log_validation: Mock,
    ):
        job.main(
            BUCKET_NAME,
            "run_log",
            SOURCE_PATH,
            COMPARE_PATH,
            OTHER_OUTPUT_PATH,
            REPORTS_PATH,
        )

        mock_run_log_validation.assert_called_once_with(
            BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
        )
        mock_estimates_validation.assert_not_called()
        mock_metadata_validation.assert_not_called()

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
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    def test_job_role_estimates_validation_runs(
        self,
        mock_get_run_number: Mock,
        mock_scan_parquet: Mock,
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
        compare_lf = pl.LazyFrame({"dummy": [1, 2]})
        mock_scan_parquet.side_effect = [mock_scanned_lf, compare_lf]

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

        mock_scan_parquet.assert_any_call(f"s3://{BUCKET_NAME}/{SOURCE_PATH}")
        mock_scan_parquet.assert_any_call(f"s3://{BUCKET_NAME}/{COMPARE_PATH}")
        assert mock_scan_parquet.call_count == 2
        mock_scanned_lf.filter.assert_called_once()

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
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    def test_job_role_metadata_validation_runs(
        self,
        mock_get_run_number: Mock,
        mock_scan_parquet: Mock,
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
        compare_lf = pl.LazyFrame({"dummy": [1, 2]})
        mock_scan_parquet.side_effect = [mock_scanned_lf, compare_lf]

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

        mock_scan_parquet.assert_any_call(f"s3://{BUCKET_NAME}/{SOURCE_PATH}")
        mock_scan_parquet.assert_any_call(f"s3://{BUCKET_NAME}/{COMPARE_PATH}")
        assert mock_scan_parquet.call_count == 2
        mock_scanned_lf.filter.assert_called_once()

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


def make_run_log_df(**overrides) -> pl.DataFrame:
    row = {
        ArchiveKeys.archive_date: date(2026, 9, 22),
        ArchiveKeys.run_number: 3,
        RunLogCols.archive_date_time: datetime(2026, 9, 22, 10, 0),
        RunLogCols.max_ascwds_workplace_import_date: date(2026, 8, 1),
        RunLogCols.max_cqc_location_import_date: date(2026, 8, 1),
        RunLogCols.max_cqc_pir_import_date: date(2026, 8, 1),
        RunLogCols.max_ct_care_home_import_date: date(2026, 8, 1),
        RunLogCols.max_ct_non_res_import_date: date(2026, 8, 1),
        RunLogCols.max_current_ons_import_date: date(2026, 8, 1),
        RunLogCols.approved: False,
        RunLogCols.commit_sha: "abc123",
        RunLogCols.tag: None,
        RunLogCols.selected_for_publication: False,
        RunLogCols.reconciled: False,
        RunLogCols.locally_checked: False,
    }
    row.update(overrides)
    return pl.DataFrame(
        {column: [value] for column, value in row.items()},
        schema_overrides={RunLogCols.tag: pl.String},
    )


class TestJobRoleRunLogValidation:
    @staticmethod
    def run_validation(
        run_log_df: pl.DataFrame, run_numbers_agree: bool = True
    ) -> Mock:
        """Runs the validation on run_log_df and returns the validation written."""
        with (
            patch(f"{PATCH_PATH}.vl.write_reports") as mock_write_reports,
            patch(f"{PATCH_PATH}.utils.scan_parquet") as mock_scan_parquet,
            patch(f"{PATCH_PATH}.aUtils.get_run_number", return_value=3),
            patch(
                f"{PATCH_PATH}.aUtils.make_run_numbers_agree_validator",
                return_value=lambda df: run_numbers_agree,
            ),
        ):
            mock_scan_parquet.return_value.filter.return_value.collect.return_value = (
                run_log_df
            )
            job.job_role_run_log_validation(
                BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
            )
        return mock_write_reports.call_args[0][0]

    def test_run_log_validation_passes_for_valid_run_log(self):
        validation = self.run_validation(make_run_log_df())

        assert validation.all_passed()

    def test_run_log_validation_passes_when_partition_columns_are_last(self):
        run_log_df = make_run_log_df()
        partition_columns = [ArchiveKeys.archive_date, ArchiveKeys.run_number]
        run_log_df = run_log_df.select(
            *[c for c in run_log_df.columns if c not in partition_columns],
            *partition_columns,
        )

        validation = self.run_validation(run_log_df)

        assert validation.all_passed()

    def test_run_log_validation_reads_run_log_and_checks_all_outputs_agree(self):
        with patch(
            f"{PATCH_PATH}.aUtils.make_run_numbers_agree_validator",
            return_value=lambda df: True,
        ) as mock_validator:
            with (
                patch(f"{PATCH_PATH}.vl.write_reports"),
                patch(f"{PATCH_PATH}.utils.scan_parquet") as mock_scan_parquet,
                patch(f"{PATCH_PATH}.aUtils.get_run_number", return_value=3),
            ):
                mock_scan_parquet.return_value.filter.return_value.collect.return_value = (
                    make_run_log_df()
                )
                job.job_role_run_log_validation(
                    BUCKET_NAME,
                    SOURCE_PATH,
                    COMPARE_PATH,
                    OTHER_OUTPUT_PATH,
                    REPORTS_PATH,
                )

        mock_scan_parquet.assert_called_once_with(f"s3://{BUCKET_NAME}/{SOURCE_PATH}")
        mock_validator.assert_called_once_with(
            [
                f"s3://{BUCKET_NAME}/{SOURCE_PATH}",
                f"s3://{BUCKET_NAME}/{COMPARE_PATH}",
                f"s3://{BUCKET_NAME}/{OTHER_OUTPUT_PATH}",
            ]
        )

    def test_run_log_validation_writes_reports_to_reports_path(self):
        with (
            patch(f"{PATCH_PATH}.vl.write_reports") as mock_write_reports,
            patch(f"{PATCH_PATH}.utils.scan_parquet") as mock_scan_parquet,
            patch(f"{PATCH_PATH}.aUtils.get_run_number", return_value=3),
        ):
            mock_scan_parquet.return_value.filter.return_value.collect.return_value = (
                make_run_log_df()
            )
            job.job_role_run_log_validation(
                BUCKET_NAME, SOURCE_PATH, COMPARE_PATH, OTHER_OUTPUT_PATH, REPORTS_PATH
            )

        assert mock_write_reports.call_args[0][1:] == (BUCKET_NAME, REPORTS_PATH)

    def test_run_log_validation_fails_when_a_column_is_missing(self):
        validation = self.run_validation(make_run_log_df().drop(RunLogCols.commit_sha))

        assert not validation.all_passed()

    def test_run_log_validation_fails_when_there_are_two_rows_for_the_run(self):
        run_log_df = pl.concat([make_run_log_df(), make_run_log_df()])

        validation = self.run_validation(run_log_df)

        assert not validation.all_passed()

    def test_run_log_validation_fails_when_there_are_no_rows_for_the_run(self):
        validation = self.run_validation(make_run_log_df().clear())

        assert not validation.all_passed()

    @pytest.mark.parametrize(
        "flag",
        [
            RunLogCols.approved,
            RunLogCols.selected_for_publication,
            RunLogCols.reconciled,
            RunLogCols.locally_checked,
        ],
    )
    def test_run_log_validation_fails_when_a_flag_is_true(self, flag: str):
        validation = self.run_validation(make_run_log_df(**{flag: True}))

        assert not validation.all_passed()

    def test_run_log_validation_fails_when_tag_is_set(self):
        validation = self.run_validation(make_run_log_df(**{RunLogCols.tag: "v1"}))

        assert not validation.all_passed()

    def test_run_log_validation_fails_when_commit_sha_is_null(self):
        run_log_df = make_run_log_df().with_columns(
            pl.lit(None, dtype=pl.String).alias(RunLogCols.commit_sha)
        )

        validation = self.run_validation(run_log_df)

        assert not validation.all_passed()

    def test_run_log_validation_fails_when_run_numbers_disagree(self):
        validation = self.run_validation(make_run_log_df(), run_numbers_agree=False)

        assert not validation.all_passed()
