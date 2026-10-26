import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._99_publication.monthly_tracker_filled_posts.fargate.validate_02_assessment as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PrimaryServiceType

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate.validate_02_assessment"

BUCKET_NAME = "bucket"
PREP_SOURCE = "prep/"
ASSESSMENT_SOURCE = "assessment/"
REPORTS_PATH = "reports/"
PREP_RUN = "s3://bucket/prep/archive_date=2026-10-01/run_number=2/"
ASSESSMENT_RUN = "s3://bucket/assessment/archive_date=2026-10-01/run_number=2/"


def _assessment_df(rows: int = 1) -> pl.DataFrame:
    return pl.DataFrame(
        {
            IndCQC.cqc_location_import_date: [date(2026, 4, 1)] * rows,
            Pub.period_label_short: ["Mar 2026"] * rows,
            Pub.period_label_long: ["March 2026"] * rows,
            IndCQC.main_job_role_clean_labelled: ["a"] * rows,
            IndCQC.current_region: ["a"] * rows,
            IndCQC.primary_service_type: ["a"] * rows,
            **{column: [1.0] * rows for column in job.METRIC_COLUMNS},
        }
    )


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.count_expected_assessment_rows")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_runs_validation_on_the_assessment_run(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        count_expected_rows_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [PREP_RUN, ASSESSMENT_RUN]
        read_parquet_mock.return_value = _assessment_df()
        count_expected_rows_mock.return_value = 1

        job.main(
            BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH, run_number=2
        )

        resolve_run_sources_mock.assert_called_once_with(
            [f"s3://{BUCKET_NAME}/prep/", f"s3://{BUCKET_NAME}/assessment/"], 2
        )
        read_parquet_mock.assert_called_once_with(
            ASSESSMENT_RUN,
            selected_columns=[name for name, _ in job.EXPECTED_SCHEMA.columns],
        )
        count_expected_rows_mock.assert_called_once_with(PREP_RUN)

        validation_arg, bucket_name_arg, reports_path_arg = (
            write_reports_mock.call_args[0]
        )
        assert bucket_name_arg == BUCKET_NAME
        assert reports_path_arg == REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}
        assert assertion_types_present == {
            "col_schema_match",
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "col_vals_ge",
            "col_vals_regex",
            "col_vals_expr",
            "col_vals_in_set",
            "specially",
        }

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.count_expected_assessment_rows")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_expects_row_count_from_the_prepared_run(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        count_expected_rows_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [PREP_RUN, ASSESSMENT_RUN]
        read_parquet_mock.return_value = _assessment_df()
        count_expected_rows_mock.return_value = 18

        job.main(BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH)

        validation_arg = write_reports_mock.call_args[0][0]
        row_count_step = next(
            item
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 18

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.count_expected_assessment_rows")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_fails_filtered_value_exceeding_its_total(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        count_expected_rows_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [PREP_RUN, ASSESSMENT_RUN]
        read_parquet_mock.return_value = _assessment_df().with_columns(
            pl.lit(5.0).alias(Pub.filtered_filled_posts_long)
        )
        count_expected_rows_mock.return_value = 1

        job.main(BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH)

        validation_arg = write_reports_mock.call_args[0][0]
        failed_briefs = [
            item["brief"]
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "col_vals_expr" and not item["all_passed"]
        ]
        assert any(Pub.filtered_filled_posts_long in brief for brief in failed_briefs)


class TestCountExpectedAssessmentRows:
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_counts_publication_and_rollup_rows(self, scan_parquet_mock: Mock):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only] * 2,
                IndCQC.main_job_role_clean_labelled: ["role_a", "role_b"],
                IndCQC.estimate_filled_posts_by_job_role: [1.0, 2.0],
                IndCQC.current_region: ["London"] * 2,
            }
        )

        # 2 roles + "All job roles" = 3, over 3 services (the service plus 2 rollups)
        # = 9, plus the same 9 for the "England" region rollup.
        assert job.count_expected_assessment_rows(PREP_RUN) == 18
        scan_parquet_mock.assert_called_once_with(
            PREP_RUN, selected_columns=job.PREP_COLUMNS
        )
