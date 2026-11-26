import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.validate_02_assessment as job
import projects._99_publication.unittest_data.polars_pub_test_data as Data
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
)

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


def _job_group_filled_posts_df(rows: list[tuple]) -> pl.DataFrame:
    """The assessment columns the job group sum check reads, from the rows of a
    JobGroupFilledPostsTestCase."""
    return pl.DataFrame(
        rows,
        schema=[
            IndCQC.cqc_location_import_date,
            IndCQC.current_region,
            IndCQC.primary_service_type,
            IndCQC.main_job_role_clean_labelled,
            Pub.unfiltered_filled_posts,
            Pub.filtered_long_sfc_filled_posts,
            Pub.filtered_medium_sfc_filled_posts,
            Pub.filtered_short_sfc_filled_posts,
        ],
        orient="row",
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
            pl.lit(5.0).alias(Pub.filtered_long_sfc_filled_posts)
        )
        count_expected_rows_mock.return_value = 1

        job.main(BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH)

        validation_arg = write_reports_mock.call_args[0][0]
        failed_briefs = [
            item["brief"]
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "col_vals_expr" and not item["all_passed"]
        ]
        assert any(
            Pub.filtered_long_sfc_filled_posts in brief for brief in failed_briefs
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.count_expected_assessment_rows")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_allows_job_group_labels_in_the_job_role_column(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        count_expected_rows_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [PREP_RUN, ASSESSMENT_RUN]
        read_parquet_mock.return_value = _assessment_df().with_columns(
            pl.lit(PublishedJobGroupLabels.direct_care).alias(
                IndCQC.main_job_role_clean_labelled
            )
        )
        count_expected_rows_mock.return_value = 1

        job.main(BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH)

        validation_arg = write_reports_mock.call_args[0][0]
        job_role_in_set_step = next(
            item
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "col_vals_in_set"
            and item["column"] == IndCQC.main_job_role_clean_labelled
        )
        assert job_role_in_set_step["all_passed"]

    @pytest.mark.parametrize(
        "case",
        # The passing and failing basic cases; the unit tests below cover the rest.
        [
            case.as_pytest_param()
            for case in Data.job_group_filled_posts_match_all_job_roles_test_cases[:2]
        ],
    )
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.count_expected_assessment_rows")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_passes_job_group_sum_check_only_when_filled_posts_match(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        count_expected_rows_mock: Mock,
        write_reports_mock: Mock,
        case,
    ):
        resolve_run_sources_mock.return_value = [PREP_RUN, ASSESSMENT_RUN]
        read_parquet_mock.return_value = _assessment_df(len(case.rows)).with_columns(
            *_job_group_filled_posts_df(case.rows).get_columns()
        )
        count_expected_rows_mock.return_value = len(case.rows)

        job.main(BUCKET_NAME, PREP_SOURCE, ASSESSMENT_SOURCE, REPORTS_PATH)

        validation_arg = write_reports_mock.call_args[0][0]
        sum_check_step = next(
            item
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "specially" and "job group" in item["brief"]
        )
        assert sum_check_step["all_passed"] == case.expected


class TestJobGroupFilledPostsMatchAllJobRoles:
    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.job_group_filled_posts_match_all_job_roles_test_cases
        ],
    )
    def test_returns_whether_job_group_filled_posts_match_all_job_roles(self, case):
        assessment_df = _job_group_filled_posts_df(case.rows)

        assert job.job_group_filled_posts_match_all_job_roles(assessment_df) is (
            case.expected
        )


class TestCountExpectedAssessmentRows:
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_includes_job_group_rows_in_expected_row_count(
        self, scan_parquet_mock: Mock
    ):
        prepared_lf = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only] * 2,
                IndCQC.main_job_role_clean_labelled: ["role_a", "role_b"],
                IndCQC.main_job_group_labelled: [PublishedJobGroupLabels.direct_care]
                * 2,
                IndCQC.estimate_filled_posts_by_job_role: [1.0, 2.0],
                IndCQC.current_region: ["London"] * 2,
            }
        )
        # Reads only the requested columns, as the real scan does, so a column the
        # row counting needs but PREP_COLUMNS omits fails here.
        scan_parquet_mock.side_effect = lambda source, selected_columns: (
            prepared_lf.select(selected_columns)
        )

        # 2 roles + 1 job group + "All job roles" = 4, over 3 services (the service
        # plus 2 rollups) = 12, plus the same 12 for the "England" region rollup.
        assert job.count_expected_assessment_rows(PREP_RUN) == 24
        scan_parquet_mock.assert_called_once_with(
            PREP_RUN, selected_columns=job.PREP_COLUMNS
        )
