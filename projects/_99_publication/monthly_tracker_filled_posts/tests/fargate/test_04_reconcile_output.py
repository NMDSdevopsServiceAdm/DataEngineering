from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate._04_reconcile_output as job
from utils.column_names.ind_cqc_pipeline_columns import ArchiveRunLogColumns as RunLog
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PublishedJobGroupLabels

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate._04_reconcile_output"
)

ASSESSMENT_SOURCE = "some/assessment/directory/"
PROMOTED_ARCHIVE_SOURCE = "some/promoted/directory/"
RUN_LOG_DESTINATION = "some/run_log/directory/"
ASSESSMENT_RUN_SOURCE = (
    "some/assessment/directory/archive_date=2026-10-01/run_number=2/"
)
ARCHIVE_RUN_SOURCE = "some/promoted/directory/archive_date=2026-10-01/run_number=2/"
TEST_TODAY = date(2026, 10, 6)
ALL_JOB_ROLES = PublishedJobGroupLabels.all_job_roles

ASSESSMENT_SCHEMA = pl.Schema(
    [
        (IndCQC.cqc_location_import_date, pl.Date()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (Pub.filled_posts, pl.Float32()),
    ]
)
ARCHIVE_SCHEMA = pl.Schema(
    [
        (IndCQC.cqc_location_import_date, pl.Date()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (IndCQC.estimate_filled_posts_by_job_role, pl.Float32()),
    ]
)


@pytest.fixture
def mocks():
    """Replaces the job's inputs and outputs with mocks and fixes today's date."""
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet,
        patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources") as resolve_run_sources,
        patch(f"{PATCH_PATH}.run_log_utils.mark_run_log_flag") as mark_run_log_flag,
        patch(f"{PATCH_PATH}.date") as date_mock,
    ):
        resolve_run_sources.return_value = [ASSESSMENT_RUN_SOURCE, ARCHIVE_RUN_SOURCE]
        date_mock.today.return_value = TEST_TODAY
        yield SimpleNamespace(
            scan_parquet=scan_parquet,
            resolve_run_sources=resolve_run_sources,
            mark_run_log_flag=mark_run_log_flag,
        )


def _set_sources(
    mocks, assessment_rows: list[tuple], archive_rows: list[tuple]
) -> None:
    """Points the mocked scan_parquet at the right fixture frame for each source."""
    assessment_lf = pl.LazyFrame(assessment_rows, ASSESSMENT_SCHEMA, orient="row")
    archive_lf = pl.LazyFrame(archive_rows, ARCHIVE_SCHEMA, orient="row")
    sources = {ASSESSMENT_RUN_SOURCE: assessment_lf, ARCHIVE_RUN_SOURCE: archive_lf}
    mocks.scan_parquet.side_effect = lambda source: sources[source]


def _run_main(mocks, run_number: int = 2) -> None:
    job.main(
        assessment_source=ASSESSMENT_SOURCE,
        promoted_archive_source=PROMOTED_ARCHIVE_SOURCE,
        run_log_destination=RUN_LOG_DESTINATION,
        run_number=run_number,
    )


class TestMain:
    def test_passes_when_totals_are_within_tolerance(self, mocks):
        _set_sources(
            mocks,
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1005.0)],  # 0.5% diff, within 1%
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_totals_exceed_tolerance(self, mocks):
        _set_sources(
            mocks,
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
            [(date(2026, 4, 1), ALL_JOB_ROLES, 2000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_resolves_both_sources_for_the_requested_run_number(self, mocks):
        _set_sources(
            mocks,
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
        )

        _run_main(mocks, run_number=3)

        mocks.resolve_run_sources.assert_called_once_with(
            [ASSESSMENT_SOURCE, PROMOTED_ARCHIVE_SOURCE], 3
        )

    def test_applies_the_same_all_job_roles_and_period_filter_to_both_sides(
        self, mocks
    ):
        # Each side's excluded rows (a non-"All job roles" row, and a pre-lookback
        # row outside the sampled quarter months -- today is 2026-10-06;
        # reduced_data_filter_expr's defaults keep 2025-04 onwards in full and only
        # Jan/Apr/Jul/Oct before that) carry a *different* huge value on each side:
        # if either filter failed to exclude them, the totals would diverge beyond
        # tolerance, where a shared leaked value could cancel out undetected.
        _set_sources(
            mocks,
            [
                (date(2026, 4, 1), ALL_JOB_ROLES, 1000.0),
                (date(2026, 4, 1), "Registered nurse", 111111.0),
                (date(2024, 5, 1), ALL_JOB_ROLES, 222222.0),
            ],
            [
                (date(2026, 4, 1), ALL_JOB_ROLES, 1000.0),
                (date(2026, 4, 1), "Registered nurse", 333333.0),
                (date(2024, 5, 1), ALL_JOB_ROLES, 444444.0),
            ],
        )

        _run_main(mocks)  # does not raise

    def test_marks_the_run_reconciled_on_match(self, mocks):
        _set_sources(
            mocks,
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
        )

        _run_main(mocks, run_number=2)

        mocks.mark_run_log_flag.assert_called_once_with(
            RUN_LOG_DESTINATION, 2, RunLog.reconciled
        )

    def test_does_not_mark_the_run_reconciled_on_mismatch(self, mocks):
        _set_sources(
            mocks,
            [(date(2026, 4, 1), ALL_JOB_ROLES, 1000.0)],
            [(date(2026, 4, 1), ALL_JOB_ROLES, 2000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError):
            _run_main(mocks)

        mocks.mark_run_log_flag.assert_not_called()


class TestIsRunPromoted:
    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_reports_run_not_yet_promoted_when_the_long_term_partition_is_missing(
        self, list_archive_runs_mock: Mock
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-01"}

        assert job.is_run_promoted(PROMOTED_ARCHIVE_SOURCE, 2) is False

    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_reports_run_promoted_when_the_long_term_partition_exists(
        self, list_archive_runs_mock: Mock
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-01", 2: "2026-10-08"}

        assert job.is_run_promoted(PROMOTED_ARCHIVE_SOURCE, 2) is True
