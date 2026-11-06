from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate._04_reconcile_output as job
from utils.column_names.ind_cqc_pipeline_columns import ArchiveRunLogColumns as RunLog
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate._04_reconcile_output"
)

ASSESSMENT_SOURCE = "some/assessment/directory/"
PROMOTED_ARCHIVE_SOURCE = "some/promoted/directory/"
PROMOTED_METADATA_SOURCE = "some/promoted/metadata/directory/"
RUN_LOG_DESTINATION = "some/run_log/directory/"
ASSESSMENT_RUN_SOURCE = (
    "some/assessment/directory/archive_date=2026-10-01/run_number=2/"
)
ARCHIVE_RUN_SOURCE = "some/promoted/directory/archive_date=2026-10-01/run_number=2/"
IN_PERIOD = date(2026, 4, 1)
OTHER_PERIOD = date(2026, 5, 1)
# a period the archive holds that the public data does not
ARCHIVE_ONLY_PERIOD = date(2013, 10, 1)

ASSESSMENT_SCHEMA = pl.Schema(
    [
        (IndCQC.cqc_location_import_date, pl.Date()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (IndCQC.current_region, pl.String()),
        (IndCQC.primary_service_type, pl.String()),
        (Pub.unfiltered_filled_posts, pl.Float32()),
    ]
)
ARCHIVE_SCHEMA = pl.Schema(
    [
        (IndCQC.cqc_location_import_date, pl.Date()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (IndCQC.estimate_filled_posts_by_job_role, pl.Float32()),
    ]
)


def _assessment_row(
    import_date: date,
    filled_posts: float,
    job_role: str = PublishedJobGroupLabels.all_job_roles,
    region: str = PublishedRegion.england,
    service: str = PublishedMainService.all_locations,
) -> tuple:
    """A row of the assessment data; defaults to the national total row."""
    return (import_date, job_role, region, service, filled_posts)


def _national_rows(
    import_date: date, filled_posts: float, job_role: str = "Registered nurse"
) -> list[tuple]:
    """The national "All job roles" row and one job role row, with equal posts."""
    return [
        _assessment_row(import_date, filled_posts),
        _assessment_row(import_date, filled_posts, job_role=job_role),
    ]


def _archive_row(
    import_date: date, estimate: float, job_role: str = "Registered nurse"
) -> tuple:
    """A row of the archive, which holds one row per location and job role."""
    return (import_date, job_role, estimate)


@pytest.fixture
def mocks():
    """Replaces the job's inputs and outputs with mocks."""
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet,
        patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources") as resolve_run_sources,
        patch(f"{PATCH_PATH}.run_log_utils.mark_run_log_flag") as mark_run_log_flag,
    ):
        resolve_run_sources.return_value = [ASSESSMENT_RUN_SOURCE, ARCHIVE_RUN_SOURCE]
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
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.05)],  # 0.005% diff, within tolerance
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_totals_exceed_tolerance(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 2000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_raises_when_a_period_is_off_though_the_totals_match(self, mocks):
        # Errors in opposite directions cancel in the overall total.
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0) + _national_rows(OTHER_PERIOD, 1000.0),
            [
                _archive_row(IN_PERIOD, 1010.0),
                _archive_row(OTHER_PERIOD, 990.0),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_names_the_periods_that_differ(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0) + _national_rows(OTHER_PERIOD, 1000.0),
            [
                _archive_row(IN_PERIOD, 1000.0),
                _archive_row(OTHER_PERIOD, 2000.0),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError) as error:
            _run_main(mocks)

        assert str(OTHER_PERIOD) in str(error.value)
        assert str(IN_PERIOD) not in str(error.value)

    def test_passes_when_every_role_is_within_tolerance(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 400.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 600.0, job_role="Care worker"),
            ],
            [
                _archive_row(IN_PERIOD, 400.02, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 600.03, job_role="Care worker"),
            ],
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_two_roles_are_off_in_opposite_directions_and_national_total_matches(
        self, mocks
    ):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 2000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Care worker"),
            ],
            [
                _archive_row(IN_PERIOD, 1100.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 900.0, job_role="Care worker"),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_names_the_role_and_period_that_differ(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 3000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Care worker"),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Support worker"),
            ],
            [
                _archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 1100.0, job_role="Care worker"),
                _archive_row(IN_PERIOD, 900.0, job_role="Support worker"),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError) as error:
            _run_main(mocks)

        assert "Care worker" in str(error.value)
        assert "Support worker" in str(error.value)
        assert str(IN_PERIOD) in str(error.value)
        assert "Registered nurse" not in str(error.value)

    def test_raises_when_a_role_is_only_in_the_archive(self, mocks):
        # The assessment keeps the total but has dropped the Care worker row.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 400.0, job_role="Registered nurse"),
            ],
            [
                _archive_row(IN_PERIOD, 400.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 600.0, job_role="Care worker"),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="only") as error:
            _run_main(mocks)

        assert "Care worker" in str(error.value)
        assert str(IN_PERIOD) in str(error.value)

    def test_raises_when_a_role_is_only_in_the_assessment(self, mocks):
        # A role renamed in the assessment leaves the total unchanged.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Renamed role"),
            ],
            [_archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse")],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="only") as error:
            _run_main(mocks)

        assert "Renamed role" in str(error.value)
        assert "Registered nurse" in str(error.value)

    def test_raises_when_a_role_total_is_nan(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, float("nan"), job_role="Registered nurse"),
            ],
            [_archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse")],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_excludes_the_all_job_roles_and_non_national_rows_from_the_role_comparison(
        self, mocks
    ):
        # Neither the "All job roles" rollup nor regional/service rollups are roles.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 99.0, region="London"),
                _assessment_row(
                    IN_PERIOD, 7.0, job_role="Care worker", region="London"
                ),
                _assessment_row(
                    IN_PERIOD,
                    5.0,
                    job_role="Care worker",
                    service=PublishedMainService.all_care_homes,
                ),
            ],
            [_archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse")],
        )

        _run_main(mocks)  # does not raise

    def test_limits_the_roles_named_in_the_error(self, mocks):
        # Alternating over and under so the national total still matches.
        roles = [f"role_{i}" for i in range(2 * job.MAX_DIFFERENCES_REPORTED)]
        _set_sources(
            mocks,
            [_assessment_row(IN_PERIOD, 1000.0 * len(roles))]
            + [_assessment_row(IN_PERIOD, 1000.0, job_role=role) for role in roles],
            [
                _archive_row(IN_PERIOD, 1500.0 if i % 2 == 0 else 500.0, job_role=role)
                for i, role in enumerate(roles)
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError) as error:
            _run_main(mocks)

        assert str(error.value).count("role_") == job.MAX_DIFFERENCES_REPORTED

    def test_resolves_both_sources_for_the_requested_run_number(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        _run_main(mocks, run_number=3)

        mocks.resolve_run_sources.assert_called_once_with(
            [ASSESSMENT_SOURCE, PROMOTED_ARCHIVE_SOURCE], 3
        )

    def test_sums_every_job_role_row_on_the_archive_side(self, mocks):
        # The archive has no "All job roles" row: a filter on that label would
        # match nothing and total 0.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 400.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 600.0, job_role="Care worker"),
            ],
            [
                _archive_row(IN_PERIOD, 400.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 600.0, job_role="Care worker"),
            ],
        )

        _run_main(mocks)  # does not raise

    def test_uses_only_the_national_all_job_roles_row_on_the_public_side(self, mocks):
        # The assessment data holds overlapping rollups (regions, service groups,
        # job roles), so summing them would count each location several times.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 111111.0, region="London"),
                _assessment_row(
                    IN_PERIOD, 222222.0, service=PublishedMainService.all_care_homes
                ),
                _assessment_row(
                    IN_PERIOD, 333333.0, job_role="Registered nurse", region="London"
                ),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
            ],
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        _run_main(mocks)  # does not raise

    def test_ignores_archive_periods_the_public_data_does_not_hold(self, mocks):
        # The archive keeps every period back to 2013; only the public data's own
        # periods are compared.
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [
                _archive_row(IN_PERIOD, 1000.0),
                _archive_row(ARCHIVE_ONLY_PERIOD, 444444.0),
            ],
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_the_archive_is_missing_a_public_period(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0) + _national_rows(OTHER_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 2000.0)],  # totals match, one period is absent
        )

        with pytest.raises(job.ReconciliationMismatchError, match="not.*archive"):
            _run_main(mocks)

    def test_raises_when_a_total_is_nan(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, float("nan")),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
            ],
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _run_main(mocks)

    def test_raises_when_the_assessment_has_no_national_row(self, mocks):
        _set_sources(
            mocks,
            [_assessment_row(IN_PERIOD, 1000.0, region="London")],
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="national"):
            _run_main(mocks)

    def test_marks_the_run_reconciled_on_match(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        _run_main(mocks, run_number=2)

        mocks.mark_run_log_flag.assert_called_once_with(
            RUN_LOG_DESTINATION, 2, RunLog.reconciled
        )

    def test_does_not_mark_the_run_reconciled_on_mismatch(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 2000.0)],
        )

        with pytest.raises(job.ReconciliationMismatchError):
            _run_main(mocks)

        mocks.mark_run_log_flag.assert_not_called()

    def test_does_not_mark_the_run_reconciled_when_only_a_role_differs(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 2000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Care worker"),
            ],
            [
                _archive_row(IN_PERIOD, 1100.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 900.0, job_role="Care worker"),
            ],
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

        assert job.is_run_promoted(PROMOTED_METADATA_SOURCE, 2) is False

    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_reports_run_promoted_when_the_long_term_partition_exists(
        self, list_archive_runs_mock: Mock
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-01", 2: "2026-10-08"}

        assert job.is_run_promoted(PROMOTED_METADATA_SOURCE, 2) is True


class TestCheckMain:
    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_returns_zero_when_the_run_is_promoted(self, list_archive_runs_mock: Mock):
        list_archive_runs_mock.return_value = {2: "2026-10-08"}

        assert job.check_main(PROMOTED_METADATA_SOURCE, 2) == 0

    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_returns_the_not_promoted_exit_code_when_the_run_is_not_promoted(
        self, list_archive_runs_mock: Mock
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-01"}

        assert job.check_main(PROMOTED_METADATA_SOURCE, 2) == job.NOT_PROMOTED_EXIT_CODE

    def test_not_promoted_exit_code_is_not_the_generic_failure_code(self):
        # A crash exits 1; the state machine retries only on the code below.
        assert job.NOT_PROMOTED_EXIT_CODE not in (0, 1)
