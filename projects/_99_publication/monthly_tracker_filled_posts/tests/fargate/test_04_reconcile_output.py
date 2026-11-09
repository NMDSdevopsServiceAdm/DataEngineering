from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

import boto3
import openpyxl
import polars as pl
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

import projects._99_publication.monthly_tracker_filled_posts.fargate._04_reconcile_output as job
from polars_utils.column_types import CategoricalColumnTypes
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    output_workbook_utils,
)
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
OUTPUT_SOURCE = "some/output/directory/"
WORKBOOK_SOURCE = "s3://bucket/some/workbook/directory/"
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
        patch(f"{PATCH_PATH}.reconcile_download") as reconcile_download,
    ):
        resolve_run_sources.return_value = [ASSESSMENT_RUN_SOURCE, ARCHIVE_RUN_SOURCE]
        yield SimpleNamespace(
            scan_parquet=scan_parquet,
            resolve_run_sources=resolve_run_sources,
            mark_run_log_flag=mark_run_log_flag,
            reconcile_download=reconcile_download,
        )


def _set_sources(
    mocks,
    assessment_rows: list[tuple],
    archive_rows: list[tuple],
    assessment_role_type: pl.DataType = pl.String(),
    archive_role_type: pl.DataType = pl.String(),
) -> None:
    """Points the mocked scan_parquet at the right fixture frame for each source."""
    role = pl.col(IndCQC.main_job_role_clean_labelled)
    assessment_lf = pl.LazyFrame(
        assessment_rows, ASSESSMENT_SCHEMA, orient="row"
    ).with_columns(role.cast(assessment_role_type))
    archive_lf = pl.LazyFrame(archive_rows, ARCHIVE_SCHEMA, orient="row").with_columns(
        role.cast(archive_role_type)
    )
    sources = {ASSESSMENT_RUN_SOURCE: assessment_lf, ARCHIVE_RUN_SOURCE: archive_lf}
    mocks.scan_parquet.side_effect = lambda source: sources[source]


def _run_main(mocks, run_number: int = 2) -> None:
    job.main(
        assessment_source=ASSESSMENT_SOURCE,
        promoted_archive_source=PROMOTED_ARCHIVE_SOURCE,
        run_log_destination=RUN_LOG_DESTINATION,
        output_source=OUTPUT_SOURCE,
        workbook_source=WORKBOOK_SOURCE,
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

        with pytest.raises(job.ReconciliationMismatchError, match="periods differ"):
            _run_main(mocks)

    def test_raises_when_only_the_national_total_differs(self, mocks):
        # The job role rows match the archive, so only the period check can fail.
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 2000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
            ],
            [_archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse")],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="periods differ"):
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

        with pytest.raises(job.ReconciliationMismatchError, match="periods differ"):
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

        with pytest.raises(job.ReconciliationMismatchError, match="job role totals"):
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

        assert f"{IN_PERIOD} Care worker (only in the archive)" in str(error.value)

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

        assert "Renamed role (only in the public output)" in str(error.value)
        assert "Registered nurse (only in the archive)" in str(error.value)

    def test_passes_when_a_role_has_no_posts_in_either_source(self, mocks):
        # A role not yet in use in a period totals 0 on both sides (0 / 0 is NaN).
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 0.0, job_role="Data analyst"),
            ],
            [
                _archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 0.0, job_role="Data analyst"),
            ],
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_a_role_has_posts_in_the_public_output_only(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, 990.0, job_role="Registered nurse"),
                _assessment_row(IN_PERIOD, 10.0, job_role="Data analyst"),
            ],
            [
                _archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse"),
                _archive_row(IN_PERIOD, 0.0, job_role="Data analyst"),
            ],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="Data analyst"):
            _run_main(mocks)

    def test_raises_when_a_role_total_is_nan(self, mocks):
        _set_sources(
            mocks,
            [
                _assessment_row(IN_PERIOD, 1000.0),
                _assessment_row(IN_PERIOD, float("nan"), job_role="Registered nurse"),
            ],
            [_archive_row(IN_PERIOD, 1000.0, job_role="Registered nurse")],
        )

        with pytest.raises(job.ReconciliationMismatchError, match="job role totals"):
            _run_main(mocks)

    @pytest.mark.parametrize(
        "assessment_role_type",
        [pl.String(), CategoricalColumnTypes.JobRoleCatType],
        ids=["assessment_string", "assessment_categorical"],
    )
    @pytest.mark.parametrize(
        "archive_role_type",
        [pl.String(), CategoricalColumnTypes.JobRoleCatType],
        ids=["archive_string", "archive_categorical"],
    )
    def test_matches_roles_whatever_their_type_in_each_source(
        self, mocks, assessment_role_type, archive_role_type
    ):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.0)],
            assessment_role_type,
            archive_role_type,
        )

        _run_main(mocks)  # does not raise

    def test_raises_when_a_period_has_job_role_rows_but_no_all_job_roles_row(
        self, mocks
    ):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0)
            + [_assessment_row(OTHER_PERIOD, 1000.0, job_role="Registered nurse")],
            [
                _archive_row(IN_PERIOD, 1000.0),
                _archive_row(OTHER_PERIOD, 1000.0),
            ],
        )

        with pytest.raises(
            job.ReconciliationMismatchError, match="no national"
        ) as error:
            _run_main(mocks)

        assert str(OTHER_PERIOD) in str(error.value)

    def test_reports_the_largest_job_role_difference(self, mocks, capsys):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.05)],
        )

        _run_main(mocks)

        assert "largest job role difference 0.0050%" in capsys.readouterr().out

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

        with pytest.raises(job.ReconciliationMismatchError, match="periods differ"):
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

    def test_marks_the_run_reconciled_only_when_archive_and_download_both_match(
        self, mocks
    ):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.0)],
        )

        _run_main(mocks, run_number=2)

        mocks.reconcile_download.assert_called_once_with(
            OUTPUT_SOURCE, WORKBOOK_SOURCE, ASSESSMENT_RUN_SOURCE
        )
        mocks.mark_run_log_flag.assert_called_once_with(
            RUN_LOG_DESTINATION, 2, RunLog.reconciled
        )

    def test_does_not_mark_the_run_reconciled_when_the_download_differs(self, mocks):
        _set_sources(
            mocks,
            _national_rows(IN_PERIOD, 1000.0),
            [_archive_row(IN_PERIOD, 1000.0)],
        )
        mocks.reconcile_download.side_effect = job.ReconciliationMismatchError(
            "download differs"
        )

        with pytest.raises(job.ReconciliationMismatchError, match="download differs"):
            _run_main(mocks)

        mocks.mark_run_log_flag.assert_not_called()


PUBLICATION_SCHEMA = pl.Schema(
    [
        (Pub.period, pl.Date()),
        (Pub.period_label_short, pl.String()),
        (Pub.region, pl.String()),
        (Pub.main_service, pl.String()),
        (Pub.estimated_filled_posts_rounded, pl.Float64()),
        (Pub.cqc_locations, pl.Int64()),
        (Pub.unfiltered_filled_posts_april_pct_change, pl.Float64()),
        (Pub.unfiltered_short_sfc_cum_pct_change, pl.Float64()),
        (Pub.unfiltered_locationid_count_april_pct_change, pl.Float64()),
        (Pub.unfiltered_short_locationid_count_cum_pct_change, pl.Float64()),
    ]
)
WORKBOOK_NAME = "monthly_tracker_data_download_2026-10-01_2.xlsx"
OTHER_RUN_WORKBOOK_NAME = "monthly_tracker_data_download_2026-10-01_3.xlsx"


def _publication_row(
    import_date: date = IN_PERIOD,
    label: str = "Mar 2026",
    region: str = "London",
    posts: float | None = 120.0,
    locations: int = 10,
    april_pct: float | None = 0.1,
    cumulative_pct: float | None = 0.2,
    location_april_pct: float | None = 0.3,
    location_cumulative_pct: float | None = 0.4,
) -> tuple:
    """A row of the publication data the download workbook is built from."""
    return (
        import_date,
        label,
        region,
        PublishedMainService.all_locations,
        posts,
        locations,
        april_pct,
        cumulative_pct,
        location_april_pct,
        location_cumulative_pct,
    )


def _publication_lf(rows: list[tuple]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, PUBLICATION_SCHEMA, orient="row")


class TestDownloadReconciliation:
    @pytest.fixture
    def s3(self):
        with mock_aws():
            client = boto3.client("s3", region_name="eu-west-2")
            client.create_bucket(
                Bucket="bucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            yield client

    @pytest.fixture
    def scan_parquet(self):
        with patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet:
            yield scan_parquet

    @staticmethod
    def _put_workbook(
        s3, tmp_path, rows: list[tuple], name: str = WORKBOOK_NAME
    ) -> None:
        """Builds a real download workbook from the rows and stores it in S3."""
        path = tmp_path / name
        output_workbook_utils.build_download_workbook(_publication_lf(rows), path)
        s3.put_object(
            Bucket="bucket",
            Key=f"some/workbook/directory/{name}",
            Body=path.read_bytes(),
        )

    @staticmethod
    def _reconcile() -> None:
        job.reconcile_download(OUTPUT_SOURCE, WORKBOOK_SOURCE, ASSESSMENT_RUN_SOURCE)

    def test_passes_when_workbook_tables_match_prepared_tables(
        self, s3, scan_parquet, tmp_path
    ):
        rows = [_publication_row(), _publication_row(OTHER_PERIOD, "Apr 2026")]
        self._put_workbook(s3, tmp_path, rows)
        scan_parquet.return_value = _publication_lf(rows)

        self._reconcile()  # does not raise

        scan_parquet.assert_called_once_with(OUTPUT_SOURCE)

    def test_raises_when_a_workbook_value_differs_from_the_prepared_value(
        self, s3, scan_parquet, tmp_path
    ):
        self._put_workbook(s3, tmp_path, [_publication_row(cumulative_pct=0.2)])
        scan_parquet.return_value = _publication_lf(
            [_publication_row(cumulative_pct=0.9)]
        )

        with pytest.raises(job.ReconciliationMismatchError, match="T1"):
            self._reconcile()

    def test_raises_when_a_workbook_row_is_missing(self, s3, scan_parquet, tmp_path):
        self._put_workbook(s3, tmp_path, [_publication_row()])
        scan_parquet.return_value = _publication_lf(
            [_publication_row(), _publication_row(OTHER_PERIOD, "Apr 2026")]
        )

        with pytest.raises(job.ReconciliationMismatchError, match="row"):
            self._reconcile()

    def test_raises_when_a_table_sheet_is_missing_from_the_workbook(
        self, s3, scan_parquet, tmp_path
    ):
        rows = [_publication_row()]
        path = tmp_path / WORKBOOK_NAME
        output_workbook_utils.build_download_workbook(_publication_lf(rows), path)
        workbook = openpyxl.load_workbook(path)
        del workbook["T2"]
        workbook.save(path)
        s3.put_object(
            Bucket="bucket",
            Key=f"some/workbook/directory/{WORKBOOK_NAME}",
            Body=path.read_bytes(),
        )
        scan_parquet.return_value = _publication_lf(rows)

        with pytest.raises(job.ReconciliationMismatchError, match="T2"):
            self._reconcile()

    def test_raises_when_the_workbook_is_missing_from_s3(self, s3, scan_parquet):
        scan_parquet.return_value = _publication_lf([_publication_row()])

        with pytest.raises(ClientError):
            self._reconcile()

    def test_locates_the_workbook_by_archive_date_and_run_number(
        self, s3, scan_parquet, tmp_path
    ):
        rows = [_publication_row()]
        # Another run's workbook, holding different data, sits in the same folder.
        self._put_workbook(
            s3, tmp_path, [_publication_row(posts=9999.0)], OTHER_RUN_WORKBOOK_NAME
        )
        self._put_workbook(s3, tmp_path, rows)
        scan_parquet.return_value = _publication_lf(rows)

        self._reconcile()  # does not raise

    def test_raises_when_a_t0_value_differs_in_one_period(
        self, s3, scan_parquet, tmp_path
    ):
        self._put_workbook(
            s3,
            tmp_path,
            [
                _publication_row(posts=100.0),
                _publication_row(OTHER_PERIOD, "Apr 2026", posts=200.0),
            ],
        )
        scan_parquet.return_value = _publication_lf(
            [
                _publication_row(posts=100.0),
                _publication_row(OTHER_PERIOD, "Apr 2026", posts=300.0),
            ]
        )

        with pytest.raises(job.ReconciliationMismatchError, match="T0") as error:
            self._reconcile()

        assert "1 cells differ" in str(error.value)
        assert "200" in str(error.value)
        assert "300" in str(error.value)

    @pytest.mark.parametrize(
        "row",
        [
            pytest.param(_publication_row(posts=None), id="t0_suppressed_label_s"),
            pytest.param(
                _publication_row(april_pct=None, location_cumulative_pct=None),
                id="perc_change_null_label_x",
            ),
            pytest.param(
                _publication_row(april_pct=0.12345, cumulative_pct=-0.06789),
                id="perc_change_rounded_to_1dp",
            ),
        ],
    )
    def test_compares_prepared_labels_not_raw_values(
        self, s3, scan_parquet, tmp_path, row
    ):
        # The workbook shows "[s]" / "[x]" and rounded percentages where the data
        # holds nulls and unrounded fractions: the prepared values are compared.
        self._put_workbook(s3, tmp_path, [row])
        scan_parquet.return_value = _publication_lf([row])

        self._reconcile()  # does not raise


TABLE_HEADER = (
    Pub.period,
    Pub.region,
    Pub.main_service,
    Pub.estimated_filled_posts,
    Pub.cqc_locations,
)
TABLE_ROW = ("Mar 2026", "London", "All CQC locations", 120.0, 10)


def _tables() -> dict[str, list[tuple]]:
    """Three identical rows-as-tuples tables, header first, keyed by sheet name."""
    return {sheet: [TABLE_HEADER, TABLE_ROW] for sheet in ("T0", "T1", "T2")}


class TestCompareDownloadTables:
    def test_raises_when_a_workbook_table_has_an_extra_column(self):
        workbook_tables = _tables()
        workbook_tables["T1"] = [TABLE_HEADER + ("Extra",), TABLE_ROW + (1,)]

        with pytest.raises(job.ReconciliationMismatchError, match="T1.*column"):
            job.compare_download_tables(workbook_tables, _tables(), 2)

    def test_raises_when_a_workbook_table_lacks_a_column(self):
        workbook_tables = _tables()
        workbook_tables["T2"] = [TABLE_HEADER[:-1], TABLE_ROW[:-1]]

        with pytest.raises(job.ReconciliationMismatchError, match="T2.*column"):
            job.compare_download_tables(workbook_tables, _tables(), 2)

    def test_passes_when_the_tables_match(self):
        job.compare_download_tables(_tables(), _tables(), 2)  # does not raise


def _role_totals(rows: list[tuple], value_column: str) -> pl.DataFrame:
    """Period and job role totals as the comparison receives them."""
    schema = {
        IndCQC.cqc_location_import_date: pl.Date(),
        IndCQC.main_job_role_clean_labelled: pl.String(),
        value_column: pl.Float64(),
    }
    return pl.DataFrame(rows, schema, orient="row")


def _raise_if_roles_differ(
    public_posts: list[float], archive_posts: list[float]
) -> None:
    """Compares totals for roles named role_0, role_1 and so on in one period."""
    roles = [f"role_{i}" for i in range(len(public_posts))]
    job._raise_if_roles_differ(
        _role_totals(
            [(IN_PERIOD, role, posts) for role, posts in zip(roles, public_posts)],
            Pub.unfiltered_filled_posts,
        ),
        _role_totals(
            [(IN_PERIOD, role, posts) for role, posts in zip(roles, archive_posts)],
            IndCQC.estimate_filled_posts_by_job_role,
        ),
        run_number=2,
    )


class TestRaiseIfRolesDiffer:
    def test_raises_when_the_archive_total_is_nan(self):
        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _raise_if_roles_differ([1000.0], [float("nan")])

    def test_raises_when_both_totals_are_nan(self):
        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _raise_if_roles_differ([float("nan")], [float("nan")])

    def test_raises_when_a_role_is_just_over_tolerance(self):
        with pytest.raises(job.ReconciliationMismatchError, match="differ"):
            _raise_if_roles_differ([1000.2], [1000.0])  # 0.02%

    def test_passes_when_a_role_is_just_within_tolerance(self):
        _raise_if_roles_differ([1000.09], [1000.0])  # 0.009%, does not raise

    def test_names_the_largest_differences_first(self):
        # The archive differs more with each role, so the worst roles sort last
        # alphabetically.
        count = job.MAX_DIFFERENCES_REPORTED + 2
        public_posts = [1000.0] * count
        archive_posts = [1000.0 + 10 * (i + 1) for i in range(count)]

        with pytest.raises(job.ReconciliationMismatchError) as error:
            _raise_if_roles_differ(public_posts, archive_posts)

        assert f"role_{count - 1}" in str(error.value)
        assert "role_0" not in str(error.value)


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
