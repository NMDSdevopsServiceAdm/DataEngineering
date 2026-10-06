from unittest.mock import Mock, patch

import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate._01_merge as job

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate._01_merge"

TEST_ESTIMATES_ROOT = "some/directory/"
TEST_METADATA_ROOT = "some/metadata/directory/"
TEST_ESTIMATES_SOURCE = "some/directory/archive_date=2026-10-01/run_number=2/"
TEST_METADATA_SOURCE = "some/metadata/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_runs(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]
        archived_jr_estimate_lf = Mock(name="archived_jr_estimate_lf")
        archived_jr_metadata_lf = Mock(name="archived_jr_metadata_lf")
        scan_parquet_mock.side_effect = [
            archived_jr_estimate_lf,
            archived_jr_metadata_lf,
        ]

        job.main(
            TEST_ESTIMATES_ROOT,
            TEST_METADATA_ROOT,
            TEST_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        scan_parquet_mock.assert_any_call(
            TEST_ESTIMATES_SOURCE,
            selected_columns=job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
        )
        scan_parquet_mock.assert_any_call(
            TEST_METADATA_SOURCE,
            selected_columns=job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
        )

        archived_jr_estimate_lf.join.assert_called_once_with(
            archived_jr_metadata_lf,
            on=job.IndCQC.id_per_locationid_import_date,
            how="left",
        )
        joined_metadata_lf = archived_jr_estimate_lf.join.return_value

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=joined_metadata_lf,
            output_path=TEST_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_resolves_latest_run_when_no_run_number_given(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        resolve_run_sources_mock.assert_called_once_with(
            [TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT], None
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_resolves_requested_run_number(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(
            TEST_ESTIMATES_ROOT,
            TEST_METADATA_ROOT,
            TEST_DESTINATION,
            run_number=3,
        )

        resolve_run_sources_mock.assert_called_once_with(
            [TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT], 3
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_logs_the_resolved_run_sources(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        capsys: pytest.CaptureFixture,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        logged = capsys.readouterr().out
        assert TEST_ESTIMATES_SOURCE in logged
        assert TEST_METADATA_SOURCE in logged
