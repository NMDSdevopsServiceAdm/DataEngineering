from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates as job

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates"

ESTIMATES_SOURCE = "some/estimates/directory"
METADATA_SOURCE = "some/metadata/directory"
ESTIMATES_DESTINATION = "some/estimates/destination"
METADATA_DESTINATION = "some/metadata/destination"
GEOGRAPHY_DESTINATION = "some/geography/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_job_role_estimates_source_with_reduced_columns(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        scan_parquet_mock.assert_any_call(
            ESTIMATES_SOURCE,
            selected_columns=job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_metadata_source_for_metadata_columns(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        scan_parquet_mock.assert_any_call(
            METADATA_SOURCE,
            selected_columns=job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_metadata_source_for_geography_columns(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        scan_parquet_mock.assert_any_call(
            METADATA_SOURCE,
            selected_columns=job.JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_sinks_each_lazyframe_to_its_own_destination(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        estimates_lf = Mock(name="estimates_lf")
        metadata_lf = Mock(name="metadata_lf")
        geography_lf = Mock(name="geography_lf")
        scan_parquet_mock.side_effect = [estimates_lf, metadata_lf, geography_lf]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        assert sink_to_parquet_mock.call_count == 3
        sink_to_parquet_mock.assert_has_calls(
            [
                call(estimates_lf, ESTIMATES_DESTINATION),
                call(metadata_lf, METADATA_DESTINATION),
                call(geography_lf, GEOGRAPHY_DESTINATION),
            ]
        )
