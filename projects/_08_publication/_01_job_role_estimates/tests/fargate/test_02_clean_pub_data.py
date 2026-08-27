from unittest.mock import Mock, patch

import projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data as job

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data"
)

TEST_SOURCE = "some/directory"
TEST_ASSESSMENT_DESTINATION = "some/assessment/directory"
TEST_PUBLICATION_DESTINATION = "some/publication/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.split_into_assessment_and_publication_data")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_dispersion_filter")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_consistent_service")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_has_ct_data")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs_all_steps_and_sinks_both_outputs(
        self,
        scan_parquet_mock: Mock,
        add_ct_filter_has_ct_data_mock: Mock,
        add_ct_filter_consistent_service_mock: Mock,
        add_ct_filter_dispersion_filter_mock: Mock,
        split_into_assessment_and_publication_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        merged_lf = Mock(name="merged_lf")
        scan_parquet_mock.return_value = merged_lf
        with_columns_lf = Mock(name="with_columns_lf")
        merged_lf.with_columns.return_value = with_columns_lf
        assessment_lf = Mock(name="assessment_lf")
        publication_lf = Mock(name="publication_lf")
        split_into_assessment_and_publication_data_mock.return_value = (
            assessment_lf,
            publication_lf,
        )

        job.main(TEST_SOURCE, TEST_ASSESSMENT_DESTINATION, TEST_PUBLICATION_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)
        merged_lf.with_columns.assert_called_once_with(
            add_ct_filter_has_ct_data_mock.return_value,
            add_ct_filter_consistent_service_mock.return_value,
            add_ct_filter_dispersion_filter_mock.return_value,
        )
        split_into_assessment_and_publication_data_mock.assert_called_once_with(
            with_columns_lf
        )

        assert sink_to_parquet_mock.call_count == 2
        sink_to_parquet_mock.assert_any_call(
            lazy_df=assessment_lf,
            output_path=TEST_ASSESSMENT_DESTINATION,
        )
        sink_to_parquet_mock.assert_any_call(
            lazy_df=publication_lf,
            output_path=TEST_PUBLICATION_DESTINATION,
        )
