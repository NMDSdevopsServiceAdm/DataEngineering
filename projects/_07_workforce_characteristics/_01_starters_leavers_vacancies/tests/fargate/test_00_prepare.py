from unittest.mock import Mock, call, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare"

CLEANED_ASCWDS_WORKPLACE_SOURCE = "some/source"
PREPARED_DATA_DESTINATION = "some/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.prepare_utils.convert_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.prepare_utils.discover_job_role_codes")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_scans_with_columns_narrowed_to_discovered_job_role_columns(
        self,
        scan_parquet_mock: Mock,
        discover_job_role_codes_mock: Mock,
        convert_job_role_columns_to_rows_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        discover_job_role_codes_mock.return_value = [
            job.prepare_utils.JobRoleCodeColumns(
                job_role_code="1",
                employees="jr01emp",
                starters="jr01strt",
                leavers="jr01stop",
                vacancies="jr01vacy",
            ),
        ]

        job.main(CLEANED_ASCWDS_WORKPLACE_SOURCE, PREPARED_DATA_DESTINATION)

        assert scan_parquet_mock.call_args_list == [
            call(CLEANED_ASCWDS_WORKPLACE_SOURCE),
            call(
                CLEANED_ASCWDS_WORKPLACE_SOURCE,
                selected_columns=[
                    *job.INDEX_COLUMNS,
                    "jr01emp",
                    "jr01strt",
                    "jr01stop",
                    "jr01vacy",
                ],
            ),
        ]

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.prepare_utils.convert_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.prepare_utils.discover_job_role_codes")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_reshapes_the_narrowly_scanned_lazyframe_and_sinks_it(
        self,
        scan_parquet_mock: Mock,
        discover_job_role_codes_mock: Mock,
        convert_job_role_columns_to_rows_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        discover_job_role_codes_mock.return_value = []

        job.main(CLEANED_ASCWDS_WORKPLACE_SOURCE, PREPARED_DATA_DESTINATION)

        convert_job_role_columns_to_rows_mock.assert_called_once_with(
            scan_parquet_mock.return_value, job.INDEX_COLUMNS, []
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=convert_job_role_columns_to_rows_mock.return_value,
            output_path=PREPARED_DATA_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.prepare_utils.convert_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.prepare_utils.discover_job_role_codes")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_still_sinks_when_query_plan_graph_rendering_fails(
        self,
        scan_parquet_mock: Mock,
        discover_job_role_codes_mock: Mock,
        convert_job_role_columns_to_rows_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        discover_job_role_codes_mock.return_value = []
        convert_job_role_columns_to_rows_mock.return_value.show_graph.side_effect = (
            RuntimeError("graphviz not installed")
        )

        job.main(CLEANED_ASCWDS_WORKPLACE_SOURCE, PREPARED_DATA_DESTINATION)

        sink_to_parquet_mock.assert_called_once()
