from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean_slv as job

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean_slv"
)


class TestMain:
    GENERAL_CLEANED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.GENERAL_CLEANED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.GENERAL_CLEANED_DATA_SOURCE)

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
