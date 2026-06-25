import unittest
from unittest.mock import ANY, Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._02_clean as job

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._02_clean"


class MainTests(unittest.TestCase):
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.MERGED_DATA_SOURCE)

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
