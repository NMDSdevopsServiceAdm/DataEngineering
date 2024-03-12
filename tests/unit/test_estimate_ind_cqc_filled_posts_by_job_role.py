import unittest
from unittest.mock import ANY, patch, Mock
import jobs.estimate_ind_cqc_filled_posts_by_job_role as job


class BaseSetup(unittest.TestCase):
    def setUp(self) -> None:
        pass


class MainTests(BaseSetup):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_function(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        SOURCE = "some/source"
        OUTPUT_DIR = "some/destination"

        job.main(SOURCE, OUTPUT_DIR)

        read_from_parquet_mock.assert_called_once_with(SOURCE)
        write_to_parquet_mock.assert_called_once_with(
            ANY, OUTPUT_DIR, "overwrite", ["year", "month", "day", "import_date"]
        )
