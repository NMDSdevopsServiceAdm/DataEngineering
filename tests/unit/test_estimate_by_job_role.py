import unittest
from unittest.mock import patch, Mock
import jobs.estimate_by_job_role as job


class BaseSetup(unittest.TestCase):
    def setUp(self) -> None:
        pass

class MainTests(BaseSetup):
    @patch("utils.utils.read_from_parquet")
    def test_main_function(self, read_from_parquet_mock: Mock):
        SOURCE = 'some/file/source'

        job.main(SOURCE)
        
        read_from_parquet_mock.assert_called_once_with(SOURCE)

