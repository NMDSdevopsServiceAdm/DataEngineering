import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

import jobs.ingest_ascwds_worker_data as job

from tests.test_file_generator import generate_worker_csv

class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"

    def setUp(self) -> None:
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_test_ingest_ascwds_dataset"
        ).getOrCreate()

        
        columns = ["locationid", "workerid", "mainjrid", "import_date"]

        rows = [
            ("1-000000001", "100", 1, "20220101"),
            ("1-000000001", "101", 1, "20220101"),
            ("1-000000001", "102", 1, "20220101"),
            ("1-000000001", "103", 1, "20220101"),
            ("1-000000001", "104", 2, "20220101"),
            ("1-000000001", "105", 3, "20220101"),
            ("1-000000002", "106", 1, "20220101"),
            ("1-000000002", "107", 3, "20220101"),
            ("1-000000002", "108", 2, "20220101"),
            ("1-000000003", "109", 1, "20220101"),
            ("1-000000003", "110", 2, "20220101"),
            ("1-000000003", "111", 3, "20220101"),
            ("1-000000004", "112", 1, "20220101"),
            ("1-000000004", "113", 2, "20220101"),
            ("1-000000004", "114", 3, "20220101"),
        ]

        self.test_ascwds_worker_df = self.spark.createDataFrame(rows, columns)

        self.spark_mock = Mock()
        type(self.spark_mock).read = self.spark_mock
        self.spark_mock.option.return_value = self.spark_mock

    @patch("utils.utils.get_spark")
    def test_main(self, get_spark_mock):
        get_spark_mock.return_value = self.spark_mock
        self.spark_mock.csv.return_value = self.test_ascwds_worker_df

        job.main('some source', 'some destination')
        

if __name__ == "__main__":
    unittest.main(warnings="ignore")