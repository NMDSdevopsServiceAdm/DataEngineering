import unittest
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F

from utils import utils

import jobs.flatten_cqc_ratings as job

from tests.test_file_data import FlattenCQCRatings as Data
from tests.test_file_schemas import FlattenCQCRatings as Schema


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_LOCATIONS_SOURCE = "some/directory"
    TEST_PROVIDERS_SOURCE = "some/directory"
    TEST_WORKPLACE_SOURCE = "some/directory"
    TEST_CQC_RATINGS_DESTINATION = "some/other/directory"
    TEST_BENCHMARK_RATINGS_DESTINATION = "some/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_locations_df = self.spark.createDataFrame(
            Data.test_cqc_locations_rows, schema=Schema.test_cqc_locations_schema
        )
        self.test_cqc_providers_df = self.spark.createDataFrame(
            Data.test_cqc_providers_rows, schema=Schema.test_cqc_providers_schema
        )
        self.test_ascwds_df = self.spark.createDataFrame(
            Data.test_ascwds_workplace_rows, schema=Schema.test_ascwds_workplace_schema
        )


class MainTests(CleanCQCProviderDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_locations_df,
            self.test_cqc_providers_df,
            self.test_ascwds_df,
        ]
        job.main(self.TEST_LOCATIONS_SOURCE, self.TEST_PROVIDERS_SOURCE, self.TEST_WORKPLACE_SOURCE, self.TEST_CQC_RATINGS_DESTINATION, self.TEST_BENCHMARK_RATINGS_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_CQC_RATINGS_DESTINATION,
            mode="overwrite",
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
