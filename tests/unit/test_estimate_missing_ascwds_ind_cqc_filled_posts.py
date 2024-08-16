import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.estimate_missing_ascwds_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateMissingAscwdsFilledPostsData as Data
from tests.test_file_schemas import EstimateMissingAscwdsFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)


class EstimateMissingAscwdsFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    ESTIMATES_DESTINATION = "estimates destination"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows, Schemas.cleaned_ind_cqc_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(EstimateMissingAscwdsFilledPostsTests):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_cleaned_ind_cqc_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.ESTIMATES_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)
        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class NumericalValuesTests(EstimateMissingAscwdsFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_number_of_days_in_rolling_average_value(self):
        self.assertEqual(job.NumericalValues.NUMBER_OF_DAYS_IN_ROLLING_AVERAGE, 185)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
