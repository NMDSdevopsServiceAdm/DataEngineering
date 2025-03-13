import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.impute_ind_cqc_ascwds_and_pir as job
from tests.test_file_data import EstimateMissingAscwdsFilledPostsData as Data
from tests.test_file_schemas import EstimateMissingAscwdsFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


class EstimateMissingAscwdsFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    ESTIMATES_DESTINATION = "estimates destination"
    NON_RES_PIR_MODEL = (
        "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/"
    )
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
    @patch(
        "jobs.impute_ind_cqc_ascwds_and_pir.blend_pir_and_ascwds_when_ascwds_out_of_date"
    )
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        blend_pir_and_ascwds_when_ascwds_out_of_date_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.return_value = self.test_cleaned_ind_cqc_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.ESTIMATES_DESTINATION,
            self.NON_RES_PIR_MODEL,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        blend_pir_and_ascwds_when_ascwds_out_of_date_mock.assert_called_once()
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
