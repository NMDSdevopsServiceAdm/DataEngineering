import unittest
from unittest.mock import Mock, patch
import warnings

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from tests.test_file_data import CleanAscwdsAndPirOutliersData as Data
from tests.test_file_schemas import CleanAscwdsAndPirOutliersSchema as Schemas
from utils import utils
import utils.ind_cqc_filled_posts_utils.clean_ascwds_and_pir_outliers as job


class CleanAscwdsAndPirOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_rows, Schemas.clean_ascwds_and_pir_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_ascwds_and_pir_rows, Schemas.clean_ascwds_and_pir_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_clean_ascwds_and_pir_outliers_nulls_rows_where_pir_data_is_greater_than_ascwds_data(
        self,
    ):
        returned_df = job.clean_ascwds_and_pir_outliers(self.test_df)
        self.assertEqual(returned_df.sort(IndCQC.location_id).collect())
