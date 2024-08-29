import unittest
import warnings

from tests.test_file_data import (
    NullFilledPostsUsingMissingDataCodeData as Data,
)
from tests.test_file_schemas import (
    NullFilledPostsUsingMissingDataCodeSchema as Schemas,
)

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DoubleType,
    IntegerType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers import (
    null_filled_posts_where_locations_use_missing_data_code as job,
)


class NullFilledPostsUsingMissingDataCodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.null_filled_posts_using_missing_data_code_rows,
            Schemas.null_filled_posts_using_missing_data_code_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(NullFilledPostsUsingMissingDataCodeTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.null_filled_posts_where_locations_use_missing_data_code(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_filled_posts_using_missing_data_code_rows,
            Schemas.null_filled_posts_using_missing_data_code_schema,
        )

    def test_null_filled_posts_where_locations_use_missing_data_code_returns_correct_values(
        self,
    ):
        self.assertEqual(self.expected_df.collect(), self.returned_df.collect())
