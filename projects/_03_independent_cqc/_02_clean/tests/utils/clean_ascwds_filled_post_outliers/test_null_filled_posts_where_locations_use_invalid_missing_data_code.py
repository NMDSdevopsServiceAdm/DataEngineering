import unittest
import warnings

from projects._03_independent_cqc._02_clean.utils.clean_ascwds_filled_post_outliers import (
    null_filled_posts_where_locations_use_invalid_missing_data_code as job,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    NullFilledPostsUsingInvalidMissingDataCodeData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    NullFilledPostsUsingInvalidMissingDataCodeSchema as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class NullFilledPostsUsingInvalidMissingDataCodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.null_filled_posts_using_invalid_missing_data_code_rows,
            Schemas.null_filled_posts_using_invalid_missing_data_code_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(NullFilledPostsUsingInvalidMissingDataCodeTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = (
            job.null_filled_posts_where_locations_use_invalid_missing_data_code(
                self.test_df
            )
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_filled_posts_using_invalid_missing_data_code_rows,
            Schemas.null_filled_posts_using_invalid_missing_data_code_schema,
        )

    def test_null_filled_posts_where_locations_use_invalid_missing_data_code_returns_correct_values(
        self,
    ):
        self.assertEqual(
            self.expected_df.collect(),
            self.returned_df.sort(IndCQC.location_id).collect(),
        )


class InvalidMissingDataCodeTests(NullFilledPostsUsingInvalidMissingDataCodeTests):
    def setUp(self) -> None:
        super().setUp()

    def test_missing_data_code_is_correct(
        self,
    ):
        self.assertEqual(job.INVALID_MISSING_DATA_CODE, 999.0)
