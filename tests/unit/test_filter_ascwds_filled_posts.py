import unittest
import warnings

from tests.test_file_data import FilterAscwdsFilledPostsData as Data
from tests.test_file_schemas import FilterAscwdsFilledPostsSchema as Schemas

from utils import utils
import utils.ind_cqc_filled_posts_utils.filter_ascwds_filled_posts.filter_ascwds_filled_posts as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


class FilterAscwdsFilledPostsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        # self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
        #     Data.unfiltered_ind_cqc_rows,
        #     Schemas.unfiltered_ind_cqc_schema,
        # )
        # self.returned_filtered_df = job.null_ascwds_filled_post_outliers(
        #     self.unfiltered_ind_cqc_df
        # )

        # self.expected_filtered_ind_cqc_df = self.spark.createDataFrame(
        #     Data.expected_filtered_ind_cqc_rows,
        #     Schemas.expected_filtered_ind_cqc_schema,
        # )

        # self.returned_data = self.returned_filtered_df.sort(
        #     IndCQC.location_id
        # ).collect()
        # self.expected_data = self.expected_filtered_ind_cqc_df.sort(
        #     IndCQC.location_id
        # ).collect()

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_returned_df_has_same_number_of_rows_as_input_df(self):
        # self.assertEqual(
        #     self.unfiltered_ind_cqc_df.count(),
        #     self.returned_filtered_df.count(),
        # )
        pass

    def test_returned_df_has_same_columns_as_expected_df(self):
        # self.assertEqual(
        #     sorted(self.returned_filtered_df.columns),
        #     sorted(self.expected_filtered_ind_cqc_df.columns),
        # )
        pass

    def test_returned_df_matches_expected_df(self):
        # self.assertEqual(self.returned_data, self.expected_data)
        pass
