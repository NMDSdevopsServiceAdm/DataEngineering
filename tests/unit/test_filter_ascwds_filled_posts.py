import unittest
import warnings

from tests.test_file_data import FilterAscwdsFilledPostsData as Data
from tests.test_file_schemas import FilterAscwdsFilledPostsSchema as Schemas

from utils import utils

import utils.ind_cqc_filled_posts_utils.filter_ascwds_filled_posts.filter_ascwds_filled_posts as job


class FilterAscwdsFilledPostsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_data = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.unfiltered_ind_cqc_schema,
        )
        self.returned_filtered_df = job.null_ascwds_filled_post_outliers(
            self.unfiltered_ind_cqc_data
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):
        self.assertEqual(
            self.unfiltered_ind_cqc_data.count(),
            self.returned_filtered_df.count(),
        )
