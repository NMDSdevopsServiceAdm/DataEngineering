import unittest
from unittest.mock import Mock, patch
import warnings

from tests.test_file_data import NullAscwdsFilledPostOutliersData as Data
from tests.test_file_schemas import NullAscwdsFilledPostOutliersSchema as Schemas

from utils import utils
import utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_ascwds_filled_post_outliers as job


class NullAscwdsFilledPostOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.unfiltered_ind_cqc_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch(
        "utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers"
    )
    def test_functions_are_called(
        self, null_care_home_filled_posts_per_bed_ratio_outliers_mock: Mock
    ):
        job.null_ascwds_filled_post_outliers(self.unfiltered_ind_cqc_df)
        null_care_home_filled_posts_per_bed_ratio_outliers_mock.assert_called_once()
