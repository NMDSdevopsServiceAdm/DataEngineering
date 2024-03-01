import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import jobs.create_care_home_features_ind_cqc_filled_posts as job
from utils import utils
from tests.test_file_generator import generate_prepared_locations_file_parquet


class CareHomeFeaturesIndCqcFilledPosts(unittest.TestCase):
    IND_FILLED_POSTS_CLEANED_DIR = "source_dir"
    CARE_HOME_FEATURES_DIR = "destination_dir"

    PREPARED_LOCATIONS_TEST_DATA = (
        "tests/test_data/domain=data_engineering/dataset=prepared_locations"
    )

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = generate_prepared_locations_file_parquet(
            self.PREPARED_LOCATIONS_TEST_DATA
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR,
            self.CARE_HOME_FEATURES_DIR,
        )

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CARE_HOME_FEATURES_DIR,
            mode=ANY,
            partitionKeys=["year", "month", "day", "import_date"],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
