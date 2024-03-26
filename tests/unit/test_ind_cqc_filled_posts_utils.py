import unittest
import warnings

from tests.test_file_data import IndCQCDataUtils as Data
from tests.test_file_schemas import IndCQCDataUtils as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.utils as job


class TestPopulateEstimateFilledPostsAndSource(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class TestFilledPostsAndSourceAdded(TestPopulateEstimateFilledPostsAndSource):
    def setUp(self) -> None:
        super().setUp()

    def test_populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
        self,
    ):
        pass


class TestSourceDescriptionAdded(TestPopulateEstimateFilledPostsAndSource):
    def setUp(self) -> None:
        super().setUp()

    def test_add_source_description_to_source_column(self):
        pass
