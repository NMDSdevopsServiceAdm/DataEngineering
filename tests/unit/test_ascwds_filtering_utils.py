import unittest
import warnings

from tests.test_file_data import (
    ASCWDSFilteringUtilsData as Data,
)
from tests.test_file_schemas import (
    ASCWDSFilteringUtilsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.null_outlier_columns import NullOutlierColumns
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers import (
    null_care_home_filled_posts_per_bed_ratio_outliers as job,
)


class ASCWDSFilteringUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class AddFilteringRuleTests(ASCWDSFilteringUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_filtering_rule(
        self,
    ):
        pass
