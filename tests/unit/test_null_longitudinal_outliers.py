import unittest
import warnings

from tests.test_file_data import (
    NullLongitudinalOutliersData as Data,
)
from tests.test_file_schemas import (
    NullLongitudinalOutliersSchema as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers import (
    null_longitudinal_outliers as job,
)


class NullLongitudinalOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.null_longitudinal_outliers_rows,
            Schemas.null_longitudinal_outliers_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(NullLongitudinalOutliersTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.null_longitudinal_outliers(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_longitudinal_outliers_rows,
            Schemas.null_longitudinal_outliers_schema,
        )

    def test_null_longitudinal_outliers_returns_correct_values(
        self,
    ):
        self.returned_df.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).show()
        self.expected_df.show()
        self.assertEqual(
            self.expected_df.collect(),
            self.returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )
