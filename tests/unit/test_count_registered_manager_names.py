import unittest

from tests.test_file_data import RegisteredManagerNamesCountData as Data
from tests.test_file_schemas import RegisteredManagerNamesCountSchema as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_utils.ascwds_mapped_column_job_role_count.count_registered_manager_names import (
    count_registered_manager_names,
)


class CountRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_count_registered_manager_names_when_location_has_one_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_one_registered_manager,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_one_registered_manager,
            Schemas.location_with_list_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
