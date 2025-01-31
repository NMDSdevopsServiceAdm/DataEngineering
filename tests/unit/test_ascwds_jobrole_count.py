import unittest

from utils import utils

from tests.test_file_data import AscwdsJobroleCountData as Data
from tests.test_file_schemas import AscwdsJobroleCountSchema as Schemas

from utils.ind_cqc_filled_posts_utils.ascwds_jobrole_count.ascwds_jobrole_count import count_job_roles_per_establishment, mapped_column

class AscwdsJobroleCount(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountJobRolesPerEstablishmentTests(AscwdsJobroleCount):

    def test_count_job_roles_per_establishment_counts_two_workers_that_are_the_same(self):
        test_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.workplace_with_two_workers_rows,
            Schemas.workplace_with_two_workers_schema,
        )
        expected_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_rows,
            Schemas.expected_workplace_with_two_workers_schema,
        )

        returned_df = count_job_roles_per_establishment(test_workplace_with_two_workers_df)