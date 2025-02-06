import unittest

from tests.test_file_data import AscwdsJobroleCountData as Data
from tests.test_file_schemas import AscwdsJobroleCountSchema as Schemas

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

from utils.estimate_filled_posts_by_job_role_utils.ascwds_job_role_count.ascwds_job_role_count import (
    count_job_role_per_establishment_as_columns,
)


class AscwdsJobroleCount(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountJobRolesPerEstablishmentTests(AscwdsJobroleCount):
    def setUp(self) -> None:
        super().setUp()

    def test_workplace_with_one_of_every_possible_job_role(
        self,
    ):
        test_workplace_with_one_of_every_possible_job_role_df = (
            self.spark.createDataFrame(
                Data.workplace_with_one_of_every_possible_job_role,
                Schemas.ascwds_worker_schema,
            )
        )
        expected_workplace_with_one_of_every_possible_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_one_of_every_possible_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        returned_df = count_job_role_per_establishment_as_columns(
            test_workplace_with_one_of_every_possible_job_role_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_one_of_every_possible_job_role_df.collect(),
        )

    def test_workplace_with_one_job_role(
        self,
    ):
        test_workplace_with_one_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_one_job_role,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_one_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_one_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = count_job_role_per_establishment_as_columns(
            test_workplace_with_one_job_role_df
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_one_job_role_df.collect(),
        )

    def test_workplace_three_jobs_roles_with_two_being_distinct(
        self,
    ):
        test_workplace_three_jobs_roles_with_two_being_distinct_df = (
            self.spark.createDataFrame(
                Data.workplace_three_jobs_roles_with_two_being_distinct,
                Schemas.ascwds_worker_schema,
            )
        )
        expected_workplace_three_jobs_roles_with_two_being_distinct_df = self.spark.createDataFrame(
            Data.exptected_workplace_three_job_roles_with_two_being_distinct,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = count_job_role_per_establishment_as_columns(
            test_workplace_three_jobs_roles_with_two_being_distinct_df
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_three_jobs_roles_with_two_being_distinct_df.collect(),
        )
