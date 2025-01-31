import unittest

from utils import utils

from tests.test_file_data import AscwdsJobroleCountData as Data
from tests.test_file_schemas import AscwdsJobroleCountSchema as Schemas

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

from utils.ind_cqc_filled_posts_utils.ascwds_jobrole_count.ascwds_jobrole_count import (
    count_job_roles_per_establishment,
    mapped_column,
)


class AscwdsJobroleCount(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountJobRolesPerEstablishmentTests(AscwdsJobroleCount):
    def test_count_job_roles_per_establishment_counts_two_workers_that_are_the_same(
        self,
    ):
        test_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.workplace_with_two_workers_rows,
            Schemas.worker_schema,
        )
        expected_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_rows,
            Schemas.worker_with_jobrole_count_schema,
        )

        returned_df = count_job_roles_per_establishment(
            test_workplace_with_two_workers_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_two_workers_df.collect(),
        )

    def test_count_job_roles_per_establishment_counts_when_job_roles_are_different(
        self,
    ):
        test_workplace_with_different_jobroles_df = self.spark.createDataFrame(
            Data.workplace_with_two_different_roles_rows,
            Schemas.worker_schema,
        )
        expected_workplace_with_different_jobroles_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_different_roles_rows,
            Schemas.worker_with_jobrole_count_schema,
        )

        returned_df = count_job_roles_per_establishment(
            test_workplace_with_different_jobroles_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_different_jobroles_df.collect(),
        )

    def test_count_job_roles_per_establishment_counts_when_jobrole_is_same_at_different_workplaces(
        self,
    ):
        test_two_workplaces_with_same_jobrole_df = self.spark.createDataFrame(
            Data.two_workplaces_with_same_jobrole_rows,
            Schemas.worker_schema,
        )
        expected_two_workplaces_with_same_jobrole_df = self.spark.createDataFrame(
            Data.expected_two_workplaces_with_same_jobrole_rows,
            Schemas.worker_with_jobrole_count_schema,
        )

        returned_df = count_job_roles_per_establishment(
            test_two_workplaces_with_same_jobrole_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_two_workplaces_with_same_jobrole_df.collect(),
        )

    def test_count_job_roles_per_establishment_counts_when_importdate_is_different(
        self,
    ):
        test_workplace_across_different_importdates_same_jobrole_df = (
            self.spark.createDataFrame(
                Data.workplace_across_different_importdates_same_jobrole_rows,
                Schemas.worker_schema,
            )
        )
        expected_workplace_across_different_importdates_same_jobrole_df = (
            self.spark.createDataFrame(
                Data.expected_workplace_across_different_importdates_same_jobrole_rows,
                Schemas.worker_with_jobrole_count_schema,
            )
        )

        returned_df = count_job_roles_per_establishment(
            test_workplace_across_different_importdates_same_jobrole_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_across_different_importdates_same_jobrole_df.collect(),
        )

    def test_count_job_roles_per_establishment_counts_when_jobrole_is_null(self):
        test_workplace_with_null_jobrole_df = self.spark.createDataFrame(
            Data.workplace_with_null_jobrole_rows,
            Schemas.worker_schema,
        )
        expected_workplace_with_null_jobrole_df = self.spark.createDataFrame(
            Data.expected_workplace_with_null_jobrole_rows,
            Schemas.worker_with_jobrole_count_schema,
        )

        returned_df = count_job_roles_per_establishment(
            test_workplace_with_null_jobrole_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_null_jobrole_df.collect(),
        )

    def test_count_job_roles_per_establishment_adds_one_column(self):
        test_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.workplace_with_two_workers_rows,
            Schemas.worker_schema,
        )
        expected_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_rows,
            Schemas.worker_with_jobrole_count_schema,
        )

        returned_df = count_job_roles_per_establishment(
            test_workplace_with_two_workers_df
        )

        expected_workplace_with_two_workers_df.show()
        returned_df.show()

        self.assertEqual(
            returned_df.columns, expected_workplace_with_two_workers_df.columns
        )
