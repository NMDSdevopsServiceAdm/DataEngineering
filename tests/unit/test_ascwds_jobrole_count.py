import unittest

from tests.test_file_data import AscwdsJobroleCountData as Data
from tests.test_file_schemas import AscwdsJobroleCountSchema as Schemas

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.ind_cqc_filled_posts_utils.ascwds_job_role_count.ascwds_job_role_count import (
    count_job_role_per_establishment,
    convert_job_role_count_to_job_role_map,
    count_job_role_per_establishment_as_columns,
)


class AscwdsJobroleCount(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountJobRolesPerEstablishmentTests(AscwdsJobroleCount):
    def setUp(self) -> None:
        super().setUp()

    def test_count_job_role_per_establishment_counts_two_workers_that_are_the_same(
        self,
    ):
        test_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.workplace_with_two_workers_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_workplace_with_two_workers_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_two_workers_df.collect(),
        )

    def test_count_job_role_per_establishment_counts_when_job_roles_are_different(
        self,
    ):
        test_workplace_with_different_job_roles_df = self.spark.createDataFrame(
            Data.workplace_with_two_different_roles_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_different_job_roles_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_different_roles_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_workplace_with_different_job_roles_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_different_job_roles_df.collect(),
        )

    def test_count_job_role_per_establishment_counts_when_job_role_is_same_at_different_workplaces(
        self,
    ):
        test_two_workplaces_with_same_job_role_df = self.spark.createDataFrame(
            Data.two_workplaces_with_same_job_role_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_two_workplaces_with_same_job_role_df = self.spark.createDataFrame(
            Data.expected_two_workplaces_with_same_job_role_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_two_workplaces_with_same_job_role_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_two_workplaces_with_same_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_counts_when_job_role_is_different_at_different_workplaces(
        self,
    ):
        test_two_workplaces_with_different_job_role_df = self.spark.createDataFrame(
            Data.two_workplaces_with_different_job_role_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_two_workplaces_with_different_job_role_df = self.spark.createDataFrame(
            Data.expected_two_workplaces_with_different_job_role_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_two_workplaces_with_different_job_role_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_two_workplaces_with_different_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_counts_when_import_date_is_different(
        self,
    ):
        test_workplace_across_different_import_dates_same_job_role_df = (
            self.spark.createDataFrame(
                Data.workplace_across_different_import_dates_same_job_role_rows,
                Schemas.ascwds_worker_schema,
            )
        )
        expected_workplace_across_different_import_dates_same_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_across_different_import_dates_same_job_role_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_workplace_across_different_import_dates_same_job_role_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_across_different_import_dates_same_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_counts_when_job_role_is_null(self):
        test_workplace_with_null_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_null_job_role_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_null_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_null_job_role_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_workplace_with_null_job_role_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_null_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_adds_one_column(self):
        test_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.workplace_with_two_workers_rows,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_two_workers_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )

        returned_df = count_job_role_per_establishment(
            test_workplace_with_two_workers_df
        )

        self.assertEqual(
            returned_df.columns, expected_workplace_with_two_workers_df.columns
        )


class ConvertJobRoleCountToJobRoleMap(AscwdsJobroleCount):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_job_role_count_to_job_role_map_converts_one_row_into_one_dict(
        self,
    ):
        test_workplace_with_one_job_role_and_two_workers_counted_df = (
            self.spark.createDataFrame(
                Data.workplace_with_one_job_role_and_two_workers_counted_rows,
                Schemas.ascwds_worker_with_job_role_count_schema,
            )
        )
        expected_workplace_with_two_workers_counted_df = self.spark.createDataFrame(
            Data.expected_workplace_with_one_job_role_and_two_workers_counted_rows,
            Schemas.ascwds_worker_with_job_role_map_schema,
        )

        returned_df = convert_job_role_count_to_job_role_map(
            test_workplace_with_one_job_role_and_two_workers_counted_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_two_workers_counted_df.collect(),
        )

    def test_convert_job_role_count_to_job_role_map_converts_two_rows_into_one_dict(
        self,
    ):
        test_workplace_with_one_worker_in_two_roles_counted_df = (
            self.spark.createDataFrame(
                Data.workplace_with_two_workers_in_different_job_roles_counted_rows,
                Schemas.ascwds_worker_with_job_role_count_schema,
            )
        )
        expected_workplace_with_one_worker_in_two_roles_counted_df = self.spark.createDataFrame(
            Data.expected_workplace_with_two_workers_in_different_job_roles_counted_rows,
            Schemas.ascwds_worker_with_job_role_map_schema,
        )

        returned_df = convert_job_role_count_to_job_role_map(
            test_workplace_with_one_worker_in_two_roles_counted_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_one_worker_in_two_roles_counted_df.collect(),
        )

    def test_convert_job_role_count_to_job_role_map_converts_two_establishments_with_same_role_into_two_dicts(
        self,
    ):
        test_two_workplaces_with_same_job_role_counted_rows = (
            self.spark.createDataFrame(
                Data.two_workplaces_with_same_job_role_counted_rows,
                Schemas.ascwds_worker_with_job_role_count_schema,
            )
        )
        expected_two_workplaces_with_same_job_role_counted_rows = (
            self.spark.createDataFrame(
                Data.expected_two_workplaces_with_same_job_role_counted_rows,
                Schemas.ascwds_worker_with_job_role_map_schema,
            )
        )

        returned_df = convert_job_role_count_to_job_role_map(
            test_two_workplaces_with_same_job_role_counted_rows
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_two_workplaces_with_same_job_role_counted_rows.collect(),
        )

    def test_convert_job_role_count_to_job_role_map_converts_one_establishment_at_different_import_dates_into_two_dicts(
        self,
    ):
        test_workplace_across_different_import_dates_same_job_role_counted_df = (
            self.spark.createDataFrame(
                Data.workplace_across_different_import_dates_same_job_role_counted_rows,
                Schemas.ascwds_worker_with_job_role_count_schema,
            )
        )
        expected_workplace_across_different_import_dates_same_job_role_counted_df = self.spark.createDataFrame(
            Data.expected_workplace_across_different_import_dates_same_job_role_counted_rows,
            Schemas.ascwds_worker_with_job_role_map_schema,
        )

        returned_df = convert_job_role_count_to_job_role_map(
            test_workplace_across_different_import_dates_same_job_role_counted_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_across_different_import_dates_same_job_role_counted_df.collect(),
        )

    def test_convert_job_role_count_to_job_role_map_converts_when_establishments_have_different_number_of_unique_job_roles(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.workplaces_with_different_number_of_unique_job_roles_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_workplaces_with_different_number_of_unique_job_roles_rows,
            Schemas.ascwds_worker_with_job_role_map_schema,
        )

        returned_df = convert_job_role_count_to_job_role_map(test_df)

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_df.collect(),
        )


class PivotJobRoleCountIntoColumns(AscwdsJobroleCount):
    def setUp(self) -> None:
        super().setUp()

    def test_pivot_job_role_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.workplaces_with_different_number_of_unique_job_roles_rows,
            Schemas.ascwds_worker_with_job_role_count_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_workplaces_with_different_number_of_unique_job_roles_when_pivoted_rows,
            Schemas.ascwds_worker_with_job_role_count_pivoted_schema,
        )

        returned_df = count_job_role_per_establishment_as_columns(test_df)

        returned_df.sort(AWKClean.establishment_id).show()
        expected_df.sort(AWKClean.establishment_id).show()

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_df.sort(AWKClean.establishment_id).collect(),
        )
