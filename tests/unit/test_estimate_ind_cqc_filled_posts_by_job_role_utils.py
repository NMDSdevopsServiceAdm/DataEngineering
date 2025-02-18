import unittest

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils import utils as job
from tests.test_file_data import EstimateFilledPostsByJobRoleData as Data
from tests.test_file_schemas import EstimateFilledPostsByJobRoleSchema as Schemas


class EstimateFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountRegisteredManagerNamesTests(EstimateFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    def test_count_registered_manager_names_when_location_has_one_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_one_registered_manager_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_one_registered_manager_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_when_location_has_two_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_two_registered_managers_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_two_registered_managers_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_returns_zero_when_location_has_null_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_null_registered_manager_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_null_registered_manager_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_returns_zero_when_location_has_empty_list(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_empty_list_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_empty_list_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_different_number_of_registered_managers_at_different_import_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )


class CountJobRolesPerEstablishmentTests(EstimateFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    def test_count_job_role_per_establishment_as_columns_returns_expected_number_of_columns_when_called(
        self,
    ):
        test_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_schema,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_three_distinct_job_role_df, Data.list_of_job_roles
        )
        self.assertEqual(
            len(returned_df.columns),
            len(test_workplace_with_three_distinct_job_role_df.columns) + 3,
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_three_distinct_job_roles(
        self,
    ):
        test_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_three_distinct_job_role_df, Data.list_of_job_roles
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_three_distinct_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_has_none_job_role(
        self,
    ):
        test_workplace_with_none_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_none_job_role,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_none_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_none_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_none_job_role_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_none_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_different_import_date(
        self,
    ):
        test_workplace_with_different_import_date_df = self.spark.createDataFrame(
            Data.workplace_with_different_import_date,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_different_import_date_df = self.spark.createDataFrame(
            Data.expected_workplace_with_different_import_date,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_different_import_date_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(
                AWKClean.establishment_id, AWKClean.ascwds_worker_import_date
            ).collect(),
            expected_workplace_with_different_import_date_df.sort(
                AWKClean.establishment_id, AWKClean.ascwds_worker_import_date
            ).collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_different_establishmentid(
        self,
    ):
        test_workplace_with_different_establishmentid_df = self.spark.createDataFrame(
            Data.workplace_with_different_establishmentid,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_different_establishmentid_df = self.spark.createDataFrame(
            Data.expected_workplace_with_different_establishmentid,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_different_establishmentid_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_different_establishmentid_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_three_job_roles_with_two_being_distinct(
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
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_three_jobs_roles_with_two_being_distinct_df,
            Data.list_of_job_roles,
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_three_jobs_roles_with_two_being_distinct_df.collect(),
        )


class AscwdsJobRoleCountsToRatios(EstimateFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    def test_transform_job_role_counts_to_ratios_returns_expected_ratios_when_given_counts_have_only_one_above_zero(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_when_only_one_above_zero,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_rows = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_when_only_one_above_zero,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        ).collect()

        returned_rows = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(Data.list_of_job_role_columns)):
            self.assertAlmostEqual(
                returned_rows[0][Data.list_of_job_role_columns[i]],
                expected_rows[0][Data.list_of_job_role_columns[i]],
                places=3,
                msg=f"Returned value in column {i} does not match expected",
            )

    def test_transform_job_role_counts_to_ratios_returns_expected_ratios_when_given_counts_are_all_above_zero(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_when_all_above_zero_rows,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_rows = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_when_all_above_zero_rows,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        ).collect()

        returned_rows = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(Data.list_of_job_role_columns)):
            self.assertAlmostEqual(
                returned_rows[0][Data.list_of_job_role_columns[i]],
                expected_rows[0][Data.list_of_job_role_columns[i]],
                places=3,
                msg=f"Returned value in column {i} does not match expected",
            )

    def test_transform_job_role_counts_to_ratios_returns_expected_ratios_when_given_counts_all_equal_zero(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_when_all_equal_zero,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_rows = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_when_all_equal_zero,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        ).collect()

        returned_rows = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(Data.list_of_job_role_columns)):
            self.assertAlmostEqual(
                returned_rows[0][Data.list_of_job_role_columns[i]],
                expected_rows[0][Data.list_of_job_role_columns[i]],
                places=3,
                msg=f"Returned value in column {i} does not match expected",
            )

    def test_transform_job_role_counts_to_ratios_returns_ratio_of_zero_for_null_count_when_given_counts_contain_a_null_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_when_a_count_is_null,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_rows = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_when_a_count_is_null,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        ).collect()

        returned_rows = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(Data.list_of_job_role_columns)):
            self.assertAlmostEqual(
                returned_rows[0][Data.list_of_job_role_columns[i]],
                expected_rows[0][Data.list_of_job_role_columns[i]],
                places=3,
                msg=f"Returned value in column {i} does not match expected",
            )

    def test_transform_job_role_counts_to_ratios_returns_all_null_ratio_values_when_all_counts_are_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_when_all_are_null,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_when_all_are_null,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        )

        returned_df = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        )

        self.assertEqual(
            returned_df.collect(),
            expected_df.collect(),
        )

    def test_transform_job_role_counts_to_ratios_returns_expected_ratios_when_given_multiple_establishments(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_at_different_establishments,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )
        expected_rows = self.spark.createDataFrame(
            Data.expected_ascwds_job_role_counts_to_ratios_at_different_establishments,
            Schemas.expected_ascwds_job_role_counts_to_ratios_schema,
        ).collect()

        returned_rows = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
                    msg=f"Returned value in row {i} at column {j} does not match expected",
                )

    def test_transform_job_role_counts_to_ratios_adds_ratio_column_for_all_given_count_columns(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_at_different_establishments,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )

        returned_df = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        )

        self.assertEqual(
            len(returned_df.columns),
            len(test_df.columns) + len(Data.list_of_job_role_columns),
        )

    def test_transform_job_role_counts_to_ratios_only_adds_columns_from_given_list_and_replaces_word_count_with_ratio(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_at_different_establishments,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )

        test_columns = test_df.columns

        expected_columns_added = [
            i.replace("count", "ratio") for i in Data.list_of_job_role_columns
        ]

        returned_columns = job.transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).columns

        new_columns_returned = [k for k in returned_columns if k not in test_columns]

        for l in range(len(expected_columns_added)):
            self.assertEqual(expected_columns_added[l], new_columns_returned[l])
