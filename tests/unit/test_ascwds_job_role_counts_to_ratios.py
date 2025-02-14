import unittest

from tests.test_file_data import AscwdsJobRoleCountsToRatiosData as Data
from tests.test_file_schemas import AscwdsJobRoleCountsToRatiosSchema as Schemas

from utils import utils

from utils.ind_cqc_filled_posts_utils.ascwds_job_role_count.ascwds_job_role_counts_to_ratios import (
    transform_job_role_counts_to_ratios,
)


class AscwdsJobRoleCountsToRatios(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

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

        returned_rows = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
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

        returned_rows = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
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

        returned_rows = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
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

        returned_rows = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
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

        returned_df = transform_job_role_counts_to_ratios(
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

        returned_rows = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        ).collect()

        for i in range(len(returned_rows)):
            for j in range(len(Data.list_of_job_role_columns)):
                self.assertAlmostEqual(
                    returned_rows[i][Data.list_of_job_role_columns[j]],
                    expected_rows[i][Data.list_of_job_role_columns[j]],
                    places=3,
                )

    def test_transform_job_role_counts_to_ratios_adds_ratio_column_for_all_given_count_columns(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.ascwds_job_role_counts_to_ratios_at_different_establishments,
            Schemas.ascwds_job_role_counts_to_ratios_schema,
        )

        returned_df = transform_job_role_counts_to_ratios(
            test_df, Data.list_of_job_role_columns
        )

        self.assertEqual(
            len(returned_df.columns),
            len(test_df.columns) + len(Data.list_of_job_role_columns),
        )
