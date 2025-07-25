import unittest
from unittest.mock import ANY, Mock, patch

import projects._02_sfc_internal.cqc_coverage.jobs.merge_coverage_data as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    MergeCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    MergeCoverageData as Schemas,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns

from utils.column_values.categorical_column_values import (
    CQCLatestRating,
    CQCCurrentOrHistoricValues,
)


PATCH_PATH = "projects._02_sfc_internal.cqc_coverage.jobs.merge_coverage_data"


class SetupForTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/directory"
    TEST_CQC_RATINGS_SOURCE = "some/other/directory"
    TEST_MERGED_DESTINATION = "some/other/directory"
    TEST_REDUCED_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.clean_cqc_location_for_merge_rows,
            Schemas.clean_cqc_location_for_merge_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.clean_ascwds_workplace_for_merge_rows,
            Schemas.clean_ascwds_workplace_for_merge_schema,
        )
        self.test_cqc_ratings_df = self.spark.createDataFrame(
            Data.sample_cqc_ratings_for_merge_rows,
            Schemas.sample_cqc_ratings_for_merge_schema,
        )


class MainTests(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.filter_df_to_maximum_value_in_column")
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.add_columns_for_locality_manager_dashboard")
    @patch(f"{PATCH_PATH}.join_latest_cqc_rating_into_coverage_df")
    @patch(f"{PATCH_PATH}.rUtils.add_parents_or_singles_and_subs_col_to_df")
    @patch(f"{PATCH_PATH}.add_flag_for_in_ascwds")
    @patch(f"{PATCH_PATH}.join_ascwds_data_into_cqc_location_df")
    @patch(f"{PATCH_PATH}.cUtils.remove_duplicates_based_on_column_order")
    @patch(f"{PATCH_PATH}.cUtils.reduce_dataset_to_earliest_file_per_month")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        reduce_dataset_to_earliest_file_per_month_mock: Mock,
        remove_duplicates_based_on_column_order_mock: Mock,
        join_ascwds_data_into_cqc_location_df_mock: Mock,
        add_flag_for_in_ascwds_mock: Mock,
        add_parents_or_singles_and_subs_col_to_df_mock: Mock,
        join_latest_cqc_rating_into_coverage_df_mock: Mock,
        add_columns_for_locality_manager_dashboard_mock: Mock,
        write_to_parquet_mock: Mock,
        filter_df_to_maximum_value_in_column_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_clean_ascwds_workplace_df,
            self.test_cqc_ratings_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_CQC_RATINGS_SOURCE,
            self.TEST_MERGED_DESTINATION,
            self.TEST_REDUCED_DESTINATION,
        )

        self.assertEqual(read_from_parquet_mock.call_count, 3)
        reduce_dataset_to_earliest_file_per_month_mock.assert_called_once()
        self.assertEqual(remove_duplicates_based_on_column_order_mock.call_count, 2)
        join_ascwds_data_into_cqc_location_df_mock.assert_called_once()
        add_flag_for_in_ascwds_mock.assert_called_once()
        add_parents_or_singles_and_subs_col_to_df_mock.assert_called_once()
        join_latest_cqc_rating_into_coverage_df_mock.assert_called_once()
        add_columns_for_locality_manager_dashboard_mock.assert_called_once()

        write_to_parquet_mock.assert_called_with(
            ANY,
            self.TEST_MERGED_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

        filter_df_to_maximum_value_in_column_mock.assert_called_once()

        write_to_parquet_mock.assert_called_with(
            ANY,
            self.TEST_REDUCED_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class JoinAscwdsIntoCqcLocationsTests(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

    def test_join_ascwds_data_into_cqc_location_df(self):
        returned_df = job.join_ascwds_data_into_cqc_location_df(
            self.test_clean_cqc_location_df,
            self.test_clean_ascwds_workplace_df,
            CQCLClean.cqc_location_import_date,
            AWPClean.ascwds_workplace_import_date,
        )

        expected_merged_df = self.spark.createDataFrame(
            Data.expected_cqc_and_ascwds_merged_rows,
            Schemas.expected_cqc_and_ascwds_merged_schema,
        )

        returned_data = returned_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()
        expected_data = expected_merged_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()

        self.assertEqual(returned_data, expected_data)


class AddFlagForInAscwdsTests(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

        self.sample_in_ascwds_df = self.spark.createDataFrame(
            Data.sample_in_ascwds_rows, schema=Schemas.sample_in_ascwds_schema
        )

        self.returned_in_ascwds_df = job.add_flag_for_in_ascwds(
            self.sample_in_ascwds_df
        )

        self.expected_in_ascwds_df = self.spark.createDataFrame(
            Data.expected_in_ascwds_rows, Schemas.expected_in_ascwds_schema
        )

    def test_add_flag_for_in_ascwds_adds_1_column_with_given_name(self):
        self.assertTrue(CoverageColumns.in_ascwds in self.returned_in_ascwds_df.columns)

        self.assertEqual(
            len(self.returned_in_ascwds_df.columns),
            len(self.expected_in_ascwds_df.columns),
        )

    def test_add_flag_for_in_ascwds_has_expected_values(self):
        returned_rows = self.returned_in_ascwds_df.collect()
        expected_rows = self.expected_in_ascwds_df.collect()

        self.assertEqual(returned_rows, expected_rows)


class FilterForLatestCqcRatings(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_in_ascwds_df = job.filter_for_latest_cqc_ratings(
            self.test_cqc_ratings_df
        )

    def test_filter_for_latest_cqc_ratings_contains_latest_ratings(self):
        latest_rating_column_df = self.returned_in_ascwds_df.select(
            CQCRatingsColumns.latest_rating_flag
        )

        distinct_latest_rating_rows = latest_rating_column_df.distinct().collect()

        self.assertEqual(len(distinct_latest_rating_rows), 1)
        self.assertEqual(
            distinct_latest_rating_rows[0][0], CQCLatestRating.is_latest_rating
        )

    def test_filter_for_latest_cqc_ratings_contains_current_ratings(self):
        latest_rating_column_df = self.returned_in_ascwds_df.select(
            CQCRatingsColumns.current_or_historic
        )

        distinct_latest_rating_rows = latest_rating_column_df.distinct().collect()

        self.assertEqual(len(distinct_latest_rating_rows), 1)
        self.assertEqual(
            distinct_latest_rating_rows[0][0], CQCCurrentOrHistoricValues.current
        )

    def test_filter_for_latest_cqc_ratings_has_expected_row_count(self):
        self.assertEqual(self.returned_in_ascwds_df.count(), 2)


class JoinLatestCqcRatingsIntoCoverageTests(SetupForTests):
    def setUp(self) -> None:
        super().setUp()

        self.sample_cqc_locations_df = self.spark.createDataFrame(
            Data.sample_cqc_locations_rows,
            Schemas.sample_cqc_locations_schema,
        )

        self.returned_df = job.join_latest_cqc_rating_into_coverage_df(
            self.sample_cqc_locations_df, self.test_cqc_ratings_df
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_cqc_locations_and_latest_cqc_rating_rows,
            Schemas.expected_cqc_locations_and_latest_cqc_rating_schema,
        )

    def test_join_latest_cqc_rating_into_coverage_df_adds_expected_columns(self):
        self.assertEqual(len(self.returned_df.columns), len(self.expected_df.columns))

    def test_join_latest_cqc_rating_into_coverage_df_does_not_add_any_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_join_latest_cqc_rating_into_coverage_df_has_no_duplicate_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(list(set(self.returned_df.columns))),
        )

    def test_join_latest_cqc_rating_into_coverage_df_has_expected_values(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
