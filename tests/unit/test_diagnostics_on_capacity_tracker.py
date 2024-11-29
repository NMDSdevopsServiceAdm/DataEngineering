import unittest
from unittest.mock import patch, Mock

import jobs.diagnostics_on_capacity_tracker as job
from tests.test_file_data import (
    DiagnosticsOnCapacityTrackerData as Data,
)
from tests.test_file_schemas import (
    DiagnosticsOnCapacityTrackerSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


class DiagnosticsOnCapacityTrackerTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    CAPACITY_TRACKER_CARE_HOME_SOURCE = "a/directory"
    CAPACITY_TRACKER_NON_RES_SOURCE = "other/directory"
    CARE_HOME_DIAGNOSTICS_DESTINATION = "some/other/directory"
    CARE_HOME_SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    NON_RES_DIAGNOSTICS_DESTINATION = "some/other/directory"
    NON_RES_SUMMARY_DIAGNOSTICS_DESTINATION = "yet/another/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_filled_posts_rows,
            Schemas.estimate_filled_posts_schema,
        )
        self.ct_care_home_df = self.spark.createDataFrame(
            Data.capacity_tracker_care_home_rows,
            Schemas.capacity_tracker_care_home_schema,
        )
        self.ct_non_res_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_res_rows, Schemas.capacity_tracker_non_res_schema
        )


class MainTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.estimate_jobs_df,
            self.ct_care_home_df,
            self.ct_non_res_df,
        ]

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.CAPACITY_TRACKER_CARE_HOME_SOURCE,
            self.CAPACITY_TRACKER_NON_RES_SOURCE,
            self.CARE_HOME_DIAGNOSTICS_DESTINATION,
            self.CARE_HOME_SUMMARY_DIAGNOSTICS_DESTINATION,
            self.NON_RES_DIAGNOSTICS_DESTINATION,
            self.NON_RES_SUMMARY_DIAGNOSTICS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 3)
        self.assertEqual(write_to_parquet_patch.call_count, 4)


class CheckConstantsTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    def test_absolute_value_cutoff_is_expected_value(self):
        self.assertEqual(job.absolute_value_cutoff, 10.0)
        self.assertIsInstance(job.absolute_value_cutoff, float)

    def test_percentage_value_cutoff_is_expected_value(self):
        self.assertEqual(job.percentage_value_cutoff, 0.25)
        self.assertIsInstance(job.percentage_value_cutoff, float)

    def test_standardised_value_cutoff_is_expected_value(self):
        self.assertEqual(job.standardised_value_cutoff, 1.0)
        self.assertIsInstance(job.standardised_value_cutoff, float)

    def test_number_of_days_in_rolling_average_is_expected_value(self):
        self.assertEqual(job.number_of_days_in_rolling_average, 185)
        self.assertIsInstance(job.number_of_days_in_rolling_average, int)

    def test_care_worker_ratio_is_expected_value(self):
        self.assertEqual(
            job.care_worker_ratio,
            {"micro": 0.61, "small": 0.74, "medium_or_large": 0.79},
        )

    def test_org_size_care_worker_upper_limit_is_expected_value(self):
        self.assertEqual(
            job.org_size_care_worker_upper_limit,
            {
                "micro": 10 * job.care_worker_ratio["micro"],
                "small": 50 * job.care_worker_ratio["small"],
            },
        )


class JoinCapacityTrackerTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()
        self.join_capacity_tracker_care_home_df = self.spark.createDataFrame(
            Data.join_capacity_tracker_care_home_rows,
            Schemas.join_estimates_schema,
        )
        self.returned_care_home_df = job.join_capacity_tracker_data(
            self.join_capacity_tracker_care_home_df,
            self.ct_care_home_df,
            care_home=True,
        )
        self.expected_care_home_df = self.spark.createDataFrame(
            Data.expected_joined_care_home_rows,
            Schemas.expected_joined_care_home_schema,
        )
        self.join_capacity_tracker_non_res_df = self.spark.createDataFrame(
            Data.join_capacity_tracker_non_res_rows,
            Schemas.estimate_filled_posts_schema,
        )
        self.returned_non_res_df = job.join_capacity_tracker_data(
            self.join_capacity_tracker_non_res_df, self.ct_non_res_df, care_home=False
        )
        self.expected_non_res_df = self.spark.createDataFrame(
            Data.expected_joined_non_res_rows, Schemas.expected_joined_non_res_schema
        )

    def test_join_capacity_tracker_data_adds_correct_columns_when_care_home_is_true(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_care_home_df.columns),
            sorted(self.expected_care_home_df.columns),
        )

    def test_join_capacity_tracker_data_correctly_joins_data_when_care_home_is_true(
        self,
    ):
        self.assertEqual(
            self.returned_care_home_df.select(self.expected_care_home_df.columns)
            .sort(IndCQC.location_id, IndCQC.cqc_location_import_date)
            .collect(),
            self.expected_care_home_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )

    def test_join_capacity_tracker_data_adds_correct_columns_when_care_home_is_false(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_non_res_df.columns),
            sorted(self.expected_non_res_df.columns),
        )

    def test_join_capacity_tracker_data_correctly_joins_data_when_care_home_is_false(
        self,
    ):
        self.assertEqual(
            self.returned_non_res_df.select(self.expected_non_res_df.columns)
            .sort(IndCQC.location_id, IndCQC.cqc_location_import_date)
            .collect(),
            self.expected_non_res_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )


class ConvertToAllPostsUsingRatioTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_to_all_posts_using_ratio_returns_correct_values_when_org_is_micro(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_to_all_posts_using_ratio_micro_rows,
            Schemas.convert_to_all_posts_using_ratio_schema,
        )
        expected_data = self.spark.createDataFrame(
            Data.expected_convert_to_all_posts_using_ratio_micro_rows,
            Schemas.expected_convert_to_all_posts_using_ratio_schema,
        ).collect()
        returned_data = (
            job.convert_to_all_posts_using_ratio(
                test_df,
            )
            .sort(IndCQC.location_id)
            .collect()
        )

        for i in range(len(returned_data)):
            self.assertAlmostEquals(
                returned_data[i][2],
                expected_data[i][2],
                places=2,
            )

    def test_convert_to_all_posts_using_ratio_returns_correct_values_when_org_is_small(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_to_all_posts_using_ratio_small_rows,
            Schemas.convert_to_all_posts_using_ratio_schema,
        )
        expected_data = self.spark.createDataFrame(
            Data.expected_convert_to_all_posts_using_ratio_small_rows,
            Schemas.expected_convert_to_all_posts_using_ratio_schema,
        ).collect()
        returned_data = (
            job.convert_to_all_posts_using_ratio(
                test_df,
            )
            .sort(IndCQC.location_id)
            .collect()
        )

        for i in range(len(returned_data)):
            self.assertAlmostEquals(
                returned_data[i][2],
                expected_data[i][2],
                places=2,
            )

    def test_convert_to_all_posts_using_ratio_returns_correct_values_when_org_is_medium_or_large(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_to_all_posts_using_ratio_medium_or_large_rows,
            Schemas.convert_to_all_posts_using_ratio_schema,
        )
        expected_data = self.spark.createDataFrame(
            Data.expected_convert_to_all_posts_using_ratio_medium_or_large_rows,
            Schemas.expected_convert_to_all_posts_using_ratio_schema,
        ).collect()
        returned_data = (
            job.convert_to_all_posts_using_ratio(
                test_df,
            )
            .sort(IndCQC.location_id)
            .collect()
        )

        for i in range(len(returned_data)):
            self.assertAlmostEquals(
                returned_data[i][2],
                expected_data[i][2],
                places=2,
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
