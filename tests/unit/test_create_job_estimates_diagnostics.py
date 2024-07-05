import unittest
import warnings
from unittest.mock import patch, Mock, ANY, call
from datetime import date


from tests.test_helpers import remove_file_path
from tests.test_file_schemas import (
    CreateJobEstimatesDiagnosticsSchemas as Schemas,
)
from tests.test_file_data import (
    CreateJobEstimatesDiagnosticsData as Data,
)
import jobs.create_job_estimates_diagnostics as job
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)

from utils import utils
from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
    Prefixes,
    TestColumns,
    ResidualsRequired,
)
from utils.column_names.capacity_tracker_columns import CapacityTrackerColumns as CT


class CreateJobEstimatesDiagnosticsTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    CAPACITY_TRACKER_CARE_HOME_SOURCE = "some/directory"
    CAPACITY_TRACKER_NON_RESIDENTIAL_SOURCE = "some/directory"
    DIAGNOSTICS_DESTINATION = "some/directory"
    RESIDUALS_DESTINATION = "some/directory"
    DESCRIPTION_OF_CHANGE = "some text"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_jobs_rows,
            schema=Schemas.estimate_jobs,
        )
        self.capacity_tracker_care_home_df = self.spark.createDataFrame(
            Data.capacity_tracker_care_home_rows,
            schema=Schemas.capacity_tracker_care_home,
        )
        self.capacity_tracker_non_residential_df = self.spark.createDataFrame(
            Data.capacity_tracker_non_residential_rows,
            schema=Schemas.capacity_tracker_non_residential,
        )


class MainTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()
        self.write_to_parquet_calls = [
            call(
                ANY,
                self.RESIDUALS_DESTINATION,
                mode="append",
                partitionKeys=["run_year", "run_month", "run_day"],
            ),
            call(
                ANY,
                self.DIAGNOSTICS_DESTINATION,
                mode="append",
                partitionKeys=["run_year", "run_month", "run_day"],
            ),
        ]

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock
    ):
        read_from_parquet_patch.side_effect = [
            self.estimate_jobs_df,
            self.capacity_tracker_care_home_df,
            self.capacity_tracker_non_residential_df,
        ]

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.CAPACITY_TRACKER_CARE_HOME_SOURCE,
            self.CAPACITY_TRACKER_NON_RESIDENTIAL_SOURCE,
            self.DIAGNOSTICS_DESTINATION,
            self.RESIDUALS_DESTINATION,
            self.DESCRIPTION_OF_CHANGE,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 3)
        write_to_parquet_patch.assert_has_calls(self.write_to_parquet_calls)


class MergeDataFramesTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_snapshot_date_to_capacity_tracker_dataframe_adds_snapshot_date_column(
        self,
    ):
        output_df = job.add_snapshot_date_to_capacity_tracker_dataframe(
            self.capacity_tracker_care_home_df, IndCQC.cqc_location_import_date
        )

        expected_df_size = len(self.capacity_tracker_care_home_df.columns) + 1
        expected_rows = self.capacity_tracker_care_home_df.count()
        expected_value = date.fromisoformat(
            Values.capacity_tracker_snapshot_date_formatted
        )

        output_df_list = output_df.collect()
        output_df_size = len(output_df_list[0])
        output_df_rows = len(output_df_list)

        self.assertEqual(output_df_size, expected_df_size)
        self.assertEqual(output_df_rows, expected_rows)
        self.assertEqual(
            output_df_list[0][IndCQC.cqc_location_import_date], expected_value
        )

    def test_test_merge_dataframes_does_not_add_additional_rows(self):
        output_df = job.merge_dataframes(
            self.estimate_jobs_df,
            self.capacity_tracker_care_home_df,
            self.capacity_tracker_non_residential_df,
        )
        expected_rows = 1
        self.assertEqual(output_df.count(), expected_rows)


class PrepareCapacityTrackerTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_prepare_capacity_tracker_care_home_data_calculates_total_of_employed_columns(
        self,
    ):
        diagnostics_df = self.spark.createDataFrame(
            Data.prepare_capacity_tracker_care_home_rows, schema=Schemas.diagnostics
        )

        output_df = job.prepare_capacity_tracker_care_home_data(diagnostics_df)

        expected_totals = [41.0, None]

        output_df_list = output_df.sort(IndCQC.location_id).collect()

        self.assertEqual(output_df_list[0][CT.care_home_employed], expected_totals[0])
        self.assertEqual(output_df_list[1][CT.care_home_employed], expected_totals[1])

    def test_prepare_capacity_tracker_non_residential_data_estimates_total_of_employed_staff(
        self,
    ):
        diagnostics_df = self.spark.createDataFrame(
            Data.prepare_capacity_tracker_non_residential_rows,
            schema=Schemas.diagnostics,
        )

        output_df = job.prepare_capacity_tracker_non_residential_data(diagnostics_df)

        expected_totals = [None, 97.5]

        output_df_list = output_df.sort(IndCQC.location_id).collect()

        self.assertEqual(
            output_df_list[0][CT.non_residential_employed], expected_totals[0]
        )
        self.assertEqual(
            output_df_list[1][CT.non_residential_employed], expected_totals[1]
        )


class CalculateResidualsTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()
        self.calculate_residuals_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows, schema=Schemas.diagnostics_prepared
        )

    def test_calculate_residuals_adds_a_column(self):
        output_df = job.calculate_residuals(
            self.calculate_residuals_df,
            model=IndCQC.estimate_filled_posts,
            service=Values.non_res,
            data_source_column=IndCQC.people_directly_employed,
        )

        output_df_size = len(output_df.columns)

        expected_df_size = len(self.calculate_residuals_df.columns) + 1
        self.assertEqual(output_df_size, expected_df_size)

    def test_calculate_residuals_adds_residual_value(self):
        output_df = job.calculate_residuals(
            self.calculate_residuals_df,
            model=IndCQC.estimate_filled_posts,
            service=Values.non_res,
            data_source_column=IndCQC.people_directly_employed,
        )

        output_df_list = output_df.sort(IndCQC.location_id).collect()
        expected_values = [
            0.0,
            -5.0,
            15.0,
            None,
        ]
        new_column_name = TestColumns.residuals_test_column_names[0]

        self.assertEqual(output_df_list[0][new_column_name], expected_values[0])
        self.assertEqual(output_df_list[1][new_column_name], expected_values[1])
        self.assertEqual(output_df_list[2][new_column_name], expected_values[2])
        self.assertEqual(output_df_list[3][new_column_name], expected_values[3])

    def test_create_residuals_column_name(
        self,
    ):
        model = IndCQC.estimate_filled_posts
        service = Values.non_res
        data_source_column = IndCQC.people_directly_employed

        output = job.create_residuals_column_name(model, service, data_source_column)
        expected_output = TestColumns.residuals_test_column_names[0]

        self.assertEqual(output, expected_output)

    def test_run_residuals_creates_additional_columns(
        self,
    ):
        run_residuals_df = self.spark.createDataFrame(
            Data.run_residuals_rows, schema=Schemas.diagnostics_prepared
        )

        residuals_list = job.create_residuals_list(
            ResidualsRequired.models,
            ResidualsRequired.services,
            ResidualsRequired.data_source_columns,
        )

        output_df = job.run_residuals(run_residuals_df, residuals_list=residuals_list)
        output_df_size = len(output_df.columns)

        expected_df_size = len(run_residuals_df.columns)
        self.assertGreater(output_df_size, expected_df_size)

    def test_create_residuals_list_includes_all_permutations(self):
        models = [
            IndCQC.estimate_filled_posts,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ]

        services = [
            Values.care_home,
            Values.non_res,
        ]

        data_source_columns = [
            IndCQC.ascwds_filled_posts_dedup_clean,
            CT.care_home_employed,
            CT.non_residential_employed,
        ]

        output = job.create_residuals_list(models, services, data_source_columns)

        expected_output = [
            [
                IndCQC.estimate_filled_posts,
                Values.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                Values.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.estimate_filled_posts,
                Values.non_res,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                Values.non_res,
                CT.non_residential_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.non_res,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.non_res,
                CT.non_residential_employed,
            ],
        ]
        self.assertEqual(output, expected_output)


class ColumnNameListsTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_column_names_list_adds_the_correct_number_of_columns(self):
        residuals_list = [
            [
                IndCQC.estimate_filled_posts,
                Values.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                Values.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.estimate_filled_posts,
                Values.non_res,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [IndCQC.estimate_filled_posts, Values.non_res, CT.care_home_employed],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.non_res,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                Values.non_res,
                CT.care_home_employed,
            ],
        ]

        output = job.create_column_names_list(residuals_list)

        expected_size = len(residuals_list)
        output_size = len(output)

        self.assertEqual(output_size, expected_size)


class CalculateAverageResidualTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_average_residual_creates_column_of_average_residuals(self):
        residuals_df = self.spark.createDataFrame(
            Data.residuals_rows, schema=Schemas.residuals
        )
        output_column_name = Prefixes.avg + TestColumns.residuals_test_column_names[0]

        output = job.calculate_average_residual(
            residuals_df, TestColumns.residuals_test_column_names[0], output_column_name
        )
        output_rows = output.collect()

        expected_output = 2.0

        self.assertEqual(output_rows[0][output_column_name], expected_output)


class CreateEmptyDataFrameTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_empty_dataframe_creates_a_dataframe_with_one_string_colum(self):
        output_df = job.create_empty_dataframe(Data.description_of_change, self.spark)
        output_df_rows = output_df.collect()
        output_df_row_count = output_df.count()
        output_df_column_count = len(output_df_rows)

        expected_value = Data.description_of_change
        expected_row_count = 1
        expected_column_count = 1

        self.assertEqual(output_df_rows[0][CT.description_of_changes], expected_value)
        self.assertEqual(output_df_row_count, expected_row_count)
        self.assertEqual(output_df_column_count, expected_column_count)


class RunAverageResidualsTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_run_average_residuals_creates_df_of_average_residuals(self):
        residuals_df = self.spark.createDataFrame(
            Data.residuals_rows, schema=Schemas.residuals
        )

        blank_df = job.create_empty_dataframe(Data.description_of_change, self.spark)

        output_df = job.run_average_residuals(
            residuals_df, blank_df, TestColumns.residuals_test_column_names
        )
        output_df_rows = output_df.collect()

        expected_output = [2.0, 0.0]
        output_column_names = [
            Prefixes.avg + TestColumns.residuals_test_column_names[0],
            Prefixes.avg + TestColumns.residuals_test_column_names[1],
        ]

        self.assertEqual(output_df_rows[0][output_column_names[0]], expected_output[0])
        self.assertEqual(output_df_rows[0][output_column_names[1]], expected_output[1])


class AddTimestampColumnTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_timestamp_column_adds_a_column_with_the_specified_timestamp_as_a_string(
        self,
    ):
        add_timestamps_df = self.spark.createDataFrame(
            Data.add_timestamps_rows, schema=Schemas.residuals
        )

        output_df = job.add_timestamp_column(add_timestamps_df, Data.run_timestamp)
        output_df_rows = output_df.collect()

        self.assertEqual(output_df_rows[0][CT.run_timestamp], Data.run_timestamp)
        self.assertEqual(output_df_rows[0][CT.run_timestamp], Data.run_timestamp)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
