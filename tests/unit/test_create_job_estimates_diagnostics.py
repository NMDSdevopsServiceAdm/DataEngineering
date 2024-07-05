import unittest
from unittest.mock import patch, Mock, ANY, call
from datetime import date


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
    ResidualsRequired,
)
from utils.column_names.capacity_tracker_columns import CapacityTrackerColumns as CT
from utils.column_values.categorical_column_values import CareHome


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

    def test_add_import_date_to_capacity_tracker_dataframe_adds_snapshot_date_column(
        self,
    ):
        returned_df = job.add_import_date_to_capacity_tracker_dataframe(
            self.capacity_tracker_care_home_df,
            CT.capacity_tracker_care_homes_import_date,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_add_date_to_capacity_tracker_rows,
            Schemas.expected_add_date_to_capacity_tracker_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_dataframes_does_not_add_additional_rows(self):
        returned_df = job.merge_dataframes(
            self.estimate_jobs_df,
            self.capacity_tracker_care_home_df,
            self.capacity_tracker_non_residential_df,
        )
        expected_rows = self.estimate_jobs_df.count()
        self.assertEqual(returned_df.count(), expected_rows)


class PrepareCapacityTrackerTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_prepare_capacity_tracker_care_home_data_calculates_total_of_employed_columns(
        self,
    ):
        diagnostics_df = self.spark.createDataFrame(
            Data.prepare_capacity_tracker_care_home_rows, schema=Schemas.diagnostics
        )

        returned_df = job.prepare_capacity_tracker_care_home_data(diagnostics_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_prepare_capacity_tracker_care_home_rows,
            Schemas.expected_prepare_capacity_tracker_care_home_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_prepare_capacity_tracker_non_residential_data_estimates_total_of_employed_staff(
        self,
    ):
        diagnostics_df = self.spark.createDataFrame(
            Data.prepare_capacity_tracker_non_residential_rows,
            schema=Schemas.diagnostics,
        )

        returned_df = job.prepare_capacity_tracker_non_residential_data(diagnostics_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_prepare_capacity_tracker_non_residential_rows,
            Schemas.expected_prepare_capacity_tracker_non_residential_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class CalculateResidualsTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()
        self.calculate_residuals_df = self.spark.createDataFrame(
            Data.calculate_residuals_rows, schema=Schemas.diagnostics_prepared
        )

    def test_calculate_residuals_adds_a_column(self):
        returned_df = job.calculate_residuals(
            self.calculate_residuals_df,
            model=IndCQC.estimate_filled_posts,
            service=CareHome.not_care_home,
            data_source_column=IndCQC.people_directly_employed,
        )

        returned_df_size = len(returned_df.columns)

        expected_df_size = len(self.calculate_residuals_df.columns) + 1
        self.assertEqual(returned_df_size, expected_df_size)

    def test_calculate_residuals_adds_residual_value(self):
        returned_df = job.calculate_residuals(
            self.calculate_residuals_df,
            model=IndCQC.estimate_filled_posts,
            service=CareHome.not_care_home,
            data_source_column=IndCQC.people_directly_employed,
        )

        returned_df_list = returned_df.sort(IndCQC.location_id).collect()
        expected_values = [
            0.0,
            -5.0,
            15.0,
            None,
        ]
        new_column_name = IndCQC.residuals_estimate_filled_posts_non_res_pir

        self.assertEqual(returned_df_list[0][new_column_name], expected_values[0])
        self.assertEqual(returned_df_list[1][new_column_name], expected_values[1])
        self.assertEqual(returned_df_list[2][new_column_name], expected_values[2])
        self.assertEqual(returned_df_list[3][new_column_name], expected_values[3])

    def test_create_residuals_column_name(
        self,
    ):
        model = IndCQC.estimate_filled_posts
        service = CareHome.not_care_home
        data_source_column = IndCQC.people_directly_employed

        output = job.create_residuals_column_name(model, service, data_source_column)
        expected_output = IndCQC.residuals_estimate_filled_posts_non_res_pir

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

        returned_df = job.run_residuals(run_residuals_df, residuals_list=residuals_list)
        returned_df_size = len(returned_df.columns)

        expected_df_size = len(run_residuals_df.columns)
        self.assertGreater(returned_df_size, expected_df_size)

    def test_create_residuals_list_includes_all_permutations(self):
        models = [
            IndCQC.estimate_filled_posts,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ]

        services = [
            CareHome.care_home,
            CareHome.not_care_home,
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
                CareHome.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.not_care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.not_care_home,
                CT.non_residential_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.not_care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.not_care_home,
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
                CareHome.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.not_care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.estimate_filled_posts,
                CareHome.not_care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.care_home,
                CT.care_home_employed,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.not_care_home,
                IndCQC.ascwds_filled_posts_dedup_clean,
            ],
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                CareHome.not_care_home,
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
        output_column_name = (
            Prefixes.avg + IndCQC.residuals_estimate_filled_posts_non_res_pir
        )

        output = job.calculate_average_residual(
            residuals_df,
            IndCQC.residuals_estimate_filled_posts_non_res_pir,
            output_column_name,
        )
        output_rows = output.collect()

        expected_output = 2.0

        self.assertEqual(output_rows[0][output_column_name], expected_output)


class CreateEmptyDataFrameTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_empty_dataframe_creates_a_dataframe_with_one_string_colum(self):
        returned_df = job.create_empty_dataframe(Data.description_of_change, self.spark)
        returned_df_rows = returned_df.collect()
        returned_df_row_count = returned_df.count()
        returned_df_column_count = len(returned_df_rows)

        expected_value = Data.description_of_change
        expected_row_count = 1
        expected_column_count = 1

        self.assertEqual(returned_df_rows[0][CT.description_of_changes], expected_value)
        self.assertEqual(returned_df_row_count, expected_row_count)
        self.assertEqual(returned_df_column_count, expected_column_count)


class RunAverageResidualsTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_run_average_residuals_creates_df_of_average_residuals(self):
        residuals_df = self.spark.createDataFrame(
            Data.residuals_rows, schema=Schemas.residuals
        )

        blank_df = job.create_empty_dataframe(Data.description_of_change, self.spark)

        returned_df = job.run_average_residuals(
            residuals_df, blank_df, IndCQC.residuals_estimate_filled_posts_non_res_pir
        )
        returned_df_rows = returned_df.collect()

        expected_output = [2.0, 0.0]
        output_column_names = [
            Prefixes.avg + IndCQC.residuals_estimate_filled_posts_non_res_pir,
            Prefixes.avg + IndCQC.residuals_ascwds_filled_posts_clean_dedup_non_res_pir,
        ]

        self.assertEqual(
            returned_df_rows[0][output_column_names[0]], expected_output[0]
        )
        self.assertEqual(
            returned_df_rows[0][output_column_names[1]], expected_output[1]
        )


class AddTimestampColumnTests(CreateJobEstimatesDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_timestamp_column_adds_a_column_with_the_specified_timestamp_as_a_string(
        self,
    ):
        add_timestamps_df = self.spark.createDataFrame(
            Data.add_timestamps_rows, schema=Schemas.residuals
        )

        returned_df = job.add_timestamp_column(add_timestamps_df, Data.run_timestamp)
        returned_df_rows = returned_df.collect()

        self.assertEqual(returned_df_rows[0][CT.run_timestamp], Data.run_timestamp)
        self.assertEqual(returned_df_rows[0][CT.run_timestamp], Data.run_timestamp)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
