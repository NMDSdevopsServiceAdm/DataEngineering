import unittest
import warnings
from unittest.mock import patch

from pyspark.sql import SparkSession


from tests.test_helpers import remove_file_path
from tests.test_schemas.schemas_for_tests import (
    CreateJobEstimatesDiagnosticsSchemas as Schemas,
)
import jobs.create_job_estimates_diagnostics as job
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    PEOPLE_DIRECTLY_EMPLOYED,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
)
from utils.diagnostics_utils.diagnostics_meta_data import (
    CategoricalVariables as Values,
    Prefixes,
    Columns,
    TestColumns,
    ResidualsRequired,
)



class CreateJobEstimatesDiagnosticsTests(unittest.TestCase):
    ESTIMATED_JOB_COUNTS = "tests/test_data/tmp/estimated_job_counts/"
    CAPACITY_TRACKER_CARE_HOME_DATA = (
        "tests/test_data/tmp/capacity_tracker_care_home_data/"
    )
    CAPACITY_TRACKER_NON_RESIDENTIAL_DATA = (
        "tests/test_data/tmp/capacity_tracker_non_residential_data/"
    )
    PIR_DATA = "tests/test_data/tmp/pir_data/"
    DIAGNOSTICS_DESTINATION = "tests/test_data/tmp/diagnostics/"
    RESIDUALS_DESTINATION = "tests/test_data/tmp/residuals/"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_create_job_estimates_diagnostics"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        remove_file_path(self.ESTIMATED_JOB_COUNTS)
        remove_file_path(self.CAPACITY_TRACKER_CARE_HOME_DATA)
        remove_file_path(self.CAPACITY_TRACKER_NON_RESIDENTIAL_DATA)
        remove_file_path(self.PIR_DATA)
        remove_file_path(self.DIAGNOSTICS_DESTINATION)
        remove_file_path(self.RESIDUALS_DESTINATION)


    @patch("jobs.create_job_estimates_diagnostics.main")
    def test_create_job_estimates_diagnostics_completes(self, mock_main):
        # fmt: off
        estimate_jobs_rows = [
            ("location_1", 40.0, 40.0, Values.care_home_with_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45,),
        ]
        # fmt: on

        capacity_tracker_care_home_rows = [
            ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
        ]
        capacity_tracker_non_residential_rows = [
            ("location_2", 67.0),
        ]

        estimate_jobs_df = self.spark.createDataFrame(
            estimate_jobs_rows, schema=Schemas.estimate_jobs,
        )
        capacity_tracker_care_home_df = self.spark.createDataFrame(
            capacity_tracker_care_home_rows,
            schema=Schemas.capacity_tracker_care_home,
        )
        capacity_tracker_non_residential_df = self.spark.createDataFrame(
            capacity_tracker_non_residential_rows,
            schema=Schemas.capacity_tracker_non_residential,
        )

        job.main(
            estimate_jobs_df,
            capacity_tracker_care_home_df,
            capacity_tracker_non_residential_df,
            self.DIAGNOSTICS_DESTINATION,
            self.RESIDUALS_DESTINATION,
        )

        mock_main.assert_called_once()

    def test_test_merge_dataframes_does_not_add_additional_rows(self):
        # fmt: off
        estimate_jobs_rows = [
            ("location_1", 40.0, 40.0, Values.care_home_with_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45,),
        ]
        # fmt: on

        capacity_tracker_care_home_rows = [
            ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
        ]
        capacity_tracker_non_residential_rows = [
            ("location_2", 67.0),
        ]

        estimate_jobs_df = self.spark.createDataFrame(
            estimate_jobs_rows, schema=Schemas.estimate_jobs
        )
        capacity_tracker_care_home_df = self.spark.createDataFrame(
            capacity_tracker_care_home_rows,
            schema=Schemas.capacity_tracker_care_home,
        )
        capacity_tracker_non_residential_df = self.spark.createDataFrame(
            capacity_tracker_non_residential_rows,
            schema=Schemas.capacity_tracker_non_residential,
        )

        output_df = job.merge_dataframes(
            estimate_jobs_df,
            capacity_tracker_care_home_df,
            capacity_tracker_non_residential_df,
        )
        expected_rows = 1
        self.assertEqual(output_df.count(), expected_rows)

    def test_prepare_capacity_tracker_care_home_data_calculates_total_of_employed_columns(
        self,
    ):
        # fmt: off
        diagnostics_rows = [
            ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
            ("location_2", Values.non_residential,  None, None, None, None, None, None, 30.0,),
        ]
        # fmt: on
        diagnostics_df = self.spark.createDataFrame(
            diagnostics_rows, schema=Schemas.diagnostics
        )

        output_df = job.prepare_capacity_tracker_care_home_data(diagnostics_df)

        expected_totals = [41.0, None]

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(output_df_list[0][Columns.CARE_HOME_EMPLOYED], expected_totals[0])
        self.assertEqual(output_df_list[1][Columns.CARE_HOME_EMPLOYED], expected_totals[1])

    def test_prepare_capacity_tracker_non_residential_data_estimates_total_of_employed_staff(
        self,
    ):
        # fmt: off
        diagnostics_rows = [
            ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
            ("location_2", Values.non_residential, None, None, None, None, None, None, 75.0,),
        ]
        # fmt: on
        diagnostics_df = self.spark.createDataFrame(
            diagnostics_rows, schema=Schemas.diagnostics
        )

        output_df = job.prepare_capacity_tracker_non_residential_data(diagnostics_df)

        expected_totals = [None, 97.5]

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(
            output_df_list[0][Columns.NON_RESIDENTIAL_EMPLOYED], expected_totals[0]
        )
        self.assertEqual(
            output_df_list[1][Columns.NON_RESIDENTIAL_EMPLOYED], expected_totals[1]
        )

    def test_calculate_residuals_adds_a_column(self):
        # fmt: off
        diagnostics_prepared_rows = [
            ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, None, None, 40.0,),
            ("location_3", 40.0, 40.0, Values.care_home_without_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
            ("location_4", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, None, None,),
            ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
        ]
        # fmt: on

        known_values_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=Schemas.diagnostics_prepared
        )

        output_df = job.calculate_residuals(
            known_values_df,
            model=ESTIMATE_JOB_COUNT,
            service=Values.non_residential,
            data_source_column=PEOPLE_DIRECTLY_EMPLOYED,
        )

        output_df_size = len(output_df.columns)

        expected_df_size = len(known_values_df.columns) + 1
        self.assertEqual(output_df_size, expected_df_size)

    def test_calculate_residuals_adds_residual_value(self):
        # fmt: off
        diagnostics_prepared_rows = [
            ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 40, None, 40.0,),
            ("location_3", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
            ("location_4", None, None, Values.non_residential, 60.9, 23.4, None, None, None, 60.0, 45, None, None,),
            ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
        ]
        # fmt: on

        known_values_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=Schemas.diagnostics_prepared
        )

        output_df = job.calculate_residuals(
            known_values_df,
            model=ESTIMATE_JOB_COUNT,
            service=Values.non_residential,
            data_source_column=PEOPLE_DIRECTLY_EMPLOYED,
        )

        output_df_list = output_df.sort(LOCATION_ID).collect()
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
        model = ESTIMATE_JOB_COUNT
        service = Values.non_residential
        data_source_column = PEOPLE_DIRECTLY_EMPLOYED

        output = job.create_residuals_column_name(model, service, data_source_column)
        expected_output = TestColumns.residuals_test_column_names[0]

        self.assertEqual(output, expected_output)

    def test_run_residuals_creates_additional_columns(
        self,
    ):
        # fmt: off
        diagnostics_prepared_rows = [
            ("location_1", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
            ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, None, None, 40.0,),
            ("location_3", 40.0, 40.0, Values.care_home_without_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
            ("location_4", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, None, None,),
            ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
            ("location_6", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, 50.0, None,),
            ("location_7", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, None,),
            ("location_8", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, None, None, 40.0,),
            ("location_9", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, 40.0,),
            ("location_10", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
        ]
        # fmt: on

        diagnostics_prepared_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=Schemas.diagnostics_prepared
        )

        residuals_list = job.create_residuals_list(
            ResidualsRequired.models,
            ResidualsRequired.services,
            ResidualsRequired.data_source_columns,
        )

        output_df = job.run_residuals(
            diagnostics_prepared_df, residuals_list=residuals_list
        )
        output_df_size = len(output_df.columns)

        expected_df_size = len(diagnostics_prepared_df.columns)
        self.assertGreater(output_df_size, expected_df_size)

    def test_create_residuals_list_includes_all_permutations(self):
        models = [
            ESTIMATE_JOB_COUNT,
            JOB_COUNT,
        ]

        services = [
            Values.care_home,
            Values.non_res,
        ]

        data_source_columns = [
            JOB_COUNT_UNFILTERED,
            Columns.CARE_HOME_EMPLOYED,
        ]

        output = job.create_residuals_list(models, services, data_source_columns)
        expected_output = [
            [ESTIMATE_JOB_COUNT, Values.care_home, JOB_COUNT_UNFILTERED],
            [ESTIMATE_JOB_COUNT, Values.care_home, Columns.CARE_HOME_EMPLOYED],
            [ESTIMATE_JOB_COUNT, Values.non_res, JOB_COUNT_UNFILTERED],
            [ESTIMATE_JOB_COUNT, Values.non_res, Columns.CARE_HOME_EMPLOYED],
            [JOB_COUNT, Values.care_home, JOB_COUNT_UNFILTERED],
            [JOB_COUNT, Values.care_home, Columns.CARE_HOME_EMPLOYED],
            [JOB_COUNT, Values.non_res, JOB_COUNT_UNFILTERED],
            [JOB_COUNT, Values.non_res, Columns.CARE_HOME_EMPLOYED],
        ]
        self.assertEqual(output, expected_output)

    def test_calculate_average_residual_creates_column_of_average_residuals(self):
        # fmt: off
        residuals_rows = [
            ("location_1", 0.0, 0.0,),
            ("location_2", -1.0, 0.0,),
            ("location_3", 3.0, 0.0,),
            ("location_4", None, 0.0,),
            ("location_5", 10.5, 0.0,),
            ("location_6", -2.5, 0.0,),
            ("location_7", None, None,),
            ("location_8", None, None,),
        ]
        # fmt: on

        residuals_df = self.spark.createDataFrame(
            residuals_rows, schema=Schemas.residuals
        )
        output_column_name = Prefixes.avg + TestColumns.residuals_test_column_names[0]

        output = job.calculate_average_residual(
            residuals_df, TestColumns.residuals_test_column_names[0], output_column_name
        )
        output.printSchema()
        output_rows = output.collect()

        expected_output = 2.0

        self.assertEqual(output_rows[0][output_column_name], expected_output)

    def test_create_empty_dataframe_creates_a_dataframe_with_one_string_colum(self):
        description_of_change: str = "test"

        output_df = job.create_empty_dataframe(description_of_change, self.spark)
        output_df_rows = output_df.collect()
        output_df_row_count = output_df.count()
        output_df_column_count = len(output_df_rows)

        expected_value = description_of_change
        expected_row_count = 1
        expected_column_count = 1

        self.assertEqual(output_df_rows[0][Columns.DESCRIPTION_OF_CHANGES], expected_value)
        self.assertEqual(output_df_row_count, expected_row_count)
        self.assertEqual(output_df_column_count, expected_column_count)

    def test_run_average_residuals_creates_df_of_average_residuals(self):
        # fmt: off
        residuals_rows = [
            ("location_1", 0.0, 0.0,),
            ("location_2", -1.0, 0.0,),
            ("location_3", 3.0, 0.0,),
            ("location_4", None, 0.0,),
            ( "location_5", 10.5, 0.0,),
            ("location_6", -2.5, 0.0,),
            ("location_7", None, None,),
            ("location_8", None, None,),
        ]
        # fmt: on

        residuals_df = self.spark.createDataFrame(
            residuals_rows, schema=Schemas.residuals
        )

        blank_df = job.create_empty_dataframe("test", self.spark)

        output_df = job.run_average_residuals(
            residuals_df, blank_df, TestColumns.residuals_test_column_names
        )
        output_df.printSchema()
        output_df_rows = output_df.collect()

        expected_output = [2.0, 0.0]
        output_column_names = [
            Prefixes.avg + TestColumns.residuals_test_column_names[0],
            Prefixes.avg + TestColumns.residuals_test_column_names[1],
        ]

        self.assertEqual(output_df_rows[0][output_column_names[0]], expected_output[0])
        self.assertEqual(output_df_rows[0][output_column_names[1]], expected_output[1])

    def test_add_timestamp_column_adds_a_column_with_the_specified_timestamp_as_a_string(
        self,
    ):
        # fmt: off
        residuals_rows = [
            ("location_1", 0.0, 0.0,),
            ("location_2", -1.0, 0.0,),
        ]
        # fmt: on

        residuals_df = self.spark.createDataFrame(
            residuals_rows, schema=Schemas.residuals
        )
        run_timestamp = "12/24/2018, 04:59:31"

        output_df = job.add_timestamp_column(residuals_df, run_timestamp)
        output_df_rows = output_df.collect()

        self.assertEqual(output_df_rows[0][Columns.RUN_TIMESTAMP], run_timestamp)
        self.assertEqual(output_df_rows[0][Columns.RUN_TIMESTAMP], run_timestamp)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
