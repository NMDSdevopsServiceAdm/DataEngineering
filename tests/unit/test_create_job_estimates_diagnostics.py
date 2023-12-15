import unittest
import warnings
from datetime import datetime, date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)

from tests.test_file_generator import generate_prepared_locations_file_parquet
from tests.test_helpers import remove_file_path
import jobs.create_job_estimates_diagnostics as job
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    PEOPLE_DIRECTLY_EMPLOYED,
    SNAPSHOT_DATE,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
    ROLLING_AVERAGE_MODEL,
    EXTRAPOLATION_MODEL,
    CARE_HOME_MODEL,
    INTERPOLATION_MODEL,
    NON_RESIDENTIAL_MODEL,
)
from utils.estimate_job_count.capacity_tracker_column_names import (
    CQC_ID,
    NURSES_EMPLOYED,
    CARE_WORKERS_EMPLOYED,
    NON_CARE_WORKERS_EMPLOYED,
    AGENCY_NURSES_EMPLOYED,
    AGENCY_CARE_WORKERS_EMPLOYED,
    AGENCY_NON_CARE_WORKERS_EMPLOYED,
    CQC_CARE_WORKERS_EMPLOYED,
    CARE_HOME_EMPLOYED,
    NON_RESIDENTIAL_EMPLOYED,
    RESIDUAL_CATEGORY,
)
from utils.estimate_job_count.capacity_tracker_column_values import (
    known,
    unknown,
    care_home_with_nursing,
    care_home_without_nursing,
    non_residential,
    pir,
    care_home,
    non_res,
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

    estimate_jobs_schema = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(
                JOB_COUNT_UNFILTERED,
                FloatType(),
                True,
            ),
            StructField(JOB_COUNT, FloatType(), True),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(ROLLING_AVERAGE_MODEL, FloatType(), True),
            StructField(CARE_HOME_MODEL, FloatType(), True),
            StructField(EXTRAPOLATION_MODEL, FloatType(), True),
            StructField(INTERPOLATION_MODEL, FloatType(), True),
            StructField(NON_RESIDENTIAL_MODEL, FloatType(), True),
            StructField(ESTIMATE_JOB_COUNT, FloatType(), True),
            StructField(PEOPLE_DIRECTLY_EMPLOYED, IntegerType(), True),
        ]
    )
    capacity_tracker_care_home_schema = StructType(
        [
            StructField(CQC_ID, StringType(), False),
            StructField(
                NURSES_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(AGENCY_NURSES_EMPLOYED, FloatType(), True),
            StructField(AGENCY_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(AGENCY_NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
        ]
    )
    capacity_tracker_non_residential_schema = StructType(
        [
            StructField(CQC_ID, StringType(), False),
            StructField(
                CQC_CARE_WORKERS_EMPLOYED,
                FloatType(),
                True,
            ),
        ]
    )

    diagnostics_schema = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(
                JOB_COUNT_UNFILTERED,
                FloatType(),
                True,
            ),
            StructField(JOB_COUNT, FloatType(), True),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(ROLLING_AVERAGE_MODEL, FloatType(), True),
            StructField(CARE_HOME_MODEL, FloatType(), True),
            StructField(EXTRAPOLATION_MODEL, FloatType(), True),
            StructField(INTERPOLATION_MODEL, FloatType(), True),
            StructField(NON_RESIDENTIAL_MODEL, FloatType(), True),
            StructField(ESTIMATE_JOB_COUNT, FloatType(), True),
            StructField(PEOPLE_DIRECTLY_EMPLOYED, IntegerType(), True),
            StructField(
                NURSES_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(AGENCY_NURSES_EMPLOYED, FloatType(), True),
            StructField(AGENCY_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(AGENCY_NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(
                CQC_CARE_WORKERS_EMPLOYED,
                FloatType(),
                True,
            ),
        ]
    )
    diagnostics_prepared_schema = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(
                JOB_COUNT_UNFILTERED,
                FloatType(),
                True,
            ),
            StructField(JOB_COUNT, FloatType(), True),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(ROLLING_AVERAGE_MODEL, FloatType(), True),
            StructField(CARE_HOME_MODEL, FloatType(), True),
            StructField(EXTRAPOLATION_MODEL, FloatType(), True),
            StructField(INTERPOLATION_MODEL, FloatType(), True),
            StructField(NON_RESIDENTIAL_MODEL, FloatType(), True),
            StructField(ESTIMATE_JOB_COUNT, FloatType(), True),
            StructField(PEOPLE_DIRECTLY_EMPLOYED, IntegerType(), True),
            StructField(
                CARE_HOME_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(NON_RESIDENTIAL_EMPLOYED, FloatType(), True),
        ]
    )

    

    @patch("jobs.create_job_estimates_diagnostics.main")
    def test_create_job_estimates_diagnostics_completes(self, mock_main):
        estimate_jobs_rows = [
            (
                "location_1",
                40.0,
                40.0,
                care_home_with_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
            ),
        ]

        capacity_tracker_care_home_rows = [
            ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
        ]
        capacity_tracker_non_residential_rows = [
            ("location_2", 67.0),
        ]

        estimate_jobs_df = self.spark.createDataFrame(
            estimate_jobs_rows, schema=self.estimate_jobs_schema
        )
        capacity_tracker_care_home_df = self.spark.createDataFrame(
            capacity_tracker_care_home_rows,
            schema=self.capacity_tracker_care_home_schema,
        )
        capacity_tracker_non_residential_df = self.spark.createDataFrame(
            capacity_tracker_non_residential_rows,
            schema=self.capacity_tracker_non_residential_schema,
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
        estimate_jobs_rows = [
            (
                "location_1",
                40.0,
                40.0,
                care_home_with_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
            ),
        ]

        capacity_tracker_care_home_rows = [
            ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
        ]
        capacity_tracker_non_residential_rows = [
            ("location_2", 67.0),
        ]

        estimate_jobs_df = self.spark.createDataFrame(
            estimate_jobs_rows, schema=self.estimate_jobs_schema
        )
        capacity_tracker_care_home_df = self.spark.createDataFrame(
            capacity_tracker_care_home_rows,
            schema=self.capacity_tracker_care_home_schema,
        )
        capacity_tracker_non_residential_df = self.spark.createDataFrame(
            capacity_tracker_non_residential_rows,
            schema=self.capacity_tracker_non_residential_schema,
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
        diagnostics_rows = [
            (
                "location_1",
                40.0,
                40.0,
                care_home_with_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                8.0,
                12.0,
                15.0,
                1.0,
                3.0,
                2.0,
                None,
            ),
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                None,
                None,
                None,
                None,
                None,
                None,
                30.0,
            ),
        ]

        diagnostics_df = self.spark.createDataFrame(
            diagnostics_rows, schema=self.diagnostics_schema
        )

        output_df = job.prepare_capacity_tracker_care_home_data(diagnostics_df)

        expected_totals = [41.0, None]

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(output_df_list[0][CARE_HOME_EMPLOYED], expected_totals[0])
        self.assertEqual(output_df_list[1][CARE_HOME_EMPLOYED], expected_totals[1])

    def test_prepare_capacity_tracker_non_residential_data_estimates_total_of_employed_staff(
        self,
    ):
        diagnostics_rows = [
            (
                "location_1",
                40.0,
                40.0,
                care_home_with_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                8.0,
                12.0,
                15.0,
                1.0,
                3.0,
                2.0,
                None,
            ),
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                None,
                None,
                None,
                None,
                None,
                None,
                75.0,
            ),
        ]

        diagnostics_df = self.spark.createDataFrame(
            diagnostics_rows, schema=self.diagnostics_schema
        )

        output_df = job.prepare_capacity_tracker_non_residential_data(diagnostics_df)

        expected_totals = [None, 97.5]

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(
            output_df_list[0][NON_RESIDENTIAL_EMPLOYED], expected_totals[0]
        )
        self.assertEqual(
            output_df_list[1][NON_RESIDENTIAL_EMPLOYED], expected_totals[1]
        )

    def test_add_catagorisation_column_adds_known_when_data_is_in_ascwds_or_capacity_tracker_or_pir(
        self,
    ):
        diagnostics_prepared_rows = [
            (
                "location_1",
                40.0,
                40.0,
                care_home_with_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                None,
                None,
                40.0,
            ),
            (
                "location_3",
                40.0,
                40.0,
                care_home_without_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_4",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_5",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                50.0,
                None,
            ),
            (
                "location_6",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                50.0,
                None,
            ),
            (
                "location_7",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_8",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                None,
                None,
                40.0,
            ),
            (
                "location_9",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                40.0,
            ),
        ]

        diagnostics_prepared_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        output_df = job.add_categorisation_column(diagnostics_prepared_df)

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(output_df_list[0][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[1][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[2][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[3][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[4][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[5][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[6][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[7][RESIDUAL_CATEGORY], known)
        self.assertEqual(output_df_list[8][RESIDUAL_CATEGORY], known)

    def test_add_catagorisation_column_adds_unknown_when_no_comparison_data_is_available(
        self,
    ):
        diagnostics_prepared_rows = [
            (
                "location_1",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                None,
                None,
            ),
        ]

        diagnostics_prepared_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        output_df = job.add_categorisation_column(diagnostics_prepared_df)

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertEqual(output_df_list[0][RESIDUAL_CATEGORY], unknown)

    def test_add_catagorisation_column_leaves_no_rows_blank(
        self,
    ):
        diagnostics_prepared_rows = [
            (
                "location_1",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                None,
                None,
            ),
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                None,
                None,
                40.0,
            ),
            (
                "location_3",
                40.0,
                40.0,
                care_home_without_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_4",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_5",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                50.0,
                None,
            ),
            (
                "location_6",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                50.0,
                None,
            ),
            (
                "location_7",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_8",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                None,
                None,
                40.0,
            ),
            (
                "location_9",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                40.0,
            ),
            (
                "location_10",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                None,
                None,
            ),
        ]

        diagnostics_prepared_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        output_df = job.add_categorisation_column(diagnostics_prepared_df)

        output_df_list = output_df.sort(LOCATION_ID).collect()

        self.assertIsNotNone(output_df_list[0][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[1][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[2][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[3][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[4][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[5][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[6][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[7][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[8][RESIDUAL_CATEGORY])
        self.assertIsNotNone(output_df_list[9][RESIDUAL_CATEGORY])

    def test_calculate_residuals_adds_a_column(self):
        diagnostics_prepared_rows = [
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                None,
                None,
                40.0,
            ),
            (
                "location_3",
                40.0,
                40.0,
                care_home_without_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_4",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_5",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                50.0,
                None,
            ),
        ]

        known_values_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        output_df = job.calculate_residuals(
            known_values_df,
            model=ESTIMATE_JOB_COUNT,
            service=non_residential,
            data_source_column=PEOPLE_DIRECTLY_EMPLOYED,
        )

        output_df_size = len(output_df.columns)

        expected_df_size = len(known_values_df.columns) + 1
        self.assertEqual(output_df_size, expected_df_size)

    def test_calculate_residuals_adds_residual_value(self):
        diagnostics_prepared_rows = [
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                40,
                None,
                40.0,
            ),
            (
                "location_3",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_4",
                None,
                None,
                non_residential,
                60.9,
                23.4,
                None,
                None,
                None,
                60.0,
                45,
                None,
                None,
            ),
            (
                "location_5",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                50.0,
                None,
            ),
        ]

        known_values_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        output_df = job.calculate_residuals(
            known_values_df,
            model=ESTIMATE_JOB_COUNT,
            service=non_residential,
            data_source_column=PEOPLE_DIRECTLY_EMPLOYED,
        )

        output_df_list = output_df.sort(LOCATION_ID).collect()
        expected_values = [
            0.0,
            -5.0,
            15.0,
            None,
        ]
        new_column_name = "residuals_estimate_job_count_non_res_pir"

        self.assertEqual(output_df_list[0][new_column_name], expected_values[0])
        self.assertEqual(output_df_list[1][new_column_name], expected_values[1])
        self.assertEqual(output_df_list[2][new_column_name], expected_values[2])
        self.assertEqual(output_df_list[3][new_column_name], expected_values[3])

    def test_create_residuals_column_name(
        self,
    ):
        model = ESTIMATE_JOB_COUNT
        service = care_home_with_nursing
        data_source_column = PEOPLE_DIRECTLY_EMPLOYED

        output = job.create_residuals_column_name(model, service, data_source_column)
        expected_output = "residuals_estimate_job_count_care_home_pir"

        self.assertEqual(output, expected_output)


    def test_run_residuals_creates_additional_columns(
        self,
    ):
        
        diagnostics_prepared_rows = [
            (
                "location_1",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                None,
                None,
            ),
            (
                "location_2",
                40.0,
                40.0,
                non_residential,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                None,
                None,
                40.0,
            ),
            (
                "location_3",
                40.0,
                40.0,
                care_home_without_nursing,
                60.9,
                23.4,
                45.1,
                None,
                None,
                40.0,
                45,
                41.0,
                None,
            ),
            (
                "location_4",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_5",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                50.0,
                None,
            ),
            (
                "location_6",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                45,
                50.0,
                None,
            ),
            (
                "location_7",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                None,
            ),
            (
                "location_8",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                None,
                None,
                40.0,
            ),
            (
                "location_9",
                None,
                None,
                non_residential,
                60.9,
                None,
                None,
                None,
                40.0,
                60.9,
                45,
                None,
                40.0,
            ),
            (
                "location_10",
                None,
                None,
                care_home_with_nursing,
                60.9,
                23.4,
                None,
                None,
                None,
                60.9,
                None,
                None,
                None,
            ),
        ]

        diagnostics_prepared_df = self.spark.createDataFrame(
            diagnostics_prepared_rows, schema=self.diagnostics_prepared_schema
        )

        
        residuals_list = job.create_residuals_list(ResidualsRequired.models,
                                                ResidualsRequired.services,
                                                ResidualsRequired.data_source_columns)

        output_df = job.run_residuals(diagnostics_prepared_df, residuals_list=residuals_list)
        output_df_size = len(output_df.columns)

        expected_df_size = len(diagnostics_prepared_df.columns)
        self.assertGreater(output_df_size, expected_df_size)


    def test_create_residuals_list_includes_all_permutations(self):
        models = [
            ESTIMATE_JOB_COUNT,
            JOB_COUNT,
        ]

        services = [
            care_home,
            non_res,
        ]

        data_source_columns = [
            JOB_COUNT_UNFILTERED,
            CARE_HOME_EMPLOYED,
        ]

        output = job.create_residuals_list(models, services, data_source_columns)
        expected_output = [
            [ESTIMATE_JOB_COUNT, care_home, JOB_COUNT_UNFILTERED],
            [ESTIMATE_JOB_COUNT, care_home, CARE_HOME_EMPLOYED],
            [ESTIMATE_JOB_COUNT, non_res, JOB_COUNT_UNFILTERED],
            [ESTIMATE_JOB_COUNT, non_res, CARE_HOME_EMPLOYED],
            [JOB_COUNT, care_home, JOB_COUNT_UNFILTERED],
            [JOB_COUNT, care_home, CARE_HOME_EMPLOYED],
            [JOB_COUNT, non_res, JOB_COUNT_UNFILTERED],
            [JOB_COUNT, non_res, CARE_HOME_EMPLOYED],
        ]
        self.assertEqual(output, expected_output)


    @unittest.skip("not written yet")
    def test_calculate_average_residual_adds_column_with_average_residual(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
