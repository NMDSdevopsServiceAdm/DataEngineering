import unittest
import warnings
from unittest.mock import ANY, Mock, patch
from datetime import datetime, date


import jobs.estimate_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateIndCQCFilledPostsData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCqc,
)


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    CARE_HOME_FEATURES = "care home features"
    NON_RES_FEATURES = "non res features"
    CARE_HOME_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/1.0.0/"
    )
    NON_RES_MODEL = "tests/test_models/non_residential_with_pir_jobs_prediction/1.0.0/"
    METRICS_DESTINATION = "metrics destination"
    ESTIMATES_DESTINATION = "estimates destination"
    JOB_RUN_ID = "job run id"
    JOB_NAME = "job name"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]
    partition_keys_for_metrics = [IndCqc.model_name, IndCqc.model_version]

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows, Schemas.cleaned_ind_cqc_schema
        )
        self.test_care_home_features_df = self.spark.createDataFrame(
            Data.care_home_features_rows, Schemas.features_schema
        )
        self.test_non_res_features_df = self.spark.createDataFrame(
            Data.non_res_features_rows, Schemas.features_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_ind_cqc_df,
            self.test_care_home_features_df,
            self.test_non_res_features_df,
            self.CARE_HOME_MODEL,
            self.NON_RES_MODEL,
            self.METRICS_DESTINATION,
            self.ESTIMATES_DESTINATION,
            self.JOB_RUN_ID,
            self.JOB_NAME,
        ]

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.CARE_HOME_FEATURES,
            self.NON_RES_FEATURES,
            self.CARE_HOME_MODEL,
            self.NON_RES_MODEL,
            self.METRICS_DESTINATION,
            self.ESTIMATES_DESTINATION,
            self.JOB_RUN_ID,
            self.JOB_NAME,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 3)

        # TODO add back in once saving metrics
        # self.assertEqual(write_to_parquet_patch.call_count, 3)

        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )
        # TODO add back in once saving metrics
        # write_to_parquet_patch.assert_any_call(
        #     ANY,
        #     self.METRICS_DESTINATION,
        #     mode="append",
        #     partitionKeys=self.partition_keys_for_metrics,
        # )

    def test_populate_known_jobs_use_filled_posts_from_current_date(self):
        test_df = self.spark.createDataFrame(
            Data.populate_known_jobs_rows, Schemas.populate_known_jobs_schema
        )

        returned_df = job.populate_estimate_jobs_when_filled_posts_known(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_populate_known_jobs_rows, Schemas.populate_known_jobs_schema
        )

        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)

    @patch("utils.utils.write_to_parquet")
    def test_write_metrics_df_creates_metrics_df(self, write_to_parquet_mock: Mock):
        job.write_metrics_df(
            metrics_destination=self.METRICS_DESTINATION,
            r2=0.99,
            data_percentage=50.0,
            model_version="1.0.0",
            model_name="care_home_jobs_prediction",
            latest_import_date="20220601",
            job_run_id="abc1234",
            job_name="estimate_filled_postss",
        )
        df = write_to_parquet_mock.call_args[0][0]

        expected_columns = [
            IndCqc.r2,
            IndCqc.percentage_data,
            IndCqc.latest_import_date,
            IndCqc.job_run_id,
            IndCqc.job_name,
            IndCqc.metrics_date,
            IndCqc.model_name,
            IndCqc.model_version,
        ]

        self.assertEqual(sorted(expected_columns), sorted(df.columns))
        self.assertAlmostEqual(df.first()["r2"], 0.99, places=2)
        self.assertEqual(df.first()[IndCqc.model_version], "1.0.0")
        self.assertEqual(df.first()[IndCqc.model_name], "care_home_jobs_prediction")
        self.assertEqual(df.first()[IndCqc.latest_import_date], "20220601")
        self.assertEqual(df.first()[IndCqc.job_name], "estimate_filled_postss")
        self.assertIsInstance(df.first()[IndCqc.metrics_date], datetime)

    def test_number_of_days_constant_is_eighty_eight(self):
        self.assertEqual(job.NUMBER_OF_DAYS_IN_ROLLING_AVERAGE, 88)

    def test_max_import_date_returns_correct_date(self):
        returned_date = job.get_max_import_date(
            self.test_cleaned_ind_cqc_df, IndCqc.cqc_location_import_date
        )
        expected_date = "20220422"

        self.assertEqual(expected_date, returned_date)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
