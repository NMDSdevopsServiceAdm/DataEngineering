import unittest
import warnings
from unittest.mock import ANY, Mock, patch
from datetime import datetime


import jobs.estimate_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateIndCQCFilledPostsData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    CARE_HOME_FEATURES = "care home features"
    NON_RES_FEATURES = "non res features"
    CARE_HOME_MODEL = "tests/test_models/care_home_with_nursing_historical_jobs_prediction/1.0.0/"
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

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(Data.cleaned_ind_cqc_rows, Schemas.cleaned_ind_cqc_schema)
        self.test_care_home_features_df = self.spark.createDataFrame(Data.care_home_features_rows, Schemas.features_schema)
        self.test_non_res_features_df = self.spark.createDataFrame(Data.non_res_features_rows, Schemas.features_schema)   
        self.care_home_model = utils.read_from_parquet(self.CARE_HOME_MODEL)
        self.non_res_model = utils.read_from_parquet(self.NON_RES_MODEL)
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
            self.care_home_model,
            self.non_res_model,
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

        write_to_parquet_patch.assert_called(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    @unittest.skip("not refactored test yet")
    @patch("utils.utils.get_s3_sub_folders_for_path")
    @patch("jobs.estimate_job_counts.date")
    def test_main_partitions_data_based_on_todays_date(
        self, mock_date, mock_get_s3_folders
    ):
        mock_get_s3_folders.return_value = ["1.0.0"]
        mock_date.today.return_value = date(2022, 6, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        generate_prepared_locations_file_parquet(self.PREPARED_LOCATIONS_CLEANED_DIR)
        features = self.generate_features_df()
        features.write.mode("overwrite").partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(self.LOCATIONS_FEATURES_DIR)

        job.main(
            self.PREPARED_LOCATIONS_CLEANED_DIR,
            self.LOCATIONS_FEATURES_DIR,
            self.LOCATIONS_FEATURES_DIR,
            self.DESTINATION,
            self.CAREHOME_MODEL,
            self.NON_RES_WITH_PIR_MODEL,
            self.METRICS_DESTINATION,
            job_run_id="abc1234",
            job_name="estimate_job_counts",
        )

        first_partitions = os.listdir(self.DESTINATION)
        year_partition = next(
            re.match("^run_year=([0-9]{4})$", path)
            for path in first_partitions
            if re.match("^run_year=([0-9]{4})$", path)
        )

        self.assertIsNotNone(year_partition)
        self.assertEqual(year_partition.groups()[0], "2022")

        second_partitions = os.listdir(f"{self.DESTINATION}/{year_partition.string}/")
        month_partition = next(
            re.match("^run_month=([0-9]{2})$", path) for path in second_partitions
        )
        self.assertIsNotNone(month_partition)
        self.assertEqual(month_partition.groups()[0], "06")

        third_partitions = os.listdir(
            f"{self.DESTINATION}/{year_partition.string}/{month_partition.string}/"
        )
        day_partition = next(
            re.match("^run_day=([0-9]{2})$", path) for path in third_partitions
        )
        self.assertIsNotNone(day_partition)
        self.assertEqual(day_partition.groups()[0], "29")

    
    def test_populate_known_jobs_use_job_count_from_current_date(self):
        test_df = self.spark.createDataFrame(Data.populate_known_jobs_rows,Schemas.populate_known_jobs_schema)

        returned_df = job.populate_estimate_jobs_when_job_count_known(test_df)
        expected_df = self.spark.createDataFrame(Data.expected_populate_known_jobs_rows, Schemas.populate_known_jobs_schema)
    
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        
        self.assertEqual(returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)


    def generate_features_df(self):
        # fmt: off
        feature_columns = ["locationid", "primary_service_type", "job_count", "carehome", "ons_region", "number_of_beds", "snapshot_date", "care_home_features", "non_residential_inc_pir_features", "people_directly_employed", "snapshot_year", "snapshot_month", "snapshot_day"]

        feature_rows = [
            ("1-000000001", "Care home with nursing", 10, "Y", "South West", 67, "2022-03-29", Vectors.sparse(46, {0: 1.0, 1: 60.0, 3: 1.0, 32: 97.0, 33: 1.0}), None, 34, "2021", "05", "05"),
            ("1-000000002", "non-residential", 10, "N", "Merseyside", 12, "2022-03-29", None, Vectors.sparse(211, {0: 1.0, 1: 60.0, 3: 1.0, 32: 97.0, 33: 1.0}), 45, "2021", "05", "05"),
            ("1-000000003", "Care home with nursing", 20, "N", "Merseyside", 34, "2022-03-29", None, None, 0, "2021", "05", "05"),
            ("1-000000004", "non-residential", 10, "N", None, 0, "2022-03-29", None, None, None, "2021", "05", "05"),
        ]
        # fmt: on
        return self.spark.createDataFrame(
            feature_rows,
            schema=feature_columns,
        )

    """
    def generate_predictions_df(self):
        # fmt: off
        columns = ["locationid", "primary_service_type", "job_count", "carehome", "ons_region", "number_of_beds", "snapshot_date", "prediction"]

        rows = [
            ("1-000000001", "Care home with nursing", 50, "Y", "South West", 67, "2022-03-29", 56.89),
            ("1-000000004", "non-residential", 10, "N", None, 0, "2022-03-29", 12.34),
        ]
        # fmt: on
        return self.spark.createDataFrame(
            rows,
            schema=columns,
        )

    def generate_locations_df(self):
        # fmt: off
        columns = [
            "locationid",
            "primary_service_type",
            "estimate_job_count",
            "estimate_job_count_source",
            "carehome",
            "ons_region",
            "number_of_beds",
            "snapshot_date"
        ]
        rows = [
            ("1-000000001", "Care home with nursing", None, None, "Y", "South West", 67, "2022-03-29"),
            ("1-000000002", "Care home without nursing", None, None, "N", "Merseyside", 12, "2022-03-29"),
            ("1-000000003", "Care home with nursing", None, None, None, "Merseyside", 34, "2022-03-29"),
            ("1-000000004", "non-residential", 10, "already_populated", "N", None, 0, "2022-03-29"),
            ("1-000000001", "non-residential", None, None, "N", None, 0, "2022-02-20"),
        ]
        # fmt: on
        return self.spark.createDataFrame(rows, columns)
    """
    @patch("utils.utils.write_to_parquet")
    def test_write_metrics_df_creates_metrics_df(self, write_to_parquet_mock: Mock):
        job.write_metrics_df(
            metrics_destination=self.METRICS_DESTINATION,
            r2=0.99,
            data_percentage=50.0,
            model_version="1.0.0",
            model_name="care_home_jobs_prediction",
            latest_snapshot="20220601",
            job_run_id="abc1234",
            job_name="estimate_job_counts",
        )
        df = write_to_parquet_mock.call_args[0][0]

        expected_columns = [
            "r2",
            "percentage_data",
            "latest_snapshot",
            "job_run_id",
            "job_name",
            "generated_metric_date",
            "model_name",
            "model_version",
        ]

        self.assertEqual(sorted(expected_columns), sorted(df.columns))
        self.assertAlmostEqual(df.first()["r2"], 0.99, places=2)
        self.assertEqual(df.first()["model_version"], "1.0.0")
        self.assertEqual(df.first()["model_name"], "care_home_jobs_prediction")
        self.assertEqual(df.first()["latest_snapshot"], "20220601")
        self.assertEqual(df.first()["job_name"], "estimate_job_counts")
        self.assertIsInstance(df.first()["generated_metric_date"], datetime)



    def test_number_of_days_constant_is_eighty_eight(self):
        self.assertEqual(job.NUMBER_OF_DAYS_IN_ROLLING_AVERAGE, 88)






if __name__ == "__main__":
    unittest.main(warnings="ignore")
