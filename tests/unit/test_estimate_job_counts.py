import unittest
import warnings
from datetime import datetime, date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from tests.test_file_generator import generate_prepared_locations_file_parquet
from tests.test_helpers import remove_file_path
from jobs import estimate_job_counts as job


class EstimateJobCountTests(unittest.TestCase):
    CAREHOME_WITH_HISTORICAL_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/"
    )
    NON_RES_WITH_PIR_MODEL = (
        "tests/test_models/non_residential_with_pir_jobs_prediction/"
    )
    METRICS_DESTINATION = "tests/test_data/tmp/data_engineering/model_metrics/"
    PREPARED_LOCATIONS_DIR = "tests/test_data/tmp/prepared_locations/"
    LOCATIONS_FEATURES_DIR = "tests/test_data/tmp/location_features/"
    DESTINATION = "tests/test_data/tmp/estimated_job_counts/"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        remove_file_path(self.PREPARED_LOCATIONS_DIR)
        remove_file_path(self.LOCATIONS_FEATURES_DIR)
        remove_file_path(self.DESTINATION)
        remove_file_path(self.METRICS_DESTINATION)

    @patch("utils.utils.get_s3_sub_folders_for_path")
    @patch("jobs.estimate_job_counts.date")
    def test_main_partitions_data_based_on_todays_date(
        self, mock_date, mock_get_s3_folders
    ):
        mock_get_s3_folders.return_value = ["1.0.0"]
        mock_date.today.return_value = date(2022, 6, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        generate_prepared_locations_file_parquet(self.PREPARED_LOCATIONS_DIR)
        features = self.generate_features_df()
        features.write.mode("overwrite").partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(self.LOCATIONS_FEATURES_DIR)

        job.main(
            self.PREPARED_LOCATIONS_DIR,
            self.LOCATIONS_FEATURES_DIR,
            self.DESTINATION,
            self.CAREHOME_WITH_HISTORICAL_MODEL,
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

    def test_determine_ascwds_primary_service_type(self):
        columns = ["locationid", "services_offered"]
        rows = [
            (
                "1-000000001",
                [
                    "Care home service with nursing",
                    "Care home service without nursing",
                    "Fake service",
                ],
            ),
            ("1-000000002", ["Care home service without nursing", "Fake service"]),
            ("1-000000003", ["Fake service"]),
            ("1-000000003", []),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.determine_ascwds_primary_service_type(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["primary_service_type"], "Care home with nursing")
        self.assertEqual(df[1]["primary_service_type"], "Care home without nursing")
        self.assertEqual(df[2]["primary_service_type"], "non-residential")
        self.assertEqual(df[3]["primary_service_type"], "non-residential")

    def test_populate_known_jobs_use_job_count_from_current_snapshot(self):
        columns = ["locationid", "job_count", "snapshot_date", "estimate_job_count"]
        rows = [
            ("1-000000001", 1, "2022-03-04", None),
            ("1-000000002", None, "2022-03-04", None),
            ("1-000000003", 5, "2022-03-04", 4),
            ("1-000000004", 10, "2022-03-04", None),
            ("1-000000002", 7, "2022-02-04", None),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.populate_estimate_jobs_when_job_count_known(df)
        self.assertEqual(df.count(), 5)

        df = df.collect()
        self.assertEqual(df[0]["estimate_job_count"], 1)
        self.assertEqual(df[1]["estimate_job_count"], None)
        self.assertEqual(df[2]["estimate_job_count"], 4)
        self.assertEqual(df[3]["estimate_job_count"], 10)

    def test_last_known_job_count_takes_job_count_from_current_snapshot(self):
        columns = ["locationid", "job_count", "snapshot_date"]
        rows = [
            ("1-000000001", 1, "2022-03-04"),
            ("1-000000002", None, "2022-03-04"),
            ("1-000000003", 5, "2022-03-04"),
            ("1-000000004", 10, "2022-03-04"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.populate_last_known_job_count(df)

        df = df.collect()
        self.assertEqual(df[0].last_known_job_count, 1)
        self.assertEqual(df[1].last_known_job_count, None)
        self.assertEqual(df[2].last_known_job_count, 5)
        self.assertEqual(df[3].last_known_job_count, 10)

    def test_last_known_job_count_takes_job_count_from_previous_snapshot(self):
        columns = ["locationid", "job_count", "snapshot_date"]
        rows = [
            ("1-000000001", None, "2022-03-04"),
            ("1-000000002", None, "2022-03-04"),
            ("1-000000003", 5, "2022-03-04"),
            ("1-000000004", 10, "2022-03-04"),
            ("1-000000001", 4, "2022-02-04"),
            ("1-000000002", None, "2022-02-04"),
            ("1-000000003", 5, "2022-02-04"),
            ("1-000000004", 12, "2022-02-04"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = (
            job.populate_last_known_job_count(df)
            .filter(df["snapshot_date"] == "2022-03-04")
            .collect()
        )

        self.assertEqual(df[0].last_known_job_count, 4)
        self.assertEqual(df[1].last_known_job_count, None)
        self.assertEqual(df[2].last_known_job_count, 5)
        self.assertEqual(df[3].last_known_job_count, 10)

    def test_last_known_job_count_takes_job_count_from_most_recent_snapshot(self):
        columns = ["locationid", "job_count", "snapshot_date"]
        rows = [
            ("1-000000001", None, "2022-03-04"),
            ("1-000000002", None, "2022-03-04"),
            ("1-000000001", None, "2022-02-04"),
            ("1-000000002", 5, "2022-02-04"),
            ("1-000000001", 4, "2021-03-04"),
            ("1-000000002", 7, "2021-02-04"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = (
            job.populate_last_known_job_count(df)
            .filter(df["snapshot_date"] == "2022-03-04")
            .collect()
        )

        self.assertEqual(df[0].last_known_job_count, 4)
        self.assertEqual(df[1].last_known_job_count, 5)

    def test_model_non_res_historical(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "estimate_job_count",
        ]
        rows = [
            ("1-000000001", "non-residential", 10, None),
            ("1-000000002", "Care home with nursing", 10, None),
            ("1-000000003", "non-residential", 20, None),
            ("1-000000004", "non-residential", 10, 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_non_res_historical(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_job_count"], 10.3)
        self.assertEqual(df[1]["estimate_job_count"], None)
        self.assertEqual(df[2]["estimate_job_count"], 20.6)
        self.assertEqual(df[3]["estimate_job_count"], 10)

    def test_model_non_res_default(self):
        columns = [
            "locationid",
            "primary_service_type",
            "estimate_job_count",
        ]
        rows = [
            ("1-000000001", "non-residential", None),
            ("1-000000002", "Care home with nursing", None),
            ("1-000000003", "non-residential", None),
            ("1-000000004", "non-residential", 10),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.model_non_res_default(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_job_count"], 54.09)
        self.assertEqual(df[1]["estimate_job_count"], None)
        self.assertEqual(df[2]["estimate_job_count"], 54.09)
        self.assertEqual(df[3]["estimate_job_count"], 10)

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
            "last_known_job_count",
            "estimate_job_count",
            "carehome",
            "ons_region",
            "number_of_beds",
            "snapshot_date"
        ]
        rows = [
            ("1-000000001", "Care home with nursing", 10, None, "Y", "South West", 67, "2022-03-29"),
            ("1-000000002", "Care home without nursing", 10, None, "N", "Merseyside", 12, "2022-03-29"),
            ("1-000000003", "Care home with nursing", 20, None, None, "Merseyside", 34, "2022-03-29"),
            ("1-000000004", "non-residential", 10, 10, "N", None, 0, "2022-03-29"),
            ("1-000000001", "non-residential", 10, None, "N", None, 0, "2022-02-20"),
        ]
        # fmt: on
        return self.spark.createDataFrame(rows, columns)

    def test_model_care_home_with_historical_returns_all_locations(self):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = job.model_care_home_with_historical(
            locations_df, features_df, f"{self.CAREHOME_WITH_HISTORICAL_MODEL}1.0.0"
        )

        self.assertEqual(df.count(), 5)

    def test_model_care_home_with_historical_estimates_jobs_for_care_homes_only(self):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = job.model_care_home_with_historical(
            locations_df, features_df, f"{self.CAREHOME_WITH_HISTORICAL_MODEL}1.0.0"
        )
        expected_location_with_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-03-29")
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df["locationid"] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(expected_location_with_prediction.estimate_job_count)
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)

    def test_model_non_residential_with_pir_estimates_jobs_for_non_res_with_pir_only(
        self,
    ):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = job.model_non_residential_with_pir(
            locations_df, features_df, f"{self.NON_RES_WITH_PIR_MODEL}1.0.0"
        )
        expected_location_without_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-03-29")
        ).collect()[0]
        expected_location_with_prediction = df.where(
            df["locationid"] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(expected_location_with_prediction.estimate_job_count)
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)

    def test_insert_predictions_into_locations_doesnt_remove_existing_estimates(self):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = job.insert_predictions_into_locations(locations_df, predictions_df)

        expected_location_with_prediction = df.where(
            df["locationid"] == "1-000000004"
        ).collect()[0]
        self.assertEqual(expected_location_with_prediction.estimate_job_count, 10)

    def test_insert_predictions_into_locations_does_so_when_locationid_matches(
        self,
    ):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = job.insert_predictions_into_locations(locations_df, predictions_df)

        expected_location_with_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-03-29")
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df["locationid"] == "1-000000003"
        ).collect()[0]
        self.assertEqual(expected_location_with_prediction.estimate_job_count, 56.89)
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)

    def test_insert_predictions_into_locations_only_inserts_for_matching_snapshots(
        self,
    ):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = job.insert_predictions_into_locations(locations_df, predictions_df)

        expected_location_without_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-02-20")
        ).collect()[0]
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)

    def test_insert_predictions_into_locations_removes_all_columns_from_predictions_df(
        self,
    ):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = job.insert_predictions_into_locations(locations_df, predictions_df)
        self.assertEqual(locations_df.columns, df.columns)

    def test_generate_r2_metric(self):
        df = self.generate_predictions_df()
        r2 = job.generate_r2_metric(df, "prediction", "job_count")

        self.assertAlmostEqual(r2, 0.93, places=2)

    def test_write_metrics_df_creates_metrics_df(self):
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
        df = self.spark.read.parquet(self.METRICS_DESTINATION)
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

        self.assertEqual(expected_columns, df.columns)
        self.assertAlmostEqual(df.first()["r2"], 0.99, places=2)
        self.assertEqual(df.first()["model_version"], "1.0.0")
        self.assertEqual(df.first()["model_name"], "care_home_jobs_prediction")
        self.assertEqual(df.first()["latest_snapshot"], "20220601")
        self.assertEqual(df.first()["job_name"], "estimate_job_counts")
        self.assertIsInstance(df.first()["generated_metric_date"], datetime)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
