import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from utils.estimate_job_count.models.care_homes import model_care_homes


class TestModelCareHome(unittest.TestCase):
    CAREHOME_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/"
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

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

    def generate_features_df(self):
        # fmt: off
        feature_columns = ["locationid", "primary_service_type", "job_count", "carehome", "ons_region",
                           "number_of_beds", "snapshot_date", "care_home_features", "non_residential_inc_pir_features",
                           "people_directly_employed", "snapshot_year", "snapshot_month", "snapshot_day"]

        feature_rows = [
            ("1-000000001", "Care home with nursing", 10, "Y", "South West", 67, "2022-03-29",
             Vectors.sparse(46, {0: 1.0, 1: 60.0, 3: 1.0, 32: 97.0, 33: 1.0}), None, 34, "2021", "05", "05"),
            ("1-000000002", "non-residential", 10, "N", "Merseyside", 12, "2022-03-29", None,
             Vectors.sparse(211, {0: 1.0, 1: 60.0, 3: 1.0, 32: 97.0, 33: 1.0}), 45, "2021", "05", "05"),
            ("1-000000003", "Care home with nursing", 20, "N", "Merseyside", 34, "2022-03-29", None, None, 0, "2021",
             "05", "05"),
            ("1-000000004", "non-residential", 10, "N", None, 0, "2022-03-29", None, None, None, "2021", "05", "05"),
        ]
        # fmt: on
        return self.spark.createDataFrame(
            feature_rows,
            schema=feature_columns,
        )

    def test_model_care_homes_returns_all_locations(self):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = model_care_homes(
            locations_df, features_df, f"{self.CAREHOME_MODEL}1.0.0"
        )

        self.assertEqual(df.count(), 5)

    def test_model_care_homes_estimates_jobs_for_care_homes_only(self):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = model_care_homes(
            locations_df, features_df, f"{self.CAREHOME_MODEL}1.0.0"
        )
        expected_location_with_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-03-29")
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df["locationid"] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(expected_location_with_prediction.estimate_job_count)
        self.assertIsNotNone(
            expected_location_with_prediction.estimate_job_count_source
        )
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)
        self.assertIsNone(
            expected_location_without_prediction.estimate_job_count_source
        )
