import warnings

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from utils.estimate_job_count.models.insert_predictions_into_locations import insert_predictions_into_locations
import unittest


class TestModelNonResWithPir(unittest.TestCase):

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
            "last_known_job_count",
            "estimate_job_count",
            "estimate_job_count_source",
            "carehome",
            "ons_region",
            "number_of_beds",
            "snapshot_date"
        ]
        rows = [
            ("1-000000001", "Care home with nursing", 10, None, None, "Y", "South West", 67, "2022-03-29"),
            ("1-000000002", "Care home without nursing", 10, None, None, "N", "Merseyside", 12, "2022-03-29"),
            ("1-000000003", "Care home with nursing", 20, None, None, None, "Merseyside", 34, "2022-03-29"),
            ("1-000000004", "non-residential", 10, 10, "already_populated", "N", None, 0, "2022-03-29"),
            ("1-000000001", "non-residential", 10, None, None, "N", None, 0, "2022-02-20"),
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


    def test_insert_predictions_into_locations_doesnt_remove_existing_estimates(self):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = insert_predictions_into_locations(locations_df, predictions_df)

        expected_location_with_prediction = df.where(
            df["locationid"] == "1-000000004"
        ).collect()[0]
        self.assertEqual(expected_location_with_prediction.estimate_job_count, 10)


    def test_insert_predictions_into_locations_does_so_when_locationid_matches(
            self,
    ):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = insert_predictions_into_locations(locations_df, predictions_df)

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

        df = insert_predictions_into_locations(locations_df, predictions_df)

        expected_location_without_prediction = df.where(
            (df["locationid"] == "1-000000001") & (df["snapshot_date"] == "2022-02-20")
        ).collect()[0]
        self.assertIsNone(expected_location_without_prediction.estimate_job_count)


    def test_insert_predictions_into_locations_removes_all_columns_from_predictions_df(
            self,
    ):
        locations_df = self.generate_locations_df()
        predictions_df = self.generate_predictions_df()

        df = insert_predictions_into_locations(locations_df, predictions_df)
        self.assertEqual(locations_df.columns, df.columns)
