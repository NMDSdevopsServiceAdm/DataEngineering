from jobs import estimate_job_counts as job
import unittest


class test_model_care_homes_returns_all_locations(unittest.TestCase):
    CAREHOME_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/"
    )
    NON_RES_WITH_PIR_MODEL = (
        "tests/test_models/non_residential_with_pir_jobs_prediction/"
    )
    METRICS_DESTINATION = "tests/test_data/tmp/data_engineering/model_metrics/"
    PREPARED_LOCATIONS_DIR = "tests/test_data/tmp/prepared_locations/"
    LOCATIONS_FEATURES_DIR = "tests/test_data/tmp/location_features/"
    DESTINATION = "tests/test_data/tmp/estimated_job_counts/"

    locations_df = self.generate_locations_df()
    features_df = selgenerate_features_df()

    df, _ = job.model_care_homes(locations_df, features_df, f"{CAREHOME_MODEL}1.0.0")

    self.assertEqual(df.count(), 5)

    def test_model_care_homes_estimates_jobs_for_care_homes_only(self):
        locations_df = self.generate_locations_df()
        features_df = self.generate_features_df()

        df, _ = job.model_care_homes(
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
