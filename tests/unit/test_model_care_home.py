import unittest
import warnings
from datetime import date

from utils.estimate_filled_posts.models.care_homes import (
    model_care_homes,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelCareHomes as Data
from tests.test_file_schemas import ModelCareHomes as Schemas
from utils import utils


class TestModelCareHome(unittest.TestCase):
    CAREHOME_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/"
    )

    def setUp(self):
        self.spark = utils.get_spark()
        self.care_homes_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.care_homes_cleaned_ind_cqc_rows,
            Schemas.care_homes_cleaned_ind_cqc_schema,
        )
        self.care_homes_features_df = self.spark.createDataFrame(
            Data.care_homes_features_rows, Schemas.care_homes_features_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_care_homes_returns_all_locations(self):
        cleaned_ind_cqc_df = self.care_homes_cleaned_ind_cqc_df
        features_df = self.care_homes_features_df

        df, _ = model_care_homes(
            cleaned_ind_cqc_df, features_df, f"{self.CAREHOME_MODEL}1.0.0"
        )

        self.assertEqual(df.count(), 5)

    def test_model_care_homes_estimates_jobs_for_care_homes_only(self):
        df, _ = model_care_homes(
            self.care_homes_cleaned_ind_cqc_df,
            self.care_homes_features_df,
            f"{self.CAREHOME_MODEL}1.0.0",
        )
        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df[IndCqc.location_id] == "1-000000002"
        ).collect()[0]

        self.assertIsNotNone(expected_location_with_prediction.estimate_filled_posts)
        self.assertIsNotNone(
            expected_location_with_prediction.estimate_filled_posts_source
        )
        self.assertIsNone(expected_location_without_prediction.estimate_filled_posts)
        self.assertIsNone(
            expected_location_without_prediction.estimate_filled_posts_source
        )
