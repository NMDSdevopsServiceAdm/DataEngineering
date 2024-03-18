import unittest
import warnings
from datetime import date

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from tests.test_file_data import InsertPredictionsIntoLocations as Data
from tests.test_file_schemas import InsertPredictionsIntoLocations as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


class TestModelNonResWithPir(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.cleaned_cqc_ind_df = self.spark.createDataFrame(
            Data.cleaned_cqc_rows, Schemas.cleaned_cqc_schema
        )
        self.care_home_features_df = self.spark.createDataFrame(
            Data.care_home_features_rows, Schemas.care_home_features_schema
        )
        self.predictions_df = self.spark.createDataFrame(
            Data.predictions_rows, Schemas.predictions_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_insert_predictions_into_locations_doesnt_remove_existing_estimates(self):
        df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        expected_location_with_prediction = df.where(
            df[IndCqc.location_id] == "1-000000004"
        ).collect()[0]
        self.assertEqual(expected_location_with_prediction.estimate_filled_posts, 10)

    def test_insert_predictions_into_locations_does_so_when_locationid_matches(
        self,
    ):
        df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df[IndCqc.location_id] == "1-000000003"
        ).collect()[0]
        self.assertAlmostEqual(
            expected_location_with_prediction.estimate_filled_posts, 56.889999389, 8
        )
        self.assertIsNone(expected_location_without_prediction.estimate_filled_posts)

    def test_insert_predictions_into_locations_only_inserts_for_matching_snapshots(
        self,
    ):
        df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        expected_location_without_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 2, 20))
        ).collect()[0]
        self.assertIsNone(expected_location_without_prediction.estimate_filled_posts)

    def test_insert_predictions_into_locations_adds_extra_column(self):
        df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )
        assert IndCqc.care_home_model in df.columns

    def test_insert_model_column_name_into_locations_does_so_when_locationid_matches(
        self,
    ):
        model_column_name = IndCqc.care_home_model

        df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df, self.predictions_df, model_column_name
        )

        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_without_prediction = df.where(
            df[IndCqc.location_id] == "1-000000003"
        ).collect()[0]
        self.assertAlmostEqual(
            expected_location_with_prediction.care_home_model, 56.889999389, 8
        )
        self.assertIsNone(expected_location_without_prediction.care_home_model)
