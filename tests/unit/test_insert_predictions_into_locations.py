import unittest
import warnings
from datetime import date

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from tests.test_file_data import InsertPredictionsIntoLocations as Data
from tests.test_file_schemas import InsertPredictionsIntoLocations as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


class InsertPredictionsIntoLocationsTest(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.cleaned_cqc_ind_df = self.spark.createDataFrame(
            Data.cleaned_cqc_rows, Schemas.cleaned_cqc_schema
        )
        self.predictions_df = self.spark.createDataFrame(
            Data.predictions_rows, Schemas.predictions_schema
        )
        self.returned_df = insert_predictions_into_locations(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_insert_predictions_into_locations_adds_extra_column(self):
        self.assertTrue(IndCqc.care_home_model in self.returned_df.columns)

    def test_insert_predictions_into_locations_does_so_when_join_matches(self):
        df = self.returned_df

        expected_location_with_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]

        self.assertAlmostEqual(
            expected_location_with_prediction[IndCqc.care_home_model], 56.8899, 3
        )

    def test_insert_predictions_into_locations_returns_null_if_no_match(self):
        df = self.returned_df

        expected_location_without_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 2, 20))
        ).collect()[0]

        self.assertIsNone(
            expected_location_without_prediction[IndCqc.estimate_filled_posts]
        )
