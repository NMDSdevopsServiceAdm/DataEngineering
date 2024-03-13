import unittest
import warnings
from datetime import date

from utils.estimate_filled_posts.models.non_res_with_pir import (
    model_non_residential_with_pir,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelNonResidential as Data
from tests.test_file_schemas import ModelNonResidential as Schemas
from utils import utils


class TestModelNonResWithPir(unittest.TestCase):
    NON_RES_WITH_PIR_MODEL = (
        "tests/test_models/non_residential_with_pir_jobs_prediction/"
    )

    def setUp(self):
        self.spark = utils.get_spark()
        self.cleaned_cqc_ind_df = self.spark.createDataFrame(
            Data.cleaned_cqc_ind_rows, Schemas.cleaned_cqc_ind_schema
        )
        self.non_res_features_df = self.spark.createDataFrame(
            Data.non_res_features_rows, Schemas.non_res_features_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_non_res_with_pir_returns_all_locations(self):
        cleaned_ind_cqc_df = self.cleaned_cqc_ind_df
        features_df = self.non_res_features_df

        df, _ = model_non_residential_with_pir(
            cleaned_ind_cqc_df, features_df, f"{self.NON_RES_WITH_PIR_MODEL}1.0.0"
        )

        self.assertEqual(df.count(), 5)

    def test_model_non_residential_with_pir_estimates_jobs_for_non_res_with_pir_only(
        self,
    ):
        df, _ = model_non_residential_with_pir(
            self.cleaned_cqc_ind_df,
            self.non_res_features_df,
            f"{self.NON_RES_WITH_PIR_MODEL}1.0.0",
        )
        expected_location_without_prediction = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]
        expected_location_with_prediction = df.where(
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
