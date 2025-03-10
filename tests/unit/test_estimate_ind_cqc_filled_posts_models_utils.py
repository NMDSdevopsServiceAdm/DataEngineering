import unittest
import warnings
from datetime import date

from utils.estimate_filled_posts.models import utils as job
from tests.test_file_data import EstimateFilledPostsModelsUtils as Data
from tests.test_file_schemas import EstimateFilledPostsModelsUtils as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


class EstimateFilledPostsModelsUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class InsertPredictionsIntoPipelineTest(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.cleaned_cqc_ind_df = self.spark.createDataFrame(
            Data.cleaned_cqc_rows, Schemas.cleaned_cqc_schema
        )
        self.predictions_df = self.spark.createDataFrame(
            Data.predictions_rows, Schemas.predictions_schema
        )
        self.returned_df = job.insert_predictions_into_pipeline(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_insert_predictions_into_pipeline_adds_extra_column(self):
        self.assertTrue(IndCqc.care_home_model in self.returned_df.columns)

    def test_insert_predictions_into_pipeline_does_so_when_join_matches(self):
        df = self.returned_df

        expected_df = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]

        self.assertAlmostEqual(expected_df[IndCqc.care_home_model], 56.89, places=2)

    def test_insert_predictions_into_pipeline_returns_null_if_no_match(self):
        df = self.returned_df

        expected_df = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 2, 20))
        ).collect()[0]

        self.assertIsNone(expected_df[IndCqc.estimate_filled_posts])
