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


class SetMinimumValueTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.set_min_value_when_below_minimum_rows,
            Schemas.set_min_value_schema,
        )

    def test_set_min_value_replaces_values_below_min_value(self):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, 2.0)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_min_value_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_replaces_value_with_the_default_when_below_min_value_and_value_not_set(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_minimum_and_default_not_set_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_replaces_value_with_the_greatest_value_when_both_are_negative(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, -5.0)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_minimum_and_min_value_is_negative_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_does_not_replace_value_when_min_value_is_none(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, None)

        self.assertEqual(returned_df.collect(), self.test_df.collect())

    def test_set_min_value_does_not_replace_predictions_above_minimum_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.set_min_value_when_above_minimum_rows,
            Schemas.set_min_value_schema,
        )
        returned_df = job.set_min_value(test_df, IndCqc.prediction, 1.0)
        expected_df = test_df

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_does_not_replace_null_predictions(self):
        test_df = self.spark.createDataFrame(
            Data.set_min_value_when_null_rows,
            Schemas.set_min_value_schema,
        )
        returned_df = job.set_min_value(test_df, IndCqc.prediction, 1.0)
        expected_df = test_df

        self.assertEqual(returned_df.collect(), expected_df.collect())


class ConvertCareHomeRatiosToFilledPostsAndMergeWithFilledPostValuesTests(
    EstimateFilledPostsModelsUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows,
            Schemas.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_schema,
        )
        self.returned_df = job.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
            self.test_df,
            IndCqc.banded_bed_ratio_rolling_average_model,
            IndCqc.posts_rolling_average_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows,
            Schemas.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_schema,
        )

    def test_returned_columns_match_original_data_columns(self):
        self.assertEqual(self.returned_df.columns, self.test_df.columns)

    def test_returned_column_values_match_expected_when_not_care_home(self):
        returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        expected_data = self.expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i],
                expected_data[i],
                f"Returned row {i} does not match expected",
            )
