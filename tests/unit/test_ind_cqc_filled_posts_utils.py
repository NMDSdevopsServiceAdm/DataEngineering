import unittest
import warnings

from tests.test_file_data import IndCQCDataUtils as Data
from tests.test_file_schemas import IndCQCDataUtils as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.utils as job


class TestPopulateEstimateFilledPostsAndSource(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class TestFilledPostsAndSourceAdded(TestPopulateEstimateFilledPostsAndSource):
    def setUp(self) -> None:
        super().setUp()
        self.input_df = self.spark.createDataFrame(
            Data.input_rows_for_adding_estimate_filled_posts_and_source,
            Schemas.input_schema_for_adding_estimate_filled_posts_and_source,
        )

        self.returned_df = job.populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
            self.input_df, ["model_name_1", "model_name_2", "model_name_3"]
        )

    def test_populate_estimate_filled_posts_and_source_adds_new_columns(self):
        assert IndCQC.estimate_filled_posts in self.returned_df.columns
        assert IndCQC.estimate_filled_posts_source in self.returned_df.columns
        self.assertEqual(len(self.returned_df.columns), len(self.input_df.columns) + 2)

    def test_populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            Data.expected_rows_with_estimate_filled_posts_and_source,
            Schemas.expected_schema_with_estimate_filled_posts_and_source,
        )

        returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(self.returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)


class TestSourceDescriptionAdded(TestPopulateEstimateFilledPostsAndSource):
    def setUp(self) -> None:
        super().setUp()

    def test_add_source_description_added_to_source_column_when_required(self):
        input_df = self.spark.createDataFrame(
            Data.source_missing_rows, Schemas.estimated_source_description_schema
        )

        returned_df = job.add_source_description_to_source_column(
            input_df,
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
            "model_name",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_source_added_rows, Schemas.estimated_source_description_schema
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)
