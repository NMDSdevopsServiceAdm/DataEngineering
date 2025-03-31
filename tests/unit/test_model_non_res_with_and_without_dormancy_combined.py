import unittest
from unittest.mock import patch, Mock
import warnings

from utils import utils
import utils.estimate_filled_posts.models.non_res_with_and_without_dormancy_combined as job
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelNonResWithAndWithoutDormancyCombinedRows as Data
from tests.test_file_schemas import (
    ModelNonResWithAndWithoutDormancyCombinedSchemas as Schemas,
)


class ModelNonResWithAndWithoutDormancyCombinedTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_combine_non_res_with_and_without_dormancy_models_runs(self):
        test_df = self.spark.createDataFrame(
            Data.estimated_posts_rows, Schemas.estimated_posts_schema
        )

        returned_df = job.combine_non_res_with_and_without_dormancy_models(test_df)

        pass


# calculate_and_apply_model_ratios
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()


class AverageModelsByRelatedLocationAndTimeRegisteredTests(
    ModelNonResWithAndWithoutDormancyCombinedTests
):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.average_models_by_related_location_and_time_registered_rows,
            Schemas.average_models_by_related_location_and_time_registered_schema,
        )
        self.returned_df = job.average_models_by_related_location_and_time_registered(
            test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_average_models_by_related_location_and_time_registered_rows,
            Schemas.expected_average_models_by_related_location_and_time_registered_schema,
        )

    def test_name(self):
        pass


# calculate_adjustment_ratios
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# apply_model_ratios
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# get_first_overlap_date
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# calculate_residuals
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# apply_residuals
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# calculate_and_apply_residuals
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()

# combine_model_predictions
# class ...Tests(ModelNonResWithAndWithoutDormancyCombinedTests):
#     def setUp(self) -> None:
#         super().setUp()
