import unittest
from unittest.mock import Mock, patch
import warnings

from pyspark.sql import DataFrame

import projects._03_independent_cqc._03_impute.utils.model_and_merge_pir_filled_posts as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelAndMergePirData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelAndMergePirData as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._03_impute.utils.model_and_merge_pir_filled_posts"
)


class ModelAndMergePirTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.NON_RES_PIR_MODEL = (
            "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/"
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)


class ThresholdValuesTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()

    def test_threshold_values_are_as_expected(
        self,
    ):
        self.assertEqual(job.ThresholdValues.max_absolute_difference, 100)
        self.assertEqual(job.ThresholdValues.max_percentage_difference, 0.5)
        self.assertEqual(job.ThresholdValues.months_in_two_years, 24)


class ModelPirFilledPostsTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()
        test_df = self.spark.createDataFrame(
            Data.model_pir_filled_posts_rows,
            Schemas.model_pir_filled_posts_schema,
        )
        self.expected_data = self.spark.createDataFrame(
            Data.expected_model_pir_filled_posts_rows,
            Schemas.expected_model_pir_filled_posts_schema,
        ).collect()
        self.returned_data = (
            job.model_pir_filled_posts(test_df, self.NON_RES_PIR_MODEL)
            .sort(IndCQC.location_id)
            .collect()
        )

    def test_model_pir_filled_posts_returns_correct_values(self):
        for i in range(len(self.expected_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCQC.pir_filled_posts_model],
                self.expected_data[i][IndCQC.pir_filled_posts_model],
                places=3,
            )


class MergeAscwdsAndPirFilledPostSubmissionsTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.blend_pir_and_ascwds_rows,
            Schemas.blend_pir_and_ascwds_schema,
        )

    @patch(f"{PATCH_PATH}.drop_temporary_columns")
    @patch(f"{PATCH_PATH}.create_ascwds_pir_merged_column")
    @patch(f"{PATCH_PATH}.create_last_submission_columns")
    @patch(f"{PATCH_PATH}.create_repeated_ascwds_clean_column")
    def test_merge_ascwds_and_pir_filled_post_submissions_calls_correct_functions(
        self,
        create_repeated_ascwds_clean_column_mock: Mock,
        create_last_submission_columns_mock: Mock,
        create_ascwds_pir_merged_column_mock: Mock,
        drop_temporary_columns_mock: Mock,
    ):
        job.merge_ascwds_and_pir_filled_post_submissions(self.test_df)

        create_repeated_ascwds_clean_column_mock.assert_called_once()
        create_last_submission_columns_mock.assert_called_once()
        create_ascwds_pir_merged_column_mock.assert_called_once()
        drop_temporary_columns_mock.assert_called_once()

    def test_merge_ascwds_and_pir_filled_post_submissions_completes(self):
        returned_df = job.merge_ascwds_and_pir_filled_post_submissions(self.test_df)
        self.assertIsInstance(returned_df, DataFrame)


class CreateRepeatedAscwdsCleanColumnTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()

    def test_create_repeated_ascwds_clean_column_returns_correct_values_when_missing_earlier_and_later_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows,
            Schemas.create_repeated_ascwds_clean_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows,
            Schemas.expected_create_repeated_ascwds_clean_column_schema,
        )
        returned_df = job.create_repeated_ascwds_clean_column(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    def test_create_repeated_ascwds_clean_column_returns_correct_values_when_missing_middle_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_repeated_ascwds_clean_column_when_missing_middle_data_rows,
            Schemas.create_repeated_ascwds_clean_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_repeated_ascwds_clean_column_when_missing_middle_data_rows,
            Schemas.expected_create_repeated_ascwds_clean_column_schema,
        )
        returned_df = job.create_repeated_ascwds_clean_column(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    def test_create_repeated_ascwds_clean_column_separates_repetition_by_location_id(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows,
            Schemas.create_repeated_ascwds_clean_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows,
            Schemas.expected_create_repeated_ascwds_clean_column_schema,
        )
        returned_df = job.create_repeated_ascwds_clean_column(test_df)
        self.assertEqual(
            returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
            expected_df.collect(),
        )


class CreateLastSubmissionColumnsTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()
        test_df = self.spark.createDataFrame(
            Data.create_last_submission_columns_rows,
            Schemas.create_last_submission_columns_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_last_submission_columns_rows,
            Schemas.expected_create_last_submission_columns_schema,
        )
        self.returned_df = job.create_last_submission_columns(test_df)

    def test_create_last_submission_columns_returns_correct_values(
        self,
    ):
        self.assertEqual(
            self.returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
            self.expected_df.collect(),
        )


class CreateAscwdsPirMergedColumnTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()

    def test_create_ascwds_pir_merged_column_blends_data_when_pir_more_than_two_years_after_asc_and_difference_greater_than_absolute_and_percentage_thresholds(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
            Schemas.create_ascwds_pir_merged_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
        )
        returned_df = job.create_ascwds_pir_merged_column(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    def test_create_ascwds_pir_merged_column_does_not_blend_data_when_pir_less_than_two_years_after_asc(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows,
            Schemas.create_ascwds_pir_merged_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
        )
        returned_df = job.create_ascwds_pir_merged_column(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    def test_create_ascwds_pir_merged_column_does_not_blend_data_when_asc_after_pir(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ascwds_pir_merged_column_when_asc_after_pir_rows,
            Schemas.create_ascwds_pir_merged_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ascwds_pir_merged_column_when_asc_after_pir_rows,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
        )
        returned_df = job.create_ascwds_pir_merged_column(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    def test_create_ascwds_pir_merged_column_does_not_blend_data_when_difference_less_than_absolute_threshold(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows,
            Schemas.create_ascwds_pir_merged_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
        )
        returned_df = job.create_ascwds_pir_merged_column(test_df)
        self.assertEqual(
            returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
            expected_df.collect(),
        )

    def test_create_ascwds_pir_merged_column_does_not_blend_data_when_difference_less_than_percentage_threshold(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows,
            Schemas.create_ascwds_pir_merged_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows,
            Schemas.expected_create_ascwds_pir_merged_column_schema,
        )
        returned_df = job.create_ascwds_pir_merged_column(test_df)
        self.assertEqual(
            returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
            expected_df.collect(),
        )


class DropTemporaryColumnsTests(ModelAndMergePirTests):
    def setUp(self):
        super().setUp()

        test_df = self.spark.createDataFrame(
            [],
            Schemas.drop_temporary_columns_schema,
        )
        self.expected_columns = Schemas.expected_drop_temporary_columns
        self.returned_columns = job.drop_temporary_columns(test_df).columns

    def test_drop_temporary_columns_removes_temporary_columns(
        self,
    ):
        self.assertEqual(self.returned_columns, self.expected_columns)
