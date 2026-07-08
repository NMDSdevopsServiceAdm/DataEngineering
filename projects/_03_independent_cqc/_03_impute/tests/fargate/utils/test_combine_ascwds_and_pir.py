import warnings
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_impute.fargate.utils.combine_ascwds_and_pir as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CombineASCWDSAndPIRData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CombineASCWDSAndPIRSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._03_impute.fargate.utils.combine_ascwds_and_pir"
)


class TestCombineASCWDSAndPIR:
    test_lf = pl.LazyFrame(
        Data.blend_pir_and_ascwds_rows,
        Schemas.blend_pir_and_ascwds_schema,
        orient="row",
    )

    @patch(f"{PATCH_PATH}.drop_temporary_columns")
    @patch(f"{PATCH_PATH}.include_pir_if_never_submitted_ascwds")
    @patch(f"{PATCH_PATH}.create_ascwds_pir_merged_column")
    @patch(f"{PATCH_PATH}.create_last_submission_columns")
    @patch(f"{PATCH_PATH}.create_repeated_ascwds_clean_column")
    def test_merge_ascwds_and_pir_filled_post_submissions_calls_correct_functions(
        self,
        create_repeated_ascwds_clean_column_mock: Mock,
        create_last_submission_columns_mock: Mock,
        create_ascwds_pir_merged_column_mock: Mock,
        include_pir_if_never_submitted_ascwds_mock: Mock,
        drop_temporary_columns_mock: Mock,
    ):
        job.merge_ascwds_and_pir_filled_post_submissions(self.test_lf)

        create_repeated_ascwds_clean_column_mock.assert_called_once()
        create_last_submission_columns_mock.assert_called_once()
        create_ascwds_pir_merged_column_mock.assert_called_once()
        include_pir_if_never_submitted_ascwds_mock.assert_called_once()
        drop_temporary_columns_mock.assert_called_once()

    def test_merge_ascwds_and_pir_filled_post_submissions_completes(self):
        returned_lf = job.merge_ascwds_and_pir_filled_post_submissions(self.test_lf)
        assert type(returned_lf) == pl.LazyFrame


class TestCreateRepeatedAscwdsCleanColumn:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.create_repeated_ascwds_clean_column_test_cases
        ],
    )
    def test_create_repeated_ascwds_clean_column_returns_correct_values_when_missing_earlier_and_later_data(
        self,
        case,
    ):

        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.expected_create_repeated_ascwds_clean_column_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
        returned_lf = job.create_repeated_ascwds_clean_column(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


# class CreateLastSubmissionColumnsTests(ModelAndMergePirTests):
#     def setUp(self):
#         super().setUp()
#         test_df = self.spark.createDataFrame(
#             Data.create_last_submission_columns_rows,
#             Schemas.create_last_submission_columns_schema,
#         )
#         self.expected_df = self.spark.createDataFrame(
#             Data.expected_create_last_submission_columns_rows,
#             Schemas.expected_create_last_submission_columns_schema,
#         )
#         self.returned_df = job.create_last_submission_columns(test_df)

#     def test_create_last_submission_columns_returns_correct_values(
#         self,
#     ):
#         self.assertEqual(
#             self.returned_df.sort(
#                 IndCQC.location_id, IndCQC.cqc_location_import_date
#             ).collect(),
#             self.expected_df.collect(),
#         )


# class CreateAscwdsPirMergedColumnTests(ModelAndMergePirTests):
#     def setUp(self):
#         super().setUp()

#     def test_create_ascwds_pir_merged_column_blends_data_when_pir_more_than_two_years_after_asc_and_difference_greater_than_absolute_and_percentage_thresholds(
#         self,
#     ):
#         test_df = self.spark.createDataFrame(
#             Data.create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
#             Schemas.create_ascwds_pir_merged_column_schema,
#         )
#         expected_df = self.spark.createDataFrame(
#             Data.expected_create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
#             Schemas.expected_create_ascwds_pir_merged_column_schema,
#         )
#         returned_df = job.create_ascwds_pir_merged_column(test_df)
#         self.assertEqual(
#             returned_df.sort(IndCQC.cqc_location_import_date).collect(),
#             expected_df.collect(),
#         )

#     def test_create_ascwds_pir_merged_column_does_not_blend_data_when_pir_less_than_two_years_after_asc(
#         self,
#     ):
#         test_df = self.spark.createDataFrame(
#             Data.create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows,
#             Schemas.create_ascwds_pir_merged_column_schema,
#         )
#         expected_df = self.spark.createDataFrame(
#             Data.expected_create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows,
#             Schemas.expected_create_ascwds_pir_merged_column_schema,
#         )
#         returned_df = job.create_ascwds_pir_merged_column(test_df)
#         self.assertEqual(
#             returned_df.sort(IndCQC.cqc_location_import_date).collect(),
#             expected_df.collect(),
#         )

#     def test_create_ascwds_pir_merged_column_does_not_blend_data_when_asc_after_pir(
#         self,
#     ):
#         test_df = self.spark.createDataFrame(
#             Data.create_ascwds_pir_merged_column_when_asc_after_pir_rows,
#             Schemas.create_ascwds_pir_merged_column_schema,
#         )
#         expected_df = self.spark.createDataFrame(
#             Data.expected_create_ascwds_pir_merged_column_when_asc_after_pir_rows,
#             Schemas.expected_create_ascwds_pir_merged_column_schema,
#         )
#         returned_df = job.create_ascwds_pir_merged_column(test_df)
#         self.assertEqual(
#             returned_df.sort(IndCQC.cqc_location_import_date).collect(),
#             expected_df.collect(),
#         )

#     def test_create_ascwds_pir_merged_column_does_not_blend_data_when_difference_less_than_absolute_threshold(
#         self,
#     ):
#         test_df = self.spark.createDataFrame(
#             Data.create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows,
#             Schemas.create_ascwds_pir_merged_column_schema,
#         )
#         expected_df = self.spark.createDataFrame(
#             Data.expected_create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows,
#             Schemas.expected_create_ascwds_pir_merged_column_schema,
#         )
#         returned_df = job.create_ascwds_pir_merged_column(test_df)
#         self.assertEqual(
#             returned_df.sort(
#                 IndCQC.location_id, IndCQC.cqc_location_import_date
#             ).collect(),
#             expected_df.collect(),
#         )

#     def test_create_ascwds_pir_merged_column_does_not_blend_data_when_difference_less_than_percentage_threshold(
#         self,
#     ):
#         test_df = self.spark.createDataFrame(
#             Data.create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows,
#             Schemas.create_ascwds_pir_merged_column_schema,
#         )
#         expected_df = self.spark.createDataFrame(
#             Data.expected_create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows,
#             Schemas.expected_create_ascwds_pir_merged_column_schema,
#         )
#         returned_df = job.create_ascwds_pir_merged_column(test_df)
#         self.assertEqual(
#             returned_df.sort(
#                 IndCQC.location_id, IndCQC.cqc_location_import_date
#             ).collect(),
#             expected_df.collect(),
#         )


# class IncludePirIfNeverSubmittedAscwdsTests(ModelAndMergePirTests):
#     def setUp(self):
#         super().setUp()

#         self.test_df = self.spark.createDataFrame(
#             Data.include_pir_if_never_submitted_ascwds_rows,
#             Schemas.include_pir_if_never_submitted_ascwds_schema,
#         )
#         self.returned_df = job.include_pir_if_never_submitted_ascwds(self.test_df)

#         expected_df = self.spark.createDataFrame(
#             Data.expected_include_pir_if_never_submitted_ascwds_rows,
#             Schemas.include_pir_if_never_submitted_ascwds_schema,
#         )
#         self.returned_data = self.returned_df.sort(
#             IndCQC.location_id, IndCQC.cqc_location_import_date
#         ).collect()
#         self.expected_data = expected_df.collect()

#     def test_include_pir_if_never_submitted_ascwds_returns_original_columns(self):
#         self.assertEqual(self.returned_df.columns, self.test_df.columns)

#     def test_include_pir_if_never_submitted_ascwds_returns_expected_data(self):
#         self.assertEqual(self.returned_data, self.expected_data)


# class DropTemporaryColumnsTests(ModelAndMergePirTests):
#     def setUp(self):
#         super().setUp()

#         test_df = self.spark.createDataFrame(
#             [],
#             Schemas.drop_temporary_columns_schema,
#         )
#         self.expected_columns = Schemas.expected_drop_temporary_columns
#         self.returned_columns = job.drop_temporary_columns(test_df).columns

#     def test_drop_temporary_columns_removes_temporary_columns(
#         self,
#     ):
#         self.assertEqual(self.returned_columns, self.expected_columns)
