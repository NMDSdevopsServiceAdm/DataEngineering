import unittest
import warnings
from datetime import date
from unittest.mock import ANY, Mock, patch

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DateType,
)

import utils.ind_cqc_filled_posts_utils.ascwds_pir_utils.blend_ascwds_pir as job

from tests.test_file_data import BlendAscwdsPirData as Data
from tests.test_file_schemas import BlendAscwdsPirData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


class BlendAscwdsPirTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.NON_RES_PIR_MODEL = (
            "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/"
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)


class BlendPirAndAscwdsWhenAscwdsOutOfDateTests(BlendAscwdsPirTests):
    def setUp(self):
        super().setUp()

    @unittest.skip("TODO")
    def test_blend_pir_and_ascwds_when_ascwds_out_of_date_returns_correct_values(
        self,
    ):
        pass


class CreateRepeatedAscwdsCleanColumnTests(BlendAscwdsPirTests):
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

    def test_create_repeated_ascwds_clean_column_returns_correct_values_when_missing_later_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_repeated_ascwds_clean_column_when_missing_later_data_rows,
            Schemas.create_repeated_ascwds_clean_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_repeated_ascwds_clean_column_when_missing_later_data_rows,
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

    def test_create_repeated_ascwds_clean_column_returns_correct_values_when_missing_earlier_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_repeated_ascwds_clean_column_when_missing_earlier_data_rows,
            Schemas.create_repeated_ascwds_clean_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_repeated_ascwds_clean_column_when_missing_earlier_data_rows,
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


class CreatePeopleDirectlyEmployedDedupModelledColumnTests(BlendAscwdsPirTests):
    def setUp(self):
        super().setUp()
        test_df = self.spark.createDataFrame(
            Data.create_people_directly_employed_dedup_modelled_column_rows,
            Schemas.create_people_directly_employed_dedup_modelled_column_schema,
        )
        self.expected_data = self.spark.createDataFrame(
            Data.expected_create_people_directly_employed_dedup_modelled_column_rows,
            Schemas.expected_create_people_directly_employed_dedup_modelled_column_schema,
        ).collect()
        self.returned_data = (
            job.create_people_directly_employed_dedup_modelled_column(
                test_df, self.NON_RES_PIR_MODEL
            )
            .sort(IndCQC.location_id)
            .collect()
        )

    def test_create_people_directly_employed_dedup_modelled_column_returns_correct_values(
        self,
    ):
        for i in range(len(self.expected_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCQC.people_directly_employed_filled_posts],
                self.expected_data[i][IndCQC.people_directly_employed_filled_posts],
                places=3,
            )


class CreateLastSubmissionColumnsTests(BlendAscwdsPirTests):
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


class BlendModelledPirAndAscwds(BlendAscwdsPirTests):
    def setUp(self):
        super().setUp()

    def test_merge_people_directly_employed_modelled_into_ascwds_clean_column_blends_data_when_pir_more_than_two_years_after_asc_and_difference_greater_than_absolute_and_percentage_thresholds(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_people_directly_employed_modelled_into_ascwds_clean_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
            Schemas.blend_modelled_pir_ands_ascwds_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_people_directly_employed_modelled_into_ascwds_clean_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows,
            Schemas.expected_blend_modelled_pir_ands_ascwds_schema,
        )
        returned_df = (
            job.merge_people_directly_employed_modelled_into_ascwds_clean_column(
                test_df
            )
        )
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    @unittest.skip("TODO")
    def test_merge_people_directly_employed_modelled_into_ascwds_clean_column_does_not_blend_data_when_pir_less_than_two_years_after_asc(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_people_directly_employed_modelled_into_ascwds_clean_column_when_pir_less_than_two_years_after_asc_rows,
            Schemas.blend_modelled_pir_ands_ascwds_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_people_directly_employed_modelled_into_ascwds_clean_column_when_pir_less_than_two_years_after_asc_rows,
            Schemas.expected_blend_modelled_pir_ands_ascwds_schema,
        )
        returned_df = (
            job.merge_people_directly_employed_modelled_into_ascwds_clean_column(
                test_df
            )
        )
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    @unittest.skip("TODO")
    def test_merge_people_directly_employed_modelled_into_ascwds_clean_column_does_not_blend_data_when_asc_after_pir(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_people_directly_employed_modelled_into_ascwds_clean_column_when_asc_after_pir_rows,
            Schemas.blend_modelled_pir_ands_ascwds_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_people_directly_employed_modelled_into_ascwds_clean_column_when_asc_after_pir_rows,
            Schemas.expected_blend_modelled_pir_ands_ascwds_schema,
        )
        returned_df = (
            job.merge_people_directly_employed_modelled_into_ascwds_clean_column(
                test_df
            )
        )
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    @unittest.skip("TODO")
    def test_merge_people_directly_employed_modelled_into_ascwds_clean_column_does_not_blend_data_when_difference_less_than_absolute_threshold(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_people_directly_employed_modelled_into_ascwds_clean_column_when_difference_less_than_absolute_threshold_rows,
            Schemas.blend_modelled_pir_ands_ascwds_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_people_directly_employed_modelled_into_ascwds_clean_column_when_difference_less_than_absolute_threshold_rows,
            Schemas.expected_blend_modelled_pir_ands_ascwds_schema,
        )
        returned_df = (
            job.merge_people_directly_employed_modelled_into_ascwds_clean_column(
                test_df
            )
        )
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )

    @unittest.skip("TODO")
    def test_merge_people_directly_employed_modelled_into_ascwds_clean_column_does_not_blend_data_when_difference_less_than_percentage_threshold(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_people_directly_employed_modelled_into_ascwds_clean_column_when_difference_less_than_percentage_threshold_rows,
            Schemas.blend_modelled_pir_ands_ascwds_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_people_directly_employed_modelled_into_ascwds_clean_column_when_difference_less_than_percentage_threshold_rows,
            Schemas.expected_blend_modelled_pir_ands_ascwds_schema,
        )
        returned_df = (
            job.merge_people_directly_employed_modelled_into_ascwds_clean_column(
                test_df
            )
        )
        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )
