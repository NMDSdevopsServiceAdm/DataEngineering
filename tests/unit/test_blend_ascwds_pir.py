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


# TODO create test suite for modelled column
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


# TODO create test suite for last submission columns
class CreateLastSubmissionColumnsTests(BlendAscwdsPirTests):
    def setUp(self):
        super().setUp()

    @unittest.skip("TODO")
    def test_create_last_submission_columns_returns_correct_values(
        self,
    ):
        pass


# TODO create test suite for adding pir modelled values into ascwds
class BlendModelledPirAndAscwds(BlendAscwdsPirTests):
    def setUp(self):
        super().setUp()

    @unittest.skip("TODO")
    def test_blend_modelled_pir_and_ascwds_returns_correct_values(
        self,
    ):
        pass
