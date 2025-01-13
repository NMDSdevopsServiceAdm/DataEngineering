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
        warnings.filterwarnings("ignore", category=ResourceWarning)


# TODO create test suite for script


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
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )


# TODO create test suite for modelled column
# TODO create test suite for last submission columns
# TODO create test suite for adding pir modelled values into ascwds
