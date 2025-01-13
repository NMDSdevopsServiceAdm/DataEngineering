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

import jobs.clean_ind_cqc_filled_posts as job

from tests.test_file_data import CleanIndCQCData as Data
from tests.test_file_schemas import CleanIndCQCData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


class CleanIndFilledPostsTests(unittest.TestCase):
    MERGE_IND_CQC_SOURCE = "input_dir"
    CLEANED_IND_CQC_DESTINATION = "output_dir"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.merge_ind_cqc_test_df = self.spark.createDataFrame(
            Data.merged_rows_for_cleaning_job,
            Schemas.merged_schema_for_cleaning_job,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)


class AddColumnWithAscwdsRepeatedTests(CleanIndFilledPostsTests):
    def setUp(self):
        super().setUp()
        test_rows = [
            ("loc 1", date(2024, 1, 1), None),
            ("loc 1", date(2024, 2, 1), 100),
            ("loc 1", date(2024, 3, 1), None),
            ("loc 2", date(2024, 1, 1), 50),
            ("loc 2", date(2024, 2, 1), None),
            ("loc 2", date(2024, 3, 1), None),
            ("loc 3", date(2024, 1, 1), 40),
            ("loc 3", date(2024, 2, 1), None),
            ("loc 3", date(2024, 3, 1), 60),
        ]
        test_schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(
                    IndCQC.ascwds_filled_posts_dedup_clean, IntegerType(), True
                ),
            ]
        )
        expected_rows = [
            ("loc 1", date(2024, 1, 1), None, None),
            ("loc 1", date(2024, 2, 1), 100, 100),
            ("loc 1", date(2024, 3, 1), None, 100),
            ("loc 2", date(2024, 1, 1), 50, 50),
            ("loc 2", date(2024, 2, 1), None, 50),
            ("loc 2", date(2024, 3, 1), None, 50),
            ("loc 3", date(2024, 1, 1), 40, 40),
            ("loc 3", date(2024, 2, 1), None, 40),
            ("loc 3", date(2024, 3, 1), 60, 60),
        ]
        expected_schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(
                    IndCQC.ascwds_filled_posts_dedup_clean, IntegerType(), True
                ),
                StructField("ascwds_clean_repeated", IntegerType(), True),
            ]
        )
        self.test_df = self.spark.createDataFrame(test_rows, test_schema)
        self.expected_df = self.spark.createDataFrame(expected_rows, expected_schema)
        self.returned_df = job.create_repeated_ascwds_clean_column(self.test_df)

    def test_function_returns_correct_values(self):
        self.assertEqual(
            self.returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
            self.expected_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )
