import unittest
import warnings

import utils.estimate_filled_posts.models.primary_service_rolling_average as job
from tests.test_file_data import ModelPrimaryServiceRollingAverage as Data
from tests.test_file_schemas import ModelPrimaryServiceRollingAverage as Schemas
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils import utils


class TestModelPrimaryServiceRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.estimates_df = self.spark.createDataFrame(
            Data.input_rows, Schemas.input_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class RollingAverageModelTests(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.returned_df = job.model_primary_service_rolling_average(
            self.estimates_df, 88
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows, Schemas.expected_rolling_average_schema
        )
        self.returned_row_object = (
            self.returned_df.select(
                IndCqc.location_id,
                IndCqc.cqc_location_import_date,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.primary_service_type,
                IndCqc.rolling_average_model,
            )
            .sort(IndCqc.location_id)
            .collect()
        )
        self.expected_row_object = self.expected_df.collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.estimates_df.columns) + 1, len(self.returned_df.columns)
        )
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_average_calculates_correctly_when_deduplicated_data_exists_on_all_rows_for_an_import_date(
        self,
    ):
        self.assertEqual(
            self.returned_row_object[0][IndCqc.rolling_average_model],
            self.expected_row_object[0][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[5][IndCqc.rolling_average_model],
            self.expected_row_object[5][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[10][IndCqc.rolling_average_model],
            self.expected_row_object[10][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[15][IndCqc.rolling_average_model],
            self.expected_row_object[15][IndCqc.rolling_average_model],
        )

    def test_average_calculates_correctly_when_deduplicated_data_exists_on_some_rows_for_an_import_date(
        self,
    ):
        self.assertEqual(
            self.returned_row_object[2][IndCqc.rolling_average_model],
            self.expected_row_object[2][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[3][IndCqc.rolling_average_model],
            self.expected_row_object[3][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[4][IndCqc.rolling_average_model],
            self.expected_row_object[4][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[6][IndCqc.rolling_average_model],
            self.expected_row_object[6][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[7][IndCqc.rolling_average_model],
            self.expected_row_object[7][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[8][IndCqc.rolling_average_model],
            self.expected_row_object[8][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[12][IndCqc.rolling_average_model],
            self.expected_row_object[12][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[13][IndCqc.rolling_average_model],
            self.expected_row_object[13][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[14][IndCqc.rolling_average_model],
            self.expected_row_object[14][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[16][IndCqc.rolling_average_model],
            self.expected_row_object[16][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[17][IndCqc.rolling_average_model],
            self.expected_row_object[17][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[18][IndCqc.rolling_average_model],
            self.expected_row_object[18][IndCqc.rolling_average_model],
        )

    def test_average_calculates_correctly_when_deduplicated_data_does_not_exists_on_any_rows_for_an_import_date(
        self,
    ):
        self.assertEqual(
            self.returned_row_object[9][IndCqc.rolling_average_model],
            self.expected_row_object[9][IndCqc.rolling_average_model],
        )
        self.assertEqual(
            self.returned_row_object[19][IndCqc.rolling_average_model],
            self.expected_row_object[19][IndCqc.rolling_average_model],
        )


class CreateRollingAverageColumn(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.calculate_rolling_average_column_rows,
            Schemas.calculate_rolling_average_column_schema,
        )
        self.returned_df = job.create_rolling_average_column(self.test_df, 88)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rolling_average_column_rows,
            Schemas.expected_calculate_rolling_average_column_schema,
        )
        self.returned_df.show()

    def test_create_rolling_average_column_does_not_add_any_rows(self):

        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_create_rolling_average_column_returns_correct_values(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class CalculateRollingSum(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.rolling_sum_df = self.spark.createDataFrame(
            Data.rolling_sum_rows, Schemas.rolling_sum_schema
        )

    def test_calculate_rolling_sum(self):
        df = job.calculate_rolling_sum(
            self.rolling_sum_df, "col_to_sum", 3, "rolling_total"
        )
        self.assertEqual(df.count(), 7)
        df = df.collect()
        self.assertEqual(df[0]["rolling_total"], 10.0)
        self.assertEqual(df[3]["rolling_total"], 54.0)
        self.assertEqual(df[4]["rolling_total"], 64.0)
        self.assertEqual(df[6]["rolling_total"], 21.0)


class AddFlagIfIncludedInCount(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.add_flag_rows, Schemas.add_flag_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_flag_rows, Schemas.expected_add_flag_schema
        )
        self.returned_df = job.add_flag_if_included_in_count(self.test_df)

    def test_add_flag_if_included_in_count(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())
