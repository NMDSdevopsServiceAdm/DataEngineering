import unittest
import warnings


import utils.estimate_filled_posts.models.extrapolation as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from tests.test_file_data import ModelExtrapolation as Data
from tests.test_file_schemas import ModelExtrapolation as Schemas


class TestModelExtrapolation(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.extrapolation_df = self.spark.createDataFrame(
            Data.extrapolation_rows, Schemas.extrapolation_schema
        )
        self.data_to_filter_df = self.spark.createDataFrame(
            Data.data_to_filter_rows, Schemas.data_to_filter_schema
        )
        self.data_for_first_and_last_submissions_df = self.spark.createDataFrame(
            Data.first_and_last_submission_rows,
            Schemas.first_and_last_submission_schema,
        )
        self.data_for_extrapolated_values_df = self.spark.createDataFrame(
            Data.extrapolated_values_rows, Schemas.extrapolated_values_schema
        )
        self.data_for_extrapolated_values_to_be_added_into_df = (
            self.spark.createDataFrame(
                Data.extrapolated_values_to_be_added_rows,
                Schemas.extrapolated_values_to_be_added_schema,
            )
        )
        self.data_for_extrapolated_ratios_df = self.spark.createDataFrame(
            Data.extrapolated_ratios_rows, Schemas.extrapolated_ratios_schema
        )
        self.data_for_extrapolated_model_outputs_df = self.spark.createDataFrame(
            Data.extrapolated_model_outputs_rows,
            Schemas.extrapolated_model_outputs_schema,
        )
        self.extrapolation_model_column_name = "extrapolation_rolling_average_model"
        self.model_column_name = IndCqc.rolling_average_model

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_extrapolation_row_count_unchanged(self):
        output_df = job.model_extrapolation(
            self.extrapolation_df, self.model_column_name
        )
        self.assertEqual(output_df.count(), self.extrapolation_df.count())

        self.assertEqual(
            output_df.columns,
            [
                IndCqc.location_id,
                IndCqc.cqc_location_import_date,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.primary_service_type,
                self.model_column_name,
                self.extrapolation_model_column_name,
            ],
        )

    def test_model_extrapolation_outputted_values_correct(self):
        df = job.model_extrapolation(self.extrapolation_df, self.model_column_name)
        df = df.sort(IndCqc.location_id, IndCqc.cqc_location_import_date).collect()

        self.assertEqual(df[1][self.extrapolation_model_column_name], None)

        self.assertAlmostEqual(
            df[4][self.extrapolation_model_column_name],
            4.0159045,
            places=5,
        )
        self.assertAlmostEqual(
            df[6][self.extrapolation_model_column_name],
            3.9840954,
            places=5,
        )

        self.assertAlmostEqual(
            df[7][self.extrapolation_model_column_name],
            19.920792,
            places=5,
        )
        self.assertAlmostEqual(
            df[9][self.extrapolation_model_column_name],
            20.079207,
            places=5,
        )

        self.assertEqual(df[10][self.extrapolation_model_column_name], None)

    def test_filter_to_locations_who_have_a_filled_posts_at_some_point(self):
        output_df = job.filter_to_locations_who_have_a_filled_posts_at_some_point(
            self.data_to_filter_df
        )

        self.assertEqual(output_df.count(), 3)
        self.assertEqual(
            output_df.columns,
            [
                IndCqc.location_id,
                IndCqc.max_filled_posts,
                IndCqc.cqc_location_import_date,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.primary_service_type,
            ],
        )

        output_df = output_df.sort(
            IndCqc.location_id, IndCqc.cqc_location_import_date
        ).collect()
        self.assertEqual(output_df[0][IndCqc.location_id], "1-000000001")
        self.assertEqual(output_df[0][IndCqc.max_filled_posts], 15.0)
        self.assertEqual(output_df[1][IndCqc.location_id], "1-000000003")
        self.assertEqual(output_df[1][IndCqc.max_filled_posts], 20.0)
        self.assertEqual(output_df[2][IndCqc.location_id], "1-000000003")
        self.assertEqual(output_df[2][IndCqc.max_filled_posts], 20.0)

    def test_add_first_and_last_submission_date_cols(self):
        output_df = job.add_first_and_last_submission_date_cols(
            self.data_for_first_and_last_submissions_df
        )

        self.assertEqual(output_df.count(), 6)

        output_df = output_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        self.assertEqual(output_df[0][IndCqc.first_submission_time], 1675209600)
        self.assertEqual(output_df[0][IndCqc.last_submission_time], 1675209600)
        self.assertEqual(output_df[2][IndCqc.first_submission_time], 1675209600)
        self.assertEqual(output_df[2][IndCqc.last_submission_time], 1675209600)
        self.assertEqual(output_df[3][IndCqc.first_submission_time], 1672531200)
        self.assertEqual(output_df[3][IndCqc.last_submission_time], 1675209600)
        self.assertEqual(output_df[5][IndCqc.first_submission_time], 1672531200)
        self.assertEqual(output_df[5][IndCqc.last_submission_time], 1675209600)

    def test_add_filled_posts_and_model_value_for_first_and_last_submission(
        self,
    ):
        output_df = job.add_filled_posts_and_model_value_for_first_and_last_submission(
            self.data_for_first_and_last_submissions_df,
            self.model_column_name,
        )

        self.assertEqual(output_df.count(), 6)

        output_df = output_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        self.assertEqual(output_df[1][IndCqc.first_filled_posts], 5.0)
        self.assertEqual(output_df[1][IndCqc.first_rolling_average], 15.0)
        self.assertEqual(output_df[1][IndCqc.last_filled_posts], 5.0)
        self.assertEqual(output_df[1][IndCqc.last_rolling_average], 15.0)

        self.assertEqual(output_df[4][IndCqc.first_filled_posts], 4.0)
        self.assertEqual(output_df[4][IndCqc.first_rolling_average], 12.0)
        self.assertEqual(output_df[4][IndCqc.last_filled_posts], 6.0)
        self.assertEqual(output_df[4][IndCqc.last_rolling_average], 15.0)

    def test_create_extrapolation_ratio_column(self):
        output_df = job.create_extrapolation_ratio_column(
            self.data_for_extrapolated_ratios_df,
            self.model_column_name,
        )

        self.assertEqual(output_df.count(), 3)

        output_df = output_df.sort(IndCqc.location_id).collect()
        self.assertEqual(output_df[0][IndCqc.extrapolation_ratio], 0.5)
        self.assertEqual(output_df[1][IndCqc.extrapolation_ratio], 1.0)
        self.assertAlmostEqual(
            output_df[2][IndCqc.extrapolation_ratio], 1.17647059, places=5
        )

    def test_create_extrapolation_model_column(self):
        output_df = job.create_extrapolation_model_column(
            self.data_for_extrapolated_model_outputs_df,
            self.model_column_name,
        )

        self.assertEqual(output_df.count(), 3)
        self.assertEqual(
            output_df.columns,
            [
                IndCqc.location_id,
                IndCqc.cqc_location_import_date,
                self.extrapolation_model_column_name,
            ],
        )

        output_df = output_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        self.assertEqual(output_df[0][self.extrapolation_model_column_name], 7.5)
        self.assertEqual(output_df[1][self.extrapolation_model_column_name], 15.0)
        self.assertAlmostEqual(
            output_df[2][self.extrapolation_model_column_name], 22.0323678, places=5
        )

    def test_add_extrapolated_values(self):
        output_df = job.add_extrapolated_values(
            self.data_for_extrapolated_values_to_be_added_into_df,
            self.data_for_extrapolated_values_df,
            self.model_column_name,
        )

        self.assertEqual(output_df.count(), 11)

        output_df = output_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        self.assertEqual(output_df[0][self.extrapolation_model_column_name], None)
        self.assertEqual(output_df[1][self.extrapolation_model_column_name], None)
        self.assertEqual(output_df[4][self.extrapolation_model_column_name], 60.0)
        self.assertEqual(output_df[5][self.extrapolation_model_column_name], 20.0)
        self.assertAlmostEqual(
            output_df[6][self.extrapolation_model_column_name], 11.7647058, places=5
        )
        self.assertAlmostEqual(
            output_df[8][self.extrapolation_model_column_name], 23.5294117, places=5
        )
        self.assertEqual(output_df[10][self.extrapolation_model_column_name], None)
