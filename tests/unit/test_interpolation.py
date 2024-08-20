import unittest
import warnings

import utils.estimate_filled_posts.models.interpolation as job
from utils import utils
from tests.test_file_data import ModelInterpolation as Data
from tests.test_file_schemas import ModelInterpolation as Schemas
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


class TestModelInterpolation(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.interpolation_df = self.spark.createDataFrame(
            Data.interpolation_rows, Schemas.interpolation_schema
        )
        self.data_for_calculating_submission_dates = self.spark.createDataFrame(
            Data.calculating_submission_dates_rows,
            Schemas.calculating_submission_dates_schema,
        )
        self.data_for_creating_timeseries_df = self.spark.createDataFrame(
            Data.creating_timeseries_rows, Schemas.creating_timeseries_schema
        )

        self.data_for_merging_exploded_df = self.spark.createDataFrame(
            Data.merging_exploded_data_rows, Schemas.merging_exploded_data_schema
        )

        self.data_for_merging_known_values_df = self.spark.createDataFrame(
            Data.merging_known_values_rows, Schemas.merging_known_values_schema
        )
        self.data_for_calculating_interpolated_values_df = self.spark.createDataFrame(
            Data.calculating_interpolated_values_rows,
            Schemas.calculating_interpolated_values_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_interpolation_row_count_unchanged(self):
        df = job.model_interpolation(
            self.interpolation_df, IndCqc.ascwds_filled_posts_dedup_clean
        )
        self.assertEqual(df.count(), self.interpolation_df.count())

        self.assertEqual(
            df.columns,
            [
                IndCqc.location_id,
                IndCqc.unix_time,
                IndCqc.cqc_location_import_date,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.interpolation_model,
            ],
        )

    def test_model_interpolation_outputted_values_correct(self):
        df = job.model_interpolation(
            self.interpolation_df, IndCqc.ascwds_filled_posts_dedup_clean
        )
        df = df.sort(IndCqc.location_id, IndCqc.unix_time).collect()

        self.assertEqual(df[0][IndCqc.interpolation_model], None)
        self.assertEqual(df[1][IndCqc.interpolation_model], 30.0)
        self.assertEqual(df[2][IndCqc.interpolation_model], None)

        self.assertEqual(df[5][IndCqc.interpolation_model], 4.5)
        self.assertEqual(df[8][IndCqc.interpolation_model], 10.0)
        self.assertEqual(df[9][IndCqc.interpolation_model], 15.0)

    def test_filter_to_locations_with_a_known_value(self):
        filtered_df = job.filter_to_locations_with_a_known_value(
            self.interpolation_df, IndCqc.ascwds_filled_posts_dedup_clean
        )

        self.assertEqual(filtered_df.count(), 5)
        self.assertEqual(
            filtered_df.columns,
            [
                IndCqc.location_id,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
            ],
        )

    def test_calculate_first_and_last_submission_date_per_location(self):
        output_df = job.calculate_first_and_last_submission_date_per_location(
            self.data_for_calculating_submission_dates
        )

        self.assertEqual(output_df.count(), 2)
        self.assertEqual(
            output_df.columns,
            [
                IndCqc.location_id,
                IndCqc.first_submission_time,
                IndCqc.last_submission_time,
            ],
        )

        output_df = output_df.sort(IndCqc.location_id).collect()
        self.assertEqual(output_df[0][IndCqc.first_submission_time], 1672617600)
        self.assertEqual(output_df[0][IndCqc.last_submission_time], 1672617600)
        self.assertEqual(output_df[1][IndCqc.first_submission_time], 1672704000)
        self.assertEqual(output_df[1][IndCqc.last_submission_time], 1673222400)

    def test_convert_first_and_last_known_years_into_exploded_df(self):
        df = job.convert_first_and_last_known_years_into_exploded_df(
            self.data_for_creating_timeseries_df
        )

        self.assertEqual(df.count(), 6)
        self.assertEqual(
            df.columns,
            [IndCqc.location_id, IndCqc.unix_time],
        )

    def test_merge_known_values_with_exploded_dates(self):
        output_df = job.merge_known_values_with_exploded_dates(
            self.data_for_merging_exploded_df,
            self.data_for_merging_known_values_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
        )

        self.assertEqual(output_df.count(), 5)
        self.assertEqual(
            output_df.columns,
            [
                IndCqc.location_id,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.value_unix_time,
            ],
        )

        output_df = output_df.sort(IndCqc.location_id, IndCqc.unix_time).collect()
        self.assertEqual(output_df[0][IndCqc.ascwds_filled_posts_dedup_clean], None)
        self.assertEqual(output_df[0][IndCqc.value_unix_time], None)
        self.assertEqual(output_df[1][IndCqc.ascwds_filled_posts_dedup_clean], 1.0)
        self.assertEqual(output_df[1][IndCqc.value_unix_time], 1672704000)
        self.assertEqual(output_df[2][IndCqc.ascwds_filled_posts_dedup_clean], None)
        self.assertEqual(output_df[2][IndCqc.value_unix_time], None)
        self.assertEqual(output_df[3][IndCqc.ascwds_filled_posts_dedup_clean], 2.5)
        self.assertEqual(output_df[3][IndCqc.value_unix_time], 1672876800)
        self.assertEqual(output_df[4][IndCqc.ascwds_filled_posts_dedup_clean], 15.0)
        self.assertEqual(output_df[4][IndCqc.value_unix_time], 1672790400)

    def test_interpolate_values_for_all_dates(self):
        output_df = job.interpolate_values_for_all_dates(
            self.data_for_calculating_interpolated_values_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
        )

        self.assertEqual(output_df.count(), 8)
        self.assertEqual(
            output_df.columns,
            [IndCqc.location_id, IndCqc.unix_time, IndCqc.interpolation_model],
        )

        output_df = output_df.sort(IndCqc.location_id).collect()
        self.assertEqual(output_df[0][IndCqc.interpolation_model], 30.0)
        self.assertEqual(output_df[1][IndCqc.interpolation_model], 4.0)
        self.assertEqual(output_df[2][IndCqc.interpolation_model], 4.5)
        self.assertEqual(output_df[3][IndCqc.interpolation_model], 5.0)
        self.assertEqual(output_df[4][IndCqc.interpolation_model], 5.0)
        self.assertAlmostEqual(
            output_df[5][IndCqc.interpolation_model], 6.1666, places=3
        )
        self.assertAlmostEqual(
            output_df[6][IndCqc.interpolation_model], 7.3333, places=3
        )
        self.assertEqual(output_df[7][IndCqc.interpolation_model], 8.5)
