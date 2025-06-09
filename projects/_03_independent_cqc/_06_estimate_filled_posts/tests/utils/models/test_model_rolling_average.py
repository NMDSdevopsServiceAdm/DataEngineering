import unittest

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.rolling_average as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelRollingAverageData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelRollingAverageSchemas as Schemas,
)


class ModelRollingAverageTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class MainTests(ModelRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3

        test_df = self.spark.createDataFrame(
            Data.rolling_average_rows,
            Schemas.rolling_average_schema,
        )
        self.returned_df = job.model_calculate_rolling_average(
            df=test_df,
            column_to_average=IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days=number_of_days,
            column_to_partition_by=IndCqc.location_id,
            new_column_name=IndCqc.posts_rolling_average_model,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows,
            Schemas.expected_rolling_average_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_model_calculate_rolling_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rolling_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.posts_rolling_average_model],
                self.expected_data[i][IndCqc.posts_rolling_average_model],
                2,
                f"Returned row {i} does not match expected",
            )
