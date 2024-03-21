import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.estimate_ind_cqc_filled_posts as job
from tests.test_file_data import EstimateIndCQCFilledPostsData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    CARE_HOMES_FEATURES = "care home features"
    CARE_HOME_MODEL = (
        "pipeline-resources/models/care_home_filled_posts_prediction/0.0.1/"
    )
    ESTIMATES_DESTINATION = "estimates destination"
    METRICS_DESTINATION = "metrics destination"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_cleaned_ind_cqc_df = self.spark.createDataFrame(
            Data.cleaned_ind_cqc_rows, Schemas.cleaned_ind_cqc_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("jobs.estimate_ind_cqc_filled_posts.model_care_homes")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        model_care_homes_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cleaned_ind_cqc_df,
            self.ESTIMATES_DESTINATION,
        ]

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.CARE_HOMES_FEATURES,
            self.CARE_HOME_MODEL,
            self.ESTIMATES_DESTINATION,
            self.METRICS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(model_care_homes_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)
        write_to_parquet_patch.assert_any_call(
            ANY,
            self.ESTIMATES_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_populate_known_jobs_use_filled_posts_from_current_date(self):
        test_df = self.spark.createDataFrame(
            Data.populate_known_jobs_rows, Schemas.populate_known_jobs_schema
        )

        returned_df = job.populate_estimate_jobs_when_filled_posts_known(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_populate_known_jobs_rows, Schemas.populate_known_jobs_schema
        )

        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
