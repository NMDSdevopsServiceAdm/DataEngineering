import unittest

import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._04_model.utils import training_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelTrainingUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelTrainingUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class SplitTrainTestTests(unittest.TestCase):
    def setUp(self):
        self.seed = 99
        self.fraction = 0.5
        self.test_df = pl.DataFrame(
            Data.split_train_test_rows, Schemas.split_train_test_schema, orient="row"
        )

    def test_produces_same_dataframes_with_same_fraction_and_seed(self):
        train1, test1 = job.split_train_test(self.test_df, self.fraction, self.seed)
        train2, test2 = job.split_train_test(self.test_df, self.fraction, self.seed)

        pl_testing.assert_frame_equal(train1, train2)
        pl_testing.assert_frame_equal(test1, test2)

    def test_locations_do_not_appear_in_both_train_and_test(self):
        train_df, test_df = job.split_train_test(self.test_df, self.fraction, self.seed)

        train_groups = set(train_df[IndCQC.location_id].unique().to_list())
        test_groups = set(test_df[IndCQC.location_id].unique().to_list())

        self.assertTrue(train_groups.isdisjoint(test_groups))

    def test_all_original_rows_are_included_in_train_or_test(self):
        train_df, test_df = job.split_train_test(self.test_df, self.fraction, self.seed)

        combined_df = pl.concat([train_df, test_df])

        pl_testing.assert_frame_equal(combined_df, self.test_df, check_row_order=False)
