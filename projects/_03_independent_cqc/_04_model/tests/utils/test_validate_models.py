import unittest

import polars as pl

import projects._03_independent_cqc._04_model.utils.validate_models as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateModelsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateModelsSchemas as Schemas,
)


class TestGetExpectedRowCount(unittest.TestCase):
    def setUp(self):
        self.df_test = pl.DataFrame(
            Data.non_res_with_dormancy_rows,
            Schemas.non_res_with_dormancy_schema,
            orient="row",
        )

    def test_get_expected_row_count_returns_error_if_model_is_not_recognised(
        self,
    ):
        with self.assertRaises(ValueError):
            job.get_expected_row_count_for_model_features(
                self.df_test, "unrecognised_model"
            )

    def test_get_expected_row_count_for_non_res_with_dormancy_model(
        self,
    ):
        expected_row_count = Data.expected_get_expected_row_count_rows
        returned_row_count = job.get_expected_row_count_for_model_features(
            self.df_test, "non_res_with_dormancy_model"
        )
        self.assertEqual(expected_row_count, returned_row_count)

    def test_get_expected_row_count_for_care_home_model(
        self,
    ):
        expected_row_count = Data.expected_get_expected_row_count_rows
        returned_row_count = job.get_expected_row_count_for_model_features(
            self.df_test, "care_home_model"
        )
        self.assertEqual(expected_row_count, returned_row_count)
