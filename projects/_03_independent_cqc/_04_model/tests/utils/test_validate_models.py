import unittest

import polars as pl

import projects._03_independent_cqc._04_model.utils.validate_models as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateModelsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateModelsSchemas as Schemas,
)


class TestAddListColumnValidationCheckFlags(unittest.TestCase):

    def test_get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy(
        self,
    ):
        df_test = pl.DataFrame(
            Data.get_expected_row_count_comapre_df_rows,
            Schemas.get_expected_row_count_comapre_df_schema,
            orient="row",
        )

        expected_row_count = Data.expected_get_expected_row_count_rows
        returned_row_count = job.get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy(
            df_test
        )
        self.assertEqual(expected_row_count, returned_row_count)
