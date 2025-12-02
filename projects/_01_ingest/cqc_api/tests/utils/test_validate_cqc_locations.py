import unittest
import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.utils.validate_cqc_locations as job


class TestAddListColumnValidationCheckFlags(unittest.TestCase):

    def test_add_list_column_validation_check_flags(self):
        df_test = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "my_list": [[1, 2], [], None, [3, None]],
                "another_list": [[None], [5], [], None],
                "other_col": ["A", "B", "C", "D"],
            }
        )

        df_expected = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "other_col": ["A", "B", "C", "D"],
                "my_list_has_no_empty_or_null": [1, 0, 1, 0],
                "my_list_is_not_null": [1, 1, 0, 1],
                "another_list_has_no_empty_or_null": [0, 1, 0, 1],
                "another_list_is_not_null": [1, 1, 1, 0],
            }
        )
        df_result = job.add_list_column_validation_check_flags(
            df_test, ["my_list", "another_list"]
        )
        pl_testing.assert_frame_equal(df_result, df_expected)
