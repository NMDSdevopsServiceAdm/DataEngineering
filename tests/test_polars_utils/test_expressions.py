import unittest

import polars as pl

from polars_utils.expressions import has_value, str_length_cols


class TestExpressions(unittest.TestCase):
    def setUp(self):
        self.df_simple = pl.DataFrame(
            {
                "group": ["x", "x", "y", "y"],
                "a": [1, 2, None, None],
                "b": [1, 2, None, 1],
                "c": ["foobar", "foo", None, None],
                "d": ["foobar", "bar", None, "baz"],
                "e": [[1, 2], [], None, []],
                "f": [[1, 2], [], None, [3]],
                "g": ["ab", "abc", "abcd", "abcde"],
            }
        )


class TestHasValue(TestExpressions):
    def test_null_numeric(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "a", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, False, False])

    def test_non_null_numeric(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "b", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, True, True])

    def test_null_string(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "c", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, False, False])

    def test_non_null_string(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "d", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, True, True])

    def test_partial_list_column(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "e", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, False, False])

    def test_list_has_column(self):
        df = self.df_simple.with_columns(
            has_value(self.df_simple, "f", "group").alias("has_value"),
        )
        self.assertEqual(df["has_value"].to_list(), [True, True, True, True])


class TestStrLengthCols(TestExpressions):
    def test_single_column(self):
        df = self.df_simple.select(str_length_cols(["c"]))
        self.assertEqual(df["c_length"].to_list(), [6, 3, None, None])

    def test_multiple_columns(self):
        df = self.df_simple.with_columns(str_length_cols(["c", "d"]))
        self.assertEqual(df["c_length"].to_list(), [6, 3, None, None])
        self.assertEqual(df["d_length"].to_list(), [6, 3, None, 3])

    def test_empty_string(self):
        df = pl.DataFrame({"str_col": ["", "abc", None]}).with_columns(
            str_length_cols(["str_col"])
        )
        self.assertEqual(df["str_col_length"].to_list(), [0, 3, None])

    def test_non_string_column(self):
        # Given
        df = pl.DataFrame({"num_col": [1, 2, 3]})
        with self.assertRaises(pl.exceptions.SchemaError):
            df.with_columns(str_length_cols(["num_col"]))


if __name__ == "__main__":
    unittest.main()
