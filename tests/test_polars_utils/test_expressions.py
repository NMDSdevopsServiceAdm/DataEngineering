import unittest

import polars as pl
import polars.testing as pl_testing
import pytest

from polars_utils.expressions import has_value, percentage_share, str_length_cols


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


class TestPercentageShare:
    def test_over_whole_dataset(self):
        input_lf = pl.LazyFrame({"vals": [1, 2, 2]})
        expected_lf = pl.LazyFrame({"ratios": [0.2, 0.4, 0.4]})
        returned_lf = input_lf.select(percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_over_groups(self):
        expected_lf = pl.LazyFrame(
            data=[
                ("1", 1, 0.333),
                ("1", 2, 0.667),
                ("2", 2, 0.4),
                ("2", 3, 0.6),
            ],
            schema=["group", "vals", "ratios"],
            orient="row",
        )
        input_lf = expected_lf.select("group", "vals")
        returned_lf = input_lf.with_columns(
            percentage_share("vals").over("group").alias("ratios"),
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)

    def test_when_passed_an_expression(self):
        """Test that the function accepts a Polars expression instead of just a string."""
        input_lf = pl.LazyFrame({"vals": [10, 20, 80]})
        expression = pl.col("vals") - 10
        expected_lf = pl.LazyFrame({"ratios": [0.0, 0.125, 0.875]})

        returned_lf = input_lf.select(percentage_share(expression).alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(
                [None, 3, None, 2],
                [None, 0.6, None, 0.4],
                id="when_some_values_are_null",
            ),
            pytest.param(
                [None, None, None, None],
                [None, None, None, None],
                id="when_all_values_are_null",
            ),
            pytest.param(
                [2, 0, 3, 0],
                [0.4, 0.0, 0.6, 0.0],
                id="when_some_values_are_zero",
            ),
            pytest.param(
                # This returns NaN rather than Null because of divide by zero.
                # https://docs.pola.rs/user-guide/expressions/missing-data/#not-a-number-or-nan-values
                [0, 0, 0, 0],
                [float("nan")] * 4,
                id="when_all_values_are_zero",
            ),
        ],
    )
    def test_edge_cases(self, input_, expected):
        input_lf = pl.LazyFrame({"values": input_}).cast(pl.Float64)
        expected_lf = pl.LazyFrame({"output": expected}).cast(pl.Float64)
        returned_lf = input_lf.select(percentage_share("values").alias("output"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


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
