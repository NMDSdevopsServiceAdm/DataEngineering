import unittest
from dataclasses import dataclass

import polars as pl
import polars.testing as pl_testing
import pytest

import polars_utils.expressions as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


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


class TestPercentageShare:
    def test_over_whole_dataset(self):
        input_lf = pl.LazyFrame({"vals": [1, 2, 2]})
        expected_lf = pl.LazyFrame({"ratios": [0.2, 0.4, 0.4]})
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
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
            job.percentage_share("vals").over("group").alias("ratios"),
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)

    def test_when_passed_an_expression(self):
        """Test that the function accepts a Polars expression instead of just a string."""
        input_lf = pl.LazyFrame({"vals": [10, 20, 80]})
        expression = pl.col("vals") - 10
        expected_lf = pl.LazyFrame({"ratios": [0.0, 0.125, 0.875]})

        returned_lf = input_lf.select(job.percentage_share(expression).alias("ratios"))
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
        returned_lf = input_lf.select(job.percentage_share("values").alias("output"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


@dataclass
class StrLengthColsTestCase:
    id: str
    dtype: pl.DataType
    values: list[str | None]
    expected_lengths: list[int | None]

    def as_pytest_param(self):
        return pytest.param(self.dtype, self.values, self.expected_lengths, id=self.id)


str_length_cols_cases = [
    StrLengthColsTestCase(
        id="string_column",
        dtype=pl.String,
        values=["foobar", "foo", None],
        expected_lengths=[6, 3, None],
    ),
    StrLengthColsTestCase(
        id="string_column_with_empty_string",
        dtype=pl.String,
        values=["", "abc", None],
        expected_lengths=[0, 3, None],
    ),
    StrLengthColsTestCase(
        id="categorical_column",
        dtype=pl.Categorical,
        values=["foobar", "foo", None],
        expected_lengths=[6, 3, None],
    ),
]


class TestStrLengthCols:
    @pytest.mark.parametrize(
        "dtype, values, expected_lengths",
        [case.as_pytest_param() for case in str_length_cols_cases],
    )
    def test_returns_length_of_each_value(self, dtype, values, expected_lengths):
        input_lf = pl.LazyFrame({"col": values}, schema={"col": dtype})

        returned_lf = input_lf.select(job.str_length_cols(["col"]))

        expected_lf = pl.LazyFrame(
            {"col_length": expected_lengths}, schema={"col_length": pl.UInt32}
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_a_length_column_for_each_column(self):
        input_lf = pl.LazyFrame({"c": ["foobar", "foo"], "d": ["a", None]})

        returned_lf = input_lf.select(job.str_length_cols(["c", "d"]))

        expected_lf = pl.LazyFrame(
            {"c_length": [6, 3], "d_length": [1, None]},
            schema={"c_length": pl.UInt32, "d_length": pl.UInt32},
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestIsCareHome(TestExpressions):
    def test_is_care_home(self):
        df = pl.DataFrame({"careHome": ["Y", "N", "Y", None]}).with_columns(
            job.is_care_home().alias("is_care_home")
        )
        self.assertEqual(df["is_care_home"].to_list(), [True, False, True, None])


class TestIsNotCareHome(TestExpressions):
    def test_is_not_care_home(self):
        df = pl.DataFrame({"careHome": ["Y", "N", "Y", None]}).with_columns(
            job.is_not_care_home().alias("is_not_care_home")
        )
        self.assertEqual(df["is_not_care_home"].to_list(), [False, True, False, None])


class TestIsDormant(TestExpressions):
    def test_is_dormant(self):
        df = pl.DataFrame({"dormancy": ["Y", "N", "Y", None]}).with_columns(
            job.is_dormant().alias("is_dormant")
        )
        self.assertEqual(df["is_dormant"].to_list(), [True, False, True, None])


class TestJrColsSelector(unittest.TestCase):
    def test_selects_expected_columns(self):
        test_lf = pl.LazyFrame(
            {
                AWPClean.job_role_01_employees: "1",
                AWPClean.job_role_01_starters: "1",
                AWPClean.job_role_01_leavers: "1",
                AWPClean.job_role_01_vacancies: "1",
                AWPClean.job_role_01_temporary: "1",
                AWPClean.job_role_01_flag: "1",
                "jr01permdate": "1",
                "any_other_col": "1",
            }
        )
        returned_cols = (
            test_lf.select(job.is_slv_job_role_column()).collect_schema().names()
        )
        expected_cols = [
            AWPClean.job_role_01_starters,
            AWPClean.job_role_01_leavers,
            AWPClean.job_role_01_vacancies,
        ]

        self.assertEqual(returned_cols, expected_cols)


if __name__ == "__main__":
    unittest.main()
