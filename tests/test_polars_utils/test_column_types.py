import pytest

from tests.test_polars_utils_data import ColumnTypesData as Data


class TestCategoricalColumnTypes:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.categorical_column_type_cases],
    )
    def test_constant_matches_expected_dtype(self, case):
        assert case.actual == case.expected
