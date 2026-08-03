from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    PrimaryServiceType,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)


@dataclass
class CategoricalColumnTypeTestCase:
    id: str
    actual: Any
    expected: Any

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.actual, self.expected, id=self.id)


categorical_column_type_test_cases = [
    CategoricalColumnTypeTestCase(
        id="location_cat_type_uses_filled_posts_namespace",
        actual=CatColType.LocationCatType,
        expected=pl.Categorical(pl.Categories("location", namespace="filled_posts")),
    ),
    CategoricalColumnTypeTestCase(
        id="establishment_cat_type_uses_filled_posts_namespace",
        actual=CatColType.EstablishmentCatType,
        expected=pl.Categorical(
            pl.Categories("establishment", namespace="filled_posts")
        ),
    ),
    CategoricalColumnTypeTestCase(
        id="provider_cat_type_uses_filled_posts_namespace",
        actual=CatColType.ProviderCatType,
        expected=pl.Categorical(pl.Categories("provider", namespace="filled_posts")),
    ),
    CategoricalColumnTypeTestCase(
        id="brand_cat_type_uses_filled_posts_namespace",
        actual=CatColType.BrandCatType,
        expected=pl.Categorical(pl.Categories("brand", namespace="filled_posts")),
    ),
    CategoricalColumnTypeTestCase(
        id="job_role_enum_type_matches_main_job_role_labels_values",
        actual=CatColType.JobRoleEnumType,
        expected=pl.Enum(CatVals.main_job_role_labels_column_values.categorical_values),
    ),
    CategoricalColumnTypeTestCase(
        id="job_group_enum_type_matches_main_job_group_labels_values",
        actual=CatColType.JobGroupEnumType,
        expected=pl.Enum(
            CatVals.main_job_group_labels_column_values.categorical_values
        ),
    ),
    CategoricalColumnTypeTestCase(
        id="estimates_filled_post_source_enum_type_covers_all_sources",
        actual=CatColType.EstimatesFilledPostSourceEnumType,
        expected=pl.Enum(
            [
                EstimateFilledPostsSource.imputed_pir_filled_posts_model,
                EstimateFilledPostsSource.ascwds_pir_merged,
                EstimateFilledPostsSource.imputed_posts_care_home_model,
                EstimateFilledPostsSource.care_home_model,
                EstimateFilledPostsSource.imputed_posts_non_res_combined_model,
                EstimateFilledPostsSource.non_res_combined_model,
                EstimateFilledPostsSource.posts_rolling_average_model,
            ]
        ),
    ),
    CategoricalColumnTypeTestCase(
        id="primary_service_enum_type_covers_all_service_types",
        actual=CatColType.PrimaryServiceEnumType,
        expected=pl.Enum(
            [
                PrimaryServiceType.care_home_only,
                PrimaryServiceType.care_home_with_nursing,
                PrimaryServiceType.non_residential,
            ]
        ),
    ),
    CategoricalColumnTypeTestCase(
        id="job_role_filtering_rule_cat_type_uses_uint8_physical_type",
        actual=CatColType.JobRoleFilteringRuleCatType,
        expected=pl.Categorical(
            pl.Categories(
                "job_role_filtering_rule", namespace="filled_posts", physical=pl.UInt8
            )
        ),
    ),
]


class TestCategoricalColumnTypes:
    @pytest.mark.parametrize(
        "actual, expected",
        [case.as_pytest_param() for case in categorical_column_type_test_cases],
    )
    def test_constant_matches_expected_dtype(self, actual, expected):
        assert actual == expected
