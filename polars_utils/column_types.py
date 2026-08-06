from dataclasses import dataclass

import polars as pl

from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    PrimaryServiceType,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
    SLVPrepareCategoricalValues,
)


@dataclass
class CategoricalColumnTypes:
    """Reusable polars Categorical and Enum dtype constants."""

    LocationCatType = pl.Categorical(
        pl.Categories("location", namespace="filled_posts")
    )
    EstablishmentCatType = pl.Categorical(
        pl.Categories("establishment", namespace="filled_posts")
    )
    ProviderCatType = pl.Categorical(
        pl.Categories("provider", namespace="filled_posts")
    )
    BrandCatType = pl.Categorical(pl.Categories("brand", namespace="filled_posts"))
    JobRoleEnumType = pl.Enum(
        CatVals.main_job_role_labels_column_values.categorical_values
    )
    JobGroupEnumType = pl.Enum(
        CatVals.main_job_group_labels_column_values.categorical_values
    )
    PublishedJobRoleLabelEnumType = pl.Enum(
        SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
    )
    EstimatesFilledPostSourceEnumType = pl.Enum(
        [
            EstimateFilledPostsSource.imputed_pir_filled_posts_model,
            EstimateFilledPostsSource.ascwds_pir_merged,
            EstimateFilledPostsSource.imputed_posts_care_home_model,
            EstimateFilledPostsSource.care_home_model,
            EstimateFilledPostsSource.imputed_posts_non_res_combined_model,
            EstimateFilledPostsSource.non_res_combined_model,
            EstimateFilledPostsSource.posts_rolling_average_model,
        ]
    )
    PrimaryServiceEnumType = pl.Enum(
        [
            PrimaryServiceType.care_home_only,
            PrimaryServiceType.care_home_with_nursing,
            PrimaryServiceType.non_residential,
        ]
    )
    JobRoleFilteringRuleCatType = pl.Categorical(
        pl.Categories(
            "job_role_filtering_rule", namespace="filled_posts", physical=pl.UInt8
        )
    )
