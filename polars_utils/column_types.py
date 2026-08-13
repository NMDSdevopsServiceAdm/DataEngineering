from dataclasses import dataclass

import polars as pl

from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CQCLocationCatVals,
)


@dataclass
class CategoricalColumnTypes:
    """Reusable polars Categorical and Enum dtype constants."""

    BrandCatType = pl.Categorical(pl.Categories("brand", namespace="filled_posts"))
    CareHomeEnumType = pl.Enum(
        CQCLocationCatVals.care_home_column_values.categorical_values
    )
    CqcSectorEnumType = pl.Enum(
        CQCLocationCatVals.sector_column_values.categorical_values
    )
    DormancyEnumType = pl.Enum(
        CQCLocationCatVals.dormancy_column_values.categorical_values
    )
    EstablishmentCatType = pl.Categorical(
        pl.Categories("establishment", namespace="filled_posts")
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
    JobGroupCatType = pl.Categorical(
        pl.Categories("job_group", namespace="filled_posts")
    )
    JobRoleCatType = pl.Categorical(pl.Categories("job_role", namespace="filled_posts"))
    JobRoleFilteringRuleCatType = pl.Categorical(
        pl.Categories(
            "job_role_filtering_rule", namespace="filled_posts", physical=pl.UInt8
        )
    )
    LocationCatType = pl.Categorical(
        pl.Categories("location", namespace="filled_posts")
    )
    OnsCssrCatType = pl.Categorical(pl.Categories("ons_cssr", namespace="filled_posts"))
    OnsIcbCatType = pl.Categorical(pl.Categories("ons_icb", namespace="filled_posts"))
    OnsIcbRegionCatType = pl.Categorical(
        pl.Categories("ons_icb_region", namespace="filled_posts")
    )
    OnsRegionCatType = pl.Categorical(
        pl.Categories("ons_region", namespace="filled_posts")
    )
    OnsRuralUrbanInd11EnumType = pl.Enum(
        CQCLocationCatVals.current_rui_column_values.categorical_values
    )
    OnsSubIcbCatType = pl.Categorical(
        pl.Categories("ons_sub_icb", namespace="filled_posts")
    )
    PrimaryServiceEnumType = pl.Enum(
        CQCLocationCatVals.primary_service_type_column_values.categorical_values
    )
    PrimaryServiceTypeSecondLevelCatType = pl.Categorical(
        pl.Categories("primary_service_type_second_level", namespace="filled_posts")
    )
    ProviderCatType = pl.Categorical(
        pl.Categories("provider", namespace="filled_posts")
    )
    PublishedJobRoleLabelCatType = pl.Categorical(
        pl.Categories("published_job_role_label", namespace="filled_posts")
    )
