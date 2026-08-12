from dataclasses import dataclass

import polars as pl

from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
    LocationsApiCleanedCategoricalValues as CQCLocationCatVals,
    SLVPrepareCategoricalValues,
)


@dataclass
class CategoricalColumnTypes:
    """Reusable polars Categorical and Enum dtype constants."""

    # ASCWDS workplace
    EstablishmentCatType = pl.Categorical(
        pl.Categories("establishment", namespace="ascwds_workplace")
    )
    EstablishmentTypeCatType = pl.Categorical(
        pl.Categories("establishment_type", namespace="ascwds_workplace")
    )
    IsBulkUploaderCatType = pl.Categorical(
        pl.Categories("is_bulk_uploader", namespace="ascwds_workplace")
    )
    IsParentCatType = pl.Categorical(
        pl.Categories("is_parent", namespace="ascwds_workplace")
    )
    LaPermissionCatType = pl.Categorical(
        pl.Categories("la_permission", namespace="ascwds_workplace")
    )
    MainServiceIdCatType = pl.Categorical(
        pl.Categories("main_service_id", namespace="ascwds_workplace")
    )
    RegionIdCatType = pl.Categorical(
        pl.Categories("region_id", namespace="ascwds_workplace")
    )
    RegistrationTypeCatType = pl.Categorical(
        pl.Categories("registration_type", namespace="ascwds_workplace")
    )

    # CQC locations ingest cleaning
    CareHomeEnumType = pl.Enum(
        CQCLocationCatVals.care_home_column_values.categorical_values
    )
    DormancyEnumType = pl.Enum(
        CQCLocationCatVals.dormancy_column_values.categorical_values
    )
    CqcSectorEnumType = pl.Enum(
        CQCLocationCatVals.sector_column_values.categorical_values
    )
    PrimaryServiceEnumType = pl.Enum(
        CQCLocationCatVals.primary_service_type_column_values.categorical_values
    )
    PrimaryServiceTypeSecondLevelEnumType = pl.Enum(
        CQCLocationCatVals.primary_service_type_second_level_column_values.categorical_values
    )
    CurrentRuralUrbanInd11EnumType = pl.Enum(
        CQCLocationCatVals.current_rui_column_values.categorical_values
    )
    BrandCatType = pl.Categorical(pl.Categories("brand", namespace="cqc_location"))
    LocationCatType = pl.Categorical(
        pl.Categories("location", namespace="cqc_location")
    )
    ProviderCatType = pl.Categorical(
        pl.Categories("provider", namespace="cqc_location")
    )
    SpecialismDementiaCatType = pl.Categorical(
        pl.Categories("specialism_dementia", namespace="cqc_location")
    )
    SpecialismLearningDisabilitiesCatType = pl.Categorical(
        pl.Categories("specialism_learning_disabilities", namespace="cqc_location")
    )
    SpecialismMentalHealthCatType = pl.Categorical(
        pl.Categories("specialism_mental_health", namespace="cqc_location")
    )
    ContemporaryRegionCatType = pl.Categorical(
        pl.Categories("contemporary_region", namespace="ons_postcode_directory")
    )
    ContemporaryCssrCatType = pl.Categorical(
        pl.Categories("contemporary_cssr", namespace="ons_postcode_directory")
    )
    ContemporarySubIcbCatType = pl.Categorical(
        pl.Categories("contemporary_sub_icb", namespace="ons_postcode_directory")
    )
    ContemporaryIcbCatType = pl.Categorical(
        pl.Categories("contemporary_icb", namespace="ons_postcode_directory")
    )
    ContemporaryIcbRegionCatType = pl.Categorical(
        pl.Categories("contemporary_icb_region", namespace="ons_postcode_directory")
    )
    CurrentRegionCatType = pl.Categorical(
        pl.Categories("current_region", namespace="ons_postcode_directory")
    )
    CurrentCssrCatType = pl.Categorical(
        pl.Categories("current_cssr", namespace="ons_postcode_directory")
    )
    CurrentIcbCatType = pl.Categorical(
        pl.Categories("current_icb", namespace="ons_postcode_directory")
    )

    # CQC providers
    ProviderTypeCatType = pl.Categorical(
        pl.Categories("type", namespace="cqc_provider")
    )
    RegistrationStatusEnumType = pl.Enum(
        CQCLocationCatVals.registration_status_column_values.categorical_values
    )

    # Estimate Filled Posts pipeline
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
    JobRoleFilteringRuleCatType = pl.Categorical(
        pl.Categories(
            "job_role_filtering_rule", namespace="filled_posts", physical=pl.UInt8
        )
    )
