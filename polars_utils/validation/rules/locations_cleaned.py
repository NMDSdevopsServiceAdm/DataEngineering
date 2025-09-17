from enum import Enum

import polars as pl

from polars_utils.expressions import has_value
from polars_utils.logger import get_logger
from polars_utils.raw_data_adjustments import is_invalid_location
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.validation_table_columns import (
    Validation,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    LocationType,
    PrimaryServiceType,
    RegistrationStatus,
    Services,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

raw_cqc_locations_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.type,
    CQCL.registration_status,
    CQCL.gac_service_types,
    CQCL.regulated_activities,
]

logger = get_logger(__name__)


class Rules(tuple, Enum):
    complete_columns = [
        CQCLClean.location_id,
        CQCLClean.cqc_location_import_date,
        CQCLClean.care_home,
        CQCLClean.provider_id,
        CQCLClean.cqc_sector,
        CQCLClean.registration_status,
        CQCLClean.imputed_registration_date,
        CQCLClean.primary_service_type,
        CQCLClean.name,
        CQCLClean.contemporary_ons_import_date,
        CQCLClean.contemporary_cssr,
        CQCLClean.contemporary_region,
        CQCLClean.current_ons_import_date,
        CQCLClean.current_cssr,
        CQCLClean.current_region,
        CQCLClean.current_rural_urban_ind_11,
    ]
    index_columns = [
        CQCLClean.location_id,
        CQCLClean.cqc_location_import_date,
    ]
    time_registered = (CQCLClean.time_registered, 1)
    number_of_beds = (CQCLClean.number_of_beds, 0, 500)
    location_id_length = (Validation.location_id_length, 3, 14)
    provider_id_length = (Validation.provider_id_length, 3, 14)
    care_home = (
        CQCLClean.care_home,
        CatValues.care_home_column_values.categorical_values,
    )
    cqc_sector = (
        CQCLClean.cqc_sector,
        CatValues.sector_column_values.categorical_values,
    )
    registration_status = (
        CQCLClean.registration_status,
        CatValues.registration_status_column_values.categorical_values,
    )
    dormancy = (CQCLClean.dormancy, CatValues.dormancy_column_values.categorical_values)
    primary_service_type = (
        CQCLClean.primary_service_type,
        CatValues.primary_service_type_column_values.categorical_values,
    )
    contemporary_cssr = (
        CQCLClean.contemporary_cssr,
        CatValues.contemporary_cssr_column_values.categorical_values,
    )
    contemporary_region = (
        CQCLClean.contemporary_region,
        CatValues.contemporary_region_column_values.categorical_values,
    )
    current_cssr = (
        CQCLClean.current_cssr,
        CatValues.current_cssr_column_values.categorical_values,
    )
    current_region = (
        CQCLClean.current_region,
        CatValues.current_region_column_values.categorical_values,
    )
    current_rural_urban_ind_11 = (
        CQCLClean.current_rural_urban_ind_11,
        CatValues.current_rui_column_values.categorical_values,
    )
    related_location = (
        CQCLClean.related_location,
        CatValues.related_location_column_values.categorical_values,
    )
    care_home_count = (
        CQCLClean.care_home,
        CatValues.care_home_column_values.count_of_categorical_values,
    )
    cqc_sector_count = (
        CQCLClean.cqc_sector,
        CatValues.sector_column_values.count_of_categorical_values,
    )
    registration_status_count = (
        CQCLClean.registration_status,
        CatValues.registration_status_column_values.count_of_categorical_values,
    )
    dormancy_count = (
        CQCLClean.dormancy,
        CatValues.dormancy_column_values.count_of_categorical_values,
    )
    primary_service_type_count = (
        CQCLClean.primary_service_type,
        CatValues.primary_service_type_column_values.count_of_categorical_values,
    )
    contemporary_cssr_count = (
        CQCLClean.contemporary_cssr,
        CatValues.contemporary_cssr_column_values.count_of_categorical_values,
    )
    contemporary_region_count = (
        CQCLClean.contemporary_region,
        CatValues.contemporary_region_column_values.count_of_categorical_values,
    )
    current_cssr_count = (
        CQCLClean.current_cssr,
        CatValues.current_cssr_column_values.count_of_categorical_values,
    )
    current_region_count = (
        CQCLClean.current_region,
        CatValues.current_region_column_values.count_of_categorical_values,
    )
    current_rural_urban_ind_11_count = (
        CQCLClean.current_rural_urban_ind_11,
        CatValues.current_rui_column_values.count_of_categorical_values,
    )
    related_location_count = (
        CQCLClean.related_location,
        CatValues.related_location_column_values.count_of_categorical_values,
    )

    @staticmethod
    def custom_type() -> pl.Expr:
        return (
            (pl.col(IndCQC.care_home) == CareHome.not_care_home)
            & (
                pl.col(IndCQC.primary_service_type)
                == PrimaryServiceType.non_residential
            )
        ) | (
            (pl.col(IndCQC.care_home) == CareHome.care_home)
            & (
                pl.col(IndCQC.primary_service_type).is_in(
                    [
                        PrimaryServiceType.care_home_with_nursing,
                        PrimaryServiceType.care_home_only,
                    ]
                )
            )
        )

    @staticmethod
    def expected_size(df: pl.DataFrame) -> int:
        has_activity: str = "has_a_known_regulated_activity"
        has_provider: str = "has_a_known_provider_id"

        cleaned_df = df.with_columns(
            # nullify empty lists to allow avoid index out of bounds error
            pl.when(pl.col(CQCL.gac_service_types).list.len() > 0).then(
                pl.col(CQCL.gac_service_types)
            ),
        ).filter(
            has_value(df, CQCL.regulated_activities, has_activity, CQCL.location_id),
            has_value(df, CQCL.provider_id, has_provider, CQCL.location_id),
            pl.col(CQCL.type) == LocationType.social_care_identifier,
            pl.col(CQCL.registration_status) == RegistrationStatus.registered,
            ~is_invalid_location(),
            ~(
                (pl.col(CQCL.gac_service_types).list.len() == 1)
                & (
                    pl.col(CQCL.gac_service_types)
                    .list[0]
                    .struct.field(CQCL.description)
                    == Services.specialist_college_service
                )
                & (pl.col(CQCL.gac_service_types).is_not_null())
            ),
        )
        logger.info(f"Expected size {cleaned_df.height}")
        return cleaned_df.height
