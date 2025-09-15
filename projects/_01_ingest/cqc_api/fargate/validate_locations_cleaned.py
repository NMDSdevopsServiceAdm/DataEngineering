import sys
from enum import Enum

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils import validate as vl
from polars_utils.expressions import has_value, str_length_cols
from polars_utils.logger import get_logger
from polars_utils.validation.raw_data_adjustments import is_invalid_location
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
    CleanedIndCQCCategoricalValues as CatValues,
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

GLOBAL_THRESHOLDS = pb.Thresholds(warning=1)
GLOBAL_ACTIONS = pb.Actions(
    warning="{LEVEL}: {type} validation failed with {n_failed} records for column {col}."
)


class Values(tuple, Enum):
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


def expected_size(df: pl.DataFrame) -> int:
    has_activity: str = "has_a_known_regulated_activity"
    has_provider: str = "has_a_known_provider_id"

    cleaned_df = df.with_columns(
        # nullify empty lists to allow prevent index out of bounds error
        pl.when(pl.col(CQCL.gac_service_types).list.len() > 0).then(
            pl.col(CQCL.gac_service_types)
        ),
    ).filter(
        has_value(df, CQCL.regulated_activities, has_activity, CQCL.location_id).alias(
            has_activity
        ),
        has_value(df, CQCL.provider_id, has_provider, CQCL.location_id).alias(
            has_provider
        ),
        pl.col(CQCL.type) == LocationType.social_care_identifier,
        pl.col(CQCL.registration_status) == RegistrationStatus.registered,
        ~is_invalid_location(df),
        ~(
            (pl.col(CQCL.gac_service_types).list.len() == 1)
            & (
                pl.col(CQCL.gac_service_types).list[0].struct.field(CQCL.description)
                == Services.specialist_college_service
            )
            & (pl.col(CQCL.gac_service_types).is_not_null())
        ),
    )
    logger.info(f"Compared dataset has {cleaned_df.height} records")
    return cleaned_df.height


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): optional path to a dataset to compare against for expected size
    """
    source_df = vl.read_parquet(f"""s3://{bucket_name}/{source_path.strip("/")}/""")
    compare_df = vl.read_parquet(
        f"""s3://{bucket_name}/{compare_path.strip("/")}/""",
        selected_columns=raw_cqc_locations_columns_to_import,
    )

    pre_df = source_df.with_columns(
        str_length_cols(source_df, [CQCL.location_id, CQCL.provider_id]),
    )

    validation = (
        pb.Validate(
            data=pre_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .col_count_match(expected_size(compare_df))
        # complete columns
        .col_vals_not_null(*Values.complete_columns)
        # index columns
        .rows_distinct(*Values.index_columns)
        # min values
        .col_vals_ge(*Values.time_registered)
        # between (inclusive)
        .col_vals_between(*Values.number_of_beds)
        .col_vals_between(*Values.location_id_length)
        .col_vals_between(*Values.provider_id_length)
        # categorical
        .col_vals_in_set(*Values.care_home)
        .col_vals_in_set(*Values.cqc_sector)
        .col_vals_in_set(*Values.registration_status)
        .col_vals_in_set(*Values.dormancy)
        .col_vals_in_set(*Values.primary_service_type)
        .col_vals_in_set(*Values.contemporary_cssr)
        .col_vals_in_set(*Values.contemporary_region)
        .col_vals_in_set(*Values.current_cssr)
        .col_vals_in_set(*Values.current_region)
        .col_vals_in_set(*Values.current_rural_urban_ind_11)
        .col_vals_in_set(*Values.related_location)
        # distinct values
        .specially(vl.is_unique_count_equal(*Values.care_home_count))
        .specially(vl.is_unique_count_equal(*Values.cqc_sector_count))
        .specially(vl.is_unique_count_equal(*Values.registration_status_count))
        .specially(vl.is_unique_count_equal(*Values.dormancy_count))
        .specially(vl.is_unique_count_equal(*Values.primary_service_type_count))
        .specially(vl.is_unique_count_equal(*Values.contemporary_cssr_count))
        .specially(vl.is_unique_count_equal(*Values.contemporary_region_count))
        .specially(vl.is_unique_count_equal(*Values.current_cssr_count))
        .specially(vl.is_unique_count_equal(*Values.current_region_count))
        .specially(vl.is_unique_count_equal(*Values.current_rural_urban_ind_11_count))
        .specially(vl.is_unique_count_equal(*Values.related_location_count))
        # custom validtion
        .col_vals_expr(
            (
                (pl.col(IndCQC.care_home) == CareHome.not_care_home)
                & (
                    pl.col(IndCQC.primary_service_type)
                    == PrimaryServiceType.non_residential
                )
            )
            | (
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
        )
    )
    vl.write_reports(validation, bucket_name, reports_path.strip("/"))


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        ("--compare_path", "The filepath to output reports"),
    )
    logger.info(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    logger.info(f"Validation of {args.source_path} complete")
