import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    ).with_columns(
        str_length_cols([CQCLClean.location_id, CQCLClean.provider_id]),
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # complete columns
        .col_vals_not_null(
            [
                CQCLClean.location_id,
                Keys.import_date,
                CQCLClean.cqc_location_import_date,
                CQCLClean.name,
                CQCLClean.type,
                CQCLClean.imputed_registration_date,
                CQCLClean.registration_status,
                CQCLClean.provider_id,
                CQCLClean.primary_service_type,
                CQCLClean.primary_service_type_second_level,
                CQCLClean.care_home,
                CQCLClean.cqc_sector,
                CQCLClean.related_location,
                CQCLClean.specialism_dementia,
                CQCLClean.specialism_learning_disabilities,
                CQCLClean.specialism_mental_health,
                CQCLClean.contemporary_ons_import_date,
                CQCLClean.contemporary_cssr,
                CQCLClean.contemporary_region,
                CQCLClean.current_ons_import_date,
                CQCLClean.current_cssr,
                CQCLClean.current_region,
                CQCLClean.current_rural_urban_ind_11,
            ]
        )
        # index columns
        .rows_distinct(
            [
                CQCLClean.location_id,
                CQCLClean.cqc_location_import_date,
                Keys.import_date,
            ]
        )
        # categorical column values match expected set
        .col_vals_in_set(CQCLClean.type, [LocationType.social_care_identifier])
        .col_vals_in_set(CQCLClean.registration_status, [RegistrationStatus.registered])
        .col_vals_in_set(
            CQCLClean.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.primary_service_type_second_level,
            CatValues.primary_service_type_second_level_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.care_home, CatValues.care_home_column_values.categorical_values
        )
        .col_vals_in_set(
            CQCLClean.cqc_sector, CatValues.sector_column_values.categorical_values
        )
        .col_vals_in_set(
            CQCLClean.related_location,
            CatValues.related_location_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.specialism_dementia,
            CatValues.specialism_dementia_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.specialism_learning_disabilities,
            CatValues.specialism_learning_disabilities_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.specialism_mental_health,
            CatValues.specialism_mental_health_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.dormancy,
            # na_pass is not an optional parameter to .col_vals_in_set
            # extending list to allow for null values as not included
            # in categorical values
            [*CatValues.dormancy_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            CQCLClean.contemporary_cssr,
            CatValues.contemporary_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.contemporary_region,
            CatValues.contemporary_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            CQCLClean.current_rural_urban_ind_11,
            CatValues.current_rui_column_values.categorical_values,
        )
        # catagorical column unique values count matches expected count
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.primary_service_type_second_level,
                CatValues.primary_service_type_second_level_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.primary_service_type_second_level} needs to be one of {CatValues.primary_service_type_second_level_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.care_home} needs to be one of {CatValues.care_home_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.cqc_sector,
                CatValues.sector_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.cqc_sector} needs to be one of {CatValues.sector_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.related_location,
                CatValues.related_location_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.related_location} needs to be one of {CatValues.related_location_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.specialism_dementia,
                CatValues.specialism_dementia_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.specialism_dementia} needs to be one of {CatValues.specialism_dementia_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.specialism_learning_disabilities,
                CatValues.specialism_learning_disabilities_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.specialism_learning_disabilities} needs to be one of {CatValues.specialism_learning_disabilities_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.specialism_mental_health,
                CatValues.specialism_mental_health_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.specialism_mental_health} needs to be one of {CatValues.specialism_mental_health_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.dormancy} needs to be null, or one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.contemporary_cssr,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.contemporary_cssr} needs to be null, or one of {CatValues.contemporary_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.contemporary_region,
                CatValues.contemporary_region_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.contemporary_region} needs to be null, or one of {CatValues.contemporary_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.current_cssr} needs to be null, or one of {CatValues.current_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.current_region} needs to be null, or one of {CatValues.current_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                CQCLClean.current_rural_urban_ind_11,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{CQCLClean.current_rural_urban_ind_11} needs to be null, or one of {CatValues.current_rui_column_values.categorical_values}",
        )
        # numeric column values are between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(Validation.provider_id_length, 3, 14)
        # numeric column values are greater than or equal to
        .col_vals_ge(CQCLClean.number_of_beds, 0, na_pass=True)
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
