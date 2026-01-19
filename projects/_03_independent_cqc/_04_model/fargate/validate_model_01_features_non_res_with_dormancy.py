import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._04_model.utils.validate_models import (
    add_list_column_validation_check_flags,
    get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_column_values import (
    LocationType,
    NumericTrueFalse,
    RegistrationStatus,
)

compare_columns_to_import = [
    Keys.import_date,
    IndCQC.location_id,
    IndCQC.care_home,
    IndCQC.dormancy,
]


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): path to a dataset to compare against for expected size
    """
    source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=False
    ).with_columns(
        str_length_cols([IndCQC.location_id, IndCQC.provider_id]),
    )

    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=compare_columns_to_import,
    )
    expected_row_count = (
        get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy(
            compare_df
        )
    )
    source_df = add_list_column_validation_check_flags(
        source_df,
        [
            IndCQC.specialisms_offered,
            IndCQC.regulated_activities_offered,
            IndCQC.services_offered,
            IndCQC.registered_manager_names,
        ],
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Cleaned file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCQC.location_id,
                Keys.import_date,
                IndCQC.cqc_location_import_date,
                IndCQC.name,
                IndCQC.type,
                IndCQC.imputed_registration_date,
                IndCQC.registration_status,
                IndCQC.provider_id,
                IndCQC.primary_service_type,
                IndCQC.primary_service_type_second_level,
                IndCQC.care_home,
                IndCQC.cqc_sector,
                IndCQC.related_location,
                IndCQC.specialism_dementia,
                IndCQC.specialism_learning_disabilities,
                IndCQC.specialism_mental_health,
                IndCQC.contemporary_ons_import_date,
                IndCQC.contemporary_cssr,
                IndCQC.contemporary_region,
                IndCQC.current_ons_import_date,
                IndCQC.current_cssr,
                IndCQC.current_region,
                IndCQC.current_rural_urban_ind_11,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCQC.location_id,
                IndCQC.cqc_location_import_date,
                Keys.import_date,
            ]
        )
        # Complex column validation for completeness
        .col_vals_in_set(CQCLVal.services_offered_is_not_null, [NumericTrueFalse.true])
        .col_vals_in_set(
            CQCLVal.regulated_activities_offered_is_not_null, [NumericTrueFalse.true]
        )
        # Complex column validation for empty list and null within list
        .col_vals_in_set(
            CQCLVal.services_offered_has_no_empty_or_null, [NumericTrueFalse.true]
        )
        .col_vals_in_set(
            CQCLVal.regulated_activities_offered_has_no_empty_or_null,
            [NumericTrueFalse.true],
        )
        .col_vals_in_set(
            CQCLVal.registered_manager_names_has_no_empty_or_null,
            [NumericTrueFalse.true],
        )
        .col_vals_in_set(
            CQCLVal.specialisms_offered_has_no_empty_or_null, [NumericTrueFalse.true]
        )
        # categorical column values match expected set
        .col_vals_in_set(IndCQC.type, [LocationType.social_care_identifier])
        .col_vals_in_set(IndCQC.registration_status, [RegistrationStatus.registered])
        .col_vals_in_set(
            IndCQC.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.primary_service_type_second_level,
            CatValues.primary_service_type_second_level_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.care_home, CatValues.care_home_column_values.categorical_values
        )
        .col_vals_in_set(
            IndCQC.cqc_sector, CatValues.sector_column_values.categorical_values
        )
        .col_vals_in_set(
            IndCQC.related_location,
            CatValues.related_location_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.specialism_dementia,
            CatValues.specialism_dementia_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.specialism_learning_disabilities,
            CatValues.specialism_learning_disabilities_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.specialism_mental_health,
            CatValues.specialism_mental_health_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.dormancy,
            # na_pass is not an optional parameter to .col_vals_in_set
            # extending list to allow for null values as not included
            # in categorical values
            [*CatValues.dormancy_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            IndCQC.contemporary_cssr,
            CatValues.contemporary_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.contemporary_region,
            CatValues.contemporary_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.current_rural_urban_ind_11,
            CatValues.current_rui_column_values.categorical_values,
        )
        # catagorical column unique values count matches expected count
        .specially(
            vl.is_unique_count_equal(
                IndCQC.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.primary_service_type_second_level,
                CatValues.primary_service_type_second_level_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.primary_service_type_second_level} needs to be one of {CatValues.primary_service_type_second_level_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.care_home} needs to be one of {CatValues.care_home_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.cqc_sector,
                CatValues.sector_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.cqc_sector} needs to be one of {CatValues.sector_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.related_location,
                CatValues.related_location_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.related_location} needs to be one of {CatValues.related_location_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.specialism_dementia,
                CatValues.specialism_dementia_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.specialism_dementia} needs to be one of {CatValues.specialism_dementia_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.specialism_learning_disabilities,
                CatValues.specialism_learning_disabilities_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.specialism_learning_disabilities} needs to be one of {CatValues.specialism_learning_disabilities_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.specialism_mental_health,
                CatValues.specialism_mental_health_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.specialism_mental_health} needs to be one of {CatValues.specialism_mental_health_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.dormancy} needs to be null, or one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.contemporary_cssr,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.contemporary_cssr} needs to be null, or one of {CatValues.contemporary_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.contemporary_region,
                CatValues.contemporary_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.contemporary_region} needs to be null, or one of {CatValues.contemporary_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_cssr} needs to be null, or one of {CatValues.current_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_region} needs to be null, or one of {CatValues.current_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_rural_urban_ind_11,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_rural_urban_ind_11} needs to be null, or one of {CatValues.current_rui_column_values.categorical_values}",
        )
        # numeric column values are between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(Validation.provider_id_length, 3, 14)
        # numeric column values are greater than or equal to
        .col_vals_ge(IndCQC.number_of_beds, 0, na_pass=True)
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
        (
            "--compare_path",
            "The filepath to a dataset to compare against for expected size",
        ),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path, args.compare_path)
    print(f"Validation of {args.source_path} complete")
