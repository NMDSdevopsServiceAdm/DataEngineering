import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_columns_by_dataset import (
    CleanedIndCQCCategoricalValues as CatValues,
)

merged_locations_columns_to_import = [
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_sector,
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
        f"s3://{bucket_name}/{source_path}", exclude_complex_types=True
    ).with_columns(
        str_length_cols([IndCqcColumns.location_id]),
    )
    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=merged_locations_columns_to_import,
    )
    # TODO: Add filter function to reduce compare_df to earliest file per month and remove dual registrations.

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size - Uncomment when TODO above is done.
        # .row_count_match(
        #     expected_row_count,
        #     brief=f"Cleaned Ind CQC data file has {source_df.height} rows but expecting {expected_row_count} rows",
        # )
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.ascwds_workplace_import_date,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.care_home,
                IndCqcColumns.provider_id,
                IndCqcColumns.cqc_sector,
                IndCqcColumns.imputed_registration_date,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.contemporary_ons_import_date,
                IndCqcColumns.contemporary_cssr,
                IndCqcColumns.contemporary_region,
                IndCqcColumns.current_ons_import_date,
                IndCqcColumns.current_cssr,
                IndCqcColumns.current_region,
                IndCqcColumns.current_rural_urban_indicator_2011,
                IndCqcColumns.ascwds_filtering_rule,
                IndCqcColumns.specialism_dementia,
                IndCqcColumns.specialism_learning_disabilities,
                IndCqcColumns.specialism_mental_health,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
            ],
        )
        # above
        .col_vals_ge(IndCqcColumns.time_registered, 1)
        # between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(IndCqcColumns.number_of_beds, 0, 500, na_pass=True)
        .col_vals_between(
            IndCqcColumns.pir_people_directly_employed_cleaned, 1, 1500, na_pass=True
        )
        .col_vals_between(IndCqcColumns.total_staff_bounded, 1, 3000, na_pass=True)
        .col_vals_between(IndCqcColumns.worker_records_bounded, 1, 3000, na_pass=True)
        .col_vals_between(
            IndCqcColumns.filled_posts_per_bed_ratio, 0.0, 20.0, na_pass=True
        )
        # categorical
        .col_vals_in_set(
            IndCqcColumns.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.cqc_sector,
            CatValues.sector_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.dormancy,
            [*CatValues.dormancy_column_values.categorical_values, None],
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.contemporary_cssr,
            CatValues.contemporary_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.contemporary_region,
            CatValues.contemporary_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_cssr,
            CatValues.current_cssr_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_region,
            CatValues.current_region_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_rural_urban_indicator_2011,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.ascwds_filled_posts_source,
            CatValues.ascwds_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.ascwds_filtering_rule,
            CatValues.ascwds_filtering_rule_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.related_location,
            CatValues.related_location_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.specialism_dementia,
            CatValues.specialism_dementia_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.specialism_learning_disabilities,
            CatValues.specialism_learning_disabilities_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.specialism_mental_health,
            CatValues.specialism_mental_health_column_values.categorical_values,
        )
        # distinct values
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.care_home} needs to be one of {CatValues.care_home_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.cqc_sector,
                CatValues.sector_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.cqc_sector} needs to be one of {CatValues.sector_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.dormancy} needs to be one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.contemporary_cssr,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.contemporary_cssr} needs to be one of {CatValues.contemporary_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.contemporary_region,
                CatValues.contemporary_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.contemporary_region} needs to be one of {CatValues.contemporary_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_cssr} needs to be one of {CatValues.current_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_region} needs to be one of {CatValues.current_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_rural_urban_indicator_2011,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_rural_urban_indicator_2011} needs to be one of {CatValues.current_rui_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.ascwds_filled_posts_source,
                CatValues.ascwds_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_filled_posts_source} needs to be one of {CatValues.ascwds_filled_posts_source_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.ascwds_filtering_rule,
                CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_filtering_rule} needs to be one of {CatValues.ascwds_filtering_rule_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.related_location,
                CatValues.related_location_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.related_location} needs to be one of {CatValues.related_location_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.specialism_dementia,
                CatValues.specialism_dementia_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.specialism_dementia} needs to be one of {CatValues.specialism_dementia_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.specialism_learning_disabilities,
                CatValues.specialism_learning_disabilities_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.specialism_learning_disabilities} needs to be one of {CatValues.specialism_learning_disabilities_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.specialism_mental_health,
                CatValues.specialism_mental_health_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.specialism_mental_health} needs to be one of {CatValues.specialism_mental_health_column_values.categorical_values}",
        )
        # TODO - Add custom validation rule that the data in carehome and primary_service_type should be related.
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
