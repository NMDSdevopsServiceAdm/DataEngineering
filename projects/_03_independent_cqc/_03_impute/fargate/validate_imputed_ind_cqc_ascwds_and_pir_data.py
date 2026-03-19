import sys
import time

import pointblank as pb

from polars_utils import utils
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.validation_table_columns import Validation
from utils.column_values.categorical_columns_by_dataset import (
    ImputedIndCqcAscwdsAndPirCategoricalValues as CatValues,
)

cleaned_ind_cqc_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
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
        str_length_cols([IndCQC.location_id]),
    )
    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=cleaned_ind_cqc_columns_to_import,
    )
    expected_row_count = compare_df.height

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
            brief=f"Imputed Ind CQC data file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCQC.location_id,
                IndCQC.ascwds_workplace_import_date,
                IndCQC.cqc_location_import_date,
                IndCQC.care_home,
                IndCQC.provider_id,
                IndCQC.cqc_sector,
                IndCQC.imputed_registration_date,
                IndCQC.primary_service_type,
                IndCQC.contemporary_ons_import_date,
                IndCQC.contemporary_cssr,
                IndCQC.contemporary_region,
                IndCQC.current_ons_import_date,
                IndCQC.current_cssr,
                IndCQC.current_region,
                IndCQC.current_rural_urban_indicator_2011,
                IndCQC.ascwds_filtering_rule,
                IndCQC.unix_time,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCQC.location_id,
                IndCQC.cqc_location_import_date,
            ],
        )
        # between (inclusive)
        .col_vals_between(Validation.location_id_length, 3, 14)
        .col_vals_between(IndCQC.number_of_beds, 1, 500)
        .col_vals_between(IndCQC.pir_people_directly_employed_cleaned, 1, 1500)
        .col_vals_between(IndCQC.total_staff_bounded, 1, 3000)
        .col_vals_between(IndCQC.worker_records_bounded, 1, 3000)
        .col_vals_between(IndCQC.filled_posts_per_bed_ratio, 0.0, 20.0)
        # .col_vals_between(IndCQC.posts_rolling_average_model, 0.0, 3000.0)
        # .col_vals_between(IndCQC.imputed_filled_post_model, 0.0, 3000.0)
        # .col_vals_between(IndCQC.imputed_filled_posts_per_bed_ratio_model, 0.0, 3000.0)
        # .col_vals_between(
        #     IndCQC.unix_time, 1262304000, int(time.time())
        # )  # 1st Jan 2010 in unix time and current unix time
        # .col_vals_between(IndCQC.pir_filled_posts_model, 0.0, 3000.0)
        # categorical
        .col_vals_in_set(
            IndCQC.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.cqc_sector,
            CatValues.sector_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.dormancy,
            CatValues.dormancy_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
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
            IndCQC.current_rural_urban_indicator_2011,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.ascwds_filled_posts_source,
            CatValues.ascwds_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCQC.ascwds_filtering_rule,
            CatValues.ascwds_filtering_rule_column_values.categorical_values,
        )
        # distinct values
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
                IndCQC.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.dormancy} needs to be one of {CatValues.dormancy_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.contemporary_cssr,
                CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.contemporary_cssr} needs to be one of {CatValues.contemporary_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.contemporary_region,
                CatValues.contemporary_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.contemporary_region} needs to be one of {CatValues.contemporary_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_cssr} needs to be one of {CatValues.current_cssr_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_region} needs to be one of {CatValues.current_region_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.current_rural_urban_indicator_2011,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.current_rural_urban_indicator_2011} needs to be one of {CatValues.current_rui_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.ascwds_filled_posts_source,
                CatValues.ascwds_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.ascwds_filled_posts_source} needs to be one of {CatValues.ascwds_filled_posts_source_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCQC.ascwds_filtering_rule,
                CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCQC.ascwds_filtering_rule} needs to be one of {CatValues.ascwds_filtering_rule_column_values.categorical_values}",
        )
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
