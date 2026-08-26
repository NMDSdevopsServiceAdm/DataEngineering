import sys

import pointblank as pb
import polars as pl
import polars.selectors as cs

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from polars_utils.expressions import str_length_cols
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils import (
    validation_utils as vUtils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsCategoricalValues as CatValues,
)

imputed_ind_cqc_cols_to_import = [
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.location_id,
]

# Partial schema (col_schema_match complete=False) - just the narrowed columns.
EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.current_cssr: str(CatColType.OnsCssrCatType),
        IndCqcColumns.current_region: str(CatColType.OnsRegionCatType),
        IndCqcColumns.ascwds_filled_posts_source: str(
            CatColType.AscwdsFilledPostsSourceEnumType
        ),
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float32",
        IndCqcColumns.ascwds_pir_merged: "Float32",
        IndCqcColumns.estimate_filled_posts: "Float32",
        IndCqcColumns.estimate_filled_posts_source: str(
            CatColType.EstimatesFilledPostSourceEnumType
        ),
        IndCqcColumns.posts_rolling_average_model: "Float32",
        IndCqcColumns.care_home_model: "Float32",
        IndCqcColumns.non_res_with_dormancy_model: "Float32",
        IndCqcColumns.non_res_without_dormancy_model: "Float32",
        IndCqcColumns.imputed_pir_filled_posts_model: "Float32",
    }
)


def main(
    bucket_name: str, source_path: str, reports_path: str, compare_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (shoud correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
        compare_path (str): path to a dataset to compare against for expected size
    """

    raw_source_df = utils.read_parquet(
        f"s3://{bucket_name}/{source_path}"
    ).with_columns(str_length_cols([IndCqcColumns.location_id]))
    # services_offered (a List column) is needed by 3 of the custom business rules
    # below, so it's kept even though other Struct/List columns are excluded as
    # pointblank cannot validate them directly.
    source_df = raw_source_df.select(
        ~cs.by_dtype(pl.Struct, pl.List) | cs.by_name(IndCqcColumns.services_offered)
    )
    compare_df = utils.read_parquet(
        f"s3://{bucket_name}/{compare_path}",
        selected_columns=imputed_ind_cqc_cols_to_import,
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
        # dataset schema
        .col_schema_match(
            schema=EXPECTED_SCHEMA,
            complete=False,
            in_order=False,
            brief="Narrowed columns should match their expected Categorical/Enum/Float32 dtypes",
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Estimates file has {source_df.height} rows but expecting {expected_row_count} rows",
        )
        # complete columns
        .col_vals_not_null(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.ascwds_workplace_import_date,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.care_home,
                IndCqcColumns.primary_service_type,
                IndCqcColumns.primary_service_type_second_level,
                IndCqcColumns.current_ons_import_date,
                IndCqcColumns.current_cssr,
                IndCqcColumns.current_region,
                IndCqcColumns.estimate_filled_posts,
                IndCqcColumns.estimate_filled_posts_source,
            ]
        )
        # index columns
        .rows_distinct(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
            ],
        )
        # between (inclusive)
        .col_vals_between(IndCqcColumns.ascwds_filled_posts, 1.0, 3000.0, na_pass=True)
        .col_vals_between(IndCqcColumns.ascwds_pir_merged, 1.0, 3000.0, na_pass=True)
        .col_vals_between(IndCqcColumns.care_home_model, -100.0, 3000.0, na_pass=True)
        .col_vals_between(
            IndCqcColumns.imputed_posts_non_res_combined_model,
            -100.0,
            3000.0,
            na_pass=True,
        )
        .col_vals_between(
            IndCqcColumns.estimate_filled_posts, 1.0, 3000.0, na_pass=True
        )
        .col_vals_between(
            IndCqcColumns.non_res_with_dormancy_model, -100.0, 3000.0, na_pass=True
        )
        .col_vals_between(
            IndCqcColumns.non_res_without_dormancy_model, -100.0, 3000.0, na_pass=True
        )
        .col_vals_between(IndCqcColumns.number_of_beds, 1, 500, na_pass=True)
        .col_vals_between(
            IndCqcColumns.pir_people_directly_employed_dedup, 1, 3000, na_pass=True
        )
        .col_vals_between(
            IndCqcColumns.imputed_pir_filled_posts_model, -100.0, 3000.0, na_pass=True
        )
        .col_vals_between(IndCqcColumns.posts_rolling_average_model, 1.0, 3000.0)
        # categorical
        .col_vals_in_set(
            IndCqcColumns.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type,
            CatValues.primary_service_type_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type_second_level,
            CatValues.primary_service_type_second_level_column_values.categorical_values,
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
            IndCqcColumns.ascwds_filled_posts_source,
            [
                *CatValues.ascwds_filled_posts_source_column_values.categorical_values,
                None,
            ],
        )
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
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
                IndCqcColumns.primary_service_type,
                CatValues.primary_service_type_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type} needs to be one of {CatValues.primary_service_type_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type_second_level,
                CatValues.primary_service_type_second_level_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type_second_level} needs to be one of {CatValues.primary_service_type_second_level_column_values.categorical_values}",
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
                IndCqcColumns.ascwds_filled_posts_source,
                CatValues.ascwds_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_filled_posts_source} needs to be one of {CatValues.ascwds_filled_posts_source_column_values.categorical_values}",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} needs to be one of {CatValues.estimate_filled_posts_source_column_values.categorical_values}",
        )
        # custom business rules
        .col_vals_expr(
            vUtils.care_home_matches_primary_service_type_expr(),
            brief="care_home and primary_service_type should be related.",
        )
        .col_vals_expr(
            vUtils.shared_lives_services_offered_expr(),
            brief="If primary_service_type_second_level is 'Shared Lives', services_offered must contain 'Shared Lives'.",
        )
        .col_vals_expr(
            vUtils.care_home_with_nursing_services_offered_expr(),
            brief="If primary_service_type_second_level is 'Care home with nursing', services_offered must contain 'Care home service with nursing' and must not contain 'Shared Lives'.",
        )
        .col_vals_expr(
            vUtils.care_home_without_nursing_services_offered_expr(),
            brief="If primary_service_type_second_level is 'Care home without nursing', services_offered must contain 'Care home service without nursing' and must not contain 'Shared Lives' or 'Care home service with nursing'.",
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
