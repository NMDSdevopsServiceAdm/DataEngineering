import gc
import sys
from datetime import date

import pointblank as pb
import polars as pl
import polars.selectors as cs
from dateutil.relativedelta import relativedelta

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)
from utils.value_labels.ons_pd.onspd_icb import OnspdIcb
from utils.value_labels.ons_pd.onspd_lsoa21 import OnspdLsoa21
from utils.value_labels.ons_pd.onspd_msoa21 import OnspdMsoa21

KEY_COLS = [
    IndCqcColumns.id_per_locationid_import_date,
]

CATEGORICAL_COLS = [
    IndCqcColumns.name,
    IndCqcColumns.provider_id,
    IndCqcColumns.services_offered,
    IndCqcColumns.care_home,
    IndCqcColumns.primary_service_type_second_level,
    IndCqcColumns.ascwds_filtering_rule,
    IndCqcColumns.current_cssr,
    IndCqcColumns.current_region,
    IndCqcColumns.current_icb,
    IndCqcColumns.current_rural_urban_indicator_2011,
    IndCqcColumns.current_lsoa21,
    IndCqcColumns.current_msoa21,
    IndCqcColumns.estimate_filled_posts_source,
    IndCqcColumns.dormancy,
]

DATE_COLS = [
    IndCqcColumns.imputed_registration_date,
    IndCqcColumns.ascwds_workplace_import_date,
    IndCqcColumns.current_ons_import_date,
]

NUMERIC_COLS = [
    IndCqcColumns.estimate_filled_posts,
    IndCqcColumns.ascwds_filled_posts_dedup_clean,
    IndCqcColumns.ascwds_pir_merged,
    IndCqcColumns.number_of_beds,
    IndCqcColumns.worker_records_bounded,
    IndCqcColumns.ascwds_job_role_counts,
]

ind_cqc_estimates_cols_to_import = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
]

ASCWDS_EARLIEST_IMPORT_DATE = date(2013, 3, 1)
CQC_EARLIEST_REGISTRATION_DATE = date(2010, 9, 13)

KEY_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.id_per_locationid_import_date: "UInt32",
    }
)
CATEGORICAL_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.name: "String",
        IndCqcColumns.provider_id: "String",
        IndCqcColumns.services_offered: "List(String)",
        IndCqcColumns.care_home: "String",
        IndCqcColumns.primary_service_type_second_level: "String",
        IndCqcColumns.ascwds_filtering_rule: "String",
        IndCqcColumns.current_cssr: "String",
        IndCqcColumns.current_region: "String",
        IndCqcColumns.current_icb: "String",
        IndCqcColumns.current_rural_urban_indicator_2011: "String",
        IndCqcColumns.current_lsoa21: "String",
        IndCqcColumns.current_msoa21: "String",
        IndCqcColumns.estimate_filled_posts_source: "String",
        IndCqcColumns.dormancy: "String",
    }
)
DATE_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.imputed_registration_date: "Date",
        IndCqcColumns.ascwds_workplace_import_date: "Date",
        IndCqcColumns.current_ons_import_date: "Date",
    }
)
NUMERIC_SCHEMA = pb.Schema(
    columns={
        IndCqcColumns.estimate_filled_posts: "Float64",
        IndCqcColumns.ascwds_filled_posts_dedup_clean: "Float64",
        IndCqcColumns.ascwds_pir_merged: "Float64",
        IndCqcColumns.number_of_beds: "Float64",
        IndCqcColumns.worker_records_bounded: "Float64",
        IndCqcColumns.ascwds_job_role_counts: "Float64",
    }
)


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """

    run_key_validation(source_path, compare_path, bucket_name, reports_path)
    gc.collect()
    run_categorical_validation(source_path, bucket_name, reports_path)
    gc.collect()
    run_date_validation(source_path, bucket_name, reports_path)
    gc.collect()
    run_numeric_validation(source_path, bucket_name, reports_path)
    gc.collect()


def cast_cols_back_to_string(df: pl.DataFrame) -> pl.DataFrame:
    """Casts categorical and enums to string for validation purposes."""
    return df.with_columns((cs.categorical() | cs.enum()).cast(pl.String))


def run_key_validation(source_path, compare_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=KEY_COLS,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=ind_cqc_estimates_cols_to_import,
    )

    key_validation = (
        pb.Validate(
            data=source_df,
            label=f"Key validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            pre=cast_cols_back_to_string,
            schema=KEY_SCHEMA,
            brief="All columns have the expected data types",
        )
        # Dataset size
        .row_count_match(
            count=compare_df.height,
            brief=(
                f"Source file has {source_df.height} rows but expecting "
                f"{compare_df.height} rows to match estimates dataset"
            ),
        )
        # Uniqueness
        .rows_distinct(
            columns_subset=[IndCqcColumns.id_per_locationid_import_date],
            brief="Primary key (id_per_locationid_import_date) should be unique",
        )
        # Completeness (no nulls)
        .col_vals_not_null(
            columns=[
                IndCqcColumns.id_per_locationid_import_date,
            ],
            brief="Required columns should contain no null values",
        ).interrogate()
    )

    vl.write_reports(key_validation, bucket_name, f"{reports_path}key/")
    del source_df, compare_df, key_validation


def run_categorical_validation(source_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=CATEGORICAL_COLS,
    )

    categorical_validation = (
        pb.Validate(
            data=source_df,
            label=f"Categorical validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            pre=cast_cols_back_to_string,
            schema=CATEGORICAL_SCHEMA,
            brief="All columns have the expected data types",
        )
        # Completeness (no nulls)
        .col_vals_not_null(
            columns=[
                IndCqcColumns.name,
                IndCqcColumns.provider_id,
                IndCqcColumns.services_offered,
                IndCqcColumns.care_home,
                IndCqcColumns.primary_service_type_second_level,
                IndCqcColumns.ascwds_filtering_rule,
                IndCqcColumns.current_cssr,
                IndCqcColumns.current_region,
                IndCqcColumns.current_icb,
                IndCqcColumns.current_rural_urban_indicator_2011,
                IndCqcColumns.current_lsoa21,
                IndCqcColumns.current_msoa21,
                IndCqcColumns.estimate_filled_posts_source,
            ],
            brief="Required columns should contain no null values",
        )
        # Categorical values
        .col_vals_in_set(
            IndCqcColumns.estimate_filled_posts_source,
            CatValues.estimate_filled_posts_source_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.primary_service_type_second_level,
            CatValues.primary_service_type_second_level_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.care_home,
            CatValues.care_home_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.ascwds_filtering_rule,
            CatValues.ascwds_filtering_rule_column_values.categorical_values,
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
            IndCqcColumns.current_icb,
            OnspdIcb.labels_dict.values(),
        )
        .col_vals_in_set(
            IndCqcColumns.current_rural_urban_indicator_2011,
            CatValues.current_rui_column_values.categorical_values,
        )
        .col_vals_in_set(
            IndCqcColumns.current_lsoa21,
            OnspdLsoa21.labels_dict.values(),
        )
        .col_vals_in_set(
            IndCqcColumns.current_msoa21,
            OnspdMsoa21.labels_dict.values(),
        )
        .col_vals_in_set(
            IndCqcColumns.dormancy,
            CatValues.dormancy_column_values.categorical_values,
        )
        # Distinct value counts
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.estimate_filled_posts_source,
                CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.estimate_filled_posts_source} should have exactly {CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.primary_service_type_second_level,
                CatValues.primary_service_type_second_level_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.primary_service_type_second_level} should have exactly {CatValues.primary_service_type_second_level_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.care_home,
                CatValues.care_home_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.care_home} should have exactly {CatValues.care_home_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_cssr,
                CatValues.current_cssr_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_cssr} should have exactly {CatValues.current_cssr_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_region,
                CatValues.current_region_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_region} should have exactly {CatValues.current_region_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_icb,
                len(OnspdIcb.labels_dict.values()),
            ),
            brief=f"{IndCqcColumns.current_icb} should have exactly {len(OnspdIcb.labels_dict.values())} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_rural_urban_indicator_2011,
                CatValues.current_rui_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.current_rural_urban_indicator_2011} should have exactly {CatValues.current_rui_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_lsoa21,
                len(OnspdLsoa21.labels_dict.values()),
            ),
            brief=f"{IndCqcColumns.current_lsoa21} should have exactly {len(OnspdLsoa21.labels_dict.values())} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.current_msoa21,
                len(OnspdMsoa21.labels_dict.values()),
            ),
            brief=f"{IndCqcColumns.current_msoa21} should have exactly {len(OnspdMsoa21.labels_dict.values())} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.ascwds_filtering_rule,
                CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.ascwds_filtering_rule} should have exactly {CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values} distinct values",
        )
        .specially(
            vl.is_unique_count_equal(
                IndCqcColumns.dormancy,
                CatValues.dormancy_column_values.count_of_categorical_values,
            ),
            brief=f"{IndCqcColumns.dormancy} should have exactly {CatValues.dormancy_column_values.count_of_categorical_values} distinct values",
        )
        .interrogate()
    )

    vl.write_reports(categorical_validation, bucket_name, f"{reports_path}categorical/")
    del source_df, categorical_validation


def run_date_validation(source_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=DATE_COLS,
    )

    ons_not_before = date.today() - relativedelta(months=13)

    date_validation = (
        pb.Validate(
            data=source_df,
            label=f"Date validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            pre=cast_cols_back_to_string,
            schema=DATE_SCHEMA,
            brief="All columns have the expected data types",
        )
        # Completeness (no nulls)
        .col_vals_not_null(
            columns=[
                IndCqcColumns.imputed_registration_date,
                IndCqcColumns.ascwds_workplace_import_date,
                IndCqcColumns.current_ons_import_date,
            ],
            brief="Required columns should contain no null values",
        )
        # Date plausibility
        .col_vals_ge(
            columns=[IndCqcColumns.ascwds_workplace_import_date],
            value=ASCWDS_EARLIEST_IMPORT_DATE,
            brief=f"ascwds_workplace_import_date should not be before {ASCWDS_EARLIEST_IMPORT_DATE.strftime('%d/%m/%Y')}",
        )
        .col_vals_ge(
            columns=[IndCqcColumns.imputed_registration_date],
            value=CQC_EARLIEST_REGISTRATION_DATE,
            brief=f"imputed_registration_date should not be before {CQC_EARLIEST_REGISTRATION_DATE.strftime('%d/%m/%Y')}",
        )
        .col_vals_ge(
            columns=[IndCqcColumns.current_ons_import_date],
            value=ons_not_before,
            brief=f"current_ons_import_date should not be more than 13 months ago (not before {ons_not_before.strftime('%d/%m/%Y')})",
        )
        .interrogate()
    )

    vl.write_reports(date_validation, bucket_name, f"{reports_path}date/")
    del source_df, date_validation


def run_numeric_validation(source_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=NUMERIC_COLS,
    )

    numeric_validation = (
        pb.Validate(
            data=source_df,
            label=f"Numeric validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # Schema check
        .col_schema_match(
            pre=cast_cols_back_to_string,
            schema=NUMERIC_SCHEMA,
            brief="All columns have the expected data types",
        )
        # Completeness (no nulls)
        .col_vals_not_null(
            columns=[
                IndCqcColumns.estimate_filled_posts,
            ],
            brief="Required columns should contain no null values",
        )
        # Numeric range — strictly positive (nulls allowed)
        .col_vals_gt(
            columns=[
                IndCqcColumns.ascwds_filled_posts_dedup_clean,
                IndCqcColumns.ascwds_pir_merged,
                IndCqcColumns.number_of_beds,
                IndCqcColumns.worker_records_bounded,
            ],
            value=0,
            na_pass=True,
            brief="ascwds_filled_posts_dedup_clean, ascwds_pir_merged, number_of_beds and wkrrecs_bounded should be > 0 where present",
        )
        # Cross-column numeric constraint
        .col_vals_expr(
            expr=(
                pl.col(IndCqcColumns.ascwds_job_role_counts).is_null()
                | pl.col(IndCqcColumns.estimate_filled_posts).is_null()
                | (
                    pl.col(IndCqcColumns.ascwds_job_role_counts)
                    <= pl.col(IndCqcColumns.estimate_filled_posts)
                )
            ),
            brief="ascwds_job_role_counts <= estimate_filled_posts where both are present",
        ).interrogate()
    )

    vl.write_reports(numeric_validation, bucket_name, f"{reports_path}numeric/")
    del source_df, numeric_validation


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
