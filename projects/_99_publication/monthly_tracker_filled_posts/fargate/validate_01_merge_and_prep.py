import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    publication_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)
from utils.value_labels.ons_pd.onspd_icb import OnspdIcb
from utils.value_labels.ons_pd.onspd_lsoa21 import OnspdLsoa21
from utils.value_labels.ons_pd.onspd_msoa21 import OnspdMsoa21

KEY_COLUMNS = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.main_job_role_clean_labelled,
]

# The capacity tracker columns are excluded as they can legitimately be null.
NOT_NULL_COLUMNS = [
    IndCQC.id_per_locationid_import_date,
    *KEY_COLUMNS,
    IndCQC.primary_service_type,
    IndCQC.main_job_group_labelled,
    IndCQC.estimate_filled_posts_by_job_role,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
]

# id_per_locationid_import_date is one-to-one with location_id and import date.
ONE_ID_PER_LOCATION_IMPORT_DATE_EXPR = (
    pl.col(IndCQC.id_per_locationid_import_date)
    .n_unique()
    .over(IndCQC.location_id, IndCQC.cqc_location_import_date)
    == 1
)
ONE_LOCATION_IMPORT_DATE_PER_ID_EXPR = (
    pl.struct(IndCQC.location_id, IndCQC.cqc_location_import_date)
    .n_unique()
    .over(IndCQC.id_per_locationid_import_date)
    == 1
)

# Categorical columns and their allowed values; location_id is excluded as it has
# too many values to list.
CATEGORICAL_COLUMNS = [
    (IndCQC.primary_service_type, CatValues.primary_service_type_column_values),
    (
        IndCQC.main_job_role_clean_labelled,
        CatValues.main_job_role_labels_column_values,
    ),
    (IndCQC.main_job_group_labelled, CatValues.main_job_group_labels_column_values),
    (IndCQC.current_cssr, CatValues.current_cssr_column_values),
    (IndCQC.current_region, CatValues.current_region_column_values),
    (IndCQC.current_rural_urban_indicator_2011, CatValues.current_rui_column_values),
]

# Not every ICB/LSOA/MSOA has a location, so only allowed values are checked.
IN_SET_ONLY_COLUMNS = [
    (IndCQC.current_icb, list(OnspdIcb.labels_dict.values())),
    (IndCQC.current_lsoa21, list(OnspdLsoa21.labels_dict.values())),
    (IndCQC.current_msoa21, list(OnspdMsoa21.labels_dict.values())),
]

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCQC.id_per_locationid_import_date: "UInt32",
        IndCQC.location_id: str(CategoricalColumnTypes.LocationCatType),
        IndCQC.cqc_location_import_date: "Date",
        IndCQC.primary_service_type: str(CategoricalColumnTypes.PrimaryServiceEnumType),
        IndCQC.main_job_role_clean_labelled: str(CategoricalColumnTypes.JobRoleCatType),
        IndCQC.main_job_group_labelled: str(CategoricalColumnTypes.JobGroupCatType),
        IndCQC.estimate_filled_posts_by_job_role: "Float32",
        IndCQC.current_cssr: "Categorical",
        IndCQC.current_region: "Categorical",
        IndCQC.current_icb: "Categorical",
        IndCQC.current_rural_urban_indicator_2011: "Categorical",
        IndCQC.current_lsoa21: "Categorical",
        IndCQC.current_msoa21: "Categorical",
        IndCQC.ct_care_home_total_employed_imputed: "Float32",
        IndCQC.ct_non_res_care_workers_employed_imputed: "Float32",
    }
)


def main(
    bucket_name: str,
    jr_archive_estimates_source: str,
    jr_archive_metadata_source: str,
    prep_source: str,
    reports_path: str,
    run_number: int | None = None,
) -> None:
    """
    Validates the prepared data of one archived run.

    Checks schema, row count against the retained archived estimates, key
    uniqueness, nulls, value ranges and categories, and the period label.

    Args:
        bucket_name (str): the bucket (name only) holding the datasets and reports
        jr_archive_estimates_source (str): path of the archived job role estimates
        jr_archive_metadata_source (str): path of the archived job role metadata
        prep_source (str): path holding the prepared data of every run
        reports_path (str): the output path to write reports to
        run_number (int | None): the run to validate. Defaults to None, the latest run
    """
    estimates_run_source, _ = merge_utils.resolve_run_sources(
        [
            f"s3://{bucket_name}/{jr_archive_estimates_source}",
            f"s3://{bucket_name}/{jr_archive_metadata_source}",
        ],
        run_number,
    )
    prep_run_source = merge_utils.run_partition_path(
        f"s3://{bucket_name}/{prep_source}", estimates_run_source
    )

    prep_df = utils.read_parquet(
        prep_run_source,
        selected_columns=[column for column, _ in EXPECTED_SCHEMA.columns],
    )

    cutoff_date = publication_utils.get_retention_cutoff_date()
    expected_row_count = (
        utils.scan_parquet(
            estimates_run_source, selected_columns=[IndCQC.cqc_location_import_date]
        )
        .filter(publication_utils.reduced_data_filter_expr(cutoff_date=cutoff_date))
        .select(pl.len())
        .collect()
        .item()
    )

    validation = (
        pb.Validate(
            data=prep_df,
            label=f"Validation of {prep_source}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .col_schema_match(
            schema=EXPECTED_SCHEMA,
            complete=False,
            brief="Prepared columns should match the expected (partial) schema",
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows, the retained rows of the archived estimates",
        )
        .rows_distinct(
            columns_subset=KEY_COLUMNS,
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .col_vals_not_null(
            columns=NOT_NULL_COLUMNS,
            brief="Key, estimate, period and joined metadata columns should contain no null values",
        )
        .col_vals_expr(
            ONE_ID_PER_LOCATION_IMPORT_DATE_EXPR,
            brief="Each location_id and cqc_location_import_date should have one id_per_locationid_import_date",
        )
        .col_vals_expr(
            ONE_LOCATION_IMPORT_DATE_PER_ID_EXPR,
            brief="Each id_per_locationid_import_date should have one location_id and cqc_location_import_date",
        )
        .col_vals_ge(
            IndCQC.cqc_location_import_date,
            cutoff_date,
            brief=f"cqc_location_import_date should not be before the retention cutoff {cutoff_date}",
        )
        .col_vals_ge(
            IndCQC.estimate_filled_posts_by_job_role,
            0,
            brief="Estimated filled posts by job role should not be negative",
        )
    )

    for column, column_values in CATEGORICAL_COLUMNS:
        validation = validation.col_vals_in_set(
            column,
            column_values.categorical_values,
            brief=f"{column} should only contain {column_values.categorical_values}",
        ).specially(
            vl.is_unique_count_equal(column, column_values.count_of_categorical_values),
            brief=f"{column} should have exactly {column_values.count_of_categorical_values} distinct values",
        )

    for column, allowed_values in IN_SET_ONLY_COLUMNS:
        validation = validation.col_vals_in_set(
            column,
            allowed_values,
            brief=f"{column} should only contain known {column} labels",
        )

    validation = validation.interrogate()
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source datasets and validation report"),
        (
            "--jr_archive_estimates_source",
            "The filepath of the archived job role estimates, holding all runs",
        ),
        (
            "--jr_archive_metadata_source",
            "The filepath of the archived job role metadata, holding all runs",
        ),
        (
            "--prep_source",
            "The filepath of the prepared data, holding all runs",
        ),
        ("--reports_path", "The filepath to output reports"),
        (
            "--run_number",
            "Archived run number to validate, or 'latest'",
            False,
            merge_utils.LATEST_RUN,
        ),
    )
    print(f"Starting validation for {args.prep_source}")

    main(
        bucket_name=args.bucket_name,
        jr_archive_estimates_source=args.jr_archive_estimates_source,
        jr_archive_metadata_source=args.jr_archive_metadata_source,
        prep_source=args.prep_source,
        reports_path=args.reports_path,
        run_number=merge_utils.parse_run_number(args.run_number),
    )
    print(f"Validation of {args.prep_source} complete")
