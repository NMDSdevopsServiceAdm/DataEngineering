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
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatValues,
)

KEY_COLUMNS = [
    IndCQC.cqc_location_import_date,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.current_region,
    IndCQC.primary_service_type,
]

PERIOD_LABEL_SHORT_REGEX = r"^[A-Z][a-z]{2} \d{4}$"
PERIOD_LABEL_LONG_REGEX = r"^[A-Z][a-z]{2,8} \d{4}$"

# The period label parsed back to a date equals the month before the import date.
# Unparseable labels become null and fail, rather than raising before a report.
PERIOD_LABEL_SHORT_MATCHES_IMPORT_DATE_EXPR = (
    pl.col(Pub.period_label_short).str.to_date("%b %Y", strict=False)
    == pl.col(IndCQC.cqc_location_import_date).dt.offset_by("-1mo").dt.month_start()
)
PERIOD_LABEL_LONG_MATCHES_IMPORT_DATE_EXPR = (
    pl.col(Pub.period_label_long).str.to_date("%B %Y", strict=False)
    == pl.col(IndCQC.cqc_location_import_date).dt.offset_by("-1mo").dt.month_start()
)

METRIC_COLUMNS = [
    Pub.filled_posts,
    Pub.locationid_count,
    Pub.filtered_filled_posts_long,
    Pub.filtered_locationid_count_long,
    Pub.filtered_ct_total_employed_long,
    Pub.filtered_filled_posts_medium,
    Pub.filtered_locationid_count_medium,
    Pub.filtered_ct_total_employed_medium,
    Pub.filtered_filled_posts_short,
    Pub.filtered_locationid_count_short,
    Pub.filtered_ct_total_employed_short,
    Pub.unfiltered_ct_total_employed_long,
]

# The percentage change columns are excluded as they are null for periods with no
# baseline.
NOT_NULL_COLUMNS = [
    *KEY_COLUMNS,
    Pub.period_label_short,
    Pub.period_label_long,
    *METRIC_COLUMNS,
]

# Each filtered column is a sum over a subset of the rows summed by its total column.
# The tolerance absorbs Float32 summation order differences.
FILTERED_TOTAL_TOLERANCE = 1.0001
FILTERED_AND_TOTAL_COLUMNS = [
    (Pub.filtered_filled_posts_long, Pub.filled_posts),
    (Pub.filtered_filled_posts_medium, Pub.filled_posts),
    (Pub.filtered_filled_posts_short, Pub.filled_posts),
    (Pub.filtered_locationid_count_long, Pub.locationid_count),
    (Pub.filtered_locationid_count_medium, Pub.locationid_count),
    (Pub.filtered_locationid_count_short, Pub.locationid_count),
    (Pub.filtered_ct_total_employed_long, Pub.unfiltered_ct_total_employed_long),
    (Pub.filtered_ct_total_employed_medium, Pub.unfiltered_ct_total_employed_long),
    (Pub.filtered_ct_total_employed_short, Pub.unfiltered_ct_total_employed_long),
]

# The rollup labels are included, so a missing rollup fails the distinct value count.
CATEGORICAL_COLUMNS = [
    (
        IndCQC.main_job_role_clean_labelled,
        [
            *CatValues.main_job_role_labels_column_values.categorical_values,
            PublishedJobGroupLabels.all_job_roles,
        ],
    ),
    (
        IndCQC.current_region,
        PublishedRegion(IndCQC.current_region).categorical_values,
    ),
    (
        IndCQC.primary_service_type,
        [
            *CatValues.primary_service_type_column_values.categorical_values,
            PublishedMainService.all_locations,
            PublishedMainService.all_care_homes,
        ],
    ),
]

EXPECTED_SCHEMA = pb.Schema(
    columns={
        IndCQC.cqc_location_import_date: "Date",
        Pub.period_label_short: "String",
        Pub.period_label_long: "String",
        IndCQC.main_job_role_clean_labelled: str(CategoricalColumnTypes.JobRoleCatType),
        IndCQC.current_region: "Categorical",
        IndCQC.primary_service_type: "Categorical",
        Pub.filled_posts: "Float32",
        Pub.locationid_count: "UInt32",
        Pub.filtered_filled_posts_long: "Float32",
        Pub.filtered_locationid_count_long: "UInt32",
        Pub.filtered_ct_total_employed_long: "Float32",
        Pub.filtered_filled_posts_medium: "Float32",
        Pub.filtered_locationid_count_medium: "UInt32",
        Pub.filtered_ct_total_employed_medium: "Float32",
        Pub.filtered_filled_posts_short: "Float32",
        Pub.filtered_locationid_count_short: "UInt32",
        Pub.filtered_ct_total_employed_short: "Float32",
        Pub.unfiltered_ct_total_employed_long: "Float32",
        Pub.filtered_ct_cum_perc_change_long: "Float32",
        Pub.filtered_ct_cum_perc_change_medium: "Float32",
        Pub.filtered_ct_cum_perc_change_short: "Float32",
        Pub.unfiltered_ct_total_employed_cum_perc_change_long: "Float32",
        Pub.filled_posts_cum_perc_change_long: "Float32",
        Pub.filled_posts_cum_perc_change_short: "Float32",
        Pub.locationid_count_cum_perc_change_short: "Float32",
        Pub.filled_posts_april_perc_change: "Float32",
        Pub.locationid_count_april_perc_change: "Float32",
    }
)

PREP_COLUMNS = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.primary_service_type,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.estimate_filled_posts_by_job_role,
    IndCQC.current_region,
    Pub.period_label_short,
    Pub.period_label_long,
]


def count_expected_assessment_rows(prep_source: str) -> int:
    """
    Counts the rows the assessment should hold for a prepared run.

    Builds the publication rows and rollup rows without the assessment columns,
    so only the prepared columns the row grouping needs are read.

    Args:
        prep_source (str): path of the prepared data of one run

    Returns:
        int: the number of rows expected in the assessment data
    """
    prepared_lf = utils.scan_parquet(prep_source, selected_columns=PREP_COLUMNS)

    summary_lf = publication_utils.aggregate_to_publication_rows(
        prepared_lf, include_assessment=False
    )
    summary_lf = publication_utils.add_rows_for_publication_groups(
        prepared_lf, summary_lf, include_assessment=False
    )

    return summary_lf.select(pl.len()).collect(engine="streaming").item()


def main(
    bucket_name: str,
    prep_source: str,
    assessment_source: str,
    reports_path: str,
    run_number: int | None = None,
) -> None:
    """
    Validates the assessment data of one run.

    Checks schema, row count against the prepared run, key uniqueness, nulls,
    value ranges, filtered values against their totals, the period label, and
    categories including the rollup labels.

    Args:
        bucket_name (str): the bucket (name only) holding the datasets and reports
        prep_source (str): path holding the prepared data of every run
        assessment_source (str): path holding the assessment data of every run
        reports_path (str): the output path to write reports to
        run_number (int | None): the run to validate. Defaults to None, the latest run
    """
    prep_run_source, assessment_run_source = merge_utils.resolve_run_sources(
        [
            f"s3://{bucket_name}/{prep_source}",
            f"s3://{bucket_name}/{assessment_source}",
        ],
        run_number,
    )

    assessment_df = utils.read_parquet(
        assessment_run_source,
        selected_columns=[column for column, _ in EXPECTED_SCHEMA.columns],
    )
    expected_row_count = count_expected_assessment_rows(prep_run_source)

    validation = (
        pb.Validate(
            data=assessment_df,
            label=f"Validation of {assessment_source}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .col_schema_match(
            schema=EXPECTED_SCHEMA,
            complete=False,
            brief="Assessment columns should match the expected (partial) schema",
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows, the publication and rollup rows of the prepared run",
        )
        .rows_distinct(
            columns_subset=KEY_COLUMNS,
            brief="Primary key (import date, job role, region, service type) should be unique",
        )
        .col_vals_not_null(
            columns=NOT_NULL_COLUMNS,
            brief="Key, period label and assessment sum and count columns should contain no null values",
        )
        .col_vals_ge(
            columns=METRIC_COLUMNS,
            value=0,
            brief="Assessment sum and count columns should not be negative",
        )
        .col_vals_regex(
            Pub.period_label_short,
            PERIOD_LABEL_SHORT_REGEX,
            brief="period_label_short should be formatted as 'Mar 2026'",
        )
        .col_vals_expr(
            PERIOD_LABEL_SHORT_MATCHES_IMPORT_DATE_EXPR,
            brief="period_label_short should be the month before the import date",
        )
        .col_vals_regex(
            Pub.period_label_long,
            PERIOD_LABEL_LONG_REGEX,
            brief="period_label_long should be formatted as 'March 2026'",
        )
        .col_vals_expr(
            PERIOD_LABEL_LONG_MATCHES_IMPORT_DATE_EXPR,
            brief="period_label_long should be the month before the import date",
        )
    )

    for filtered_column, total_column in FILTERED_AND_TOTAL_COLUMNS:
        validation = validation.col_vals_expr(
            pl.col(filtered_column) <= pl.col(total_column) * FILTERED_TOTAL_TOLERANCE,
            brief=f"{filtered_column} should not exceed {total_column}",
        )

    for column, allowed_values in CATEGORICAL_COLUMNS:
        validation = validation.col_vals_in_set(
            column,
            allowed_values,
            brief=f"{column} should only contain {allowed_values}",
        ).specially(
            vl.is_unique_count_equal(column, len(allowed_values)),
            brief=f"{column} should have exactly {len(allowed_values)} distinct values",
        )

    validation = validation.interrogate()
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source datasets and validation report"),
        (
            "--prep_source",
            "The filepath of the prepared data, holding all runs",
        ),
        (
            "--assessment_source",
            "The filepath of the assessment data, holding all runs",
        ),
        ("--reports_path", "The filepath to output reports"),
        (
            "--run_number",
            "Run number to validate, or 'latest'",
            False,
            merge_utils.LATEST_RUN,
        ),
    )
    print(f"Starting validation for {args.assessment_source}")

    main(
        bucket_name=args.bucket_name,
        prep_source=args.prep_source,
        assessment_source=args.assessment_source,
        reports_path=args.reports_path,
        run_number=merge_utils.parse_run_number(args.run_number),
    )
    print(f"Validation of {args.assessment_source} complete")
