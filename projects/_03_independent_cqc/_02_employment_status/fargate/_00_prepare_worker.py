import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as pWorkerUtils
from polars_utils import utils
from polars_utils.filtering_utils import not_null_filter_expr
from projects._03_independent_cqc.utils.filtering_utils import get_matched_ascwds_dates
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(
    cleaned_ascwds_worker_source: str,
    metadata_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS worker dataset and prepare it for employment
    status estimation.

    Drops rows with a null location_id (ASCWDS includes non-CQC locations) and
    reduces import dates to the ASCWDS import dates metadata_source has already
    CQC-matched, so the output's import dates line up with job_role_estimates when
    the merge step joins them.

    Worker files carry no CQC-matched date of their own, so this filters against the
    dates matched to the *workplace* import date column in metadata_source -
    workplace and worker files always land on the same day, so that match is valid
    for reducing worker data too.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ascwds worker data
        metadata_source (str): path to the metadata dataset whose ASCWDS import dates
            have already been CQC-matched, used to select which dates to keep
        prepared_data_destination (str): destination for output
    """
    worker_lf = (
        utils.scan_parquet(cleaned_ascwds_worker_source).filter(
            not_null_filter_expr(column=AWKClean.location_id)
        )
        # .implode() wraps the matched dates into a single list value - without it,
        # is_in() treats a Series of the same dtype as the filtered column as
        # ambiguous (row-wise set membership vs. a literal list to check against).
        .filter(
            pl.col(AWKClean.ascwds_worker_import_date).is_in(
                get_matched_ascwds_dates(
                    metadata_source, IndCQC.ascwds_workplace_import_date
                ).implode()
            )
        )
    )
    worker_lf = pWorkerUtils.collapse_job_roles_to_published_labels(worker_lf)

    employment_status_summary_lf = pWorkerUtils.aggregate_employment_status_data(
        worker_lf
    )
    employment_status_summary_lf = pWorkerUtils.reshape_employment_status_data(
        employment_status_summary_lf
    )

    utils.sink_to_parquet(
        lazy_df=employment_status_summary_lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--metadata_source",
            "Source s3 directory for metadata",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared worker data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        metadata_source=args.metadata_source,
        prepared_data_destination=args.prepared_data_destination,
    )
