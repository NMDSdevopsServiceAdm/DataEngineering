import polars as pl
import polars.selectors as cs

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.prepare_workplace_utils as pWorkplaceUtils
from polars_utils import utils
from polars_utils.filtering_utils import not_null_filter_expr
from projects._03_independent_cqc.utils.filtering_utils import get_matched_ascwds_dates
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(
    cleaned_ascwds_workplace_source: str,
    metadata_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS workplace dataset and then:
        - remove rows with a null location_id (ASCWDS includes non-CQC locations).
        - reduce rows to the ASCWDS import dates metadata_source has already CQC-matched.
        - merge unpublished roles into 'other' groups
        - relabel job role columns from jrNN codes to published labels
        - reshape job role columns into one row per job role

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        metadata_source (str): path to the metadata dataset whose ASCWDS import dates
            have already been CQC-matched, used to select which dates to keep
        prepared_data_destination (str): destination for output
    """
    workplace_lf = (
        utils.scan_parquet(cleaned_ascwds_workplace_source).filter(
            not_null_filter_expr(column=AWPClean.location_id)
        )
        # .implode() wraps the matched dates into a single list value - without it,
        # is_in() treats a Series of the same dtype as the filtered column as
        # ambiguous (row-wise set membership vs. a literal list to check against).
        .filter(
            pl.col(AWPClean.ascwds_workplace_import_date).is_in(
                get_matched_ascwds_dates(
                    metadata_source, IndCQC.ascwds_workplace_import_date
                ).implode()
            )
        )
    )

    # These columns refer to the total for all job roles (28) and the total for job groups (29-32).
    # They are not real ASC-WDS job role codes (not in MainJobRoleID), so they must be
    # dropped before reduce_to_published_roles runs.
    workplace_lf = workplace_lf.drop(cs.matches(r"^jr(28|29|30|31|32)"))

    workplace_lf = pWorkplaceUtils.reduce_to_published_roles(workplace_lf)
    workplace_lf = pWorkplaceUtils.relabel_job_role_columns(workplace_lf)
    workplace_job_role_lf = pWorkplaceUtils.reshape_job_role_cols_to_rows(workplace_lf)

    utils.sink_to_parquet(
        lazy_df=workplace_job_role_lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--metadata_source",
            "Source s3 directory for metadata",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared data",
        ),
    )
    main(
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        metadata_source=args.metadata_source,
        prepared_data_destination=args.prepared_data_destination,
    )
