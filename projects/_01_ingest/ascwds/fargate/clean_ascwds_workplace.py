import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

columns_to_import = [
    AWPClean.organisation_id,
    AWPClean.period,
    AWPClean.establishment_id,
    AWPClean.establishment_id_from_nmds,
    AWPClean.parent_id,
    AWPClean.nmds_id,
    AWPClean.establishment_created_date,
    AWPClean.establishment_updated_date,
    AWPClean.master_update_date,
    AWPClean.last_logged_in,
    AWPClean.la_permission,
    AWPClean.is_bulk_uploader,
    AWPClean.is_parent,
    AWPClean.parent_permission,
    AWPClean.registration_type,
    AWPClean.provider_id,
    AWPClean.location_id,
    AWPClean.establishment_type,
    AWPClean.establishment_name,
    AWPClean.address,
    AWPClean.postcode,
    AWPClean.region_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.total_starters,
    AWPClean.total_leavers,
    AWPClean.total_vacancies,
    AWPClean.main_service_id,
    AWPClean.version,
    AWPClean.import_date,
]


def main(
    workplace_source: str,
    cleaned_workplace_destination: str,
    workplace_for_reconciliation_destination: str,
) -> None:
    """
    Clean raw AWS-WDS data.

    Args:
        workplace_source (str): path to the raw ascwds workplace data
        cleaned_workplace_destination (str): destination for cleaned ascwds workplace output
        workplace_for_reconciliation_destination (str): destination for reconciliation workplace output
    """
    lf = utils.scan_parquet(workplace_source, selected_columns=columns_to_import)

    lf = lf.filter(wUtils.valid_workplace_filter())

    lf = lf.with_columns(pl.col(AWPClean.nmds_id).str.strip_chars())

    lf = lf.rename({AWPClean.last_logged_in: AWPClean.last_logged_in_date})

    lf = cUtils.cast_date_strings_to_dates(lf)

    lf = cUtils.column_to_date(
        lf, AWPClean.import_date, AWPClean.ascwds_workplace_import_date
    )

    # Temporarily casting import_date to string to prevent it being bigint.
    lf = lf.with_columns(pl.col(AWPClean.import_date).cast(pl.String))

    # trello 1705
    # ascwds_workplace_df = cUtils.apply_categorical_labels(
    #     ascwds_workplace_df,
    #     ascwds_workplace_labels_dict,
    #     ascwds_workplace_labels_dict.keys(),
    #     add_as_new_column=False,
    # )

    # trello 1706
    # (
    #     ascwds_workplace_df,
    #     reconciliation_df,
    # ) = create_purged_dfs_for_reconciliation_and_data(ascwds_workplace_df)

    # trello 1707
    # ascwds_workplace_df = remove_workplaces_with_duplicate_location_ids(
    #     ascwds_workplace_df
    # )

    # trello 1709
    # ascwds_workplace_df = cUtils.cast_to_int(ascwds_workplace_df, COLUMNS_TO_BOUND)

    # trello 1708
    # ascwds_workplace_df = cUtils.set_column_bounds(
    #     ascwds_workplace_df,
    #     AWPClean.total_staff,
    #     AWPClean.total_staff_bounded,
    #     AscwdsScaleVariableLimits.total_staff_lower_limit,
    # )

    # trello 1708
    # ascwds_workplace_df = cUtils.set_column_bounds(
    #     ascwds_workplace_df,
    #     AWPClean.worker_records,
    #     AWPClean.worker_records_bounded,
    #     AscwdsScaleVariableLimits.worker_records_lower_limit,
    # )

    # trello 1710
    # reconciliation_df = reconciliation_df.select(cols_required_for_reconciliation_df)

    utils.sink_to_parquet(
        # trello 1710
        lazy_df=lf,
        output_path=cleaned_workplace_destination,
    )

    utils.sink_to_parquet(
        # trello 1710
        lazy_df=lf,
        output_path=workplace_for_reconciliation_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--workplace_source",
            "Source s3 directory for raw ascwds workplace data",
        ),
        (
            "--cleaned_workplace_destination",
            "Destination s3 directory for cleaned ascwds workplace output",
        ),
        (
            "--workplace_for_reconciliation_destination",
            "Destination s3 directory for reconciliation workplace output",
        ),
    )
    main(
        workplace_source=args.workplace_source,
        cleaned_workplace_destination=args.cleaned_workplace_destination,
        workplace_for_reconciliation_destination=args.workplace_for_reconciliation_destination,
    )
