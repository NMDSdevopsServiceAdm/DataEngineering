import polars as pl

from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

ascwds_workplace_columns_to_import = [
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

job_role_cols = [
    value
    for role, value in vars(AWPClean()).items()
    if role.startswith("job_role_") and "flag" not in role
]

ascwds_schema = {
    AWPClean.organisation_id: pl.String,
    AWPClean.period: pl.String,
    AWPClean.establishment_id: pl.String,
    AWPClean.establishment_id_from_nmds: pl.String,
    AWPClean.parent_id: pl.String,
    AWPClean.nmds_id: pl.String,
    AWPClean.establishment_created_date: pl.String,
    AWPClean.establishment_updated_date: pl.String,
    AWPClean.master_update_date: pl.String,
    AWPClean.last_logged_in: pl.String,
    AWPClean.la_permission: pl.String,
    AWPClean.is_bulk_uploader: pl.String,
    AWPClean.is_parent: pl.String,
    AWPClean.parent_permission: pl.String,
    AWPClean.registration_type: pl.String,
    AWPClean.provider_id: pl.String,
    AWPClean.location_id: pl.String,
    AWPClean.establishment_type: pl.String,
    AWPClean.establishment_name: pl.String,
    AWPClean.address: pl.String,
    AWPClean.postcode: pl.String,
    AWPClean.region_id: pl.String,
    AWPClean.total_staff: pl.String,
    AWPClean.worker_records: pl.String,
    AWPClean.total_starters: pl.String,
    AWPClean.total_leavers: pl.String,
    AWPClean.total_vacancies: pl.String,
    AWPClean.main_service_id: pl.String,
    AWPClean.version: pl.String,
    AWPClean.import_date: pl.String,
}

job_role_schema = ascwds_schema.update(
    dict(zip(job_role_cols, [pl.String] * len(job_role_cols)))
)


def main(
    estimates_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        merged_data_destination (str): destination for merged output
    """
    print(ascwds_schema)
    lf = utils.scan_parquet(
        estimates_source,
        schema=ascwds_schema,
        selected_columns=list(set(ascwds_workplace_columns_to_import + job_role_cols)),
    )

    # cast jr## columns to ints.
    lf = lf.with_columns(pl.col(*job_role_cols).cast(pl.Int16))

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        estimates_source=args.estimates_source,
        merged_data_destination=args.merged_data_destination,
    )
