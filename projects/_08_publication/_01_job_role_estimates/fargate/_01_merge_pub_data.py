from polars_utils import utils


def main(
    jr_archive_estimates_source: str,
    jr_archive_metadata_source: str,
    jr_archive_geography_source: str,
    merge_data_destination: str,
) -> None:
    """
    Placeholder stage: reads the estimated job role data and sinks it unchanged.

    Args:
        jr_archive_estimates_source (str): source s3 directory for archived job role estimates data
        jr_archive_metadata_source (str): source s3 directory for archived job role metadata data
        jr_archive_geography_source (str): source s3 directory for archived geography data
        merge_data_destination (str): destination s3 directory for merged data
    """
    jr_estimates_lf = utils.scan_parquet(jr_archive_estimates_source)
    metadata_lf = utils.scan_parquet(jr_archive_metadata_source)
    geography_lf = utils.scan_parquet(jr_archive_geography_source)

    utils.sink_to_parquet(
        lazy_df=jr_estimates_lf,
        output_path=merge_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--jr_archive_estimates_source",
            "Source s3 directory for archived job role estimates data",
        ),
        (
            "--jr_archive_metadata_source",
            "Source s3 directory for archived job role metadata data",
        ),
        (
            "--jr_archive_geography_source",
            "Source s3 directory for archived geography data",
        ),
        (
            "--merge_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        jr_archive_estimates_source=args.jr_archive_estimates_source,
        jr_archive_metadata_source=args.jr_archive_metadata_source,
        jr_archive_geography_source=args.jr_archive_geography_source,
        merge_data_destination=args.merge_data_destination,
    )
