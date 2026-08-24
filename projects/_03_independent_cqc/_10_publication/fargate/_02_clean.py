from polars_utils import utils


def main(
    merge_data_source: str,
    clean_destination: str,
) -> None:
    """
    Placeholder stage: reads the merged data and sinks it unchanged.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): destination s3 directory for the cleaned data
    """
    lf = utils.scan_parquet(merge_data_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=clean_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merge_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--filter_aggregate_select_destination",
            "Destination s3 directory for the filtered, aggregated and selected data",
        ),
    )
    main(
        merge_data_source=args.merge_data_source,
        clean_destination=args.clean_destination,
    )
