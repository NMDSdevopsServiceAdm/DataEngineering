from polars_utils import utils


def main(
    cleaned_ascwds_workplace_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS workplace dataset and save it unchanged.

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(cleaned_ascwds_workplace_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared data",
        ),
    )
    main(
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        prepared_data_destination=args.prepared_data_destination,
    )
