from polars_utils import utils


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
    lf = utils.scan_parquet(estimates_source)

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
