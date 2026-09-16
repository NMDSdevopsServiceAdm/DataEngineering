from polars_utils import utils


def main(
    general_cleaned_data_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Runs cleaning steps specific to the SLV pipeline, after general cleaning.

    Currently a placeholder pass-through: no SLV-only cleaning logic exists yet.

    Args:
        general_cleaned_data_source (str): path to the general-cleaned data
        cleaned_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(general_cleaned_data_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--general_cleaned_data_source",
            "Source s3 directory for general-cleaned data",
        ),
        (
            "--cleaned_data_destination",
            "Destination s3 directory for cleaned data",
        ),
    )
    main(
        general_cleaned_data_source=args.general_cleaned_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
