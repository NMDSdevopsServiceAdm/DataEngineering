from polars_utils import utils


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Impute values to fill gaps between and around known values.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(cleaned_data_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=imputed_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--imputed_data_destination",
            "Destination s3 directory for imputed data",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        imputed_data_destination=args.imputed_data_destination,
    )
