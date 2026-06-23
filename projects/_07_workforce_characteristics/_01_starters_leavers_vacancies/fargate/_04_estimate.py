from polars_utils import utils


def main(
    imputed_data_source: str,
    estimated_data_destination: str,
) -> None:
    """
    Creates estimates of starters, leavers and vacancies.

    Args:
        imputed_data_source (str): path to the imputed data
        estimated_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(imputed_data_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=estimated_data_destination,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--imputed_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated data",
        ),
    )
    main(
        imputed_data_source=args.imputed_data_source,
        estimated_data_destination=args.estimated_data_destination,
    )
