from polars_utils import utils


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
    lf = utils.scan_parquet(workplace_source)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_workplace_destination,
    )

    utils.sink_to_parquet(
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
