from polars_utils import utils
from projects._01_ingest.cqc_api.fargate.utils.convert_delta_to_full import (
    convert_delta_to_full,
)


def main(delta_source: str, full_destination: str) -> None:
    """
    Builds a full dataset from delta files for CQC locations datasets.

    Only processes import_dates not already present in the destination.

    Args:
        delta_source (str): S3 URI to read delta CQC locations data from
        full_destination (str): S3 URI to save full CQC locations data to
    """
    convert_delta_to_full(delta_source, full_destination, "locations")


if __name__ == "__main__":
    print("Running conversion from delta to full dataset job for locations dataset")

    args = utils.get_args(
        (
            "--delta_source",
            "S3 URI to read delta CQC locations data from",
        ),
        (
            "--full_destination",
            "S3 URI to save full snapshot CQC locations data to",
        ),
    )

    main(
        delta_source=args.delta_source,
        full_destination=args.full_destination,
    )

    print("Finished converting delta to full dataset job")
