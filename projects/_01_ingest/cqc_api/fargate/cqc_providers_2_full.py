from polars_utils import logger, utils
from projects._01_ingest.cqc_api.fargate.utils.convert_delta_to_full import (
    convert_delta_to_full,
)

logger = logger.get_logger(__name__)


def main(delta_source: str, full_destination: str) -> None:
    """
    Builds a full dataset from delta files for CQC providers datasets.

    Only processes import_dates not already present in the destination.

    Args:
        delta_source (str): S3 URI to read delta CQC providers data from
        full_destination (str): S3 URI to save full CQC providers data to
    """
    convert_delta_to_full(delta_source, full_destination, "providers")


if __name__ == "__main__":
    logger.info(
        "Running conversion from delta to full dataset job for providers dataset"
    )

    args = utils.get_args(
        (
            "--delta_source",
            "S3 URI to read delta CQC providers data from",
        ),
        (
            "--full_destination",
            "S3 URI to save full snapshot CQC providers data to",
        ),
    )

    convert_delta_to_full(
        delta_source=args.delta_source,
        full_destination=args.full_destination,
    )

    logger.info("Finished converting delta to full dataset job")
