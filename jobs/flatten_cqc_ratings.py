import sys

from utils import utils


def main(cqc_location_source: str, cqc_ratings_destination: str):
    cqc_location_df = utils.read_from_parquet(cqc_location_source)

    utils.write_to_parquet(
        cqc_location_df,
        cqc_ratings_destination,
        mode="overwrite",
    )


if __name__ == "__main__":
    print("Spark job 'flatten_cqc_ratings' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cqc_ratings_destination",
            "Destination s3 directory for cleaned parquet CQC ratings dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'flatten_cqc_ratings' complete")
