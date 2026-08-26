from polars_utils import utils

# Test comment.


def main(
    job_role_estimates_source: str,
    job_role_metadata_source: str,
    filled_posts_estimates_source: str,
    archive_destination: str,
) -> None:
    """
    Archives the independent CQC filled posts by job role estimates.

    Also scans the job-role metadata and overall filled posts estimates datasets in
    preparation for a future archive rework once the job-role dataset is restructured;
    neither is used yet.

    Args:
        job_role_estimates_source (str): source s3 directory for the job role
            filled posts estimates
        job_role_metadata_source (str): source s3 directory for the job role merge
            metadata (not yet used)
        filled_posts_estimates_source (str): source s3 directory for the overall
            filled posts estimates (not yet used)
        archive_destination (str): s3 URI to write job role archive data to
    """
    print("Archiving independent CQC filled posts by job role...")

    job_role_estimates_lf = utils.scan_parquet(job_role_estimates_source)
    utils.scan_parquet(job_role_metadata_source)
    utils.scan_parquet(filled_posts_estimates_source)

    print(f"Exporting as parquet to {archive_destination}")

    utils.sink_to_parquet(job_role_estimates_lf, archive_destination)

    print("Completed archive independent CQC filled posts by job role")


if __name__ == "__main__":
    print("Running Archive Independent CQC Job Role Estimates job")

    args = utils.get_args(
        (
            "--job_role_estimates_source",
            "Source s3 directory for the job role filled posts estimates",
        ),
        (
            "--job_role_metadata_source",
            "Source s3 directory for the job role merge metadata",
        ),
        (
            "--filled_posts_estimates_source",
            "Source s3 directory for the overall filled posts estimates",
        ),
        (
            "--archive_destination",
            "S3 URI to write job role archive data to",
        ),
    )

    main(
        job_role_estimates_source=args.job_role_estimates_source,
        job_role_metadata_source=args.job_role_metadata_source,
        filled_posts_estimates_source=args.filled_posts_estimates_source,
        archive_destination=args.archive_destination,
    )

    print("Finished Archive Independent CQC Job Role Estimates job")
