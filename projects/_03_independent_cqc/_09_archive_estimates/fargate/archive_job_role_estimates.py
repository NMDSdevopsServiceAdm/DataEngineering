from polars_utils import utils


def main(
    estimate_ind_cqc_filled_posts_by_job_role_source: str,
    estimate_ind_cqc_filled_posts_by_job_role_metadata_source: str,
    estimate_ind_cqc_filled_posts_source: str,
    archive_ind_cqc_filled_posts_by_job_role_destination: str,
) -> None:
    """
    Archives the independent CQC filled posts by job role estimates.

    Also scans the job-role metadata and overall filled posts estimates datasets in
    preparation for a future archive rework once the job-role dataset is restructured;
    neither is used yet.

    Args:
        estimate_ind_cqc_filled_posts_by_job_role_source (str): source s3 directory for
            estimate_ind_cqc_filled_posts_by_job_role
        estimate_ind_cqc_filled_posts_by_job_role_metadata_source (str): source s3
            directory for the job role merge metadata (not yet used)
        estimate_ind_cqc_filled_posts_source (str): source s3 directory for the overall
            estimate_ind_cqc_filled_posts (not yet used)
        archive_ind_cqc_filled_posts_by_job_role_destination (str): s3 URI to append
            job role archive data to
    """
    print("Archiving independent CQC filled posts by job role...")

    estimate_filled_posts_by_job_role_lf = utils.scan_parquet(
        estimate_ind_cqc_filled_posts_by_job_role_source
    )
    utils.scan_parquet(estimate_ind_cqc_filled_posts_by_job_role_metadata_source)
    utils.scan_parquet(estimate_ind_cqc_filled_posts_source)

    print(
        f"Exporting as parquet to {archive_ind_cqc_filled_posts_by_job_role_destination}"
    )

    utils.sink_to_parquet(
        estimate_filled_posts_by_job_role_lf,
        archive_ind_cqc_filled_posts_by_job_role_destination,
    )

    print("Completed archive independent CQC filled posts by job role")


if __name__ == "__main__":
    print("Running Archive Independent CQC Job Role Estimates job")

    args = utils.get_args(
        (
            "--estimate_ind_cqc_filled_posts_by_job_role_source",
            "Source s3 directory for estimate_ind_cqc_filled_posts_by_job_role",
        ),
        (
            "--estimate_ind_cqc_filled_posts_by_job_role_metadata_source",
            "Source s3 directory for the job role merge metadata",
        ),
        (
            "--estimate_ind_cqc_filled_posts_source",
            "Source s3 directory for the overall estimate_ind_cqc_filled_posts",
        ),
        (
            "--archive_ind_cqc_filled_posts_by_job_role_destination",
            "S3 URI to append job role archive data to",
        ),
    )

    main(
        estimate_ind_cqc_filled_posts_by_job_role_source=args.estimate_ind_cqc_filled_posts_by_job_role_source,
        estimate_ind_cqc_filled_posts_by_job_role_metadata_source=args.estimate_ind_cqc_filled_posts_by_job_role_metadata_source,
        estimate_ind_cqc_filled_posts_source=args.estimate_ind_cqc_filled_posts_source,
        archive_ind_cqc_filled_posts_by_job_role_destination=args.archive_ind_cqc_filled_posts_by_job_role_destination,
    )

    print("Finished Archive Independent CQC Job Role Estimates job")
