from polars_utils import utils


def main(
    estimates_source: str,
    job_role_estimates_source: str,
    cleaned_ascwds_workplace_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        merged_data_destination (str): destination for merged output
    """
    estimates_lf = utils.scan_parquet(estimates_source)
    job_role_estimates_lf = utils.scan_parquet(job_role_estimates_source)
    cleaned_ascwds_workplace_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)

    utils.sink_to_parquet(
        lazy_df=estimates_lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--job_role_estimates_source",
            "Source s3 directory for job role estimates data",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        estimates_source=args.estimates_source,
        job_role_estimates_source=args.job_role_estimates_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        merged_data_destination=args.merged_data_destination,
    )
