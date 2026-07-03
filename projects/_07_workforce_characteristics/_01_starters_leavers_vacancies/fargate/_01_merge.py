import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as mUtils
from polars_utils import utils


def main(
    metadata_source: str,
    job_role_estimates_source: str,
    cleaned_ascwds_workplace_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        merged_data_destination (str): destination for merged output
    """
    mUtils.create_list_of_cols_for_ascwds()  # TODO: Placeholder only

    metadata_lf = utils.scan_parquet(metadata_source)
    job_role_estimates_lf = utils.scan_parquet(job_role_estimates_source)
    cleaned_ascwds_workplace_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)

    mUtils.convert_ascwds_job_role_columns_to_rows()  # TODO: Placeholder only

    mUtils.join_datasets()  # TODO: Placeholder only

    mUtils.apply_employment_status_magic_numbers()  # TODO: Placeholder only

    utils.sink_to_parquet(
        lazy_df=job_role_estimates_lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--metadata_source",
            "Source s3 directory for metadata",
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
        metadata_source=args.metadata_source,
        job_role_estimates_source=args.job_role_estimates_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        merged_data_destination=args.merged_data_destination,
    )
