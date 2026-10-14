from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(
    employment_status_estimate_source: str,
    prepared_slv_dataset_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges the final employment status estimates with prepared SLV workplace data.

    Args:
        employment_status_estimate_source (str): path to the employment status estimates
        prepared_slv_dataset_source (str): path to the prepared ascwds workplace data
        merged_data_destination (str): destination for merged output
    """
    employment_status_estimate_lf = utils.scan_parquet(
        employment_status_estimate_source
    )
    workplace_lf = utils.scan_parquet(prepared_slv_dataset_source)

    merged_lf = employment_status_estimate_lf.join(
        workplace_lf,
        on=[
            IndCQC.establishment_id,
            IndCQC.ascwds_workplace_import_date,
            IndCQC.published_job_role_label,
        ],
        how="left",
    )

    utils.sink_to_parquet(
        lazy_df=merged_lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--employment_status_estimate_source",
            "Source s3 directory for employment status estimates",
        ),
        (
            "--prepared_slv_dataset_source",
            "Source s3 directory for prepared ascwds workplace data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        employment_status_estimate_source=args.employment_status_estimate_source,
        prepared_slv_dataset_source=args.prepared_slv_dataset_source,
        merged_data_destination=args.merged_data_destination,
    )
