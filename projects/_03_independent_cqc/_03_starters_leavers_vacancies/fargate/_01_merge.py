from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


def main(
    employment_status_clean_source: str,
    prepared_slv_dataset_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges the cleaned employment status data with prepared SLV workplace data.

    Args:
        employment_status_clean_source (str): path to the cleaned employment status data
        prepared_slv_dataset_source (str): path to the prepared ascwds workplace data
        merged_data_destination (str): destination for merged output
    """
    employment_status_clean_lf = utils.scan_parquet(employment_status_clean_source)
    workplace_lf = utils.scan_parquet(prepared_slv_dataset_source)

    merged_lf = employment_status_clean_lf.join(
        workplace_lf,
        on=[
            IndCQC.establishment_id,
            IndCQC.ascwds_workplace_import_date,
            SLVCols.published_job_role_label,
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
            "--employment_status_clean_source",
            "Source s3 directory for cleaned employment status data",
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
        employment_status_clean_source=args.employment_status_clean_source,
        prepared_slv_dataset_source=args.prepared_slv_dataset_source,
        merged_data_destination=args.merged_data_destination,
    )
