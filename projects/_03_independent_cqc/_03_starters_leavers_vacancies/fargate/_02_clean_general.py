import polars_utils.cleaning_utils as cUtils
import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.general_clean_utils as generalCleanUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


def main(
    merged_data_source: str,
    general_cleaned_data_destination: str,
) -> None:
    """
    Runs cleaning steps needed by both the SLV and employment status pipelines.

    Args:
        merged_data_source (str): path to the merged data
        general_cleaned_data_destination (str): destination for general-cleaned output
    """
    lf = utils.scan_parquet(merged_data_source)

    lf = generalCleanUtils.create_slv_rate_columns(lf)

    lf = cUtils.remove_repeated_values_over_time(
        lf,
        columns_to_clean=[
            SLVCols.starters,
            SLVCols.leavers,
            SLVCols.vacancies,
            SLVCols.turnover_rate,
            SLVCols.starter_rate,
            SLVCols.vacancy_rate,
        ],
        partition_by_columns=[IndCQC.location_id, SLVCols.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
    )

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=general_cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--general_cleaned_data_destination",
            "Destination s3 directory for general-cleaned data",
        ),
    )
    main(
        merged_data_source=args.merged_data_source,
        general_cleaned_data_destination=args.general_cleaned_data_destination,
    )
