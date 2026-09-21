import polars as pl

import polars_utils.cleaning_utils as cUtils
import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.clean_utils as cleanUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import (
    SLVEmploymentStatusColumns as SLVEmpStatus,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import SLVFilteringRule

NOT_KNOWN_CODE = 999  # '999' is used elsewhere in ASCWDS to represent not known.


def main(
    merged_data_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Cleans the merged data.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): destination for cleaned output
    """
    lf = utils.scan_parquet(merged_data_source)

    # Employees = directly employed by the workplace (permanent + temporary) only.
    lf = lf.with_columns(
        (
            pl.col(SLVEmpStatus.permanent_count) + pl.col(SLVEmpStatus.temporary_count)
        ).alias(SLVCols.employees)
    )

    # Nulls ASCWDS's 999 "not known" code in starters/leavers/vacancies, recording
    # why in a filtering-rule column per metric.
    lf = cUtils.null_not_known_values(
        lf,
        columns_to_clean=[SLVCols.starters, SLVCols.leavers, SLVCols.vacancies],
        not_known_code=NOT_KNOWN_CODE,
        populated_rule=SLVFilteringRule.populated,
        missing_rule=SLVFilteringRule.missing_data,
        not_known_rule=SLVFilteringRule.contained_invalid_missing_data_code,
    )

    lf = cUtils.remove_repeated_values_over_time(
        lf,
        columns_to_clean=[
            SLVCols.starters_cleaned,
            SLVCols.leavers_cleaned,
            SLVCols.vacancies_cleaned,
        ],
        partition_by_columns=[IndCQC.location_id, SLVCols.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
    )

    lf = cleanUtils.create_slv_rate_columns(lf)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for merged slv data",
        ),
        (
            "--cleaned_data_destination",
            "Destination s3 directory for cleaned data",
        ),
    )
    main(
        merged_data_source=args.merged_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
