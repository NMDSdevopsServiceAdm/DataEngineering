import polars as pl

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.clean_utils as cleanUtils
import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from polars_utils import utils
from polars_utils.filtering_utils import add_filtering_rule_column
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_column_values import (
    LeaversFilteringRule,
    StartersFilteringRule,
    VacanciesFilteringRule,
)


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

    # Each metric is deduplicated independently; a job role's value is only
    # nulled when no job role at that workplace/date changed it.
    lf = cleaningUtils.remove_repeated_values_over_time_as_group(
        lf,
        columns_to_clean=[
            SLVCols.ascwds_starters,
            SLVCols.ascwds_leavers,
            SLVCols.ascwds_vacancies,
        ],
        partition_by_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
        workplace_columns=[IndCQC.location_id, IndCQC.cqc_location_import_date],
        independent=True,
    )

    lf = lf.rename(
        {
            SLVCols.ascwds_starters_dedup: SLVCols.ascwds_starters_clean,
            SLVCols.ascwds_leavers_dedup: SLVCols.ascwds_leavers_clean,
            SLVCols.ascwds_vacancies_dedup: SLVCols.ascwds_vacancies_clean,
        }
    )
    lf = add_filtering_rule_column(
        lf,
        SLVCols.ascwds_starters_filtering_rule,
        SLVCols.ascwds_starters_clean,
        StartersFilteringRule.populated,
        StartersFilteringRule.missing_data,
        pl.Categorical,
    )
    lf = add_filtering_rule_column(
        lf,
        SLVCols.ascwds_leavers_filtering_rule,
        SLVCols.ascwds_leavers_clean,
        LeaversFilteringRule.populated,
        LeaversFilteringRule.missing_data,
        pl.Categorical,
    )
    lf = add_filtering_rule_column(
        lf,
        SLVCols.ascwds_vacancies_filtering_rule,
        SLVCols.ascwds_vacancies_clean,
        VacanciesFilteringRule.populated,
        VacanciesFilteringRule.missing_data,
        pl.Categorical,
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
