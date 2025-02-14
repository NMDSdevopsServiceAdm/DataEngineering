from pyspark.sql import DataFrame, functions as F

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

from utils.ind_cqc_filled_posts_utils.ascwds_job_role_count.ascwds_job_role_count import (
    list_of_job_roles,
)


def merge_dataframes(posts_df: DataFrame, workers_df: DataFrame) -> DataFrame:
    """
    Joining the IndCQC Estimates dataframe (Left Table) and the ASCWDS Worker DataFrame (Right Table) together.
    All the columns from IndCQC and the Job Count columns from ASCWDS.

    Args:
        posts_df (DataFrame): A dataframe containing cleaned IndCQC workplace Data.
        workers_df (DataFrame): ASC-WDS worker dataframe grouped to include columns with job role counts per job role. .

    Returns:
        DataFrame: The IndCQC DataFrame merged to include job role count columns.
    """

    joined_df = posts_df.join(
        workers_df,
        (posts_df[IndCQC.establishment_id] == workers_df[AWKClean.establishment_id])
        & (
            posts_df[IndCQC.ascwds_workplace_import_date]
            == workers_df[AWKClean.ascwds_worker_import_date]
        ),
        "left",
    )

    result_df = joined_df.select(posts_df, workers_df[list_of_job_roles])

    return result_df


# ascwds_pir_merge
# another column ascwds_filled_posts_deduplicated_clean. Only where this is populated. When doing the join make a new column into the dataset we are joining into, if that column has a value the new column says the source is
# EstimateFilledPostsSource.ascwds_filled_posts_dedup_clean. Only join when populated.
