import sys
from typing import Optional

from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import CareHome
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers import (
    clean_ascwds_filled_post_outliers,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    get_selected_value,
)

    w = w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    locations_df = create_repeated_ascwds_clean_column(locations_df)

    locations_df = get_selected_value(
        locations_df,
        w,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.cqc_location_import_date,
        "last_ascwds_submission",
        "last",
    )
    locations_df = get_selected_value(
        locations_df,
        w,
        IndCQC.people_directly_employed_dedup,
        IndCQC.cqc_location_import_date,
        "last_pir_submission",
        "last",
    )

def create_repeated_ascwds_clean_column(df: DataFrame):
    w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        "ascwds_clean_repeated",
        F.last(IndCQC.ascwds_filled_posts_dedup_clean, ignorenulls=True).over(w),
    )
    return df