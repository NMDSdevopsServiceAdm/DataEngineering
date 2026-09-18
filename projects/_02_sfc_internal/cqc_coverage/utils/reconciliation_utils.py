from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_values.categorical_column_values import ParentsOrSinglesAndSubs


# converted to polars -> projects/_02_sfc_internal/utils/utils.py (add_parents_or_singles_and_subs_column)
def add_parents_or_singles_and_subs_col_to_df(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        ReconColumn.parents_or_singles_and_subs,
        F.when(
            (
                (F.col(AWPClean.is_parent) == "Yes")
                | (
                    (F.col(AWPClean.is_parent) == "No")
                    & (F.col(AWPClean.parent_permission) == "Parent has ownership")
                )
            ),
            F.lit(ParentsOrSinglesAndSubs.parents),
        ).otherwise(F.lit(ParentsOrSinglesAndSubs.singles_and_subs)),
    )
    return df
