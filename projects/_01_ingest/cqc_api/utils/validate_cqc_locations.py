import polars as pl

from polars_utils.cleaning_utils import column_to_date
from projects._01_ingest.cqc_api.fargate.utils import cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
)


def get_expected_row_count_for_validation_full_clean(df: pl.DataFrame) -> int:
    """
    Returns the expected row count for validation of a fully cleaned dataset.
    This function tries to replicate the cleaning process to get the row count.

    Args:
        df (pl.DataFrame): compare Dataframe to get expect row count from

    Returns:
        int: The expected row count after performing minimum set of cleaning steps.
    """

    df = column_to_date(df, Keys.import_date, CQCLClean.cqc_location_import_date)
    df = cUtils.clean_provider_id_column(df)
    df = cUtils.impute_missing_values(
        df,
        [
            CQCLClean.provider_id,
            CQCLClean.regulated_activities_offered,
        ],
    )
    df = df.filter(
        pl.col(CQCLClean.type) == LocationType.social_care_identifier,
        pl.col(CQCLClean.registration_status) == RegistrationStatus.registered,
        pl.col(CQCLClean.provider_id).is_not_null(),
        pl.col(CQCLClean.regulated_activities_offered).is_not_null(),
    )
    df = cUtils.remove_specialist_colleges(df)
    row_count = df.height

    return row_count
