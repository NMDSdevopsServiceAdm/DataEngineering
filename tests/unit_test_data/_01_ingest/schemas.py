from dataclasses import dataclass

from pyspark.sql.types import StructType, StructField, StringType

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class IngestASCWDSData:
    raise_mainjrid_error_when_mainjrid_not_in_df_schema = StructType(
        [
            StructField(AWK.establishment_id, StringType(), False),
            StructField(AWK.location_id, StringType(), True),
        ]
    )
    raise_mainjrid_error_when_mainjrid_in_df_schema = StructType(
        [
            *raise_mainjrid_error_when_mainjrid_not_in_df_schema,
            StructField(AWK.main_job_role_id, StringType(), True),
        ]
    )

    fix_nmdssc_dates_schema = StructType(
        [
            StructField(AWK.establishment_id, StringType(), False),
            StructField(AWK.created_date, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.updated_date, StringType(), True),
        ]
    )

    fix_nmdssc_dates_with_last_logged_in_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), False),
            StructField(AWP.master_update_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWP.last_logged_in, StringType(), True),
        ]
    )
