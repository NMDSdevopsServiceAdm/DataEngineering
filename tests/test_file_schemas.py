from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
)

from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SNAPSHOT_DATE,
    PEOPLE_DIRECTLY_EMPLOYED,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
    ROLLING_AVERAGE_MODEL,
    EXTRAPOLATION_MODEL,
    CARE_HOME_MODEL,
    INTERPOLATION_MODEL,
    NON_RESIDENTIAL_MODEL,
)
from utils.diagnostics_utils.diagnostics_meta_data import (
    Columns,
    TestColumns,
)
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


@dataclass
class CreateJobEstimatesDiagnosticsSchemas:
    estimate_jobs = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(SNAPSHOT_DATE, StringType(), False),
            StructField(
                JOB_COUNT_UNFILTERED,
                FloatType(),
                True,
            ),
            StructField(JOB_COUNT, FloatType(), True),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(ROLLING_AVERAGE_MODEL, FloatType(), True),
            StructField(CARE_HOME_MODEL, FloatType(), True),
            StructField(EXTRAPOLATION_MODEL, FloatType(), True),
            StructField(INTERPOLATION_MODEL, FloatType(), True),
            StructField(NON_RESIDENTIAL_MODEL, FloatType(), True),
            StructField(ESTIMATE_JOB_COUNT, FloatType(), True),
            StructField(PEOPLE_DIRECTLY_EMPLOYED, IntegerType(), True),
        ]
    )
    capacity_tracker_care_home = StructType(
        [
            StructField(Columns.CQC_ID, StringType(), False),
            StructField(
                Columns.NURSES_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(Columns.CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_NURSES_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
        ]
    )
    capacity_tracker_non_residential = StructType(
        [
            StructField(Columns.CQC_ID, StringType(), False),
            StructField(
                Columns.CQC_CARE_WORKERS_EMPLOYED,
                FloatType(),
                True,
            ),
        ]
    )

    diagnostics = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(
                Columns.NURSES_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(Columns.CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_NURSES_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(Columns.AGENCY_NON_CARE_WORKERS_EMPLOYED, FloatType(), True),
            StructField(
                Columns.CQC_CARE_WORKERS_EMPLOYED,
                FloatType(),
                True,
            ),
        ]
    )
    diagnostics_prepared = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(
                JOB_COUNT_UNFILTERED,
                FloatType(),
                True,
            ),
            StructField(JOB_COUNT, FloatType(), True),
            StructField(PRIMARY_SERVICE_TYPE, StringType(), True),
            StructField(ROLLING_AVERAGE_MODEL, FloatType(), True),
            StructField(CARE_HOME_MODEL, FloatType(), True),
            StructField(EXTRAPOLATION_MODEL, FloatType(), True),
            StructField(INTERPOLATION_MODEL, FloatType(), True),
            StructField(NON_RESIDENTIAL_MODEL, FloatType(), True),
            StructField(ESTIMATE_JOB_COUNT, FloatType(), True),
            StructField(PEOPLE_DIRECTLY_EMPLOYED, IntegerType(), True),
            StructField(
                Columns.CARE_HOME_EMPLOYED,
                FloatType(),
                True,
            ),
            StructField(Columns.NON_RESIDENTIAL_EMPLOYED, FloatType(), True),
        ]
    )
    residuals = StructType(
        [
            StructField(LOCATION_ID, StringType(), False),
            StructField(
                TestColumns.residuals_test_column_names[0],
                FloatType(),
                True,
            ),
            StructField(
                TestColumns.residuals_test_column_names[1],
                FloatType(),
                True,
            ),
        ]
    )


@dataclass
class CalculatePaRatioSchemas:
    total_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.TOTAL_STAFF_RECODED,
                FloatType(),
                True,
            ),
        ]
    )
    average_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.AVERAGE_STAFF,
                FloatType(),
                True,
            ),
        ]
    )


@dataclass
class ASCWDSWorkerSchemas:
    worker_schema = StructType(
        [
            StructField(AWK.location_id, StringType(), True),
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
        ]
    )


@dataclass
class CleaningUtilsData:
    worker_schema = StructType(
        [
            StructField(AWK.location_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
        ]
    )
