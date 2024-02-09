from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    DateType,
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

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)

from utils.column_names.cleaned_data_files.cqc_provider_data_columns_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_data_columns import (
    CqcLocationCleanedColumns as CQCLClean,
)
from schemas.cqc_location_schema import LOCATION_SCHEMA


from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
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
class ASCWDSWorkplaceSchemas:
    workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.total_staff, StringType(), True),
            StructField(AWP.worker_records, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
            StructField(AWP.is_parent, StringType(), True),
            StructField(AWP.parent_id, StringType(), True),
            StructField(AWP.last_logged_in, StringType(), True),
        ]
    )


@dataclass
class CQCLocationsSchema:
    full_schema = StructType(
        [
            *LOCATION_SCHEMA,
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    primary_service_type_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(
                CQCL.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )

    small_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    join_provider_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
            StructField(CQCPClean.region, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_joined_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCLClean.provider_name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class CleaningUtilsSchemas:
    worker_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
        ]
    )

    replace_labels_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
        ]
    )

    labels_schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    expected_schema_with_new_columns = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
            StructField("gender_labels", StringType(), True),
            StructField("nationality_labels", StringType(), True),
        ]
    )

    expected_schema_replace_labels_with_new_columns = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField("gender_labels", StringType(), True),
        ]
    )

    scale_schema = StructType(
        [
            StructField("int", IntegerType(), True),
            StructField("float", FloatType(), True),
            StructField("non_scale", StringType(), True),
        ]
    )

    expected_scale_schema = StructType(
        [
            *scale_schema,
            StructField("bound_int", IntegerType(), True),
            StructField("bound_float", FloatType(), True),
        ]
    )

    align_dates_schema = StructType(
        [
            StructField(Keys.import_date, DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
        ]
    )

    expected_aligned_dates_schema = StructType(
        [
            StructField("snapshot_date", DateType(), True),
            StructField("primaray_dataset_date", DateType(), True),
            StructField("secondary_dataset_date", DateType(), True),
        ]
    )

    expected_merged_dates_schema = StructType(
        [
            StructField("snapshot_date", DateType(), True),
            StructField("primaray_dataset_date", DateType(), True),
            StructField("secondary_dataset_date", DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
        ]
    )


@dataclass
class CQCProviderSchema:
    expected_rows_with_cqc_sector_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class IngestONSData:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )


@dataclass
class FilterCleanedValuesSchema:
    sample_schema = StructType(
        [
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("import_date", StringType(), True),
        ]
    )
