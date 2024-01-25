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
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as LocationCols,
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
class CQCProviderSchemas:
    full_parquet_schema = StructType(
        [
            StructField("providerId", StringType(), True),
            StructField("locationIds", ArrayType(StringType(), True), True),
            StructField("organisationType", StringType(), True),
            StructField("ownershipType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("uprn", StringType(), True),
            StructField("name", StringType(), True),
            StructField("registrationStatus", StringType(), True),
            StructField("registrationDate", StringType(), True),
            StructField("deregistrationDate", StringType(), True),
            StructField("postalAddressLine1", StringType(), True),
            StructField("postalAddressTownCity", StringType(), True),
            StructField("postalAddressCounty", StringType(), True),
            StructField("region", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("onspdLatitude", FloatType(), True),
            StructField("onspdLongitude", FloatType(), True),
            StructField("mainPhoneNumber", StringType(), True),
            StructField("companiesHouseNumber", StringType(), True),
            StructField("inspectionDirectorate", StringType(), True),
            StructField("constituency", StringType(), True),
            StructField("localAuthority", StringType(), True),
        ]
    )


@dataclass
class CQCLocationsSchemas:
    full_parquet_schema = StructType(
        fields=[
            StructField(LocationCols.location_id, StringType(), True),
            StructField(LocationCols.provider_id, StringType(), True),
            StructField(LocationCols.organisation_type, StringType(), True),
            StructField(LocationCols.type, StringType(), True),
            StructField(LocationCols.name, StringType(), True),
            StructField(LocationCols.ccg_code, StringType(), True),
            StructField(LocationCols.ccg_name, StringType(), True),
            StructField(LocationCols.ods_code, StringType(), True),
            StructField(LocationCols.uprn, StringType(), True),
            StructField(LocationCols.registration_status, StringType(), True),
            StructField(LocationCols.registration_date, StringType(), True),
            StructField(LocationCols.deregistration_date, StringType(), True),
            StructField(LocationCols.dormancy, StringType(), True),
            StructField(LocationCols.number_of_beds, IntegerType(), True),
            StructField(LocationCols.website, StringType(), True),
            StructField(LocationCols.address_line_one, StringType(), True),
            StructField(LocationCols.town_or_city, StringType(), True),
            StructField(LocationCols.county, StringType(), True),
            StructField(LocationCols.region, StringType(), True),
            StructField(LocationCols.postcode, StringType(), True),
            StructField(LocationCols.latitude, FloatType(), True),
            StructField(LocationCols.longitude, FloatType(), True),
            StructField(LocationCols.care_home, StringType(), True),
            StructField(LocationCols.inspection_directorate, StringType(), True),
            StructField(LocationCols.phone_number, StringType(), True),
            StructField(LocationCols.constituancy, StringType(), True),
            StructField(LocationCols.local_authority, StringType(), True),
            StructField(
                LocationCols.last_inspection,
                StructType([StructField(LocationCols.date, StringType(), True)]),
                True,
            ),
            StructField(
                LocationCols.last_report,
                StructType(
                    [StructField(LocationCols.publication_date, StringType(), True)]
                ),
                True,
            ),
            StructField(
                LocationCols.relationships,
                ArrayType(
                    StructType(
                        [
                            StructField(
                                LocationCols.related_location_id, StringType(), True
                            ),
                            StructField(
                                LocationCols.related_location_name, StringType(), True
                            ),
                            StructField(LocationCols.type, StringType(), True),
                            StructField(LocationCols.reason, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                LocationCols.regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(LocationCols.name, StringType(), True),
                            StructField(LocationCols.code, StringType(), True),
                            StructField(
                                LocationCols.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                LocationCols.title, StringType(), True
                                            ),
                                            StructField(
                                                LocationCols.given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                LocationCols.family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                LocationCols.roles, StringType(), True
                                            ),
                                        ]
                                    )
                                ),
                                True,
                            ),
                        ]
                    )
                ),
            ),
            StructField(
                LocationCols.gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(LocationCols.name, StringType(), True),
                            StructField(LocationCols.description, StringType(), True),
                        ]
                    )
                ),
            ),
            StructField(
                LocationCols.inspection_categories,
                ArrayType(
                    StructType(
                        [
                            StructField(LocationCols.code, StringType(), True),
                            StructField(LocationCols.primary, StringType(), True),
                            StructField(LocationCols.name, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                LocationCols.specialisms,
                ArrayType(
                    StructType(
                        [
                            StructField(LocationCols.name, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                LocationCols.current_ratings,
                StructType(
                    [
                        StructField(
                            LocationCols.overall,
                            StructType(
                                [
                                    StructField(
                                        LocationCols.rating, StringType(), True
                                    ),
                                    StructField(
                                        LocationCols.report_date, StringType(), True
                                    ),
                                    StructField(
                                        LocationCols.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        LocationCols.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        LocationCols.name,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        LocationCols.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        LocationCols.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        LocationCols.report_link_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            )
                                        ),
                                    ),
                                ]
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
            StructField(
                LocationCols.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(
                                LocationCols.organisation_id, StringType(), True
                            ),
                            StructField(
                                LocationCols.report_link_id, StringType(), True
                            ),
                            StructField(LocationCols.report_date, StringType(), True),
                            StructField(
                                LocationCols.overall,
                                StructType(
                                    [
                                        StructField(
                                            LocationCols.rating, StringType(), True
                                        ),
                                        StructField(
                                            LocationCols.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            LocationCols.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            LocationCols.rating,
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ),
            StructField(
                LocationCols.reports,
                ArrayType(
                    StructType(
                        [
                            StructField(LocationCols.link_id, StringType(), True),
                            StructField(LocationCols.report_date, StringType(), True),
                            StructField(LocationCols.report_uri, StringType(), True),
                            StructField(
                                LocationCols.first_visit_date, StringType(), True
                            ),
                            StructField(LocationCols.report_type, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
