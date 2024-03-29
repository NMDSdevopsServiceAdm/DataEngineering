from dataclasses import dataclass
from pyspark.ml.linalg import VectorUDT

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    DateType,
    LongType,
    DoubleType,
)

from utils.estimate_filled_posts.column_names import (
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
from utils.column_names.raw_data_files.cqc_pir_columns import (
    CqcPirColumns as CQCPIR,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

from schemas.cqc_location_schema import OLD_LOCATION_SCHEMA


from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
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
            StructField(AWK.establishment_id, StringType(), True),
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.main_job_role_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
            StructField(AWK.year, StringType(), True),
            StructField(AWK.month, StringType(), True),
            StructField(AWK.day, StringType(), True),
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

    cast_to_int_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, StringType(), True),
            StructField(AWP.worker_records, StringType(), True),
        ]
    )

    cast_to_int_expected_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, IntegerType(), True),
            StructField(AWP.worker_records, IntegerType(), True),
        ]
    )

    location_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
        ]
    )

    purge_outdated_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
            StructField(AWP.is_parent, StringType(), True),
        ]
    )


@dataclass
class ONSData:
    sample_schema = StructType(
        [
            StructField(ONS.region, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
        ]
    )

    full_schema = StructType(
        [
            StructField(ONS.postcode, StringType(), True),
            StructField(ONS.cssr, StringType(), True),
            StructField(ONS.region, StringType(), True),
            StructField(ONS.sub_icb, StringType(), True),
            StructField(ONS.icb, StringType(), True),
            StructField(ONS.icb_region, StringType(), True),
            StructField(ONS.ccg, StringType(), True),
            StructField(ONS.latitude, StringType(), True),
            StructField(ONS.longitude, StringType(), True),
            StructField(ONS.imd_score, StringType(), True),
            StructField(ONS.lower_super_output_area_2011, StringType(), True),
            StructField(ONS.middle_super_output_area_2011, StringType(), True),
            StructField(ONS.rural_urban_indicator_2011, StringType(), True),
            StructField(ONS.lower_super_output_area_2021, StringType(), True),
            StructField(ONS.middle_super_output_area_2021, StringType(), True),
            StructField(
                ONS.westminster_parliamentary_consitituency, StringType(), True
            ),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_refactored_contemporary_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.contemporary_sub_icb, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
            StructField(ONSClean.contemporary_icb_region, StringType(), True),
            StructField(ONSClean.contemporary_ccg, StringType(), True),
            StructField(ONSClean.contemporary_latitude, StringType(), True),
            StructField(ONSClean.contemporary_longitude, StringType(), True),
            StructField(ONSClean.contemporary_imd_score, StringType(), True),
            StructField(ONSClean.contemporary_lsoa11, StringType(), True),
            StructField(ONSClean.contemporary_msoa11, StringType(), True),
            StructField(ONSClean.contemporary_rural_urban_ind_11, StringType(), True),
            StructField(ONSClean.contemporary_lsoa21, StringType(), True),
            StructField(ONSClean.contemporary_msoa21, StringType(), True),
            StructField(
                ONSClean.contemporary_constituancy,
                StringType(),
                True,
            ),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    expected_refactored_current_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(ONSClean.current_sub_icb, StringType(), True),
            StructField(ONSClean.current_icb, StringType(), True),
            StructField(ONSClean.current_icb_region, StringType(), True),
            StructField(ONSClean.current_ccg, StringType(), True),
            StructField(ONSClean.current_latitude, StringType(), True),
            StructField(ONSClean.current_longitude, StringType(), True),
            StructField(ONSClean.current_imd_score, StringType(), True),
            StructField(ONSClean.current_lsoa11, StringType(), True),
            StructField(ONSClean.current_msoa11, StringType(), True),
            StructField(ONSClean.current_rural_urban_ind_11, StringType(), True),
            StructField(ONSClean.current_lsoa21, StringType(), True),
            StructField(ONSClean.current_msoa21, StringType(), True),
            StructField(
                ONSClean.current_constituancy,
                StringType(),
                True,
            ),
        ]
    )


@dataclass
class CapacityTrackerCareHomeSchema:
    sample_schema = StructType(
        [
            StructField("local_authority", StringType(), True),
            StructField("location", StringType(), True),
            StructField("parentorganisation", StringType(), True),
            StructField("lrf", StringType(), True),
            StructField("localauthority", StringType(), True),
            StructField("region", StringType(), True),
            StructField("icb", StringType(), True),
            StructField("subicb", StringType(), True),
            StructField("cqcid", StringType(), True),
            StructField("odscode", StringType(), True),
            StructField("covidresidentstotal", StringType(), True),
            StructField("isacceptingadmissions", StringType(), True),
            StructField("nursesemployed", StringType(), True),
            StructField("nursesabsentgeneral", StringType(), True),
            StructField("nursesabsentcovid", StringType(), True),
            StructField("careworkersemployed", StringType(), True),
            StructField("careworkersabsentgeneral", StringType(), True),
            StructField("careworkersabsentcovid", StringType(), True),
            StructField("noncareworkersemployed", StringType(), True),
            StructField("noncareworkersabsentgeneral", StringType(), True),
            StructField("noncareworkersabsentcovid", StringType(), True),
            StructField("agencynursesemployed", StringType(), True),
            StructField("agencycareworkersemployed", StringType(), True),
            StructField("agencynoncareworkersemployed", StringType(), True),
            StructField("hourspaid", StringType(), True),
            StructField("hoursovertime", StringType(), True),
            StructField("hoursagency", StringType(), True),
            StructField("hoursabsence", StringType(), True),
            StructField("daysabsence", StringType(), True),
            StructField("lastupdatedutc", StringType(), True),
            StructField("lastupdatedbst", StringType(), True),
        ]
    )


@dataclass
class CapacityTrackerDomCareSchema:
    sample_schema = StructType(
        [
            StructField("local_authority", StringType(), True),
            StructField("subicbname", StringType(), True),
            StructField("icbname", StringType(), True),
            StructField("regionname", StringType(), True),
            StructField("laname", StringType(), True),
            StructField("lrfname", StringType(), True),
            StructField("laregionname", StringType(), True),
            StructField("location", StringType(), True),
            StructField("cqcid", StringType(), True),
            StructField("odscode", StringType(), True),
            StructField("cqcsurveylastupdatedutc", StringType(), True),
            StructField("cqcsurveylastupdatedbst", StringType(), True),
            StructField("serviceusercount", StringType(), True),
            StructField("legacycovidconfirmed", StringType(), True),
            StructField("legacycovidsuspected", StringType(), True),
            StructField("cqccareworkersemployed", StringType(), True),
            StructField("cqccareworkersabsent", StringType(), True),
            StructField("canprovidermorehours", StringType(), True),
            StructField("extrahourscount", StringType(), True),
            StructField("covid_vaccination_(full_course)", StringType(), True),
            StructField("covid_vaccination_(autumn_23)", StringType(), True),
            StructField("flu_vaccination_(autumn_23)", StringType(), True),
            StructField("confirmedsave", StringType(), True),
            StructField("hourspaiddomcare", StringType(), True),
            StructField("hoursovertimedomcare", StringType(), True),
            StructField("hoursagencydomcare", StringType(), True),
            StructField("hoursabsencedomcare", StringType(), True),
            StructField("daysabsencedomcare", StringType(), True),
            StructField("usersnhsla", StringType(), True),
            StructField("usersselffunded", StringType(), True),
            StructField("returnedpocpercent", StringType(), True),
        ]
    )


@dataclass
class CQCLocationsSchema:
    full_schema = StructType(
        [
            *OLD_LOCATION_SCHEMA,
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
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )

    expected_joined_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCLClean.provider_name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
        ]
    )

    invalid_postcode_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.postcode, StringType(), True),
        ]
    )

    registration_status_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    social_care_org_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.type, StringType(), True),
        ]
    )

    locations_for_ons_join_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.provider_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.postcode, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    ons_postcode_directory_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
        ]
    )

    expected_ons_join_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(CQCL.postcode, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )

    expected_split_registered_schema = StructType(
        [
            *expected_ons_join_schema,
        ]
    )

    expected_services_offered_schema = StructType(
        [
            *primary_service_type_schema,
            StructField(
                CQCLClean.services_offered,
                ArrayType(
                    StringType(),
                ),
            ),
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

    expected_schema_with_new_columns = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
            StructField("gender_labels", StringType(), True),
            StructField("nationality_labels", StringType(), True),
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

    sample_col_to_date_schema = StructType(
        [
            StructField("input_string", StringType(), True),
            StructField("expected_value", DateType(), True),
        ]
    )

    align_dates_primary_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
        ]
    )

    align_dates_secondary_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
        ]
    )

    primary_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
        ]
    )

    secondary_dates_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_aligned_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_merged_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
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

    full_schema = StructType(
        fields=[
            StructField(CQCP.provider_id, StringType(), True),
            StructField(
                CQCP.location_ids,
                ArrayType(
                    StringType(),
                ),
            ),
            StructField(CQCP.organisation_type, StringType(), True),
            StructField(CQCP.ownership_type, StringType(), True),
            StructField(CQCP.type, StringType(), True),
            StructField(CQCP.uprn, StringType(), True),
            StructField(CQCP.name, StringType(), True),
            StructField(CQCP.registration_status, StringType(), True),
            StructField(CQCP.registration_date, StringType(), True),
            StructField(CQCP.deregistration_date, StringType(), True),
            StructField(CQCP.address_line_one, StringType(), True),
            StructField(CQCP.town_or_city, StringType(), True),
            StructField(CQCP.county, StringType(), True),
            StructField(CQCP.region, StringType(), True),
            StructField(CQCP.postcode, StringType(), True),
            StructField(CQCP.latitude, FloatType(), True),
            StructField(CQCP.longitude, FloatType(), True),
            StructField(CQCP.phone_number, StringType(), True),
            StructField(CQCP.companies_house_number, StringType(), True),
            StructField(CQCP.inspection_directorate, StringType(), True),
            StructField(CQCP.constituency, StringType(), True),
            StructField(CQCP.local_authority, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class CQCPIRSchema:
    sample_schema = StructType(
        fields=[
            StructField(CQCPIR.location_id, StringType(), False),
            StructField(CQCPIR.location_name, StringType(), False),
            StructField(CQCPIR.pir_type, StringType(), False),
            StructField(CQCPIR.pir_submission_date, StringType(), False),
            StructField(
                CQCPIR.people_directly_employed,
                IntegerType(),
                True,
            ),
            StructField(
                CQCPIR.staff_leavers,
                IntegerType(),
                True,
            ),
            StructField(CQCPIR.staff_vacancies, IntegerType(), True),
            StructField(
                CQCPIR.shared_lives_leavers,
                IntegerType(),
                True,
            ),
            StructField(CQCPIR.shared_lives_vacancies, IntegerType(), True),
            StructField(CQCPIR.primary_inspection_category, StringType(), False),
            StructField(CQCPIR.region, StringType(), False),
            StructField(CQCPIR.local_authority, StringType(), False),
            StructField(CQCPIR.number_of_beds, IntegerType(), False),
            StructField(CQCPIR.domiciliary_care, StringType(), True),
            StructField(CQCPIR.location_status, StringType(), False),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    add_care_home_column_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(CQCPIR.pir_type, StringType(), True),
        ]
    )

    expected_care_home_column_schema = StructType(
        [
            *add_care_home_column_schema,
            StructField(CQCPIRClean.care_home, StringType(), True),
        ]
    )


@dataclass
class CQCPPIRCleanSchema:
    clean_subset_for_grouping_by = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_submission_date_as_date, DateType(), True),
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


@dataclass
class MergeIndCQCData:
    clean_cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), False),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.people_directly_employed, IntegerType(), True),
        ]
    )

    clean_cqc_location_for_merge_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
        ]
    )

    clean_ascwds_workplace_for_merge_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    expected_cqc_and_pir_merged_schema = StructType(
        [
            *clean_cqc_location_for_merge_schema,
            StructField(CQCPIRClean.people_directly_employed, IntegerType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
        ]
    )

    expected_cqc_and_ascwds_merged_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.total_staff, IntegerType(), True),
        ]
    )

    cqc_sector_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class CleanIndCQCData:
    merged_schema_for_cleaning_job = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.import_date, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(CQCLClean.current_cssr, StringType(), True),
            StructField(CQCLClean.current_rural_urban_ind_11, StringType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(CQCPIRClean.people_directly_employed, IntegerType(), True),
            StructField(AWPClean.total_staff_bounded, IntegerType(), True),
            StructField(AWPClean.worker_records_bounded, IntegerType(), True),
            StructField(CQCLClean.primary_service_type, StringType(), True),
        ]
    )

    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )

    repeated_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("integer_column", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_without_repeated_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("integer_column", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField("integer_column_deduplicated", IntegerType(), True),
        ]
    )


@dataclass
class FilterAscwdsFilledPostsSchema:
    input_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
        ]
    )
    care_home_filled_posts_per_bed_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
        ]
    )


@dataclass
class NonResFeaturesSchema(object):
    basic_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    filter_to_non_care_home_schema = StructType(
        [
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class CareHomeFeaturesSchema:
    clean_merged_data_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    filter_to_care_home_schema = StructType(
        [
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class EstimateIndCQCFilledPostsSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(
                IndCQC.contemporary_rural_urban_indicator_2011, StringType(), True
            ),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
        ]
    )

    populate_known_jobs_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class ModelPrimaryServiceRollingAverage:
    input_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
        ]
    )
    known_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
        ]
    )
    rolling_sum_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField("col_to_sum", StringType(), False),
        ]
    )
    rolling_average_schema = StructType(
        [
            StructField("other_col", StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
        ]
    )
    calculate_rolling_average_column_schema = StructType(
        [
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.count_of_filled_posts, IntegerType(), True),
            StructField(IndCQC.sum_of_filled_posts, DoubleType(), True),
        ]
    )


@dataclass
class ModelExtrapolation:
    extrapolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
        ]
    )
    data_to_filter_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.primary_service_type, StringType(), False),
        ]
    )
    first_and_last_submission_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
        ]
    )
    extrapolated_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.first_submission_time, LongType(), False),
            StructField(IndCQC.last_submission_time, LongType(), False),
            StructField(IndCQC.first_filled_posts, DoubleType(), True),
            StructField(IndCQC.first_rolling_average, DoubleType(), True),
            StructField(IndCQC.last_filled_posts, DoubleType(), True),
            StructField(IndCQC.last_rolling_average, DoubleType(), True),
        ]
    )
    extrapolated_values_to_be_added_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
        ]
    )
    extrapolated_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.first_submission_time, LongType(), False),
            StructField(IndCQC.last_submission_time, LongType(), False),
            StructField(IndCQC.first_rolling_average, DoubleType(), True),
            StructField(IndCQC.last_rolling_average, DoubleType(), True),
        ]
    )
    extrapolated_model_outputs_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, StringType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.first_submission_time, LongType(), False),
            StructField(IndCQC.last_submission_time, LongType(), False),
            StructField(IndCQC.first_filled_posts, DoubleType(), True),
            StructField(IndCQC.last_filled_posts, DoubleType(), True),
            StructField(IndCQC.extrapolation_ratio, DoubleType(), True),
        ]
    )


@dataclass
class ModelFeatures:
    vectorise_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("col_1", FloatType(), True),
            StructField("col_2", IntegerType(), True),
            StructField("col_3", IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    expected_vectorised_feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class ModelCareHomes:
    care_homes_cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    care_homes_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
        ]
    )


@dataclass
class InsertPredictionsIntoLocations:
    cleaned_cqc_schema = ModelCareHomes.care_homes_cleaned_ind_cqc_schema
    care_home_features_schema = ModelCareHomes.care_homes_features_schema

    predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )


@dataclass
class MLModelMetrics:
    ind_cqc_with_predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    predictions_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )

    r2_metric_schema = predictions_schema


@dataclass
class ModelInterpolation:
    interpolation_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )
    calculating_submission_dates_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )
    creating_timeseries_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.first_submission_time, LongType(), False),
            StructField(IndCQC.last_submission_time, LongType(), True),
        ]
    )
    merging_exploded_data_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, LongType(), False),
        ]
    )
    merging_known_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )
    calculating_interpolated_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.unix_time, LongType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_unix_time, LongType(), True),
        ]
    )


@dataclass
class ValidateMergedIndCqcData:
    cqc_locations_schema = MergeIndCQCData.clean_cqc_location_for_merge_schema
    merged_ind_cqc_schema = StructType(
        [
            *MergeIndCQCData.expected_cqc_and_ascwds_merged_schema,
            StructField(CQCPIRClean.people_directly_employed, IntegerType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
        ]
    )
