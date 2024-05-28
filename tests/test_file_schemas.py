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
    NewCqcLocationApiColumns as CQCL,
    NewCqcLocationApiColumns as CQCLNew,
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
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned_values import (
    AscwdsWorkerCleanedColumns as AWKClean,
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
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
)
from utils.cqc_ratings_utils.cqc_ratings_values import (
    CQCRatingsColumns as CQCRatings,
)
from utils.column_names.validation_table_columns import Validation


from schemas.cqc_location_schema import LOCATION_SCHEMA


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
    reduce_year_by_one_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField("other column", StringType(), True),
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

    mupddate_for_org_schema = StructType(
        [
            StructField(AWP.organisation_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
        ]
    )
    expected_mupddate_for_org_schema = StructType(
        [
            *mupddate_for_org_schema,
            StructField(AWPClean.master_update_date_org, DateType(), True),
        ]
    )

    add_purge_data_col_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.is_parent, StringType(), True),
            StructField(AWP.master_update_date, DateType(), True),
            StructField(AWPClean.master_update_date_org, DateType(), True),
        ]
    )
    expected_add_purge_data_col_schema = StructType(
        [
            *add_purge_data_col_schema,
            StructField(AWPClean.data_last_amended_date, DateType(), True),
        ]
    )

    add_workplace_last_active_date_col_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWPClean.data_last_amended_date, DateType(), True),
            StructField(AWPClean.last_logged_in_date, DateType(), True),
        ]
    )
    expected_add_workplace_last_active_date_col_schema = StructType(
        [
            *add_workplace_last_active_date_col_schema,
            StructField(AWPClean.workplace_last_active_date, DateType(), True),
        ]
    )

    date_col_for_purging_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
        ]
    )
    expected_date_col_for_purging_schema = StructType(
        [
            *date_col_for_purging_schema,
            StructField(AWPClean.purge_date, DateType(), True),
        ]
    )

    workplace_last_active_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), True),
            StructField("last_active", DateType(), True),
            StructField(AWPClean.purge_date, DateType(), True),
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
class PAFilledPostsByIcbAreaSchema:
    sample_ons_contemporary_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
        ]
    )

    expected_postcode_count_per_la_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
        ]
    )

    expected_postcode_count_per_la_icb_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    sample_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    expected_ratio_between_hybrid_area_and_la_area_postcodes_schema = StructType(
        [
            *sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    full_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    sample_pa_filled_posts_schema = StructType(
        [
            StructField(DP.LA_AREA, StringType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    expected_create_date_column_from_year_in_pa_estimates_schema = StructType(
        [
            *sample_pa_filled_posts_schema,
            StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
        ]
    )

    sample_postcode_proportions_before_joining_pa_filled_posts_schema = (
        expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema
    )

    sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_schema = (
        StructType(
            [
                StructField(DP.LA_AREA, StringType(), True),
                StructField(
                    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
                    DoubleType(),
                    True,
                ),
                StructField(DP.YEAR, StringType(), True),
                StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
            ]
        )
    )

    expected_postcode_proportions_after_joining_pa_filled_posts_schema = StructType(
        [
            *sample_postcode_proportions_before_joining_pa_filled_posts_schema,
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    sample_proportions_and_pa_filled_posts_schema = StructType(
        [
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
        ]
    )

    expected_pa_filled_posts_after_applying_proportions_schema = StructType(
        [
            *sample_proportions_and_pa_filled_posts_schema,
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_ICB,
                DoubleType(),
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
    detailed_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.provider_id, StringType(), True),
            StructField(CQCL.organisation_type, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.name, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.registration_date, StringType(), True),
            StructField(CQCL.deregistration_date, StringType(), True),
            StructField(CQCL.dormancy, StringType(), True),
            StructField(CQCL.number_of_beds, IntegerType(), True),
            StructField(CQCL.website, StringType(), True),
            StructField(CQCL.postal_address_line1, StringType(), True),
            StructField(CQCL.postal_address_town_city, StringType(), True),
            StructField(CQCL.postal_address_county, StringType(), True),
            StructField(CQCL.region, StringType(), True),
            StructField(CQCL.postal_code, StringType(), True),
            StructField(CQCL.onspd_latitude, StringType(), True),
            StructField(CQCL.onspd_longitude, StringType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCL.inspection_directorate, StringType(), True),
            StructField(CQCL.main_phone_number, StringType(), True),
            StructField(CQCL.local_authority, StringType(), True),
            StructField(
                CQCL.regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.code, StringType(), True),
                            StructField(
                                CQCL.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                CQCL.person_family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_roles,
                                                ArrayType(StringType(), True),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_title, StringType(), True
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
            StructField(
                CQCL.specialisms,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
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
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
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
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                True,
            ),
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
            StructField(CQCL.postal_code, StringType(), True),
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
            StructField(CQCLClean.postal_code, StringType(), True),
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
            StructField(CQCL.postal_code, StringType(), True),
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
class UtilsSchema:
    filter_to_max_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("date_type_column", DateType(), True),
            StructField("import_date_style_col", StringType(), True),
        ]
    )

    select_rows_with_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value_to_filter_on", StringType(), True),
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
    rows_without_cqc_sector_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField("some_data", StringType(), True),
        ]
    )
    expected_rows_with_cqc_sector_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField("some_data", StringType(), True),
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
            StructField(CQCP.postal_address_line1, StringType(), True),
            StructField(CQCP.postal_address_town_city, StringType(), True),
            StructField(CQCP.postal_address_county, StringType(), True),
            StructField(CQCP.region, StringType(), True),
            StructField(CQCP.postal_code, StringType(), True),
            StructField(CQCP.onspd_latitude, FloatType(), True),
            StructField(CQCP.onspd_longitude, FloatType(), True),
            StructField(CQCP.main_phone_number, StringType(), True),
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
class CQCPIRCleanSchema:
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
class IndCQCDataUtils:
    input_schema_for_adding_estimate_filled_posts_and_source = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("model_name_1", FloatType(), True),
            StructField("model_name_2", FloatType(), True),
            StructField("model_name_3", FloatType(), True),
        ]
    )

    expected_schema_with_estimate_filled_posts_and_source = StructType(
        [
            *input_schema_for_adding_estimate_filled_posts_and_source,
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    estimated_source_description_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
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
class ReconciliationSchema:
    input_ascwds_workplace_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )
    input_cqc_location_api_schema = StructType(
        [
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, StringType(), True),
        ]
    )

    expected_prepared_most_recent_cqc_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )
    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    regtype_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
        ]
    )

    remove_head_office_accounts_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
        ]
    )

    filter_to_relevant_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.registration_status, StringType(), True),
            StructField(CQCLClean.deregistration_date, DateType(), True),
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    add_singles_and_subs_description_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
        ]
    )

    expected_singles_and_subs_description_schema = StructType(
        [
            *add_singles_and_subs_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
        ]
    )

    expected_create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
        ]
    )

    final_column_selection_schema = StructType(
        [
            StructField("extra_column", StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
        ]
    )

    expected_final_column_selection_schema = StructType(
        [
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
        ]
    )

    add_subject_column_schema = StructType(
        [
            StructField("id", StringType(), True),
        ]
    )

    expected_add_subject_column_schema = StructType(
        [
            *add_subject_column_schema,
            StructField(ReconColumn.subject, StringType(), True),
        ]
    )

    new_issues_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )
    unique_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_join_array_of_nmdsids_schema = StructType(
        [
            *unique_schema,
            StructField("new_column", StringType(), True),
        ]
    )

    create_parents_description_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(ReconColumn.new_potential_subs, StringType(), True),
            StructField(ReconColumn.old_potential_subs, StringType(), True),
            StructField(
                ReconColumn.missing_or_incorrect_potential_subs, StringType(), True
            ),
        ]
    )

    expected_create_parents_description_schema = StructType(
        [
            *create_parents_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )

    cqc_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
        ]
    )
    ascwds_data_for_join_schema = StructType(
        [
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
        ]
    )
    expected_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
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
class NonResAscwdsWithDormancyFeaturesSchema(object):
    basic_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
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

    filter_to_dormancy_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
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
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.provider_name, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.organisation_id, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )


@dataclass
class FlattenCQCRatings:
    test_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCLNew.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCLNew.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCLNew.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCLNew.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCLNew.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
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
                        StructField(
                            CQCLNew.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCLNew.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
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
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCLNew.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCLNew.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCLNew.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCLNew.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCLNew.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                True,
            ),
        ]
    )
    test_ascwds_workplace_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    filter_to_first_import_of_most_recent_month_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), False),
            StructField(Keys.year, StringType(), False),
            StructField(Keys.month, StringType(), False),
            StructField(Keys.day, StringType(), False),
        ]
    )
    flatten_current_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.current_ratings,
                StructType(
                    [
                        StructField(
                            CQCL.overall,
                            StructType(
                                [
                                    StructField(
                                        CQCL.organisation_id, StringType(), True
                                    ),
                                    StructField(CQCL.rating, StringType(), True),
                                    StructField(CQCL.report_date, StringType(), True),
                                    StructField(
                                        CQCL.report_link_id, StringType(), True
                                    ),
                                    StructField(
                                        CQCL.use_of_resources,
                                        StructType(
                                            [
                                                StructField(
                                                    CQCL.organisation_id,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.summary, StringType(), True
                                                ),
                                                StructField(
                                                    CQCL.use_of_resources_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_summary,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.combined_quality_rating,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_date,
                                                    StringType(),
                                                    True,
                                                ),
                                                StructField(
                                                    CQCL.report_link_id,
                                                    StringType(),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        CQCL.key_question_ratings,
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.name, StringType(), True
                                                    ),
                                                    StructField(
                                                        CQCL.rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_date,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.organisation_id,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.report_link_id,
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
                        StructField(
                            CQCL.service_ratings,
                            ArrayType(
                                StructType(
                                    [
                                        StructField(CQCL.name, StringType(), True),
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.report_date, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.organisation_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.report_link_id, StringType(), True
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    flatten_historic_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(
                CQCL.historic_ratings,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.report_date, StringType(), True),
                            StructField(CQCL.report_link_id, StringType(), True),
                            StructField(CQCL.organisation_id, StringType(), True),
                            StructField(
                                CQCL.service_ratings,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(CQCL.name, StringType(), True),
                                            StructField(
                                                CQCL.rating, StringType(), True
                                            ),
                                            StructField(
                                                CQCL.key_question_ratings,
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                CQCL.name,
                                                                StringType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                CQCL.rating,
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
                                True,
                            ),
                            StructField(
                                CQCL.overall,
                                StructType(
                                    [
                                        StructField(CQCL.rating, StringType(), True),
                                        StructField(
                                            CQCL.use_of_resources,
                                            StructType(
                                                [
                                                    StructField(
                                                        CQCL.combined_quality_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.combined_quality_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_rating,
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        CQCL.use_of_resources_summary,
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            CQCL.key_question_ratings,
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            CQCL.name,
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField(
                                                            CQCL.rating,
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
                True,
            ),
        ]
    )
    expected_flatten_ratings_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
        ]
    )

    add_current_or_historic_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
        ]
    )
    expected_add_current_or_historic_schema = StructType(
        [
            *add_current_or_historic_schema,
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    remove_blank_rows_schema = expected_flatten_ratings_schema

    add_rating_sequence_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
        ]
    )
    expected_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
        ]
    )
    expected_reversed_add_rating_sequence_schema = StructType(
        [
            *add_rating_sequence_schema,
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )

    add_latest_rating_flag_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
        ]
    )
    expected_add_latest_rating_flag_schema = StructType(
        [
            *add_latest_rating_flag_schema,
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
        ]
    )

    create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
            StructField(CQCRatings.reversed_rating_sequence, IntegerType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
        ]
    )

    expected_create_standard_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.safe_rating, StringType(), True),
            StructField(CQCRatings.well_led_rating, StringType(), True),
            StructField(CQCRatings.caring_rating, StringType(), True),
            StructField(CQCRatings.responsive_rating, StringType(), True),
            StructField(CQCRatings.effective_rating, StringType(), True),
            StructField(CQCRatings.rating_sequence, IntegerType(), True),
            StructField(CQCRatings.latest_rating_flag, IntegerType(), True),
        ]
    )

    select_ratings_for_benchmarks_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCRatings.current_or_historic, StringType(), True),
        ]
    )

    add_good_and_outstanding_flag_column_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
        ]
    )

    expected_add_good_and_outstanding_flag_column_schema = StructType(
        [
            *add_good_and_outstanding_flag_column_schema,
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
        ]
    )

    ratings_join_establishment_ids_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("other_ratings_column", StringType(), True),
        ]
    )
    ascwds_join_establishment_ids_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(AWP.import_date, StringType(), True),
        ]
    )
    expected_join_establishment_ids_schema = StructType(
        [
            *ratings_join_establishment_ids_schema,
            StructField(AWP.establishment_id, StringType(), True),
        ]
    )
    create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.overall_rating, StringType(), True),
            StructField(CQCRatings.date, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_create_benchmark_ratings_dataset_schema = StructType(
        [
            StructField(CQCRatings.benchmarks_location_id, StringType(), True),
            StructField(CQCRatings.benchmarks_establishment_id, StringType(), True),
            StructField(CQCRatings.good_or_outstanding_flag, IntegerType(), True),
            StructField(CQCRatings.benchmarks_overall_rating, StringType(), True),
            StructField(CQCRatings.inspection_date, StringType(), True),
        ]
    )


@dataclass
class ValidationUtils:
    validation_schema = StructType(
        [
            StructField(Validation.check, StringType(), True),
            StructField(Validation.check_level, StringType(), True),
            StructField(Validation.check_status, StringType(), True),
            StructField(Validation.constraint, StringType(), True),
            StructField(Validation.constraint_status, StringType(), True),
            StructField(Validation.constraint_message, StringType(), True),
        ]
    )

    size_of_dataset_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
        ]
    )

    index_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    min_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    min_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
        ]
    )

    max_values_schema = min_values_schema
    max_values_multiple_columns_schema = min_values_multiple_columns_schema

    one_column_schema = size_of_dataset_schema
    two_column_schema = index_column_schema
    multiple_rules_schema = index_column_schema

    categorical_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
        ]
    )


@dataclass
class ValidateLocationsAPICleanedData:
    raw_cqc_locations_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )
    cleaned_cqc_locations_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.cqc_provider_import_date, DateType(), True),
            StructField(CQCLClean.care_home, StringType(), True),
            StructField(CQCLClean.name, StringType(), True),
            StructField(CQCLClean.provider_id, StringType(), True),
            StructField(CQCLClean.provider_name, StringType(), True),
            StructField(CQCLClean.cqc_sector, StringType(), True),
            StructField(CQCLClean.registration_status, StringType(), True),
            StructField(CQCLClean.registration_date, DateType(), True),
            StructField(CQCLClean.dormancy, StringType(), True),
            StructField(CQCLClean.number_of_beds, IntegerType(), True),
            StructField(CQCLClean.primary_service_type, StringType(), True),
            StructField(CQCLClean.contemporary_ons_import_date, DateType(), True),
            StructField(CQCLClean.contemporary_cssr, StringType(), True),
            StructField(CQCLClean.contemporary_region, StringType(), True),
            StructField(CQCLClean.current_ons_import_date, DateType(), True),
            StructField(CQCLClean.current_cssr, StringType(), True),
            StructField(CQCLClean.current_region, StringType(), True),
            StructField(CQCLClean.current_rural_urban_ind_11, StringType(), True),
        ]
    )
    calculate_expected_size_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.type, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
        ]
    )


@dataclass
class ValidateProvidersAPICleanedData:
    raw_cqc_providers_schema = StructType(
        [
            StructField(CQCP.provider_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    cleaned_cqc_providers_schema = StructType(
        [
            StructField(CQCPClean.provider_id, StringType(), True),
            StructField(CQCPClean.cqc_provider_import_date, DateType(), True),
            StructField(CQCPClean.name, StringType(), True),
            StructField(CQCPClean.cqc_sector, StringType(), True),
        ]
    )
    calculate_expected_size_schema = raw_cqc_providers_schema


@dataclass
class ValidatePIRCleanedData:
    cleaned_cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.people_directly_employed, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkplaceCleanedData:
    cleaned_ascwds_workplace_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.total_staff_bounded, IntegerType(), True),
            StructField(AWPClean.worker_records_bounded, IntegerType(), True),
        ]
    )


@dataclass
class ValidateASCWDSWorkerCleanedData:
    cleaned_ascwds_worker_schema = StructType(
        [
            StructField(AWKClean.establishment_id, StringType(), True),
            StructField(AWKClean.ascwds_worker_import_date, DateType(), True),
            StructField(AWKClean.worker_id, StringType(), True),
            StructField(AWKClean.main_job_role_id, StringType(), True),
            StructField(AWKClean.main_job_role_labelled, StringType(), True),
        ]
    )


@dataclass
class ValidatePostcodeDirectoryCleanedData:
    raw_postcode_directory_schema = StructType(
        [
            StructField(ONS.import_date, StringType(), True),
            StructField(ONS.postcode, StringType(), True),
        ]
    )
    cleaned_postcode_directory_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_region, StringType(), True),
            StructField(ONSClean.current_ons_import_date, DateType(), True),
            StructField(ONSClean.current_cssr, StringType(), True),
            StructField(ONSClean.current_region, StringType(), True),
            StructField(ONSClean.current_rural_urban_ind_11, StringType(), True),
        ]
    )

    calculate_expected_size_schema = raw_postcode_directory_schema


@dataclass
class ValidateCleanedIndCqcData:
    merged_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.cqc_pir_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.name, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.provider_name, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
            StructField(IndCQC.registration_date, DateType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.services_offered,
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.contemporary_ons_import_date, DateType(), True),
            StructField(IndCQC.contemporary_cssr, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.organisation_id, StringType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_clean, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.people_directly_employed_dedup, IntegerType(), True),
        ]
    )
    calculate_expected_size_schema = merged_ind_cqc_schema


@dataclass
class ValidateCareHomeIndCqcFeaturesData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
        ]
    )
    care_home_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.features, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateNonResASCWDSIncDormancyIndCqcFeaturesData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
        ]
    )
    non_res_ascwds_inc_dormancy_ind_cqc_features_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateEstimatedIndCqcFilledPostsData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_ind_cqc_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.people_directly_employed, IntegerType(), True),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.unix_time, IntegerType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.rolling_average_model, DoubleType(), True),
            StructField(IndCQC.care_home_model, DoubleType(), True),
            StructField(IndCQC.extrapolation_model, DoubleType(), True),
            StructField(IndCQC.interpolation_model, DoubleType(), True),
            StructField(IndCQC.non_res_model, DoubleType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateEstimatedIndCqcFilledPostsByJobRoleSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_ind_cqc_filled_posts_by_job_role_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class ValidateASCWDSWorkplaceRawData:
    raw_ascwds_workplace_schema = StructType(
        [
            StructField(AWP.establishment_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )


@dataclass
class ValidatePIRRawData:
    raw_cqc_pir_schema = StructType(
        [
            StructField(CQCPIR.location_id, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCPIR.people_directly_employed, StringType(), True),
        ]
    )
