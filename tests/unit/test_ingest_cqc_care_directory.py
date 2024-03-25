import unittest
import shutil
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession, Row

from utils import utils
from utils.column_names.raw_data_files.cqc_care_directory_columns import (
    CqcCareDirectoryColumns as CareDirCols,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ProviderApiCols,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    OldCqcLocationApiColumns as LocationApiCols,
)
import jobs.ingest_cqc_care_directory as job
from tests.test_file_generator import (
    generate_raw_cqc_care_directory_csv_file,
    generate_cqc_care_directory_file,
    generate_locationid_and_providerid_file,
    generate_duplicate_providerid_data_file,
    generate_care_directory_locationid_file,
    generate_multiple_boolean_columns,
    generate_care_directory_registered_manager_name,
    generate_care_directory_gac_service_types,
    generate_care_directory_specialisms,
)


class CQC_Care_Directory_Tests(unittest.TestCase):
    TEST_RAW_CQC_CARE_DIRECTORY_CSV_FILE = (
        "tests/test_data/domain=cqc/dataset=registered-provider-list"
    )
    TEST_CQC_CARE_DIRECTORY_FILE = (
        "tests/test_data/tmp/formatted-registered-provider-list"
    )
    TEST_LOCATIONID_AND_PROVIDERID_FILE = (
        "tests/test_data/tmp/locationid-and-providerid"
    )
    TEST_DUPLICATE_PROVIDER_DATA_FILE = "tests/test_data/tmp/duplicate-provider-data"
    TEST_CARE_DIRECTORY_LOCATION_DATA_FILE = (
        "tests/test_data/tmp/care-directory-location-data"
    )
    TEST_MULTIPLE_BOOLEAN_COLUMNS = "tests/test_data/tmp/multiple-boolean-column-data"
    TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME = (
        "tests/test_data/tmp/care-directory-registered-manager-name"
    )
    TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES = (
        "tests/test_data/tmp/care-directory-gac-service-types"
    )
    TEST_CARE_DIRECTORY_SPECIALISMS = "tests/test_data/tmp/care-directory-specialisms"

    REFORMAT_DICT = {
        "Column A": "name A",
        "Column B": "name B,description B",
        "Column C": "name C",
        "Column D": "name D,description D",
        "Column-E": "name E,description E",
    }

    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder.appName(
            "test_ingest_cqc_care_directory"
        ).getOrCreate()
        generate_raw_cqc_care_directory_csv_file(
            self.TEST_RAW_CQC_CARE_DIRECTORY_CSV_FILE
        )
        generate_cqc_care_directory_file(self.TEST_CQC_CARE_DIRECTORY_FILE)
        generate_locationid_and_providerid_file(
            self.TEST_LOCATIONID_AND_PROVIDERID_FILE
        )
        generate_duplicate_providerid_data_file(self.TEST_DUPLICATE_PROVIDER_DATA_FILE)
        generate_care_directory_locationid_file(
            self.TEST_CARE_DIRECTORY_LOCATION_DATA_FILE
        )
        generate_multiple_boolean_columns(self.TEST_MULTIPLE_BOOLEAN_COLUMNS)
        generate_care_directory_registered_manager_name(
            self.TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME
        )
        generate_care_directory_gac_service_types(
            self.TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES
        )
        generate_care_directory_specialisms(self.TEST_CARE_DIRECTORY_SPECIALISMS)

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(self.TEST_RAW_CQC_CARE_DIRECTORY_CSV_FILE)
            shutil.rmtree(self.TEST_CQC_CARE_DIRECTORY_FILE)
            shutil.rmtree(self.TEST_LOCATIONID_AND_PROVIDERID_FILE)
            shutil.rmtree(self.TEST_DUPLICATE_PROVIDER_DATA_FILE)
            shutil.rmtree(self.TEST_CARE_DIRECTORY_LOCATION_DATA_FILE)
            shutil.rmtree(self.TEST_MULTIPLE_BOOLEAN_COLUMNS)
            shutil.rmtree(self.TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME)
            shutil.rmtree(self.TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES)
            shutil.rmtree(self.TEST_CARE_DIRECTORY_SPECIALISMS)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_cqc_care_directory(self):
        df = job.get_cqc_care_directory(self.TEST_RAW_CQC_CARE_DIRECTORY_CSV_FILE)

        self.assertEqual(df.count(), 1)
        self.assertEqual(
            df.columns,
            [
                LocationApiCols.location_id,
                CareDirCols.registration_date,
                CareDirCols.name,
                CareDirCols.type,
                ProviderApiCols.provider_id,
            ],
        )

        collected_df = df.collect()
        self.assertEqual(collected_df[0][CareDirCols.name], "Location 1")

    def test_unique_providerids_with_array_of_their_locationids(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_LOCATIONID_AND_PROVIDERID_FILE)

        locations_at_prov_df = job.unique_providerids_with_array_of_their_locationids(
            df
        )

        self.assertEqual(locations_at_prov_df.count(), 2)
        self.assertEqual(
            locations_at_prov_df.columns,
            [ProviderApiCols.provider_id, ProviderApiCols.location_ids],
        )

        provider1check_df = locations_at_prov_df.filter(
            locations_at_prov_df[ProviderApiCols.provider_id] == "1-000000001"
        ).select(ProviderApiCols.location_ids)
        self.assertEqual(
            provider1check_df.collect(), [Row(locationIds=["1-000000001"])]
        )

        locations_at_prov_df = locations_at_prov_df.collect()
        self.assertEqual(
            sorted(locations_at_prov_df[1][ProviderApiCols.location_ids]),
            ["1-000000002", "1-000000003"],
        )

    def test_get_distinct_provider_info(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_DUPLICATE_PROVIDER_DATA_FILE)

        distinct_prov_df = job.get_distinct_provider_info(df)

        self.assertEqual(distinct_prov_df.count(), 4)
        self.assertEqual(
            distinct_prov_df.columns,
            [
                ProviderApiCols.provider_id,
                ProviderApiCols.name,
                ProviderApiCols.phone_number,
                ProviderApiCols.address_line_one,
                ProviderApiCols.town_or_city,
                ProviderApiCols.county,
                ProviderApiCols.postcode,
                ProviderApiCols.organisation_type,
                ProviderApiCols.registration_status,
            ],
        )

    def test_get_general_location_info(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_LOCATION_DATA_FILE)

        location_df = job.get_general_location_info(df)

        self.assertEqual(location_df.count(), 3)
        self.assertEqual(
            location_df.columns,
            [
                LocationApiCols.location_id,
                LocationApiCols.provider_id,
                LocationApiCols.type,
                LocationApiCols.name,
                LocationApiCols.registration_date,
                LocationApiCols.number_of_beds,
                LocationApiCols.website,
                LocationApiCols.address_line_one,
                LocationApiCols.town_or_city,
                LocationApiCols.county,
                LocationApiCols.region,
                LocationApiCols.postcode,
                LocationApiCols.care_home,
                LocationApiCols.phone_number,
                LocationApiCols.local_authority,
                LocationApiCols.organisation_type,
                LocationApiCols.registration_status,
            ],
        )

    def test_convert_multiple_boolean_columns_into_single_array(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_MULTIPLE_BOOLEAN_COLUMNS, header=True)

        new_column_name = "new_alias"
        services_df = job.convert_multiple_boolean_columns_into_single_array(
            df, self.REFORMAT_DICT, new_column_name
        )

        self.assertEqual(services_df.count(), 3)
        self.assertEqual(
            services_df.columns, [LocationApiCols.location_id, new_column_name]
        )

        services_df = services_df.collect()
        self.assertEqual(
            sorted(services_df[0][new_column_name]),
            [["name A"], ["name B", "description B"], ["name C"]],
        )
        self.assertEqual(
            sorted(services_df[1][new_column_name]),
            [["name B", "description B"], ["name D", "description D"]],
        )
        self.assertEqual(
            sorted(services_df[2][new_column_name]), [["name E", "description E"]]
        )

    def test_create_contacts_from_registered_manager_name(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME)

        df = job.create_contacts_from_registered_manager_name(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(
            df.columns, [LocationApiCols.location_id, LocationApiCols.contacts]
        )

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0][LocationApiCols.contacts],
            [
                Row(
                    personTitle="M",
                    personGivenName="Firstname",
                    personFamilyName="Surname",
                    personRoles="Registered Manager",
                )
            ],
        )
        self.assertEqual(
            collected_df[1][LocationApiCols.contacts],
            [
                Row(
                    personTitle="M",
                    personGivenName="First Name",
                    personFamilyName="Surname",
                    personRoles="Registered Manager",
                )
            ],
        )
        self.assertEqual(
            collected_df[2][LocationApiCols.contacts],
            [
                Row(
                    personTitle=None,
                    personGivenName=None,
                    personFamilyName=None,
                    personRoles=None,
                )
            ],
        )

    def test_convert_gac_service_types_to_struct(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES)

        df = job.convert_gac_service_types_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(
            df.columns, [LocationApiCols.location_id, LocationApiCols.gac_service_types]
        )

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0][LocationApiCols.gac_service_types],
            [
                Row(name="The name", description="description"),
                Row(
                    name="The name 2",
                    description="description 2",
                ),
            ],
        )
        self.assertEqual(
            collected_df[1][LocationApiCols.gac_service_types],
            [
                Row(
                    name="Another name",
                    description="Some other description",
                )
            ],
        )
        self.assertEqual(
            collected_df[2][LocationApiCols.gac_service_types],
            [],
        )

    def test_convert_specialisms_to_struct(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_SPECIALISMS)

        df = job.convert_specialisms_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(
            df.columns, [LocationApiCols.location_id, LocationApiCols.specialisms]
        )

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0][LocationApiCols.specialisms],
            [
                Row(name="The name"),
                Row(name="The name 2"),
            ],
        )
        self.assertEqual(
            collected_df[1][LocationApiCols.specialisms],
            [Row(name="Another name")],
        )
        self.assertEqual(
            collected_df[2][LocationApiCols.specialisms],
            [],
        )

    def test_convert_to_cqc_provider_api_format(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_CQC_CARE_DIRECTORY_FILE, header=True)

        provider_df = job.convert_to_cqc_provider_api_format(df)

        self.assertEqual(provider_df.count(), 4)
        self.assertEqual(
            provider_df.columns,
            [
                ProviderApiCols.provider_id,
                ProviderApiCols.location_ids,
                ProviderApiCols.organisation_type,
                ProviderApiCols.ownership_type,
                ProviderApiCols.type,
                ProviderApiCols.uprn,
                ProviderApiCols.name,
                ProviderApiCols.registration_status,
                ProviderApiCols.registration_date,
                ProviderApiCols.deregistration_date,
                ProviderApiCols.address_line_one,
                ProviderApiCols.town_or_city,
                ProviderApiCols.county,
                ProviderApiCols.region,
                ProviderApiCols.postcode,
                ProviderApiCols.latitude,
                ProviderApiCols.longitude,
                ProviderApiCols.phone_number,
                ProviderApiCols.companies_house_number,
                ProviderApiCols.inspection_directorate,
                ProviderApiCols.constituency,
                ProviderApiCols.local_authority,
            ],
        )

    def test_convert_to_cqc_location_api_format(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_CQC_CARE_DIRECTORY_FILE, header=True)

        location_df = job.convert_to_cqc_location_api_format(df)

        self.assertEqual(location_df.count(), 10)
        self.assertEqual(
            location_df.columns,
            [
                LocationApiCols.location_id,
                LocationApiCols.provider_id,
                LocationApiCols.organisation_type,
                LocationApiCols.type,
                LocationApiCols.name,
                LocationApiCols.ccg_code,
                LocationApiCols.ccg_name,
                LocationApiCols.ods_code,
                LocationApiCols.uprn,
                LocationApiCols.registration_status,
                LocationApiCols.registration_date,
                LocationApiCols.deregistration_date,
                LocationApiCols.dormancy,
                LocationApiCols.number_of_beds,
                LocationApiCols.website,
                LocationApiCols.address_line_one,
                LocationApiCols.town_or_city,
                LocationApiCols.county,
                LocationApiCols.region,
                LocationApiCols.postcode,
                LocationApiCols.latitude,
                LocationApiCols.longitude,
                LocationApiCols.care_home,
                LocationApiCols.inspection_directorate,
                LocationApiCols.phone_number,
                LocationApiCols.constituancy,
                LocationApiCols.local_authority,
                LocationApiCols.last_inspection,
                LocationApiCols.last_report,
                LocationApiCols.relationships,
                LocationApiCols.regulated_activities,
                LocationApiCols.gac_service_types,
                LocationApiCols.inspection_categories,
                LocationApiCols.specialisms,
                LocationApiCols.current_ratings,
                LocationApiCols.historic_ratings,
                LocationApiCols.reports,
            ],
        )

    def test_get_cqc_care_directory_keeps_dates_as_strings(self):
        returned_df = job.get_cqc_care_directory(self.TEST_CQC_CARE_DIRECTORY_FILE)
        self.assertEqual(
            returned_df.select(CareDirCols.registration_date).dtypes,
            [(CareDirCols.registration_date, "string")],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
