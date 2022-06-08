import unittest
import shutil

from pyspark.sql import SparkSession, Row

from utils import utils
from jobs import ingest_cqc_care_directory
from tests.test_file_generator import (
    generate_cqc_care_directory_csv_file,
    generate_locationid_and_providerid_file,
    generate_duplicate_providerid_data_file,
    generate_care_directory_locationid_file,
    generate_multiple_boolean_columns,
    generate_care_directory_registered_manager_name,
    generate_care_directory_gac_service_types,
    generate_care_directory_specialisms,
)


class CQC_Care_Directory_Tests(unittest.TestCase):

    TEST_CQC_CARE_DIRECTORY_FILE = "tests/test_data/domain=cqc/dataset=registered-provider-list"
    TEST_LOCATIONID_AND_PROVIDERID_FILE = "tests/test_data/tmp/locationid-and-providerid"
    TEST_DUPLICATE_PROVIDER_DATA_FILE = "tests/test_data/tmp/duplicate-provider-data"
    TEST_CARE_DIRECTORY_LOCATION_DATA_FILE = "tests/test_data/tmp/care-directory-location-data"
    TEST_MULTIPLE_BOOLEAN_COLUMNS = "tests/test_data/tmp/multiple-boolean-column-data"
    TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME = "tests/test_data/tmp/care-directory-registered-manager-name"
    TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES = "tests/test_data/tmp/care-directory-gac-service-types"
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
        self.spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()
        generate_cqc_care_directory_csv_file(self.TEST_CQC_CARE_DIRECTORY_FILE)
        generate_locationid_and_providerid_file(self.TEST_LOCATIONID_AND_PROVIDERID_FILE)
        generate_duplicate_providerid_data_file(self.TEST_DUPLICATE_PROVIDER_DATA_FILE)
        generate_care_directory_locationid_file(self.TEST_CARE_DIRECTORY_LOCATION_DATA_FILE)
        generate_multiple_boolean_columns(self.TEST_MULTIPLE_BOOLEAN_COLUMNS)
        generate_care_directory_registered_manager_name(self.TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME)
        generate_care_directory_gac_service_types(self.TEST_CARE_DIRECTORY_GAC_SERVICE_TYPES)
        generate_care_directory_specialisms(self.TEST_CARE_DIRECTORY_SPECIALISMS)

    @classmethod
    def tearDownClass(self):
        try:
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

    def test_unique_providerids_with_array_of_their_locationids(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_LOCATIONID_AND_PROVIDERID_FILE)

        locations_at_prov_df = ingest_cqc_care_directory.unique_providerids_with_array_of_their_locationids(df)

        self.assertEqual(locations_at_prov_df.count(), 2)
        self.assertEqual(locations_at_prov_df.columns, ["providerId", "locationIds"])

        provider1check_df = locations_at_prov_df.filter("providerId=='1-000000001'").select("locationIds")
        self.assertEqual(provider1check_df.collect(), [Row(locationIds=["1-000000001"])])

        locations_at_prov_df = locations_at_prov_df.collect()
        self.assertEqual(sorted(locations_at_prov_df[1]["locationIds"]), ["1-000000002", "1-000000003"])

    def test_get_distinct_provider_info(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_DUPLICATE_PROVIDER_DATA_FILE)

        distinct_prov_df = ingest_cqc_care_directory.get_distinct_provider_info(df)

        self.assertEqual(distinct_prov_df.count(), 4)
        self.assertEqual(
            distinct_prov_df.columns,
            [
                "providerId",
                "name",
                "mainPhoneNumber",
                "postalAddressLine1",
                "postalAddressTownCity",
                "postalAddressCounty",
                "postalCode",
                "organisationType",
                "registrationStatus",
            ],
        )

    def test_get_general_location_info(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_LOCATION_DATA_FILE)

        location_df = ingest_cqc_care_directory.get_general_location_info(df)

        self.assertEqual(location_df.count(), 3)
        self.assertEqual(
            location_df.columns,
            [
                "locationId",
                "providerId",
                "type",
                "name",
                "registrationDate",
                "numberOfBeds",
                "website",
                "postalAddressLine1",
                "postalAddressTownCity",
                "postalAddressCounty",
                "region",
                "postalCode",
                "careHome",
                "mainPhoneNumber",
                "localAuthority",
                "organisationType",
                "registrationStatus",
            ],
        )

    def test_convert_multiple_boolean_columns_into_single_array(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_MULTIPLE_BOOLEAN_COLUMNS, header=True)

        services_df = ingest_cqc_care_directory.convert_multiple_boolean_columns_into_single_array(
            df, self.REFORMAT_DICT, "new_alias"
        )

        self.assertEqual(services_df.count(), 3)
        self.assertEqual(services_df.columns, ["locationId", "new_alias"])

        services_df = services_df.collect()
        self.assertEqual(sorted(services_df[0]["new_alias"]), [["name A"], ["name B", "description B"], ["name C"]])
        self.assertEqual(
            sorted(services_df[1]["new_alias"]), [["name B", "description B"], ["name D", "description D"]]
        )
        self.assertEqual(sorted(services_df[2]["new_alias"]), [["name E", "description E"]])

    def test_create_contacts_from_registered_manager_name(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_REGISTERED_MANAGER_NAME)

        df = ingest_cqc_care_directory.create_contacts_from_registered_manager_name(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "contacts"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["contacts"],
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
            collected_df[1]["contacts"],
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
            collected_df[2]["contacts"],
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

        df = ingest_cqc_care_directory.convert_gac_service_types_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "gacservicetypes"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["gacservicetypes"],
            [
                Row(name="The name", description="description"),
                Row(
                    name="The name 2",
                    description="description 2",
                ),
            ],
        )
        self.assertEqual(
            collected_df[1]["gacservicetypes"],
            [
                Row(
                    name="Another name",
                    description="Some other description",
                )
            ],
        )
        self.assertEqual(
            collected_df[2]["gacservicetypes"],
            [],
        )

    def test_convert_specialisms_to_struct(self):
        spark = utils.get_spark()

        df = spark.read.parquet(self.TEST_CARE_DIRECTORY_SPECIALISMS)

        df = ingest_cqc_care_directory.convert_specialisms_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "specialisms"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["specialisms"],
            [
                Row(name="The name"),
                Row(name="The name 2"),
            ],
        )
        self.assertEqual(
            collected_df[1]["specialisms"],
            [Row(name="Another name")],
        )
        self.assertEqual(
            collected_df[2]["specialisms"],
            [],
        )

    def test_convert_to_cqc_provider_api_format(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_CQC_CARE_DIRECTORY_FILE)

        provider_df = ingest_cqc_care_directory.convert_to_cqc_provider_api_format(df)

        self.assertEqual(provider_df.count(), 4)
        self.assertEqual(
            provider_df.columns,
            [
                "providerId",
                "locationIds",
                "organisationType",
                "ownershipType",
                "type",
                "uprn",
                "name",
                "registrationStatus",
                "registrationDate",
                "deregistrationDate",
                "postalAddressLine1",
                "postalAddressTownCity",
                "postalAddressCounty",
                "region",
                "postalCode",
                "onspdLatitude",
                "onspdLongitude",
                "mainPhoneNumber",
                "companiesHouseNumber",
                "inspectionDirectorate",
                "constituency",
                "localAuthority",
            ],
        )

    def test_convert_to_cqc_location_api_format(self):
        spark = utils.get_spark()

        df = spark.read.csv(self.TEST_CQC_CARE_DIRECTORY_FILE)

        location_df = ingest_cqc_care_directory.convert_to_cqc_location_api_format(df)

        self.assertEqual(location_df.count(), 10)
        self.assertEqual(
            location_df.columns,
            [
                "locationId",
                "providerId",
                "organisationType",
                "type",
                "name",
                "onspdCcgCode",
                "onspdCcgName",
                "odsCode",
                "uprn",
                "registrationStatus",
                "registrationDate",
                "deregistrationDate",
                "dormancy",
                "numberOfBeds",
                "website",
                "postalAddressLine1",
                "postalAddressTownCity",
                "postalAddressCounty",
                "region",
                "postalCode",
                "onspdLatitude",
                "onspdLongitude",
                "careHome",
                "inspectionDirectorate",
                "mainPhoneNumber",
                "constituency",
                "localAuthority",
                "lastInspection",
                "lastReport",
                "relationships",
                "regulatedActivities",
                "gacServiceTypes",
                "inspectionCategories",
                "specialisms",
                "currentRatings",
                "historicRatings",
                "reports",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
