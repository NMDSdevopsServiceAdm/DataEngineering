import unittest
import boto3
from io import BytesIO
from datetime import datetime

from enum import Enum
from botocore.stub import Stubber
from botocore.response import StreamingBody
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    FloatType,
)

from _01_ingest.utils.utils import ingest_utils as job
from utils import utils


class StubberType(Enum):
    client = "client"
    resource = "resource"


class StubberClass:
    __s3_client = None
    __s3_resource = None
    __stubber = None
    __type = ""

    def __init__(self, type):
        self.__type = type
        self.decide_type()

    def decide_type(self):
        if self.__type == StubberType.client:
            self.build_client()
            self.build_stubber_client()

        if self.__type == StubberType.resource:
            self.build_resource()
            self.build_stubber_resource()

    def get_s3_client(self):
        return self.__s3_client

    def get_s3_resource(self):
        return self.__s3_resource

    def get_stubber(self):
        return self.__stubber

    def build_client(self):
        self.__s3_client = boto3.client("s3")

    def build_resource(self):
        self.__s3_resource = boto3.resource("s3")

    def build_stubber_client(self):
        self.__stubber = Stubber(self.__s3_client)

    def build_stubber_resource(self):
        self.__stubber = Stubber(self.__s3_resource.meta.client)

    def add_response(self, stubbed_method, data, params):
        self.__stubber.add_response(stubbed_method, data, params)
        self.__stubber.activate()


class IngestUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class IsCsvTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_is_csv_returns_true_when_filepath_is_a_csv_file(self):
        csv_name = "s3://sfc-data-engineering-raw/domain=test/version=0.0.1/Example CSV file.csv"
        csv_test = job.is_csv(csv_name)
        self.assertTrue(csv_test)

    def test_is_csv_returns_false_when_filepath_is_not_a_csv_file(self):
        csv_name_without_extention = (
            "s3://sfc-data-engineering-raw/domain=test/version=0.0.1/Example CSV file"
        )
        csv_test = job.is_csv(csv_name_without_extention)
        self.assertFalse(csv_test)


class GetS3ObjectsListTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_get_s3_objects_list_returns_all_objects(self):
        partial_response = {
            "Contents": [
                {
                    "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                    "Size": 123,
                },
                {
                    "Key": "version=1.0.0/import_date=20210101/some-other-data-file.csv",
                    "Size": 100,
                },
                {
                    "Key": "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
                    "Size": 150,
                },
            ]
        }

        expected_params = {
            "Bucket": "test-bucket",
            "Prefix": "version=1.0.0/import_date=20210101/",
        }

        stubber = StubberClass(StubberType.resource)
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = job.get_s3_objects_list(
            "test-bucket",
            "version=1.0.0/import_date=20210101/",
            stubber.get_s3_resource(),
        )

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list,
            [
                "version=1.0.0/import_date=20210101/some-data-file.csv",
                "version=1.0.0/import_date=20210101/some-other-data-file.csv",
                "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
            ],
        )
        self.assertEqual(len(object_list), 3)

    def test_get_s3_objects_doesnt_return_directories(self):
        partial_response = {
            "Contents": [
                {
                    "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                    "Size": 123,
                },
                {"Key": "version=1.0.0/import_date=20210101/", "Size": 0},
                {
                    "Key": "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
                    "Size": 100,
                },
            ]
        }

        expected_params = {
            "Bucket": "test-bucket",
            "Prefix": "version=1.0.0/import_date=20210101/",
        }

        stubber = StubberClass(StubberType.resource)
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = job.get_s3_objects_list(
            "test-bucket",
            "version=1.0.0/import_date=20210101/",
            stubber.get_s3_resource(),
        )

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list,
            [
                "version=1.0.0/import_date=20210101/some-data-file.csv",
                "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
            ],
        )
        self.assertEqual(len(object_list), 2)

    def test_get_s3_objects_list_returns_filtered_objects(self):
        partial_response = {
            "Contents": [
                {
                    "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                    "Size": 123,
                }
            ]
        }

        expected_params = {
            "Bucket": "test-bucket",
            "Prefix": "version=1.0.0/import_date=20210101/",
        }

        stubber = StubberClass(StubberType.resource)
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = job.get_s3_objects_list(
            "test-bucket",
            "version=1.0.0/import_date=20210101/",
            stubber.get_s3_resource(),
        )

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv"]
        )
        self.assertEqual(len(object_list), 1)


class SplitS3UriTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = job.split_s3_uri(s3_uri)

        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(prefix, "domain=ASCWDS/dataset=workplace/")


class GetFileDirectoryTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_get_file_directory(self):
        path = job.get_file_directory(
            "domain=ASCWDS/dataset=workplace/version=0.0.1/import_date=20130331/Filename.csv"
        )

        self.assertEqual(
            path,
            "domain=ASCWDS/dataset=workplace/version=0.0.1/import_date=20130331",
        )


class ConstructS3UriTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_construct_s3_uri(self):
        uri = job.construct_s3_uri(
            "sfc-data-engineering-raw",
            "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv",
        )

        self.assertEqual(
            uri,
            "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv",
        )


class ConstructDestinationPathTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_construct_new_destination_path(self):
        destination = "s3://sfc-main-datasets/"
        key = "domain=ASCWDS/dataset=workplace/version=0.0.1/workers.csv"
        destination_path = job.construct_destination_path(destination, key)

        self.assertEqual(
            destination_path,
            "s3://sfc-main-datasets/domain=ASCWDS/dataset=workplace/version=0.0.1/",
        )


class ReadPartialCsvContentTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        # increase length of string to simulate realistic file size
        self.hundred_percent_string_boost = 100
        self.smaller_string_boost = 35

    def test_read_partial_csv_content(self):
        body_data = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"

        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(BytesIO(body_encoded), byte_string_length)

        partial_response = {
            "Body": body,
            "ContentLength": byte_string_length * self.hundred_percent_string_boost,
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber = StubberClass(StubberType.client)
        stubber.add_response("get_object", partial_response, expected_params)

        obj_partial_content = job.read_partial_csv_content(
            "test-bucket", "my-test/key/", stubber.get_s3_client()
        )

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content,
            "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species",
        )

    def test_read_partial_csv_less_content(self):
        body_data = "period|establishmentid|tribalid|tribalid_worker|parentid|orgid|nmdsid|workerid|wkplacestat|createddate|updateddate|savedate|cqcpermission|lapermission|regtype|"
        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(BytesIO(body_encoded), byte_string_length)

        partial_response = {
            "Body": body,
            "ContentLength": byte_string_length * self.smaller_string_boost,
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber = StubberClass(StubberType.client)
        stubber.add_response("get_object", partial_response, expected_params)

        obj_partial_content = job.read_partial_csv_content(
            "test-bucket", "my-test/key/", stubber.get_s3_client()
        )

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content,
            "period|establishmentid|tribalid|tribalid_worker|parenti",
        )


class IdentifyCSVDelimiterTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_identify_csv_delimiter_can_identify_comma(self):
        sample = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        delimiter = job.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, ",")

    def test_identify_csv_delimiter_can_identify_pipe(self):
        sample = "period|establishmentid|tribalid|parentid|orgid|nmdsid|wkplacestat|"
        delimiter = job.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, "|")


class ReadCsvTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_csv_path = "tests/test_data/example_csv.csv"
        self.test_csv_custom_delim_path = (
            "tests/test_data/example_csv_custom_delimiter.csv"
        )

    def test_read_csv(self):
        df = job.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c", "date_col"])
        self.assertEqual(df.count(), 3)

    def test_read_with_custom_delimiter(self):
        df = job.read_csv(self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)


class ReadCsvWithDefinedSchemaTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.example_csv_for_schema_tests = (
            "tests/test_data/example_csv_for_schema_tests.csv"
        )
        self.example_csv_for_schema_tests_extra_column = (
            "tests/test_data/example_csv_for_schema_tests_extra_column.csv"
        )

        self.df_with_extra_col = self.spark.read.csv(
            self.example_csv_for_schema_tests_extra_column, header=True
        )

    def test_read_csv_with_defined_schema(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), True),
                StructField("integer_field", IntegerType(), True),
                StructField("float_field", FloatType(), True),
            ]
        )

        df = job.read_csv_with_defined_schema(self.example_csv_for_schema_tests, schema)
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        row_one = df.collect()[0]
        assert isinstance(row_one.string_field, str)
        assert isinstance(row_one.integer_field, int)
        assert isinstance(row_one.float_field, float)

    def test_read_csv_with_defined_schema_with_null_values_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df = job.read_csv_with_defined_schema(self.example_csv_for_schema_tests, schema)
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        row_two = df.collect()[1]
        assert isinstance(row_two.string_field, type(None))
        assert isinstance(row_two.integer_field, type(None))
        assert isinstance(row_two.float_field, type(None))

    def test_read_csv_with_defined_schema_with_column_missing_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
                StructField("missing_field", StringType(), True),
            ]
        )

        df = job.read_csv_with_defined_schema(self.example_csv_for_schema_tests, schema)
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")
        self.assertEqual(df.columns[3], "missing_field")

    def test_read_csv_with_defined_schema_with_extra_column_in_csv(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), False),
                StructField("integer_field", IntegerType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df_with_no_schema = self.df_with_extra_col

        df = job.read_csv_with_defined_schema(
            self.example_csv_for_schema_tests_extra_column, schema
        )
        self.assertEqual(df.columns[0], "string_field")
        self.assertEqual(df.columns[1], "integer_field")
        self.assertEqual(df.columns[2], "float_field")

        self.assertTrue(len(df.columns) < len(df_with_no_schema.columns))

    def test_read_csv_with_defined_schema_where_there_is_incorrect_value_type(self):
        schema = StructType(
            [
                StructField("string_field", IntegerType(), False),
                StructField("integer_field", StringType(), False),
                StructField("float_field", FloatType(), False),
            ]
        )

        df = job.read_csv_with_defined_schema(self.example_csv_for_schema_tests, schema)

        row_one = df.collect()[0]
        assert isinstance(row_one.string_field, type(None))
        assert isinstance(row_one.integer_field, str)
        assert isinstance(row_one.float_field, float)


class GenerateS3DatasetsDirDatePathTests(IngestUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_s3_datasets_dir_date_path_changes_version_when_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        version_number = "2.0.0"
        dir_path = job.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets",
            "test_domain",
            "test_dateset",
            dec_first_21,
            version_number,
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=2.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )

    def test_generate_s3_datasets_dir_date_path_uses_version_one_when_no_version_number_is_passed(
        self,
    ):
        dec_first_21 = datetime(2021, 12, 1)
        dir_path = job.generate_s3_datasets_dir_date_path(
            "s3://sfc-main-datasets", "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201/",
        )
