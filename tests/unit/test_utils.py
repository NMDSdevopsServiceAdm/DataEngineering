from datetime import datetime
from pathlib import Path
import shutil
import unittest
from io import BytesIO
from enum import Enum

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

import boto3
from botocore.stub import Stubber
from botocore.response import StreamingBody

from utils import utils
from tests.test_file_generator import generate_ascwds_workplace_file


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


class UtilsTests(unittest.TestCase):

    test_csv_path = "tests/test_data/example_csv.csv"
    test_csv_custom_delim_path = "tests/test_data/example_csv_custom_delimiter.csv"
    tmp_dir = "tmp-out"
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/tmp-workplace"

    # increase length of string to simulate realistic file size
    hundred_percent_string_boost = 100
    smaller_string_boost = 35

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_csv_to_parquet"
        ).getOrCreate()
        self.df = self.spark.read.csv(self.test_csv_path, header=True)
        self.test_workplace_df = generate_ascwds_workplace_file(
            self.TEST_ASCWDS_WORKPLACE_FILE
        )

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
        except OSError as e:
            pass  # Ignore dir does not exist

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

        object_list = utils.get_s3_objects_list(
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

        object_list = utils.get_s3_objects_list(
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

        object_list = utils.get_s3_objects_list(
            "test-bucket",
            "version=1.0.0/import_date=20210101/",
            stubber.get_s3_resource(),
        )

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv"]
        )
        self.assertEqual(len(object_list), 1)

    def test_get_s3_sub_folders_returns_one_common_prefix(self):
        response = {"CommonPrefixes": [{"Prefix": "models/my-model/versions/1.0.0/"}]}

        expected_params = {
            "Bucket": "test-bucket",
            "Prefix": "models/my-model/versions/",
            "Delimiter": "/",
        }

        stubber = StubberClass(StubberType.client)
        stubber.add_response("list_objects_v2", response, expected_params)

        sub_directory_list = utils.get_s3_sub_folders_for_path(
            "s3://test-bucket/models/my-model/versions/", stubber.get_s3_client()
        )
        self.assertEqual(sub_directory_list, ["1.0.0"])

    def test_get_s3_sub_folders_returns_multiple_common_prefix(self):
        response = {
            "CommonPrefixes": [
                {"Prefix": "models/my-model/1.0.0/"},
                {"Prefix": "models/my-model/apples/"},
            ]
        }

        expected_params = {
            "Bucket": "model-bucket",
            "Prefix": "models/my-model/",
            "Delimiter": "/",
        }

        stubber = StubberClass(StubberType.client)
        stubber.add_response("list_objects_v2", response, expected_params)

        sub_directory_list = utils.get_s3_sub_folders_for_path(
            "s3://model-bucket/models/my-model/", stubber.get_s3_client()
        )
        self.assertEqual(sub_directory_list, ["1.0.0", "apples"])

    def test_get_model_name_returns_model_name(self):
        path_to_model = (
            "s3://sfc-bucket/models/care_home_jobs_prediction/1.0.0/subfolder/"
        )
        model_name = utils.get_model_name(path_to_model)
        expected_model_name = "care_home_jobs_prediction"

        self.assertEqual(expected_model_name, model_name)

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

        obj_partial_content = utils.read_partial_csv_content(
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

        obj_partial_content = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", stubber.get_s3_client()
        )

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content,
            "period|establishmentid|tribalid|tribalid_worker|parenti",
        )

    def test_identify_csv_delimiter_can_identify_comma(self):
        sample = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        delimiter = utils.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, ",")

    def test_identify_csv_delimiter_can_identify_pipe(self):
        sample = "period|establishmentid|tribalid|parentid|orgid|nmdsid|wkplacestat|estabcreateddate|logincount_month|"
        delimiter = utils.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, "|")

    def test_generate_s3_main_datasets_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_main_datasets_dir_date_path(
            "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201",
        )

    def test_read_csv(self):
        df = utils.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c", "date_col"])
        self.assertEqual(df.count(), 3)

    def test_read_with_custom_delimiter(self):
        df = utils.read_csv(self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

    def test_write(self):
        df = utils.read_csv(self.test_csv_path)
        utils.write_to_parquet(df, self.tmp_dir)

        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())

    def test_format_date_fields(self):
        self.assertEqual(self.df.select("date_col").first()[0], "28/11/1993")
        formatted_df = utils.format_date_fields(self.df, raw_date_format="dd/MM/yyyy")
        self.assertEqual(str(formatted_df.select("date_col").first()[0]), "1993-11-28")

    def test_is_csv(self):
        csv_name = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv"
        csv_test = utils.is_csv(csv_name)
        self.assertTrue(csv_test)

    def test_is_csv_for_non_csv(self):
        csv_name_without_extention = (
            "Provision - March 2013 - IND - NMDS-SC - ASCWDS format"
        )
        csv_test = utils.is_csv(csv_name_without_extention)
        self.assertFalse(csv_test)

    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = utils.split_s3_uri(s3_uri)

        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(prefix, "domain=ASCWDS/dataset=workplace/")

    def test_construct_s3_uri(self):
        uri = utils.construct_s3_uri(
            "sfc-data-engineering-raw",
            "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv",
        )

        self.assertEqual(
            uri,
            "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv",
        )

    def test_get_file_directory(self):
        path = utils.get_file_directory(
            "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv"
        )

        self.assertEqual(
            path,
            "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331",
        )

    def test_construct_new_destination_path(self):
        destination = "s3://sfc-main-datasets/"
        key = "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/workers.csv"
        destination_path = utils.construct_destination_path(destination, key)

        self.assertEqual(
            destination_path,
            "s3://sfc-main-datasets/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331",
        )

    def test_extract_col_from_schema_returns_2_col_names(self):
        schema = StructType(
            fields=[
                StructField("estid", IntegerType(), True),
                StructField("userid", StringType(), True),
            ]
        )
        column_list = utils.extract_column_from_schema(schema)
        expected_column_list = ["estid", "userid"]

        self.assertEqual(column_list, expected_column_list)

    def test_extract_col_from_schema_returns_no_columns(self):
        schema = StructType(fields=[])
        column_list = utils.extract_column_from_schema(schema)

        self.assertFalse(column_list)

    def test_extract_specific_column_types(self):
        schema = StructType(
            fields=[
                StructField("tr01flag", IntegerType(), True),
                StructField("tr02flag", IntegerType(), True),
                StructField("tr01count", IntegerType(), True),
                StructField("tr01ac", IntegerType(), True),
                StructField("tr03flag", IntegerType(), True),
                StructField("tr01dn", IntegerType(), True),
            ]
        )
        training_types = utils.extract_specific_column_types("^tr[0-9]{2}flag$", schema)
        self.assertEqual(training_types, ["tr01", "tr02", "tr03"])

    def test_extract_col_with_pattern(self):
        schema = StructType(
            fields=[
                StructField("tr01flag", IntegerType(), True),
                StructField("tr01latestdate", IntegerType(), True),
                StructField("tr01count", IntegerType(), True),
                StructField("tr02flag", IntegerType(), True),
                StructField("tr02ac", IntegerType(), True),
                StructField("tr02nac", IntegerType(), True),
                StructField("tr02dn", IntegerType(), True),
                StructField("tr02latestdate", IntegerType(), True),
                StructField("tr02count", IntegerType(), True),
                StructField("training", StringType(), True),
                StructField("tr00034type", IntegerType()),
            ]
        )
        training = utils.extract_col_with_pattern("^tr[0-9]{2}[a-z]+", schema)
        tr = utils.extract_col_with_pattern("^tr\d\d(count|ac|nac|dn)$", schema)
        self.assertEqual(
            training,
            [
                "tr01flag",
                "tr01latestdate",
                "tr01count",
                "tr02flag",
                "tr02ac",
                "tr02nac",
                "tr02dn",
                "tr02latestdate",
                "tr02count",
            ],
        )
        self.assertEqual(tr, ["tr01count", "tr02ac", "tr02nac", "tr02dn", "tr02count"])

    def test_format_import_date_returns_date_format(self):
        df = utils.format_import_date(self.test_workplace_df)

        self.assertEqual(df.schema["import_date"].dataType, DateType())
        self.assertEqual(str(df.select("import_date").first()[0]), "2020-01-01")

    def test_get_max_date_partition_returns_only_partition(self):
        columns = ["id", "snapshot_year", "snapshot_month", "snapshot_day"]
        rows = [(1, "2021", "01", "01")]
        self.spark.createDataFrame(rows, columns).write.mode("overwrite").partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(self.tmp_dir)

        max_snapshot = utils.get_max_snapshot_partitions(self.tmp_dir)

        self.assertEqual(max_snapshot, ("2021", "01", "01"))

    def test_get_max_date_partition_returns_max_partition(self):
        columns = ["id", "snapshot_year", "snapshot_month", "snapshot_day"]
        rows = [
            (1, "2021", "01", "01"),
            (1, "2022", "01", "01"),
            (1, "2022", "02", "01"),
            (1, "2021", "04", "01"),
            (1, "2022", "02", "14"),
            (1, "2022", "01", "22"),
        ]
        self.spark.createDataFrame(rows, columns).write.mode("overwrite").partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(self.tmp_dir)

        max_snapshot = utils.get_max_snapshot_partitions(self.tmp_dir)

        self.assertEqual(max_snapshot, ("2022", "02", "14"))

    def test_get_max_date_partition_if_theres_no_data_returns_none(self):
        max_snapshot = utils.get_max_snapshot_partitions(self.tmp_dir)

        self.assertIsNone(max_snapshot)

    def test_get_max_date_partition_if_theres_no_location_returns_none(self):
        max_snapshot = utils.get_max_snapshot_partitions()

        self.assertIsNone(max_snapshot)

    def test_get_latest_partition_returns_only_partitions(self):
        columns = ["id", "run_year", "run_month", "run_day"]
        rows = [
            (1, "2021", "01", "01"),
            (2, "2021", "01", "01"),
            (3, "2021", "01", "01"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        result_df = utils.get_latest_partition(df)

        self.assertEqual(result_df.count(), 3)

    def test_get_latest_partition_returns_only_latest_partition(self):
        columns = ["id", "run_year", "run_month", "run_day"]
        rows = [
            (1, "2021", "01", "01"),
            (2, "2021", "01", "01"),
            (1, "2020", "01", "01"),
            (2, "2020", "01", "01"),
            (1, "2021", "03", "01"),
            (2, "2021", "03", "01"),
            (1, "2021", "03", "05"),
            (2, "2021", "03", "05"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        result_df = utils.get_latest_partition(df)

        self.assertEqual(result_df.count(), 2)

        for idx, row in enumerate(result_df.collect()):
            with self.subTest("Check row has correct partition", i=idx):
                self.assertEqual(row.run_year, "2021")
                self.assertEqual(row.run_month, "03")
                self.assertEqual(row.run_day, "05")

    def test_get_latest_partition_uses_correct_partition_keys(self):
        columns = ["id", "process_year", "process_month", "process_day"]
        rows = [
            (1, "2021", "01", "01"),
            (2, "2021", "01", "01"),
            (1, "2020", "01", "01"),
            (2, "2020", "01", "01"),
            (1, "2021", "03", "01"),
            (2, "2021", "03", "01"),
            (1, "2021", "03", "05"),
            (2, "2021", "03", "05"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        result_df = utils.get_latest_partition(
            df, partition_keys=("process_year", "process_month", "process_day")
        )

        self.assertEqual(result_df.count(), 2)

        for idx, row in enumerate(result_df.collect()):
            with self.subTest("Check row has correct partition", i=idx):
                self.assertEqual(row.process_year, "2021")
                self.assertEqual(row.process_month, "03")
                self.assertEqual(row.process_day, "05")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
