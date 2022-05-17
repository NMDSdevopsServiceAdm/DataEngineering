from datetime import datetime
from pathlib import Path
from utils import utils
import shutil
import unittest
from pyspark.sql import SparkSession
from botocore.stub import Stubber
from botocore.response import StreamingBody
from io import BytesIO
import boto3


class StubberClass():
    __s3_client = None
    __s3_resource = None
    __stubber = None
    __type = ""

    def __init__(self, type):
        self.__type = type
        self.decide_type()

    def decide_type(self):
        if self.__type == "client":
            self.build_client()
            self.build_stubber_client()
        else:
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

    def setUp(self):
        spark = SparkSession.builder.appName("sfc_data_engineering_csv_to_parquet").getOrCreate()
        self.df = spark.read.csv(self.test_csv_path, header=True)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_get_s3_objects_list_returns_all_objects(self):
        partial_response = {
            "Contents": [{
                "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                'Size': 123
            }, {
                "Key": "version=1.0.0/import_date=20210101/some-other-data-file.csv",
                'Size': 100
            }, {
                "Key": "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
                'Size': 150
            }]
        }

        expected_params = {"Bucket": "test-bucket",
                           "Prefix": "version=1.0.0/import_date=20210101/"}

        stubber = StubberClass("resource")
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/import_date=20210101/", stubber.get_s3_resource())

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv",
                          "version=1.0.0/import_date=20210101/some-other-data-file.csv",
                          "version=1.0.0/import_date=20210101/some-other-other-data-file.csv"])
        self.assertEqual(len(object_list), 3)

    def test_get_s3_objects_doesnt_return_directories(self):
        partial_response = {
            "Contents": [{
                "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                'Size': 123
            }, {
                "Key": "version=1.0.0/import_date=20210101/",
                'Size': 0
            }, {
                "Key": "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
                'Size': 100
            }]
        }

        expected_params = {"Bucket": "test-bucket",
                           "Prefix": "version=1.0.0/import_date=20210101/"}

        stubber = StubberClass("resource")
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/import_date=20210101/", stubber.get_s3_resource())

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv",
                          "version=1.0.0/import_date=20210101/some-other-other-data-file.csv"])
        self.assertEqual(len(object_list), 2)

    def test_get_s3_objects_list_returns_filtered_objects(self):
        partial_response = {
            "Contents": [{
                "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                'Size': 123
            }]
        }

        expected_params = {"Bucket": "test-bucket",
                           "Prefix": "version=1.0.0/import_date=20210101/"}

        stubber = StubberClass("resource")
        stubber.add_response("list_objects", partial_response, expected_params)

        object_list = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/import_date=20210101/", stubber.get_s3_resource())

        print(f"S3 object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv"])
        self.assertEqual(len(object_list), 1)

    def test_read_partial_csv_content(self):
        body_data = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"

        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(
            BytesIO(body_encoded),
            byte_string_length
        )

        partial_response = {
            'Body': body,
            'ContentLength': byte_string_length * 1000
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber = StubberClass("client")
        stubber.add_response("get_object", partial_response, expected_params)

        obj_partial_content = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", stubber.get_s3_client())

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content, "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species")

    def test_read_partial_csv_less_content(self):
        body_data = """period|establishmentid|tribalid|tribalid_worker|parentid|orgid|nmdsid|workerid|wkplacestat|createddate
        |updateddate|savedate|cqcpermission|lapermission|regtype|providerid|locationid|esttype|regionid|
        cssr|lauthid|parliamentaryconstituency|mainstid|emplstat|emplstat_changedate|emplstat_savedate|mainjrid|
        mainjrid_changedate|mainjrid_savedate|strtdate|strtdate_changedate|strtdate_savedate|age|age_changedate|
        age_savedate|gender|gender_changedate|gender_savedate|disabled|disabled_changedate|disabled_savedate|
        ethnicity|ethnicity_changedate|ethnicity_savedate|isbritish|nationality|isbritish_changedate|
        isbritish_savedate|britishcitizen|britishcitizen_changedate|britishcitizen_savedate|borninuk|countryofbirth|
        """

        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(
            BytesIO(body_encoded),
            byte_string_length
        )

        partial_response = {
            'Body': body,
            'ContentLength': byte_string_length * 12
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber = StubberClass("client")
        stubber.add_response("get_object", partial_response, expected_params)

        obj_partial_content = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", stubber.get_s3_client())

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content, "period|establishmentid|tribalid|tribalid_worker|parentid|orgid|nmdsid|workerid|wkplacestat|c")

    def test_identify_csv_delimiter_can_identify_comma(self):
        sample = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        delimiter = utils.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, ",")

    def test_identify_csv_delimiter_can_identify_pipe(self):
        sample = "period|establishmentid|tribalid|parentid|orgid|nmdsid|wkplacestat|estabcreateddate|logincount_month|"
        delimiter = utils.identify_csv_delimiter(sample)

        self.assertEqual(delimiter, "|")

    def test_generate_s3_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir_date_path("test_domain", "test_dateset", dec_first_21)
        self.assertEqual(
            dir_path,
            "s3://sfc-data-engineering/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201",
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
        formatted_df = utils.format_date_fields(self.df)
        self.assertEqual(str(formatted_df.select("date_col").first()[0]), "1993-11-28 00:00:00")

    def test_is_csv(self):
        csv_name = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv"
        csv_test = utils.is_csv(csv_name)
        self.assertTrue(csv_test)

    def test_is_csv_for_non_csv(self):
        csv_name_without_extention = "Provision - March 2013 - IND - NMDS-SC - ASCWDS format"
        csv_test = utils.is_csv(csv_name_without_extention)
        self.assertFalse(csv_test)

    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = utils.split_s3_uri(s3_uri)

        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(prefix, "domain=ASCWDS/dataset=workplace/")

    def test_construct_s3_uri(self):
        uri = utils.construct_s3_uri(
            "sfc-data-engineering-raw", "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

        self.assertEqual(uri, "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

    def test_get_file_directory(self):
        path = utils.get_file_directory(
            "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")
        
        self.assertEqual(
            path, "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331")

    def test_construct_new_destination_path(self):
        destination = "s3://sfc-data-engineering/"
        key = "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/workers.csv"
        destination_path = utils.construct_destination_path(destination, key)

        self.assertEqual(destination_path, "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
