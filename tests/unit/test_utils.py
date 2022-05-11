from datetime import datetime
from pathlib import Path
from utils import utils
import shutil
import unittest
from pyspark.sql import SparkSession
import botocore.session
from botocore.stub import Stubber
from botocore.response import StreamingBody
from io import BytesIO
import boto3
import json

class UtilsTests(unittest.TestCase):

    test_csv_path = "tests/test_data/example_csv.csv"
    test_csv_custom_delim_path = "tests/test_data/example_csv_custom_delimiter.csv"
    tmp_dir = "tmp-out"

    def setUp(self):
        spark = SparkSession.builder.appName(
            "sfc_data_engineering_csv_to_parquet"
        ).getOrCreate()
        self.df = spark.read.csv(self.test_csv_path, header=True)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_get_s3_objects_list_returns_all_objects(self):
        # TODO DO THIS TEST
        pass

    def test_get_s3_objects_doesnt_return_directories(self):
        # TODO DO THIS TEST
        pass

    def test_get_s3_objects_list_returns_filtered_objects(self):
        s3 = boto3.resource("s3")
        stubber = Stubber(s3.meta.client)

        partial_response = {
            "Contents": [{
                "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                'Size': 123
            }]
        }

        expected_params = {"Bucket": "test-bucket",
                           "Prefix": "version=1.0.0/import_date=20210101/"}

        stubber.add_response("list_objects", partial_response, expected_params)
        stubber.activate()

        object_list = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/import_date=20210101/", s3)

        print(f"Object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv"])
        self.assertEqual(len(object_list), 1)


    def test_read_partial_csv_content(self):
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        body_data = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"

        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(
            BytesIO(body_encoded),
            byte_string_length
        )

        partial_response = {
            'Body': body,
            'ContentLength': byte_string_length * 100
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber.add_response("get_object", partial_response, expected_params)
        stubber.activate()

        obj_partial_content = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", s3)

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content, "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species")

    def test_identify_csv_delimiter_can_identify_comma(self):
        sample = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        delimiter = utils.identify_csv_delimiter(sample)
        self.assertEqual(delimiter, ",")

    def test_identify_csv_delimiter_can_identify_pipe(self):
        sample = "Id|SepalLengthCm|SepalWidthCm|PetalLengthCm|PetalWidthCm|Species"
        delimiter = utils.identify_csv_delimiter(sample)
        self.assertEqual(delimiter, "|")


    def test_generate_s3_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir_date_path(
            "test_domain", "test_dateset", dec_first_21
        )
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
        self.assertEqual(
            str(formatted_df.select("date_col").first()[0]), "1993-11-28 00:00:00"
        )

    def test_is_csv(self):
        self.assertTrue("s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = utils.split_s3_uri(s3_uri)
        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(prefix, "domain=ASCWDS/dataset=workplace/")

    def test_construct_s3_uri(self):
        uri = utils.construct_s3_uri("sfc-data-engineering-raw", "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")
        self.assertEqual(uri, "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

    def test_get_file_directory(self):
        path = utils.get_file_directory("domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")
        self.assertEqual(path, "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331")

if __name__ == "__main__":
    unittest.main(warnings="ignore")
