import unittest
from io import BytesIO

from botocore.response import StreamingBody

import projects._01_ingest.cqc_api.utils.utils as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCLocationsSchema as Schemas,
)
from tests.unit.test_utils import StubberClass, StubberType
from utils import utils
from utils.column_values.categorical_column_values import Specialisms


class TestClassifySpecialisms(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.classify_specialisms_rows, Schemas.classify_specialisms_schema
        )
        self.returned_df = job.classify_specialisms(
            self.test_df,
            Specialisms.dementia,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_classify_specialisms_rows,
            Schemas.expected_classify_specialisms_schema,
        )

    def test_classify_specialisms_returns_expected_value(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_classify_specialisms_adds_expected_columns(
        self,
    ):
        returned_columns = self.returned_df.columns.sort()
        expected_columns = self.expected_df.columns.sort()
        self.assertEqual(returned_columns, expected_columns)


class ReadManualPostcodeCorrectionsCsvTests(unittest.TestCase):
    test_postcode_csv_path = (
        "projects/_01_ingest/unittest_data/test_manual_postcode_corrections.csv"
    )

    # increase length of string to simulate realistic file size
    smaller_string_boost = 35

    def test_read_manual_postcode_corrections_csv_to_dict(self):
        with open(self.test_postcode_csv_path, "rb") as file:
            body = file.read()
        byte_string_length = len(body)

        body = StreamingBody(BytesIO(body), byte_string_length)

        test_response = {
            "Body": body,
            "ContentLength": byte_string_length * self.smaller_string_boost,
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber = StubberClass(StubberType.client)
        stubber.add_response("get_object", test_response, expected_params)

        returned_dict = job.read_manual_postcode_corrections_csv_to_dict(
            "s3://test-bucket/my-test/key/", stubber.get_s3_client()
        )
        expected_dict = Data.expected_postcode_dict
        self.assertEqual(returned_dict, expected_dict)
