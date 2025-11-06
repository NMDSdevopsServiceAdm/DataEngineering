import unittest
from io import BytesIO

from botocore.response import StreamingBody

import projects._01_ingest.cqc_api.utils.utils as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from tests.unit.test_utils import StubberClass, StubberType


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
