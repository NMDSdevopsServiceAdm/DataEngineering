import unittest
from unittest.mock import patch, ANY, Mock, MagicMock
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import json
import base64

import utils.aws_secrets_manager_utilities as ars


class AWSSecretsManagerUtilitiesTests(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_client = MagicMock()


class GetSecretTests(AWSSecretsManagerUtilitiesTests):
    def setUp(self) -> None:
        super().setUp()
        self.error_common_response_metadata = {
            "RequestId": "1234567890ABCDEF",
            "HostId": "host ID data will appear here as a hash",
            "HTTPStatusCode": 400,
            "HTTPHeaders": {"header metadata key/values will appear here"},
            "RetryAttempts": 0,
        }
        self.error_common_message = "Whatever AWS sends"

    @patch("boto3.session.Session")
    def test_get_secret_returns_username_and_password_from_secret_string(
        self, mock_session
    ):
        mock_session.return_value.client.return_value = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "SecretString": '{"username": "test_user", "password": "test_password"}'
        }

        secret = json.loads(ars.get_secret("eu-west-2"))

        self.assertEqual(secret["username"], "test_user")
        self.assertEqual(secret["password"], "test_password")

    @patch("boto3.session.Session")
    def test_get_secret_returns_existing_code_as_string(self, mock_session):
        mock_session.return_value.client.return_value = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "SecretString": '{"cqc_api_primary_key": "SAMPLECODE"}'
        }

        secret_value = json.loads(
            ars.get_secret(secret_name="cqc_api_primary_key", region_name="eu-west-2")
        )["cqc_api_primary_key"]

        self.assertEqual(secret_value, "SAMPLECODE")
        self.assertNotEqual(type(secret_value), type(dict()))
        self.assertEqual(type(secret_value), type("SAMPLECODE"))

    @patch("boto3.session.Session")
    def test_get_secret_handles_full_response_syntax_to_return_secret_string_correctly(
        self, mock_session
    ):
        mock_session.return_value.client.return_value = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:eu-west-2:123456789012:secret:MyTestDatabaseSecret-a1b2c3",
            "CreatedDate": 1523477145.713,
            "Name": "MyTestDatabaseSecret",
            "SecretString": '{\n  "username":"david",\n  "password":"EXAMPLE-PASSWORD"\n}\n',
            "VersionId": "EXAMPLE1-90ab-cdef-fedc-ba987SECRET1",
            "VersionStages": [
                "AWSPREVIOUS",
            ],
            "ResponseMetadata": {
                "...": "...",
            },
        }

        secret_raw = ars.get_secret(
            secret_name="MyTestDatabaseSecret", region_name="eu-west-2"
        )

        self.assertEqual(
            secret_raw, '{\n  "username":"david",\n  "password":"EXAMPLE-PASSWORD"\n}\n'
        )

    @patch("boto3.session.Session")
    def test_get_secret_correctly_returns_binary_secret_data(self, mock_session):
        mock_session.return_value.client.return_value = self.mock_client
        PHRASE_TO_TEST = "super_secret_passphrase"

        b = base64.b64encode(bytes(f"{PHRASE_TO_TEST}", "utf-8"))  # bytes

        self.mock_client.get_secret_value.return_value = {"SecretBinary": b}

        secret_binary = ars.get_secret(
            secret_name="Some_Super_Secret_Password_Id", region_name="eu-west-2"
        )

        secret_string = base64.b64decode(secret_binary).decode("utf-8")

        self.assertEqual(secret_string, PHRASE_TO_TEST)

    @patch("boto3.session.Session")
    def test_get_secret_correctly_handles_client_error(self, mock_session):
        mock_session.return_value.client.return_value = self.mock_client

        self.mock_client.get_secret_value.side_effect = [
            ClientError(
                {
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": self.error_common_message,
                    },
                    "ResponseMetadata": self.error_common_response_metadata,
                },
                "get_secret_value",
            ),
            ClientError(
                {
                    "Error": {
                        "Code": "InvalidRequestException",
                        "Message": self.error_common_message,
                    },
                    "ResponseMetadata": self.error_common_response_metadata,
                },
                "get_secret_value",
            ),
            ClientError(
                {
                    "Error": {
                        "Code": "InvalidParameterException",
                        "Message": self.error_common_message,
                    },
                    "ResponseMetadata": self.error_common_response_metadata,
                },
                "get_secret_value",
            ),
            ClientError(
                {
                    "Error": {
                        "Code": "DecryptionFailure",
                        "Message": self.error_common_message,
                    },
                    "ResponseMetadata": self.error_common_response_metadata,
                },
                "get_secret_value",
            ),
            ClientError(
                {
                    "Error": {
                        "Code": "InternalServiceError",
                        "Message": self.error_common_message,
                    },
                    "ResponseMetadata": self.error_common_response_metadata,
                },
                "get_secret_value",
            ),
        ]

        with self.assertRaises(Exception) as context:
            ars.get_secret(secret_name="Hello World", region_name="eu-west-2")
        self.assertTrue(
            "The requested secret Hello World was not found" in str(context.exception)
        )

        with self.assertRaises(Exception) as context:
            ars.get_secret(secret_name="Hello World", region_name="eu-west-2")

        self.assertTrue("The request was invalid due to:" in str(context.exception))

        with self.assertRaises(Exception) as context:
            ars.get_secret(secret_name="Hello World", region_name="eu-west-2")

        self.assertTrue("The request had invalid params:" in str(context.exception))

        with self.assertRaises(Exception) as context:
            ars.get_secret(secret_name="Hello World", region_name="eu-west-2")

        self.assertTrue(
            "The requested secret can't be decrypted using the provided KMS key:"
            in str(context.exception)
        )

        with self.assertRaises(Exception) as context:
            ars.get_secret(secret_name="Hello World", region_name="eu-west-2")

        self.assertTrue("An error occurred on service side:" in str(context.exception))


if __name__ == "__main__":
    unittest.main()
