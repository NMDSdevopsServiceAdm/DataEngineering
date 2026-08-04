from dataclasses import dataclass
from unittest.mock import MagicMock, Mock, patch

import pytest

from projects._01_ingest.utils import utils

PATCH_PATH = "projects._01_ingest.utils.utils"


@dataclass
class IsCsvTestCase:
    id: str
    filename: str
    expected: bool

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.filename, self.expected, id=self.id)


is_csv_test_cases = [
    IsCsvTestCase(id="csv_extension_returns_true", filename="data.csv", expected=True),
    IsCsvTestCase(id="no_extension_returns_false", filename="data", expected=False),
    IsCsvTestCase(
        id="uppercase_extension_returns_false_as_check_is_case_sensitive",
        filename="data.CSV",
        expected=False,
    ),
]


class TestIsCsv:
    @pytest.mark.parametrize(
        "filename,expected",
        [case.as_pytest_param() for case in is_csv_test_cases],
    )
    def test_is_csv(self, filename, expected):
        assert utils.is_csv(filename) == expected


@dataclass
class IdentifyCsvDelimiterTestCase:
    id: str
    sample_csv: str
    expected_delimiter: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.sample_csv, self.expected_delimiter, id=self.id)


identify_csv_delimiter_test_cases = [
    IdentifyCsvDelimiterTestCase(
        id="identifies_comma_delimiter",
        sample_csv="Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species",
        expected_delimiter=",",
    ),
    IdentifyCsvDelimiterTestCase(
        id="identifies_pipe_delimiter",
        sample_csv="period|establishmentid|tribalid|parentid|orgid|nmdsid|wkplacestat|estabcreateddate|logincount_month|",
        expected_delimiter="|",
    ),
]


class TestIdentifyCsvDelimiter:
    @pytest.mark.parametrize(
        "sample_csv,expected_delimiter",
        [case.as_pytest_param() for case in identify_csv_delimiter_test_cases],
    )
    def test_identify_csv_delimiter(self, sample_csv, expected_delimiter):
        assert utils.identify_csv_delimiter(sample_csv) == expected_delimiter


class TestGetS3ObjectsList:
    def test_returns_keys_and_filters_out_directories(self):
        mock_object_with_content = MagicMock(key="version=1.0.0/file.csv", size=123)
        mock_directory_object = MagicMock(key="version=1.0.0/", size=0)
        mock_s3_resource = MagicMock()
        mock_s3_resource.Bucket.return_value.objects.filter.return_value = [
            mock_object_with_content,
            mock_directory_object,
        ]

        object_keys = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/", mock_s3_resource
        )

        assert object_keys == ["version=1.0.0/file.csv"]
        mock_s3_resource.Bucket.assert_called_once_with("test-bucket")
        mock_s3_resource.Bucket.return_value.objects.filter.assert_called_once_with(
            Prefix="version=1.0.0/"
        )

    @patch(f"{PATCH_PATH}.boto3.resource")
    def test_creates_own_s3_resource_when_none_provided(self, mock_boto_resource: Mock):
        mock_boto_resource.return_value.Bucket.return_value.objects.filter.return_value = (
            []
        )

        object_keys = utils.get_s3_objects_list("test-bucket", "some/prefix/")

        assert object_keys == []
        mock_boto_resource.assert_called_once_with("s3")


class TestReadPartialCsvContent:
    def test_reads_one_percent_of_content_when_under_the_cap(self):
        content = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        content_length = len(content.encode("utf-8")) * 100
        mock_body = Mock()
        mock_body.read.return_value = content.encode("utf-8")
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            "Body": mock_body,
            "ContentLength": content_length,
        }

        result = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", mock_s3_client
        )

        mock_body.read.assert_called_once_with(int(content_length * 0.01))
        assert result == content

    def test_caps_read_at_two_mb_for_very_large_files(self):
        mock_body = Mock()
        mock_body.read.return_value = b"some content"
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            "Body": mock_body,
            "ContentLength": utils.TWO_MB * 1000,
        }

        utils.read_partial_csv_content("test-bucket", "my-test/key/", mock_s3_client)

        mock_body.read.assert_called_once_with(utils.TWO_MB)


@dataclass
class ConstructS3UriTestCase:
    id: str
    bucket_name: str
    key: str
    expected_uri: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.bucket_name, self.key, self.expected_uri, id=self.id)


construct_s3_uri_test_cases = [
    ConstructS3UriTestCase(
        id="constructs_uri_from_bucket_and_key",
        bucket_name="sfc-data-engineering-raw",
        key="domain=ASCWDS/dataset=workplace/file.csv",
        expected_uri="s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/file.csv",
    ),
    ConstructS3UriTestCase(
        id="strips_whitespace_from_bucket_name",
        bucket_name="  sfc-data-engineering-raw  ",
        key="file.csv",
        expected_uri="s3://sfc-data-engineering-raw/file.csv",
    ),
]


class TestConstructS3Uri:
    @pytest.mark.parametrize(
        "bucket_name,key,expected_uri",
        [case.as_pytest_param() for case in construct_s3_uri_test_cases],
    )
    def test_construct_s3_uri(self, bucket_name, key, expected_uri):
        assert utils.construct_s3_uri(bucket_name, key) == expected_uri


@dataclass
class GetFileDirectoryTestCase:
    id: str
    filepath: str
    expected_directory: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.filepath, self.expected_directory, id=self.id)


get_file_directory_test_cases = [
    GetFileDirectoryTestCase(
        id="returns_directory_for_nested_path",
        filepath="domain=ASCWDS/dataset=workplace/version=0.0.1/workers.csv",
        expected_directory="domain=ASCWDS/dataset=workplace/version=0.0.1",
    ),
    GetFileDirectoryTestCase(
        id="returns_empty_string_for_bare_filename_with_no_slash",
        filepath="workers.csv",
        expected_directory="",
    ),
]


class TestGetFileDirectory:
    @pytest.mark.parametrize(
        "filepath,expected_directory",
        [case.as_pytest_param() for case in get_file_directory_test_cases],
    )
    def test_get_file_directory(self, filepath, expected_directory):
        assert utils.get_file_directory(filepath) == expected_directory


class TestConstructDestinationPath:
    def test_combines_destination_bucket_with_directory_of_key(self):
        destination = "s3://sfc-main-datasets/"
        key = "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/workers.csv"

        destination_path = utils.construct_destination_path(destination, key)

        assert destination_path == (
            "s3://sfc-main-datasets/domain=ASCWDS/dataset=workplace/version=0.0.1/"
            "year=2013/month=03/day=31/import_date=20130331"
        )
