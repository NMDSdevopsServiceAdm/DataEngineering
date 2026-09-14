from dataclasses import dataclass
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from utils import file_utils

PATCH_PATH = "utils.file_utils"


class TestSplitS3Uri:
    def test_splits_bucket_and_key_from_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=01_ascwds/dataset=workplace/"

        bucket_name, key_name = file_utils.split_s3_uri(s3_uri)

        assert bucket_name == "sfc-data-engineering-raw"
        assert key_name == "domain=01_ascwds/dataset=workplace/"


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
        key="domain=01_ascwds/dataset=workplace/file.csv",
        expected_uri="s3://sfc-data-engineering-raw/domain=01_ascwds/dataset=workplace/file.csv",
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
        assert file_utils.construct_s3_uri(bucket_name, key) == expected_uri


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
        filepath="domain=01_ascwds/dataset=workplace/version=0.0.1/workers.csv",
        expected_directory="domain=01_ascwds/dataset=workplace/version=0.0.1",
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
        assert file_utils.get_file_directory(filepath) == expected_directory


@dataclass
class ConstructDestinationPathTestCase:
    id: str
    key: str
    expected_path: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.key, self.expected_path, id=self.id)


construct_destination_path_test_cases = [
    ConstructDestinationPathTestCase(
        id="renames_ascwds_raw_domain_to_numbered_datasets_domain",
        key="domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/workers.csv",
        expected_path="s3://sfc-main-datasets/domain=01_ascwds/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331",
    ),
    ConstructDestinationPathTestCase(
        id="renames_cqc_raw_domain_to_numbered_datasets_domain",
        key="domain=CQC/dataset=pir/file.csv",
        expected_path="s3://sfc-main-datasets/domain=01_cqc/dataset=pir",
    ),
    ConstructDestinationPathTestCase(
        id="renames_ons_raw_domain_to_numbered_datasets_domain",
        key="domain=ONS/dataset=postcode_directory/file.csv",
        expected_path="s3://sfc-main-datasets/domain=01_ons/dataset=postcode_directory",
    ),
    ConstructDestinationPathTestCase(
        id="renames_capacity_tracker_raw_domain_to_numbered_datasets_domain",
        key="domain=capacity_tracker/dataset=capacity_tracker_care_home/file.csv",
        expected_path="s3://sfc-main-datasets/domain=01_capacity_tracker/dataset=capacity_tracker_care_home",
    ),
]


class TestConstructDestinationPath:
    @pytest.mark.parametrize(
        "key,expected_path",
        [case.as_pytest_param() for case in construct_destination_path_test_cases],
    )
    def test_combines_destination_bucket_with_renamed_directory_of_key(
        self, key, expected_path
    ):
        destination = "s3://sfc-main-datasets/"

        destination_path = file_utils.construct_destination_path(destination, key)

        assert destination_path == expected_path


@dataclass
class GenerateS3DirTestCase:
    id: str
    version: str | None
    expected_uri: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.version, self.expected_uri, id=self.id)


generate_s3_dir_test_cases = [
    GenerateS3DirTestCase(
        id="uses_version_when_passed",
        version="2.0.0",
        expected_uri="s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=2.0.0/year=2021/month=12/day=01/import_date=20211201/",
    ),
    GenerateS3DirTestCase(
        id="defaults_to_version_one_when_not_passed",
        version=None,
        expected_uri="s3://sfc-main-datasets/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201/",
    ),
]


class TestGenerateS3Dir:
    @pytest.mark.parametrize(
        "version,expected_uri",
        [case.as_pytest_param() for case in generate_s3_dir_test_cases],
    )
    def test_generate_s3_dir(self, version, expected_uri):
        dec_first_21 = datetime(2021, 12, 1)
        args = ("s3://sfc-main-datasets", "test_domain", "test_dateset", dec_first_21)
        if version is not None:
            args += (version,)

        dir_path = file_utils.generate_s3_dir(*args)

        assert dir_path == expected_uri


class TestListS3ParquetImportDates:
    @patch(f"{PATCH_PATH}.boto3.client")
    def test_returns_empty_list_when_no_import_date_folders(
        self, mock_boto_client: Mock
    ):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client.return_value = mock_s3

        result = file_utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        assert result == []

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_sorts_multiple_import_dates_chronologically(self, mock_boto_client: Mock):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2025/month=12/day=01/import_date=20251201/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2023/month=05/day=01/import_date=20230501/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2024/month=01/day=01/import_date=20240101/file.parquet"
                    },
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client.return_value = mock_s3

        result = file_utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        assert result == [20230501, 20240101, 20251201]

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_ignores_keys_without_an_import_date(self, mock_boto_client: Mock):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/other=123/file.parquet"
                    },
                    {
                        "Key": "domain=test_domain/dataset=test_dataset/year=2023/month=05/day=01/import_date=20230501/file.parquet"
                    },
                ]
            }
        ]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client.return_value = mock_s3

        result = file_utils.list_s3_parquet_import_dates(
            "s3://test_bucket/domain=test_domain/dataset=test_dataset/"
        )
        assert result == [20230501]

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_parses_bucket_and_prefix_from_s3_uri(self, mock_boto_client: Mock):
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = mock_paginator
        mock_boto_client.return_value = mock_s3

        result = file_utils.list_s3_parquet_import_dates(
            "s3://test_bucket/path/to/prefix/"
        )
        assert result == []


class TestEmptyS3Folder:
    @patch(f"{PATCH_PATH}.boto3.client")
    def test_skips_deletion_when_no_objects_match_prefix(self, mock_s3_client: Mock):
        paginator = mock_s3_client.return_value.get_paginator.return_value
        paginator.paginate.return_value.search.return_value = [None]

        file_utils.empty_s3_folder("my-bucket", "some/prefix/")

        mock_s3_client.return_value.delete_objects.assert_not_called()

    @patch(f"{PATCH_PATH}.boto3.client")
    def test_deletes_all_matching_objects(self, mock_s3_client: Mock):
        paginator = mock_s3_client.return_value.get_paginator.return_value
        paginator.paginate.return_value.search.side_effect = [
            [
                {"Key": "some/prefix/file1.parquet"},
                {"Key": "some/prefix/file2.parquet"},
                {"Key": "some/prefix/file3.parquet"},
            ]
        ]

        file_utils.empty_s3_folder("my-bucket", "some/prefix/")

        mock_s3_client.return_value.delete_objects.assert_called_once_with(
            Bucket="my-bucket",
            Delete={
                "Objects": [
                    {"Key": "some/prefix/file1.parquet"},
                    {"Key": "some/prefix/file2.parquet"},
                    {"Key": "some/prefix/file3.parquet"},
                ]
            },
        )
