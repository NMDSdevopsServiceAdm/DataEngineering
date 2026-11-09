from unittest.mock import MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.workbook_helpers as job


class TestSetColumnWidths:
    def test_function_sets_each_leading_column_to_its_width(self):
        worksheet = MagicMock()

        job.set_column_widths(worksheet, [10, 20, 30])

        assert worksheet.set_column.call_args_list == [
            ((0, 0, 10),),
            ((1, 1, 20),),
            ((2, 2, 30),),
        ]


def _create_bucket():
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.create_bucket(
        Bucket="bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3


class TestUploadToS3:
    @mock_aws
    @pytest.mark.parametrize(
        "directory", ["s3://bucket/some/prefix/", "s3://bucket/some/prefix"]
    )
    def test_uploads_file_into_directory_with_its_name(self, tmp_path, directory):
        s3 = _create_bucket()
        local_file = tmp_path / "nested" / "summary.xlsx"
        local_file.parent.mkdir()
        local_file.write_bytes(b"workbook bytes")

        job.upload_to_s3(local_file, directory)

        body = s3.get_object(Bucket="bucket", Key="some/prefix/summary.xlsx")["Body"]
        assert body.read() == b"workbook bytes"


class TestDownloadFromS3:
    @mock_aws
    @pytest.mark.parametrize(
        "directory", ["s3://bucket/some/prefix/", "s3://bucket/some/prefix"]
    )
    def test_downloads_the_named_file_from_the_directory(self, tmp_path, directory):
        _create_bucket().put_object(
            Bucket="bucket", Key="some/prefix/summary.xlsx", Body=b"workbook bytes"
        )
        local_file = tmp_path / "downloaded.xlsx"

        job.download_from_s3(directory, "summary.xlsx", local_file)

        assert local_file.read_bytes() == b"workbook bytes"

    @mock_aws
    def test_raises_when_the_file_is_missing(self, tmp_path):
        _create_bucket()

        with pytest.raises(ClientError):
            job.download_from_s3(
                "s3://bucket/some/prefix/", "summary.xlsx", tmp_path / "local.xlsx"
            )


class TestGenerateWorkbookName:
    def test_names_the_workbook_by_stem_archive_date_and_run_number(self):
        run_source = "some/dir/archive_date=2026-10-01/run_number=2/"

        assert (
            job.generate_workbook_name("some_stem", run_source)
            == "some_stem_2026-10-01_2.xlsx"
        )


class TestGetLogoBytes:
    LOGO_URI = "s3://bucket/publication/logo/logo.png"

    @mock_aws
    def test_returns_the_object_bytes_for_the_logo_uri(self):
        _create_bucket().put_object(
            Bucket="bucket", Key="publication/logo/logo.png", Body=b"png bytes"
        )

        assert job.get_logo_bytes(self.LOGO_URI) == b"png bytes"

    @mock_aws
    def test_raises_when_the_logo_object_is_missing(self):
        _create_bucket()

        with pytest.raises(job.LogoUnavailableError, match=self.LOGO_URI):
            job.get_logo_bytes(self.LOGO_URI)

    @mock_aws
    def test_raises_when_the_logo_object_is_empty(self):
        _create_bucket().put_object(
            Bucket="bucket", Key="publication/logo/logo.png", Body=b""
        )

        with pytest.raises(job.LogoUnavailableError, match=self.LOGO_URI):
            job.get_logo_bytes(self.LOGO_URI)
