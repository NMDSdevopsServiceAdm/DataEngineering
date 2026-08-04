from utils import s3_file_utils


class TestSplitS3Uri:
    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = s3_file_utils.split_s3_uri(s3_uri)

        assert bucket_name == "sfc-data-engineering-raw"
        assert prefix == "domain=ASCWDS/dataset=workplace/"
