import shutil
import unittest
from pathlib import Path
import warnings

from pyspark.sql import SparkSession
from unittest.mock import patch, Mock

import jobs.ingest_ons_data as job
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ColNames,
)
from utils import utils
from tests.unit.test_utils import StubberClass, StubberType
from tests.test_file_data import IngestONSData as Data
from tests.test_file_schemas import IngestONSData as Schemas

class IngestONSDataTests(unittest.TestCase):
    TEST_CSV_SOURCE = "some/directory/path/file.csv"
    TEST_DIRECTORY_SOURCE = "some/directory/path"
    TEST_DESTINATION = "s3://some/"
    TEST_NEW_DESTINATION = "s3://some/directory/path" #Work out why this is the new destination
    
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_ons_df = self.spark.createDataFrame(
            Data.sample_rows, Schemas.sample_schema
        )
        self.object_list = [
            "version=1.0.0/import_date=20210101/some-data-file.csv",
            "version=1.0.0/import_date=20210101/some-other-other-data-file.csv",
        ]
        self.partial_csv_content = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        
    @patch("utils.utils.read_partial_csv_content")
    @patch("utils.utils.get_s3_objects_list")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_csv")
    def test_main_runs_when_source_is_csv(
        self, read_csv_patch: Mock, write_to_parquet_patch: Mock, get_s3_objects_list_patch: Mock, read_partial_csv_content_patch: Mock
    ):  
        

        get_s3_objects_list_patch.return_value = self.object_list
        read_csv_patch.return_value = self.test_ons_df
        read_partial_csv_content_patch.return_value = self.partial_csv_content

        job.main(self.TEST_CSV_SOURCE, self.TEST_DESTINATION)

        self.assertEqual(get_s3_objects_list_patch.call_count, 0)
        self.assertEqual(read_csv_patch.call_count, 1)
        self.assertEqual(read_partial_csv_content_patch.call_count, 1)
        write_to_parquet_patch.assert_called_once_with(
            self.test_ons_df,
            self.TEST_NEW_DESTINATION,
        )




    