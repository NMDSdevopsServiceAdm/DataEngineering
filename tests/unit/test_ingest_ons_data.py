import shutil
import unittest
from pathlib import Path
import warnings

from pyspark.sql import SparkSession
from unittest.mock import patch

from jobs import ingest_ons_data


class IngestIngestONSDataTests(unittest.TestCase):
    DATA_SOURCE = "tests/test_data/tmp/domain=ONS/"
    LOOKUPS_SOURCE = (
        "tests/test_data/tmp/domain=ONS/dataset=postcode-directory-field-lookups/"
    )
    DESTINATION = "tests/test_data/tmp/output/domain=ONS/"

    def setUp(self):
        self.spark = (
            SparkSession.builder.appName("sfc_data_engineering_test_ingest_ons_dataset")
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", False)
            .getOrCreate()
        )
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.DESTINATION)
            shutil.rmtree(self.DATA_SOURCE)
            shutil.rmtree(self.LOOKUPS_SOURCE)
        except OSError:
            pass

    def generate_ons_test_csv_file(
        self, output_destination, partitions=("2021", "02", "01")
    ):
        columns = ["pcd", "nhser", "year", "month", "day", "import_date"]
        # fmt: off
        rows = [
            ("SW9 0LL", "E40000003", partitions[0], partitions[1], partitions[2], partitions[0] + partitions[1] + partitions[2]),
            ("BH1 1QZ", "E40000006", partitions[0], partitions[1], partitions[2], partitions[0] + partitions[1] + partitions[2]),
            ("PO6 2EQ", "E40000005", partitions[0], partitions[1], partitions[2], partitions[0] + partitions[1] + partitions[2]),
        ]
        # fmt: on

        df = self.spark.createDataFrame(rows, columns)
        if output_destination:
            df.coalesce(1).write.option("header", True).mode("overwrite").partitionBy(
                "year", "month", "day", "import_date"
            ).csv(output_destination)

    def write_csv(self, path, lines):
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        f = open(
            f"{path}data.csv",
            "w",
        )
        for line in lines:
            f.write(line + "\n")
        f.close()

    def generate_region_lookup_csv(self, output_destination):
        lines = [
            "RGN20CD,RGN20CDO,RGN20NM,RGN20NMW",
            "E12000001,A,North East,Gogledd Ddwyrain",
            "E12000002,B,North West,Gogledd Orllewin",
            "E12000003,D,Yorkshire and The Humber,Swydd Efrog a Humber",
            "E12000004,E,East Midlands,Dwyrain y Canolbarth",
            "E12000005,F,West Midlands,Gorllewin y Canolbarth",
        ]
        region_file_path = f"{output_destination}field=rgn/year=2021/month=01/day=01/import_date=20210101/"
        self.write_csv(region_file_path, lines)

    def generate_nhs_region_lookup_csv(self, output_destination):
        lines = [
            "NHSER19CD,NHSER19CDH,NHSER19NM",
            "E40000003,Y56,London",
            "E40000005,Y59,South East",
            "E40000006,Y58,South West",
            "E40000007,Y61,East of England",
            "E40000008,Y60,Midlands",
            "E40000009,Y63,North East and Yorkshire",
            "E40000010,Y62,North West",
        ]
        nhs_region_file_path = f"{output_destination}field=nhser/year=2021/month=01/day=01/import_date=20210101/"
        self.write_csv(nhs_region_file_path, lines)

    @patch("utils.utils.get_s3_sub_folders_for_path")
    def test_main_imports_one_csv_file(self, get_subfolder_mock):
        get_subfolder_mock.return_value = []
        self.generate_ons_test_csv_file(
            f"{self.DATA_SOURCE}/dataset=postcode-directory/"
        )

        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)

        ons_data = self.spark.read.parquet(
            f"{self.DESTINATION}/dataset=postcode-directory/"
        )
        self.assertEqual(ons_data.count(), 3)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.import_date, "20210201")

    @patch("utils.utils.get_s3_sub_folders_for_path")
    def test_main_wont_import_data_already_imported(self, get_subfolder_mock):
        get_subfolder_mock.return_value = []
        self.generate_ons_test_csv_file(
            f"{self.DATA_SOURCE}/dataset=postcode-directory/"
        )

        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)
        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)

        ons_data = self.spark.read.parquet(
            f"{self.DESTINATION}/dataset=postcode-directory/"
        )
        self.assertEqual(ons_data.count(), 3)

    @patch("utils.utils.get_s3_sub_folders_for_path")
    def test_main_will_import_new_data(self, get_subfolder_mock):
        get_subfolder_mock.return_value = []
        self.generate_ons_test_csv_file(
            f"{self.DATA_SOURCE}/dataset=postcode-directory/"
        )

        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)
        self.generate_ons_test_csv_file(
            f"{self.DATA_SOURCE}/dataset=postcode-directory/", ("2022", "01", "02")
        )
        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)

        ons_data = self.spark.read.parquet(
            f"{self.DESTINATION}/dataset=postcode-directory/"
        )
        self.assertEqual(ons_data.count(), 6)
        ons_data_partitions = ons_data.select("import_date").distinct().collect()
        self.assertEqual(len(ons_data_partitions), 2)

    @patch("utils.utils.get_s3_sub_folders_for_path")
    def test_imports_supporting_lookup_files_for_import_date(self, get_subfolder_mock):
        get_subfolder_mock.return_value = ["field=rgn", "field=nhser"]
        self.generate_ons_test_csv_file(
            f"{self.DATA_SOURCE}/dataset=postcode-directory/"
        )
        self.generate_nhs_region_lookup_csv(self.LOOKUPS_SOURCE)
        self.generate_region_lookup_csv(self.LOOKUPS_SOURCE)

        ingest_ons_data.main(self.DATA_SOURCE, self.DESTINATION)
        rgn_lookup = self.spark.read.parquet(
            f"{self.DESTINATION}/dataset=postcode-directory-field-lookups/field=rgn/"
        )
        nhser_lookup = self.spark.read.parquet(
            f"{self.DESTINATION}/dataset=postcode-directory-field-lookups/field=nhser/"
        )

        self.assertEqual(rgn_lookup.count(), 5)
        self.assertEqual(nhser_lookup.count(), 7)

        rgn_lookup_row = rgn_lookup.collect()[0]
        nhser_lookup_row = nhser_lookup.collect()[0]

        self.assertEqual(rgn_lookup_row.import_date, "20210101")
        self.assertEqual(nhser_lookup_row.import_date, "20210101")
