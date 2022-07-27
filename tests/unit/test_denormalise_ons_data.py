import shutil
import unittest
import warnings

from pyspark.sql import SparkSession

from jobs import denormalise_ons_data


class TestDenormaliseONSDataTests(unittest.TestCase):
    DATA_SOURCE = "tests/test_data/tmp/domain=ONS/dataset=postcode-directory/"
    LOOKUPS_SOURCE = (
        "tests/test_data/tmp/domain=ONS/dataset=postcode-directory-field-lookups/"
    )
    DESTINATION = (
        "tests/test_data/tmp/output/domain=ONS/dataset=post-directory-denormalised/"
    )
    PARTITION_COLS = ["year", "month", "day", "import_date"]

    def setUp(self):
        self.spark = (
            SparkSession.builder.appName("sfc_data_engineering_test_denorm_ons_dataset")
            .config("spark.sql.sources.partitionColumnTypeInference.enabled", False)
            .getOrCreate()
        )
        self.generate_ons_raw_data_file(self.DATA_SOURCE)
        self.generate_lookups()
        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.DESTINATION)
        except OSError:
            pass
        try:
            shutil.rmtree(self.DATA_SOURCE)
        except OSError:
            pass
        try:
            shutil.rmtree(self.LOOKUPS_SOURCE)
        except OSError:
            pass

    def generate_ons_raw_data_file(
        self, output_destination, partitions=("2021", "02", "01")
    ):
        import_date = partitions[0] + partitions[1] + partitions[2]
        # fmt: off
        columns = ["pcd", "rgn", "nhser", "ccg", "ctry", "imd", "lsoa11", "msoa11", "oslaua", "ru11ind"] + self.PARTITION_COLS
        partitions = (*partitions, import_date)
        rows = [
            ("SW9 0LL", "E12000001", "E40000003", "E38000006", "E92000001", "E01021988", "95AA01S1", "E02000001", "E06000001", "B1") + partitions,
        ]
        # fmt: on

        df = self.spark.createDataFrame(rows, columns)
        if output_destination:
            df.coalesce(1).write.mode("overwrite").partitionBy(
                "year", "month", "day", "import_date"
            ).parquet(output_destination)

    def create_lookup_df(self, row, columns, field):
        columns = columns + self.PARTITION_COLS
        rows = [row + ("2021", "02", "01", "20210201")]
        df = self.spark.createDataFrame(rows, columns)

        df.coalesce(1).write.mode("overwrite").partitionBy(
            "year", "month", "day", "import_date"
        ).parquet(f"{self.LOOKUPS_SOURCE}field={field}/")

    def generate_lookups(self):

        # fmt: off
        self.create_lookup_df(("E12000001", "North East"), ["RGN20CD", "RGN20NM"], "rgn")
        self.create_lookup_df(("E40000003", "London"), ["NHSER19CD", "NHSER19NM"], "nhser")
        self.create_lookup_df(("E38000006", "NHS Barnsley CCG"), ["ccg21cd", "ccg21nm"], "ccg")
        self.create_lookup_df(("E92000001", "England"), ["ctry12cd", "ctry12nm"], "ctry")
        self.create_lookup_df(("E01021988", "Tendring 018A"), ["lsoa11cd", "lsoa11nm"], "imd")
        self.create_lookup_df(("95AA01S1", "Aldergrove 1"), ["lsoa11cd", "lsoa11nm"], "lsoa11")
        self.create_lookup_df(("E02000001", "City of London 001"), ["msoa11cd", "msoa11nm"], "msoa11")
        self.create_lookup_df(("E06000001", "Hartlepool"), ["lad21cd", "lad21nm"], "oslaua")
        self.create_lookup_df(("B1", "(England/Wales) Urban minor conurbation"), ["RU11IND", "RU11NM"], "ru11ind")

        # fmt: on

    def test_main_writes_data_to_the_output(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        self.assertEqual(ons_data.count(), 1)

    def test_main_wont_import_data_already_imported(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        self.assertEqual(ons_data.count(), 1)

    def test_replaces_rgn_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.rgn, "North East")

    def test_replaces_nhser_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.nhser, "London")

    def test_replaces_ccg_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.ccg, "NHS Barnsley CCG")

    def test_replaces_ctry_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.ctry, "England")

    def test_replaces_imd_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.imd, "Tendring 018A")

    def test_replaces_lsoa_fields_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.lsoa["year_2011"], "Aldergrove 1")

    def test_replaces_msoa_fields_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.msoa["year_2011"], "City of London 001")

    def test_replaces_oslaua_field_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(ons_data_row.oslaua, "Hartlepool")

    def test_replaces_ru_ind_fields_with_lookup_values(self):
        denormalise_ons_data.main(
            self.DATA_SOURCE, self.LOOKUPS_SOURCE, self.DESTINATION
        )

        ons_data = self.spark.read.parquet(self.DESTINATION)
        ons_data_row = ons_data.collect()[0]
        self.assertEqual(
            ons_data_row.ru_ind["year_2011"],
            "(England/Wales) Urban minor conurbation",
        )
