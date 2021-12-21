from datetime import datetime
from pathlib import Path
from utils import utils
import shutil
import unittest


class UtilsTests(unittest.TestCase):

    test_csv_path = 'tests/test_data/example_csv.csv'
    test_csv_custom_delim_path = 'tests/test_data/example_csv_custom_delimiter.csv'
    tmp_dir = "tmp-out"

    def setUp(self):
        pass

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_generate_s3_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir_date_path(
            "test_domain", "test_dateset", dec_first_21)
        self.assertEqual(
            dir_path, "s3://sfc-data-engineering/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201")

    def test_read_csv(self):
        df = utils.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c", "date_col"])
        self.assertEqual(df.count(), 3)

    def test_read_with_custom_delimiter(self):
        df = utils.read_csv(
            self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

    def test_write(self):
        df = utils.read_csv(self.test_csv_path)
        utils.write_to_parquet(df, self.tmp_dir)

        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())


if __name__ == '__main__':
    unittest.main()
