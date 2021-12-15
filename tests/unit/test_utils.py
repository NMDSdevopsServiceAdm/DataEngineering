from datetime import datetime
from utils import utils
import unittest


class UtilsTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_generate_s3_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir_date_path(
            "test_domain", "test_dateset", dec_first_21)
        self.assertEqual(
            dir_path, "s3://sfc-data-engineering/domain=test_domain/dataset=test_dateset/year=2021/month=12/day=01/import_date=20211201")


if __name__ == '__main__':
    unittest.main()
