import unittest

from unittest.mock import Mock, patch

import utils.validation.validation_utils as job

from tests.test_file_data import ValidationUtils as Data
from tests.test_file_schemas import ValidationUtils as Schemas

from utils import utils


class ValidateMergedIndCQCDatasetTests(unittest.TestCase):

    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
