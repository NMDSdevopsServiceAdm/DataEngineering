# tests/base_test.py
import unittest

import pytest


class SparkBaseTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def setup_spark(self, spark):
        """
        Automatically assigns the 'spark' fixture from conftest.py
        to 'self.spark' for every test in the class.
        """
        self.spark = spark
