# tests/base_test.py
import unittest

import pytest


class SparkBaseTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def setup_spark(self, spark, tmp_path):
        """
        Automatically assigns the 'spark' fixture from conftest.py
        to 'self.spark' for every test in the class.
        """
        self.spark = spark
        self.tmp_path = tmp_path

    def get_temp_path(self, filename: str) -> str:
        """Helper to return a string path for Spark's .write or .read"""
        return str(self.tmp_path / filename)
