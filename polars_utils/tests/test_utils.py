import argparse
import sys
import unittest
from unittest.mock import patch

from polars_utils import utils


class TestUtilsGetArgs(unittest.TestCase):
    def test_get_args_has_all(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", False),
            ("--arg3", "help", False, "default"),
        )
        with patch.object(
            sys, "argv", ["prog", "--arg1", "value1", "--arg2", "value2"]
        ):
            # When
            parsed = utils.get_args(*args)
            # Then
            self.assertEqual(parsed.arg1, "value1")
            self.assertEqual(parsed.arg2, "value2")
            self.assertEqual(parsed.arg3, "default")

    def test_get_args_missing_required(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", True),
        )
        with patch.object(sys, "argv", ["prog", "--arg1", "value1"]):
            # When / Then
            with self.assertRaises(argparse.ArgumentError):
                utils.get_args(*args)

    def test_get_args_missing_optional(self):
        # Given
        args = (
            ("--arg1", "help"),
            ("--arg2", "help", False),
        )
        with patch.object(sys, "argv", ["prog", "--arg1", "value1"]):
            # When
            parsed = utils.get_args(*args)
            # Then
            self.assertEqual(parsed.arg1, "value1")
            self.assertEqual(parsed.arg2, None)

    def test_extra_args_fails(self):
        # Given
        args = (("--arg1", "help"),)
        with patch.object(
            sys, "argv", ["prog", "--arg1", "value1", "--arg2", "value2"]
        ):
            # When / Then
            with self.assertRaises(argparse.ArgumentError):
                utils.get_args(*args)


if __name__ == "__main__":
    unittest.main()
