import unittest
from unittest.mock import patch

import polars as pl

from lambda_function import lambda_handler


class TestLambdaFunction(unittest.TestCase):
    @patch("polars.read_parquet")
    def test_lambda_function(self, mock_read_parquet):
        mock_read_parquet.return_value = pl.DataFrame(
            {
                "import_date": [
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                ],
                "providerId": ["a", "b", "c", "d", "e"],
                "value": ["same", "same", "same", "same", "same"],
                "deregistrationDate": ["", "", "", "", ""],
            }
        )

        lambda_handler(
            event={"left": "test", "right": "test", "drop_cols": "value"}, context={}
        )
        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_called_with("test")
