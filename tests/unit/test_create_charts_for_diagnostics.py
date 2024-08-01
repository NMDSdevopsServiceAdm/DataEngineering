import unittest
from unittest.mock import patch, Mock, ANY, call

import utils.diagnostics_utils.create_charts_for_diagnostics as job
from tests.test_file_schemas import (
    CreateChartsForDiagnosticsSchemas as Schemas,
)
from tests.test_file_data import (
    CreateChartsForDiagnosticsData as Data,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


class CreateChartsForDiagnosticsTests(unittest.TestCase):

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.test_rows, Schemas.test_schema)


class MainTests(CreateChartsForDiagnosticsTests):
    def setUp(self) -> None:
        super().setUp()
        self.destination = "file.pdf"

    @patch("utils.diagnostics_utils.create_charts_for_diagnostics.PdfPages")
    def test_create_charts_for_diagnostics_creates_pdf(self, pdf_pages_mock: Mock):

        job.create_charts_for_diagnostics(
            self.test_df,
            self.destination,
        )

        pdf_pages_mock.assert_called_once()
