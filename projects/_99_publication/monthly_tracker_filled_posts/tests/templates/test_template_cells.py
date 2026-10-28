"""The template's cells match the cell map: sheet names and what is empty."""

import datetime

import openpyxl
import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)

# Typed values that would show as real figures if left outside the inputs.
PLACEHOLDERS = {"-", "–"}
TYPED_VALUE_TYPES = (int, float, datetime.date, datetime.datetime)


class TestTemplateCells:
    def test_has_expected_sheet_names_in_order(self, workbook: openpyxl.Workbook):
        """The six sheets keep their undated names, in order."""
        assert workbook.sheetnames == [s.name for s in cell_map.ASSESSMENT_SHEETS]

    @pytest.mark.parametrize("sheet", support.sheet_params)
    def test_input_cells_are_empty(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """No dates, figures, titles or headings are left in the template."""
        worksheet = workbook[sheet.name]
        filled = [
            f"{r.cells}: {worksheet.cell(row, col).coordinate}"
            for r in sheet.input_ranges
            for row, col in sorted(support.cells_in([r.cells]))
            if worksheet.cell(row, col).value is not None
        ]

        assert filled == [], "Input cells that still hold values"

    @pytest.mark.parametrize(
        "sheet_name, cell",
        [
            ("Filled posts", "J63"),
            ("Filled posts", "J82"),
            ("Comp SfC and CT - short term", "E27"),
            ("Comp SfC and CT - medium term", "N27"),
            ("Comp SfC and CT - long term", "E27"),
            ("No filters - long term", "N27"),
        ],
    )
    def test_cumulative_headings_are_generic(
        self, workbook: openpyxl.Workbook, sheet_name: str, cell: str
    ):
        """The % change table headings read 'Cumulative % change'."""
        assert workbook[sheet_name][cell].value == "Cumulative % change"

    @pytest.mark.parametrize("sheet", support.sheet_params)
    def test_has_no_typed_values_or_placeholders_outside_inputs(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """A typed number, date or '-' outside the inputs would show as a real figure."""
        input_cells = support.cells_in([r.cells for r in sheet.input_ranges])
        typed = [
            cell.coordinate
            for row in workbook[sheet.name].iter_rows()
            for cell in row
            if (cell.row, cell.column) not in input_cells
            and (
                isinstance(cell.value, TYPED_VALUE_TYPES)
                or str(cell.value).strip() in PLACEHOLDERS
            )
        ]

        assert typed == [], "Typed values or placeholders outside the inputs"
