"""The template's formulas give the right totals and stay blank until a slot is filled."""

import re

import openpyxl
import pytest
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)

# A formula that reads an input cell must test it for being empty first.
EMPTY_GUARD = re.compile(r'=""|SUM\([^)]*\)=0')
CELL_REFERENCE = re.compile(r"\$?[A-Z]{1,3}\$?\d+(?::\$?[A-Z]{1,3}\$?\d+)?")


class TestTemplateFormulas:
    @pytest.mark.parametrize("sheet", support.formula_sheet_params)
    def test_formula_cells_contain_formulas(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """Every cell the map lists as a formula really is one."""
        worksheet = workbook[sheet.name]
        not_formulas = [
            worksheet.cell(row, col).coordinate
            for row, col in sorted(support.cells_in(list(sheet.formula_ranges)))
            if not str(worksheet.cell(row, col).value).startswith("=")
        ]

        assert not_formulas == [], "Cells that should be formulas"

    @pytest.mark.parametrize("sheet", support.formula_sheet_params)
    def test_slot_formulas_are_wrapped_in_a_blank_guard(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """An unfilled slot shows blank (or #N/A), never an error or zero."""
        worksheet = workbook[sheet.name]
        unguarded = [
            worksheet.cell(row, col).coordinate
            for row, col in sorted(support.cells_in(list(sheet.formula_ranges)))
            if not str(worksheet.cell(row, col).value).startswith(("=IF(", "=IFERROR("))
        ]

        assert unguarded == [], "Slot formulas with no IF or IFERROR wrapper"

    @pytest.mark.parametrize("sheet", support.formula_sheet_params)
    def test_slot_formulas_guard_references_to_empty_inputs(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """An empty input counts as 0, so a formula reading one must test it first."""
        worksheet = workbook[sheet.name]
        input_cells = support.cells_in([r.cells for r in sheet.input_ranges])
        unguarded = []
        for row, col in sorted(support.cells_in(list(sheet.formula_ranges))):
            formula = str(worksheet.cell(row, col).value)
            referenced = support.cells_in(
                [ref.replace("$", "") for ref in CELL_REFERENCE.findall(formula)]
            )
            if referenced & input_cells and not EMPTY_GUARD.search(formula):
                unguarded.append(worksheet.cell(row, col).coordinate)

        assert unguarded == [], "Formulas reading inputs without an emptiness test"
