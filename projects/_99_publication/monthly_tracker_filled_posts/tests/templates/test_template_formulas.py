"""The template's formulas give the right totals and stay blank until a slot is filled."""

import re

import openpyxl
import pytest
from openpyxl.utils import get_column_letter
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

# Filled posts: C sums all three service types and D the care homes. The % change
# columns J to N are each based on the total or service type in the column below.
FILLED_POSTS_TOTAL_RANGES = [
    cells for cells, style in cell_map.FILLED_POSTS.formula_styles if style == "count"
]
FILLED_POSTS_PERCENT_RANGES = [
    cells for cells, style in cell_map.FILLED_POSTS.formula_styles if style == "percent"
]
PERCENT_COLUMN_SOURCE = dict(zip("JKLMN", "CDEFG"))


def _first_row(sheet: cell_map.SheetMap, note: str) -> int:
    """First row of the input range with this note."""
    cells = next(r.cells for r in sheet.input_ranges if r.note == note)
    return CellRange(cells).min_row


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

    @pytest.mark.parametrize("cells", FILLED_POSTS_TOTAL_RANGES)
    def test_filled_posts_totals_sum_the_right_service_types(
        self, cells: str, workbook: openpyxl.Workbook
    ):
        """All locations sums E:G; care homes sums only E:F, not non-residential."""
        worksheet = workbook[cell_map.FILLED_POSTS.name]
        block = CellRange(cells)
        wrong_total = [
            f"{column}{row}"
            for row in range(block.min_row, block.max_row + 1)
            for column, leaves in (("C", "$E{0}:$G{0}"), ("D", "$E{0}:$F{0}"))
            if leaves.format(row) not in str(worksheet[f"{column}{row}"].value)
            or (column == "D" and "$G" in str(worksheet[f"D{row}"].value))
        ]

        assert wrong_total == [], "Totals that sum the wrong columns"

    @pytest.mark.parametrize("cells", FILLED_POSTS_PERCENT_RANGES)
    def test_filled_posts_percentage_changes_use_the_expected_baseline(
        self, cells: str, workbook: openpyxl.Workbook
    ):
        """Annual rows compare with the year before; monthly rows with the first row (end of March)."""
        worksheet = workbook[cell_map.FILLED_POSTS.name]
        block = CellRange(cells)
        is_monthly = cells in cell_map.FILLED_POSTS.na_formula_ranges
        wrong_baseline = []
        for row in range(block.min_row, block.max_row + 1):
            for col in range(block.min_col, block.max_col + 1):
                column = get_column_letter(col)
                source = PERCENT_COLUMN_SOURCE[column]
                if is_monthly:
                    base = f"{source}${block.min_row}"
                else:
                    base = f"{source}{row - 1}"
                expected = f"({source}{row}-{base})/{base}"
                if expected not in str(worksheet[f"{column}{row}"].value):
                    wrong_baseline.append(f"{column}{row}")

        assert wrong_baseline == [], "% change cells with the wrong baseline"

    def test_comp_prev_differences_compare_the_matching_rows(
        self, workbook: openpyxl.Workbook
    ):
        """Each difference row is current minus previous, divided by previous, for the same row."""
        sheet = cell_map.COMP_PREV_PUBLICATION
        worksheet = workbook[sheet.name]
        previous_first = _first_row(sheet, "previous publication figures")
        current_first = _first_row(sheet, "current publication figures")
        block = CellRange("D106:H142")
        wrong_rows = []
        for row in range(block.min_row, block.max_row + 1):
            current = current_first + row - block.min_row
            previous = previous_first + row - block.min_row
            for col in range(block.min_col, block.max_col + 1):
                column = get_column_letter(col)
                expected = f"({column}{current}-{column}{previous})/{column}{previous}"
                if expected not in str(worksheet[f"{column}{row}"].value):
                    wrong_rows.append(f"{column}{row}")

        assert wrong_rows == [], "Difference cells that compare the wrong rows"

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

    @pytest.mark.parametrize("sheet", support.na_sheet_params)
    def test_percentage_formulas_return_na_for_unfilled_slots(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """Charts skip #N/A, whereas a blank text result would plot as 0%."""
        worksheet = workbook[sheet.name]
        without_na = [
            worksheet.cell(row, col).coordinate
            for row, col in sorted(support.cells_in(list(sheet.na_formula_ranges)))
            if "NA()" not in str(worksheet.cell(row, col).value)
        ]

        assert without_na == [], "Chart-fed formulas that do not return #N/A"

    @pytest.mark.parametrize("sheet", support.na_sheet_params)
    def test_na_cells_are_hidden_by_white_font_conditional_format(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """The #N/A cells are invisible on the sheet."""
        hidden = support.hidden_na_cells(workbook[sheet.name])
        visible_na = [
            support.coordinate(row, col)
            for row, col in sorted(support.cells_in(list(sheet.na_formula_ranges)))
            if (row, col) not in hidden
        ]

        assert visible_na == [], "#N/A cells with no white-font conditional format"
