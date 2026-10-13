"""The template's cells match the cell map: what is empty, formatted and bold."""

import datetime

import openpyxl
import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)

# Typed values that would show as real figures if left outside the inputs.
PLACEHOLDERS = {"-", "–"}
TYPED_VALUE_TYPES = (int, float, datetime.date, datetime.datetime)

# Comp prev publication's label columns: the previous and current blocks (inputs)
# and the difference block (formulas).
LABEL_COLUMN_BLOCKS = [
    tuple(cells.split(":"))
    for cells in (
        [
            r.cells
            for r in cell_map.COMP_PREV_PUBLICATION.input_ranges
            if r.note.endswith("labels")
        ]
        + [
            cells
            for cells, _ in cell_map.COMP_PREV_PUBLICATION.formula_styles
            if cells.startswith("B")
        ]
    )
]


class TestTemplateCells:
    def test_has_expected_sheet_names_in_order(self, workbook: openpyxl.Workbook):
        """The six sheets keep their undated names, in order."""
        assert workbook.sheetnames == [s.name for s in cell_map.DIAGNOSTICS_SHEETS]

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

    @pytest.mark.parametrize("sheet", support.sheet_params)
    def test_input_cells_have_expected_style(self, sheet: cell_map.SheetMap):
        """Input cells show the font and number format the cell map expects."""
        worksheet = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)[sheet.name]
        wrong_style = [
            f"{r.cells}: {support.coordinate(row, col)}"
            for r in sheet.input_ranges
            for row, col in sorted(support.cells_in([r.cells]))
            if support.effective_style(worksheet, row, col)
            != cell_map.CELL_STYLES[r.style]
        ]

        assert wrong_style == [], "Input cells with the wrong font or number format"

    @pytest.mark.parametrize("sheet", support.formula_sheet_params)
    def test_formula_cells_have_expected_style(self, sheet: cell_map.SheetMap):
        """Formula cells show the font and number format the cell map expects."""
        worksheet = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)[sheet.name]
        wrong_style = [
            f"{cells}: {support.coordinate(row, col)}"
            for cells, style in sheet.formula_styles
            for row, col in sorted(support.cells_in([cells]))
            if support.effective_style(worksheet, row, col)
            != cell_map.CELL_STYLES[style]
        ]

        assert wrong_style == [], "Formula cells with the wrong font or number format"

    @pytest.mark.parametrize("sheet", support.bold_heading_sheet_params)
    def test_table_headings_are_bold(
        self, sheet: cell_map.SheetMap, workbook: openpyxl.Workbook
    ):
        """Table headings stand out from the figures below them."""
        worksheet = workbook[sheet.name]
        not_bold = [c for c in sheet.bold_heading_cells if not worksheet[c].font.b]

        assert not_bold == [], "Headings that are not bold"

    @pytest.mark.parametrize("first", [first for first, _ in LABEL_COLUMN_BLOCKS])
    def test_comp_prev_label_column_has_a_header_line_above_each_block(
        self, first: str, workbook: openpyxl.Workbook
    ):
        """The first label cell of each block carries the header line."""
        first_cell = workbook["Comp prev publication"][first]

        assert first_cell.border.top.style is not None

    @pytest.mark.parametrize("first, last", LABEL_COLUMN_BLOCKS)
    def test_comp_prev_label_column_has_no_lines_between_rows(
        self, first: str, last: str, workbook: openpyxl.Workbook
    ):
        """Only the header line sits above each block; no lines between its rows."""
        cells = [
            cell for (cell,) in workbook["Comp prev publication"][f"{first}:{last}"]
        ]
        stray_lines = [c.coordinate for c in cells if c.border.bottom.style] + [
            c.coordinate for c in cells[1:] if c.border.top.style
        ]

        assert stray_lines == [], "Lines between rows"
