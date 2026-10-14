"""A filled copy of the template saves with its values, formulas, charts and styles.

The copies use illustrative values. Set DIAGNOSTICS_TEMPLATE_CHECK_DIR to keep
them for a manual check in Excel: one filled to date, one with every slot filled.
"""

import os
import zipfile
from pathlib import Path

import openpyxl
import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)

CHECK_DIR_ENV = "DIAGNOSTICS_TEMPLATE_CHECK_DIR"


def _save_filled_copy(tmp_path_factory, file_name: str, full: bool):
    workbook = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)
    written = support.fill_inputs(workbook, full=full)
    folder = Path(os.environ.get(CHECK_DIR_ENV) or tmp_path_factory.mktemp(file_name))
    path = folder / f"{file_name}.xlsx"
    workbook.save(path)
    return path, written


@pytest.fixture(scope="module")
def partly_filled(tmp_path_factory) -> tuple[Path, dict]:
    """Copy filled to date, as a real month would be."""
    return _save_filled_copy(tmp_path_factory, "diagnostics_filled_fixture", full=False)


@pytest.fixture(scope="module")
def fully_filled(tmp_path_factory) -> tuple[Path, dict]:
    """Copy with every slot filled, to check the formatting of all rows."""
    return _save_filled_copy(
        tmp_path_factory, "diagnostics_filled_fixture_full", full=True
    )


def _comp_prev_dates(path: Path) -> tuple[list, list]:
    """Previous and current publication dates, row by row."""
    sheet = openpyxl.load_workbook(path)["Comp prev publication"]
    previous = [sheet.cell(row, 3).value for row in range(23, 60)]
    current = [sheet.cell(row, 3).value for row in range(64, 101)]
    return previous, current


class TestPartlyFilledCopy:
    def test_every_written_value_is_saved(self, partly_filled):
        """Values written to the inputs are the values read back."""
        path, written = partly_filled
        saved = openpyxl.load_workbook(path)

        unsaved = [
            key
            for key, value in written.items()
            if saved[key[0]][key[1]].value != value
        ]

        assert unsaved == [], "Written values that did not save"

    def test_comp_prev_previous_rows_line_up_with_current_rows(self, partly_filled):
        """The difference block compares row by row, so a previous row is blank or the same date."""
        previous, current = _comp_prev_dates(partly_filled[0])
        mismatched = [
            row
            for row, (before, now) in enumerate(zip(previous, current))
            if before is not None and before != now
        ]

        assert mismatched == [], "Previous rows whose date differs from the current row"

    def test_comp_prev_fixture_includes_a_padded_previous_row(self, partly_filled):
        """The copy covers a new year: current has a row the previous block lacks."""
        previous, current = _comp_prev_dates(partly_filled[0])
        padded = [
            row
            for row, (before, now) in enumerate(zip(previous, current))
            if before is None and now is not None
        ]

        assert padded != []


class TestFullyFilledCopy:
    def test_every_input_slot_is_filled(self, fully_filled):
        """The fixture reaches every input cell, so the style check covers them all."""
        written = fully_filled[1]
        empty = [
            f"{sheet.name}: {r.cells}: {support.coordinate(row, col)}"
            for sheet in cell_map.DIAGNOSTICS_SHEETS
            for r in sheet.input_ranges
            for row, col in sorted(support.cells_in([r.cells]))
            if (sheet.name, support.coordinate(row, col)) not in written
        ]

        assert empty == [], "Input cells the fixture did not fill"

    @pytest.mark.parametrize("sheet", support.sheet_params)
    def test_written_cells_keep_the_expected_style(
        self, sheet: cell_map.SheetMap, fully_filled
    ):
        """Written cells show the right font and number format, as the column's style."""
        worksheet = openpyxl.load_workbook(fully_filled[0])[sheet.name]
        wrong_style = [
            f"{r.cells}: {support.coordinate(row, col)}"
            for r in sheet.input_ranges
            for row, col in sorted(support.cells_in([r.cells]))
            if support.effective_style(worksheet, row, col)
            != cell_map.CELL_STYLES[r.style]
        ]

        assert wrong_style == [], "Written cells with the wrong font or number format"

    def test_template_formulas_are_unchanged(
        self, fully_filled, workbook: openpyxl.Workbook
    ):
        """Saving a filled copy does not alter any template formula."""
        saved = openpyxl.load_workbook(fully_filled[0])
        changed = [
            f"{sheet.name}!{workbook[sheet.name].cell(row, col).coordinate}"
            for sheet in cell_map.DIAGNOSTICS_SHEETS
            for row, col in sorted(support.cells_in(list(sheet.formula_ranges)))
            if saved[sheet.name].cell(row, col).value
            != workbook[sheet.name].cell(row, col).value
        ]

        assert changed == [], "Formulas that changed on save"

    def test_charts_keep_what_they_plot_and_how_they_look(
        self, fully_filled, package: zipfile.ZipFile
    ):
        """Saving keeps each chart's series, title, axis minimums and label positions."""
        with zipfile.ZipFile(fully_filled[0]) as saved_package:
            changed = [
                sheet.name
                for sheet in cell_map.DIAGNOSTICS_SHEETS
                if support.chart_signature(saved_package, sheet)
                != support.chart_signature(package, sheet)
            ]

        assert changed == [], "Sheets whose charts changed on save"
