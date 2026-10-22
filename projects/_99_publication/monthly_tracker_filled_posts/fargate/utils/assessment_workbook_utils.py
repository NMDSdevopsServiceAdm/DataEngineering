"""Helpers for writing input cells into the assessment template."""

from copy import copy
from pathlib import Path

import openpyxl
from openpyxl.utils.cell import coordinate_to_tuple
from openpyxl.worksheet.cell_range import CellRange
from openpyxl.worksheet.dimensions import ColumnDimension
from openpyxl.worksheet.worksheet import Worksheet

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)


class TemplateMismatchError(ValueError):
    """The template does not match the cell map."""


class UnmappedCellError(ValueError):
    """A value targets a sheet or cell outside the mapped input cells."""


def cell_exists(worksheet: Worksheet, row: int, col: int) -> bool:
    """Whether the sheet already holds a cell at this position.

    `worksheet.cell()` creates a missing cell, so existence is read from the
    private `_cells` mapping (openpyxl 3.1.5, pinned). A cell stored with the
    default style still counts as existing, which a style check cannot tell
    apart from a missing cell that falls back to its column's style.

    Args:
        worksheet (Worksheet): sheet to look in.
        row (int): 1-based row number.
        col (int): 1-based column number.

    Returns:
        bool: True if the cell exists.
    """
    return (row, col) in worksheet._cells


def column_dimension(worksheet: Worksheet, col: int) -> ColumnDimension | None:
    """Column dimension covering a column; openpyxl groups adjacent equal columns.

    Args:
        worksheet (Worksheet): sheet to look in.
        col (int): 1-based column number.

    Returns:
        ColumnDimension | None: the dimension, or None if the column has none.
    """
    return next(
        (d for d in worksheet.column_dimensions.values() if d.min <= col <= d.max),
        None,
    )


def write_input_cell(worksheet: Worksheet, row: int, col: int, value: object) -> None:
    """Writes a value, giving a new cell its column's style.

    Excel doesn't store a cell whose style equals its column's, and openpyxl
    doesn't apply a column's style to the cells it creates, so a new cell would
    otherwise get the workbook default font and number format.

    Args:
        worksheet (Worksheet): sheet to write to.
        row (int): 1-based row number.
        col (int): 1-based column number.
        value (object): value to write.
    """
    existed = cell_exists(worksheet, row, col)
    cell = worksheet.cell(row, col)
    dimension = column_dimension(worksheet, col)
    if not existed and dimension is not None:
        cell._style = copy(dimension._style)
    cell.value = value


def check_template(workbook: openpyxl.Workbook) -> None:
    """Checks the template holds every mapped sheet and fixed label.

    Args:
        workbook (openpyxl.Workbook): the loaded template.

    Raises:
        TemplateMismatchError: listing every missing sheet and label that differs.
    """
    problems = []
    for sheet in cell_map.ASSESSMENT_SHEETS:
        if sheet.name not in workbook.sheetnames:
            problems.append(f"missing sheet '{sheet.name}'")
            continue
        worksheet = workbook[sheet.name]
        for cell, expected in sheet.labels:
            found = worksheet[cell].value
            if found != expected:
                problems.append(
                    f"'{sheet.name}'!{cell} expected {expected!r}, found {found!r}"
                )
    if problems:
        raise TemplateMismatchError(
            "Template does not match cell map: " + "; ".join(problems)
        )


def _input_cells(sheet: cell_map.SheetMap) -> set[tuple[int, int]]:
    return {
        (row, col)
        for input_range in sheet.input_ranges
        for row, col in CellRange(input_range.cells).cells
    }


def fill_assessment_workbook(
    values: dict[str, dict[str, object]], template_path: Path, output_path: Path
) -> None:
    """Writes values into the template's input cells and saves a copy.

    Nothing is written unless the template matches the cell map and every value
    targets a mapped input cell. Other cells are left as in the template, and
    charts keep their series references; openpyxl can drop other chart styling,
    so check a saved workbook's charts in Excel.

    Args:
        values (dict[str, dict[str, object]]): values to write, keyed by sheet
            name, then by A1 cell reference, e.g. `{"Filled posts": {"B3": "End
            of March 2026"}}`. Cells must be in the sheet's mapped input ranges.
        template_path (Path): template to load.
        output_path (Path): local path to save to.

    Raises:
        UnmappedCellError: if a value targets a sheet or cell outside the mapped
            input ranges. A template that does not match the cell map raises
            `TemplateMismatchError` first.
    """
    workbook = openpyxl.load_workbook(template_path)
    check_template(workbook)

    input_cells = {
        sheet.name: _input_cells(sheet) for sheet in cell_map.ASSESSMENT_SHEETS
    }
    unmapped = [
        f"'{name}'!{cell}"
        for name, cells in values.items()
        for cell in cells
        if coordinate_to_tuple(cell) not in input_cells.get(name, set())
    ]
    if unmapped:
        raise UnmappedCellError(
            f"Values outside the mapped input cells: {', '.join(unmapped)}"
        )

    for name, cells in values.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    workbook.save(output_path)
