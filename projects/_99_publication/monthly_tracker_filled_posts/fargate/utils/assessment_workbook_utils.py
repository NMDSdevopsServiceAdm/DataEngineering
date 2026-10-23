"""Helpers for writing input cells into the assessment template."""

import datetime
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


PERIOD_LABEL_FORMAT = "%b %Y"
MARCH = 3
ANNUAL_LABEL = "Annual"
MONTHLY_LABEL = "Monthly"
HEADING_TEXT = (
    "Comparison of Skills for Care's estimate of filled posts to Capacity Tracker"
    " - Since {date}"
)
CHART_HEADING_TEXT = (
    "Comparison of percentage change in filled posts and CQC locations since {date}"
)
CRITERIA_NOTE_TEXT = (
    "Must have a Capacity Tracker figure in all months from end of {date} onwards."
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


def _period_dates(period_labels: list[str]) -> list[datetime.datetime]:
    """Parses period labels such as "Mar 2026"; raises if there are none."""
    if not period_labels:
        raise ValueError("period_labels is empty")
    return [
        datetime.datetime.strptime(label, PERIOD_LABEL_FORMAT)
        for label in period_labels
    ]


def title_text(period_labels: list[str]) -> str:
    """Title for each sheet, naming the latest period.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".

    Returns:
        str: e.g. "End of Mar 2026".
    """
    latest = max(_period_dates(period_labels))
    return f"End of {latest.strftime(PERIOD_LABEL_FORMAT)}"


def sheet_text(period_labels: list[str]) -> dict[str, dict[str, str]]:
    """Text the writer sets, by sheet name then A1 cell.

    Every sheet gets the title. The short and medium term comparison sheets also
    get a heading and criteria note dated from March: the latest March for short
    term, the March a year before for medium term. "Filled posts" gets a chart
    heading dated from the latest March. "Comp prev publication" gets
    an "Annual" row for each March period, then a "Monthly" row for each other
    period, in each of its label ranges.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".

    Returns:
        dict[str, dict[str, str]]: text to write.

    Raises:
        ValueError: if no period label is a March, or there are more periods than
            label slots.
    """
    dates = _period_dates(period_labels)

    # March dates the headings and notes refer back to
    marches = [d for d in dates if d.month == MARCH]
    if not marches:
        raise ValueError("period_labels has no March")
    short_term_march = max(marches)
    medium_term_march = short_term_march.replace(year=short_term_march.year - 1)

    # Title on every sheet
    text = {
        sheet.name: {cell_map.TITLE_CELL: title_text(period_labels)}
        for sheet in cell_map.ASSESSMENT_SHEETS
    }

    # Filled posts: chart heading
    text[cell_map.FILLED_POSTS.name][cell_map.FILLED_POSTS_CHART_HEADING_CELL] = (
        CHART_HEADING_TEXT.format(date=short_term_march.strftime(PERIOD_LABEL_FORMAT))
    )

    # Short and medium term comparisons: heading and criteria note
    for sheet, march in (
        (cell_map.COMP_SHORT_TERM, short_term_march),
        (cell_map.COMP_MEDIUM_TERM, medium_term_march),
    ):
        date = march.strftime(PERIOD_LABEL_FORMAT)
        text[sheet.name][cell_map.HEADING_CELL] = HEADING_TEXT.format(date=date)
        text[sheet.name][cell_map.CRITERIA_NOTE_CELL] = CRITERIA_NOTE_TEXT.format(
            date=date
        )

    # Comp prev publication: an Annual or Monthly label for each period row
    unique_dates = set(dates)
    annual_count = sum(d.month == MARCH for d in unique_dates)
    row_labels = [ANNUAL_LABEL] * annual_count + [MONTHLY_LABEL] * (
        len(unique_dates) - annual_count
    )
    for label_range in cell_map.COMP_PREV_LABEL_RANGES:
        cells = CellRange(label_range)
        if len(row_labels) > cells.size["rows"]:
            raise ValueError(
                f"{len(row_labels)} periods exceed the {cells.size['rows']} rows in"
                f" {label_range}"
            )
        for i, label in enumerate(row_labels):
            text[cell_map.COMP_PREV_PUBLICATION.name][f"B{cells.min_row + i}"] = label
    return text


def _input_cells(sheet: cell_map.SheetMap) -> set[tuple[int, int]]:
    return {
        (row, col)
        for input_range in sheet.input_ranges
        for row, col in CellRange(input_range.cells).cells
    }


def fill_assessment_workbook(
    period_labels: list[str],
    values: dict[str, dict[str, object]],
    template_path: Path,
    output_path: Path,
) -> None:
    """Writes values and period text into the template's input cells and saves a copy.

    The text from `sheet_text` (titles, and the short and medium term comparison
    headings and criteria notes) replaces any value given for those cells.

    Nothing is written unless the template matches the cell map and every value
    targets a mapped input cell. Other cells are left as in the template, and
    charts keep their series references; openpyxl can drop other chart styling,
    so check a saved workbook's charts in Excel.

    Args:
        period_labels (list[str]): the run's period labels, such as "Mar 2026".
        values (dict[str, dict[str, object]]): values to write, keyed by sheet
            name, then by A1 cell reference, e.g. `{"Filled posts": {"C24":
            1000}}`. Cells must be in the sheet's mapped input ranges.
        template_path (Path): template to load.
        output_path (Path): local path to save to.

    Raises:
        UnmappedCellError: if a value targets a sheet or cell outside the mapped
            input ranges. A template that does not match the cell map raises
            `TemplateMismatchError` first, and `period_labels` that are empty
            or have no March raise `ValueError`.
    """
    text = sheet_text(period_labels)
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
    for name, cells in text.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    workbook.save(output_path)
