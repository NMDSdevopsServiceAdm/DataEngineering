"""Helpers for writing input cells into the assessment template."""

import datetime
from copy import copy
from dataclasses import dataclass
from pathlib import Path

import openpyxl
import polars as pl
from openpyxl.utils.cell import coordinate_to_tuple
from openpyxl.worksheet.cell_range import CellRange
from openpyxl.worksheet.dimensions import ColumnDimension
from openpyxl.worksheet.worksheet import Worksheet

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    publication_utils,
)


PERIOD_LABEL_SHORT_FORMAT = "%b %Y"
PERIOD_LABEL_LONG_FORMAT = "%B %Y"
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
    "- Must have a Capacity Tracker figure in all months from end of {date} onwards."
)


@dataclass(frozen=True)
class TermFromDates:
    """First import date of each comparison term.

    Attributes:
        short (datetime.date): short term start.
        medium (datetime.date): medium term start.
        long (datetime.date): long term start, shared by the long term and no
            filters sheets.
    """

    short: datetime.date
    medium: datetime.date
    long: datetime.date


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


def check_template(
    workbook: openpyxl.Workbook, values: dict[str, dict[str, object]]
) -> None:
    """Checks the template holds every mapped sheet and fixed label, and that
    every value targets a mapped input cell.

    Args:
        workbook (openpyxl.Workbook): the loaded template.
        values (dict[str, dict[str, object]]): values to write, keyed by sheet
            name, then by A1 cell reference.

    Raises:
        TemplateMismatchError: listing every missing sheet and label that differs.
        UnmappedCellError: if a value targets a sheet or cell outside the mapped
            input ranges.
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


def _period_dates(period_labels: list[str]) -> list[datetime.datetime]:
    """Parses period labels such as "Mar 2026"; raises if there are none."""
    if not period_labels:
        raise ValueError("period_labels is empty")
    return [
        datetime.datetime.strptime(label, PERIOD_LABEL_SHORT_FORMAT)
        for label in period_labels
    ]


def title_text(period_labels: list[str]) -> str:
    """Title for each sheet, naming the latest period.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".

    Returns:
        str: e.g. "End of March 2026".
    """
    latest = max(_period_dates(period_labels))
    return f"End of {latest.strftime(PERIOD_LABEL_LONG_FORMAT)}"


def comp_prev_rows(
    period_months: list[datetime.date],
) -> list[tuple[datetime.date, str]]:
    """The rows of "Comp prev publication" in order, with their labels.

    An "Annual" row for each March period, then a "Monthly" row for each period
    after the latest March. The labels and the figures both follow this list.

    Args:
        period_months (list[datetime.date]): period months, in any order, repeats
            allowed.

    Returns:
        list[tuple[datetime.date, str]]: period month and label for each row.

    Raises:
        ValueError: if no period month is a March.
    """
    unique_months = sorted(set(period_months))
    marches = [m for m in unique_months if m.month == MARCH]
    if not marches:
        raise ValueError("period months have no March")
    return [(m, ANNUAL_LABEL) for m in marches] + [
        (m, MONTHLY_LABEL) for m in unique_months if m > marches[-1]
    ]


def sheet_text(
    period_labels: list[str], term_from_dates: TermFromDates
) -> dict[str, dict[str, str]]:
    """Text the writer sets, by sheet name then A1 cell.

    Every sheet gets the title. The short and medium term comparison sheets also
    get a heading and criteria note dated from the month before their term's start
    date, as does the chart heading on "Filled posts" (the short term's).
    "Comp prev publication" gets an "Annual" row for each March period, then a "Monthly" row for each period
    after the latest March, in each of its label ranges.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".
        term_from_dates (TermFromDates): first import date of each comparison term.

    Returns:
        dict[str, dict[str, str]]: text to write.

    Raises:
        ValueError: if no period label is a March, or there are more periods than
            label slots.
    """
    dates = _period_dates(period_labels)

    # Period months the headings and notes refer back to
    short_term_period, medium_term_period = _period_months(
        [term_from_dates.short, term_from_dates.medium]
    )

    # Title on every sheet
    text = {
        sheet.name: {cell_map.TITLE_CELL: title_text(period_labels)}
        for sheet in cell_map.ASSESSMENT_SHEETS
    }

    # Filled posts: chart heading
    text[cell_map.FILLED_POSTS.name][cell_map.FILLED_POSTS_CHART_HEADING_CELL] = (
        CHART_HEADING_TEXT.format(
            date=short_term_period.strftime(PERIOD_LABEL_LONG_FORMAT)
        )
    )

    # Short and medium term comparisons: heading and criteria note
    for sheet, period in (
        (cell_map.COMP_SHORT_TERM, short_term_period),
        (cell_map.COMP_MEDIUM_TERM, medium_term_period),
    ):
        date = period.strftime(PERIOD_LABEL_LONG_FORMAT)
        text[sheet.name][cell_map.HEADING_CELL] = HEADING_TEXT.format(date=date)
        text[sheet.name][cell_map.CRITERIA_NOTE_CELL] = CRITERIA_NOTE_TEXT.format(
            date=date
        )

    # Comp prev publication: an Annual or Monthly label for each period row
    row_labels = [label for _, label in comp_prev_rows([d.date() for d in dates])]
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
    term_from_dates: TermFromDates,
    values: dict[str, dict[str, object]],
    template_path: Path,
    output_path: Path,
) -> None:
    """Fills the template with values and period text and saves a copy.

    Steps: build the period text with `sheet_text`, load the template, check it
    and the values with `check_template`, write the values and then the period
    text (which replaces any value for those cells), and save. Nothing is written
    if a check fails: `check_template` raises `TemplateMismatchError` or
    `UnmappedCellError`, and `sheet_text` raises `ValueError` for empty period
    labels or none in March.

    Args:
        period_labels (list[str]): the run's period labels, such as "Mar 2026".
        term_from_dates (TermFromDates): first import date of each comparison term.
        values (dict[str, dict[str, object]]): values to write, keyed by sheet
            name, then by A1 cell reference, e.g. `{"Filled posts": {"C24":
            1000}}`. Cells must be in the sheet's mapped input ranges.
        template_path (Path): template to load.
        output_path (Path): local path to save to.
    """
    text = sheet_text(period_labels, term_from_dates)
    workbook = openpyxl.load_workbook(template_path)
    check_template(workbook, values)

    for name, cells in values.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    for name, cells in text.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    workbook.save(output_path)


def _period_months(import_dates: list[datetime.date]) -> list[datetime.date]:
    """The month each import date reports on."""
    return (
        pl.DataFrame({"import_date": import_dates})
        .select(publication_utils.import_date_to_period_month(pl.col("import_date")))
        .to_series()
        .to_list()
    )
