"""Helpers for the diagnostics template tests.

Reads the workbook's XML, works out what Excel shows for a cell, and fills the
template with illustrative values (dates and figures are made up).
"""

import datetime
import re
import zipfile
from xml.etree import ElementTree

import openpyxl
import pytest
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_workbook_utils as workbook_utils,
)

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "xdr": "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing",
    "c": "http://schemas.openxmlformats.org/drawingml/2006/chart",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
}

sheet_params = [pytest.param(s, id=s.name) for s in cell_map.DIAGNOSTICS_SHEETS]
charted_sheet_params = [
    pytest.param(s, id=s.name) for s in cell_map.DIAGNOSTICS_SHEETS if s.chart_count
]
percentage_sheet_params = [
    pytest.param(s, id=s.name)
    for s in cell_map.DIAGNOSTICS_SHEETS
    if s.percentage_chart_count
]
comparison_sheet_params = [
    pytest.param(s, id=s.name)
    for s in cell_map.DIAGNOSTICS_SHEETS
    if s.percentage_chart_columns == ("E", "F", "N", "O")
]
bold_heading_sheet_params = [
    pytest.param(s, id=s.name)
    for s in cell_map.DIAGNOSTICS_SHEETS
    if s.bold_heading_cells
]
long_term_sheet_params = [
    pytest.param(s, id=s.name) for s in (cell_map.COMP_LONG_TERM, cell_map.NO_FILTERS)
]


# ---------------------------------------------------------------- cells
def cells_in(ranges: list[str]) -> set[tuple[int, int]]:
    """(row, column) of every cell in A1-style ranges."""
    return {
        (row, col)
        for cells in ranges
        for row, col in CellRange(cells).cells  # type: ignore[attr-defined]
    }


def coordinate(row: int, col: int) -> str:
    """A1 reference without creating the cell, which would hide its missing style."""
    return f"{get_column_letter(col)}{row}"


def effective_style(worksheet, row: int, col: int) -> tuple:
    """Font and number format Excel shows: the cell's own, else its column's.

    Use a freshly loaded workbook: reading a missing cell with `worksheet.cell()`
    creates it with the default style.
    """
    if workbook_utils.cell_exists(worksheet, row, col):
        cell = worksheet.cell(row, col)
        return (cell.font.name, cell.font.sz, bool(cell.font.b), cell.number_format)
    dimension = workbook_utils.column_dimension(worksheet, col)
    if dimension is None:
        return ("Aptos Narrow", 11.0, False, "General")
    return (
        dimension.font.name,
        dimension.font.sz,
        bool(dimension.font.b),
        dimension.number_format,
    )


# ------------------------------------------------------- workbook XML
def read_xml(package: zipfile.ZipFile, part: str) -> ElementTree.Element:
    """Parses one part of the xlsx package."""
    return ElementTree.fromstring(package.read(part))


def _relationship_targets(package: zipfile.ZipFile, part: str) -> dict[str, str]:
    """Maps relationship id to the full package path of the part it targets."""
    folder, name = part.rsplit("/", 1)
    rels_part = f"{folder}/_rels/{name}.rels"
    if rels_part not in package.namelist():
        return {}
    rels = read_xml(package, rels_part)
    return {
        rel.get("Id"): (
            rel.get("Target").lstrip("/")
            if rel.get("Target").startswith("/")
            else "/".join(p for p in f"{folder}/{rel.get('Target')}".split("/") if p)
        )
        for rel in rels.findall("rel:Relationship", NS)
    }


def _normalise(part: str) -> str:
    parts: list[str] = []
    for piece in part.split("/"):
        if piece == "..":
            parts.pop()
        else:
            parts.append(piece)
    return "/".join(parts)


def drawing(
    package: zipfile.ZipFile, sheet_name: str
) -> tuple[ElementTree.Element, str]:
    """A sheet's drawing XML and its part name; an empty element if it has none."""
    book = read_xml(package, "xl/workbook.xml")
    book_targets = _relationship_targets(package, "xl/workbook.xml")
    sheet_rid = next(
        s.get(f"{{{NS['r']}}}id")
        for s in book.findall("main:sheets/main:sheet", NS)
        if s.get("name") == sheet_name
    )
    sheet_part = _normalise(book_targets[sheet_rid])
    drawing_part = next(
        (
            _normalise(t)
            for t in _relationship_targets(package, sheet_part).values()
            if "/drawings/drawing" in t
        ),
        None,
    )
    if drawing_part is None:
        return ElementTree.Element("noDrawing"), ""
    return read_xml(package, drawing_part), drawing_part


def charts(package: zipfile.ZipFile, sheet_name: str) -> list[ElementTree.Element]:
    """The chart XML of every chart on a sheet."""
    sheet_drawing, drawing_part = drawing(package, sheet_name)
    chart_ids = [
        chart.get(f"{{{NS['r']}}}id")
        for chart in sheet_drawing.findall(".//c:chart", NS)
    ]
    if not chart_ids:
        return []
    targets = _relationship_targets(package, drawing_part)
    return [read_xml(package, _normalise(targets[rid])) for rid in chart_ids]


def split_reference(reference: str) -> tuple[str, str]:
    """('Sheet name', 'A1:B2') from a chart reference such as 'Sheet'!$A$1:$B$2."""
    sheet, cells = reference.rsplit("!", 1)
    return sheet.strip("'"), cells.replace("$", "")


def rows_of(reference: str) -> tuple[int, int]:
    """First and last row of a chart reference."""
    cells = CellRange(split_reference(reference)[1])
    return cells.min_row, cells.max_row


def row_count(reference: str) -> int:
    """Number of rows a chart reference covers."""
    first, last = rows_of(reference)
    return last - first + 1


def column_of(reference: str) -> str:
    """Column letters of a chart reference's first cell."""
    return re.match(r"[A-Z]+", split_reference(reference)[1]).group()


def axis_min(axis: ElementTree.Element) -> float | None:
    """A value axis's fixed minimum, or None when it is automatic."""
    found = axis.find("c:scaling/c:min", NS)
    return None if found is None else round(float(found.get("val")), 6)


def series_columns(chart: ElementTree.Element) -> set[str]:
    """Columns the chart's series values come from."""
    return {column_of(f.text) for f in chart.findall(".//c:ser/c:val//c:f", NS)}


def charts_with_value_columns(
    package: zipfile.ZipFile, sheet_name: str, columns: tuple[str, ...]
) -> list[ElementTree.Element]:
    """Charts whose series values all come from the given columns."""
    return [
        chart
        for chart in charts(package, sheet_name)
        if series_columns(chart) and series_columns(chart) <= set(columns)
    ]


def percentage_charts(
    package: zipfile.ZipFile, sheet: cell_map.SheetMap
) -> list[ElementTree.Element]:
    """Charts whose series values all come from the sheet's % change columns."""
    return charts_with_value_columns(
        package, sheet.name, sheet.percentage_chart_columns
    )


def chart_title(chart: ElementTree.Element) -> str:
    """Chart title text; an empty string when there is no title."""
    title = chart.find("c:chart/c:title", NS)
    if title is None:
        return ""
    return "".join(run.text or "" for run in title.findall(".//a:t", NS))


def expected_comparison_title(columns: set[str]) -> str:
    """Title a comparison-sheet chart should have, from the columns it plots."""
    table = "care homes" if columns <= set("CDEF") else "non-residential"
    kind = "% change" if columns <= set("EFNO") else "Posts"
    return f"CQC {table} - {kind}"


def chart_signature(package: zipfile.ZipFile, sheet: cell_map.SheetMap) -> list:
    """What a sheet's charts plot and how they look.

    Series references, title, axis minimums, axis label positions and data label
    positions.
    """
    return [
        (
            [f.text for f in chart.findall(".//c:f", NS)],
            chart_title(chart),
            [axis_min(axis) for axis in chart.findall(".//c:valAx", NS)],
            [e.get("val") for e in chart.findall(".//c:tickLblPos", NS)],
            [e.get("val") for e in chart.findall(".//c:dLblPos", NS)],
        )
        for chart in charts(package, sheet.name)
    ]


# ------------------------------------------------------------- fixtures
def add_months(start: datetime.datetime, months: int) -> datetime.datetime:
    """The date `months` months after `start`."""
    month_index = start.month - 1 + months
    return start.replace(
        year=start.year + month_index // 12, month=month_index % 12 + 1
    )


def slot_plan(sheet: cell_map.SheetMap, note: str, rows: int, full: bool):
    """First date, months per step, filled slots and growth per step for a range.

    The fixture's start dates are illustrative: the real long-term start is the
    first import date with data. `full` fills every slot in the range rather
    than the months to date.
    """
    if "annual" in note:
        plan = datetime.datetime(2017, 3, 1), 12, 10, 0.03
    elif "medium term" in sheet.name:
        plan = datetime.datetime(2025, 3, 1), 1, 18, 0.004
    elif "long term" in sheet.name:
        plan = datetime.datetime(2021, 4, 1), 3, 22, 0.012
    else:
        plan = datetime.datetime(2026, 3, 1), 1, 6, 0.004
    first, step, filled, growth = plan
    return first, step, rows if full else filled, growth


def fixture_cell(
    sheet: cell_map.SheetMap,
    input_range: cell_map.InputRange,
    row: int,
    col: int,
    full: bool = False,
):
    """Fixture value for one input cell, or None to leave an unfilled slot empty."""
    # Illustrative sizes: care homes about 12,000 locations, non-residential
    # (the right-hand tables, columns after H) 500 or 1,000 more.
    cells = CellRange(input_range.cells)
    i, j = row - cells.min_row, col - cells.min_col
    note = input_range.note
    first, step, filled, growth = slot_plan(sheet, note, cells.size["rows"], full)
    if "annual" in note and "% change" in note and "dates" not in note and not full:
        filled -= 1  # the first year has no change to show
    is_right_hand_table = col > 8
    if "number of locations" in note and cells.size["rows"] == 1:
        return 12000 + 500 * is_right_hand_table
    if "number of locations by period" in note:
        return (
            round((12000 + 1000 * is_right_hand_table) * (1 + growth) ** i)
            if i < filled
            else None
        )
    if cells.size["rows"] == 1:
        since = f"{first:%B %Y}" + (" (quarterly)" if step == 3 else "")
        if "% change heading" in note:
            start = f"{first:%B %Y}" if step == 3 else f"end of {first:%B %Y}"
            return f"% change since {start}"
        if "chart heading" in note:
            return f"Comparison of percentage change in filled posts and CQC locations since {first:%B %Y}"
        if "heading" in note:
            return f"Comparison of Skills for Care's estimate of filled posts to Capacity Tracker - Since {since}"
        if "onwards" in note:
            return f"- Must have a Capacity Tracker figure in all months from end of {first:%B %Y} onwards."
        return "End of August 2026"
    if "difference as percentage" in note:
        annual = 12 if full else 10
        monthly = 25 if full else 5
        if i >= annual + monthly:
            return None
        return round(0.01 + 0.001 * j + 0.0005 * i, 6)
    if "publication" in note or note in ("difference labels", "difference dates"):
        # To date: the current block has a row for the year just ended that the
        # previous block lacks, so the previous block is padded with a blank row.
        annual = 12 if full else 10
        monthly = 25 if full else (4 if "previous" in note else 5)
        is_padding_row = "previous" in note and not full and i == annual - 1
        if i >= annual + monthly or is_padding_row:
            return None
        if "labels" in note:
            return "Annual" if i < annual else "Monthly"
        if "dates" in note:
            if i < annual:
                return add_months(datetime.datetime(2017, 3, 1), 12 * i)
            last_annual = add_months(datetime.datetime(2017, 3, 1), 12 * (annual - 1))
            return add_months(last_annual, 1 + i - annual)
        scale = 1 if "previous" in note else 1.01  # current is 1% above previous
        return round(150_000 * (j + 1) * (1 + 0.004 * i) * scale)
    if i >= filled:
        return None
    if "dates" in note:
        return add_months(first, i * step)
    capacity_tracker_growth = 0.8  # Capacity Tracker grows slower than the estimate
    leaf = (
        [4_000, 8_000, 13_500] if "locations" in note else [150_000, 300_000, 700_000]
    )
    if "primary service type" in note:
        # C: total of all service types; D: total of the two care home types;
        # E:G: the service types.
        if j < 2:
            members = leaf if j == 0 else leaf[:2]
            return round(sum(members) * (1 + growth) ** i)
        return round(leaf[j - 2] * (1 + growth) ** i)
    if "% change" in note:
        return round((1 + growth * (capacity_tracker_growth if j else 1)) ** i - 1, 6)
    base = {
        "care homes SfC": 480_000,
        "care homes CT": 420_000,
        "non-res SfC": 650_000,
        "non-res CT": 560_000,
    }.get(note)
    if base:
        slower = capacity_tracker_growth if "CT" in note else 1
        return round(base * (1 + growth * slower) ** i)
    return round(leaf[j] * (1 + growth) ** i)


def fill_inputs(
    workbook: openpyxl.Workbook, full: bool = False
) -> dict[tuple[str, str], object]:
    """Writes a fixture value into every filled cell-map input; returns them.

    `full` fills every slot, to check the formatting of all rows.
    """
    written = {}
    for sheet in cell_map.DIAGNOSTICS_SHEETS:
        worksheet = workbook[sheet.name]
        for input_range in sheet.input_ranges:
            for row, col in sorted(cells_in([input_range.cells])):
                value = fixture_cell(sheet, input_range, row, col, full)
                if value is not None:
                    workbook_utils.write_input_cell(worksheet, row, col, value)
                    written[(sheet.name, coordinate(row, col))] = value
    return written
