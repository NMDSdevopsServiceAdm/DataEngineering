"""Cell map for `templates/diagnostics_template.xlsx`.

Lists the cells the workbook writer fills (`InputRange`), the template formulas
it must leave alone, and the charts and styles the template tests check.

Open points the map does not settle:
- which month's `assessment_locationid_count_<term>` fills the "Number of
  locations" cells;
- the unfiltered Capacity Tracker totals on `No filters - long term`, which no
  clean dataset column provides.
"""

from dataclasses import dataclass
from pathlib import Path

from utils.column_names.publication_columns import PublicationColumns as Pub


TEMPLATE_PATH = Path(__file__).parents[2] / "templates" / "diagnostics_template.xlsx"

# Font and number format each kind of input cell must show in the template: its
# own style, or its column's when they match (Excel stores no cell then). The
# writer gives cells it creates their column's style (`write_input_cell`).
CELL_STYLES = {
    "text": ("Arial", 12.0, False, "General"),
    "bold_text": ("Arial", 12.0, True, "General"),
    "heading": ("Arial", 14.0, True, "General"),
    "date": ("Arial", 12.0, False, "mmm-yy"),
    "count": ("Arial", 12.0, False, "#,##0"),
    "percent": ("Arial", 12.0, False, "0.0%"),
}


@dataclass(frozen=True)
class InputRange:
    """Cells the writer fills.

    Attributes:
        cells (str): A1-style range.
        source_column (str | None): clean dataset column feeding the range;
            None where the writer derives or builds the value (dates, text,
            % change) or no source column exists yet.
        note (str): what the range holds.
        style (str): key into `CELL_STYLES` for the font and number format
            every cell in the range must have in the template.
    """

    cells: str
    source_column: str | None = None
    note: str = ""
    style: str = "text"


@dataclass(frozen=True)
class SheetMap:
    """Template cells for one sheet; everything not listed is left untouched.

    Attributes:
        name (str): sheet name in the template.
        input_ranges (tuple[InputRange, ...]): cells the writer fills.
        chart_count (int): charts on the sheet.
        chart_label_ranges (tuple[str, ...]): fixed label cells charts use as
            series names.
        na_formula_ranges (tuple[str, ...]): formula cells that return #N/A for
            unfilled slots so charts skip them.
        window_rows (int): date rows each chart series must span.
        percentage_chart_columns (tuple[str, ...]): value columns that mark a
            chart as a % change chart.
        percentage_chart_count (int): % change charts on the sheet.
        bold_heading_cells (tuple[str, ...]): fixed table headings that must be
            bold.
        formula_styles (tuple[tuple[str, str], ...]): template formula ranges the
            writer must not overwrite, each with the `CELL_STYLES` key its cells
            must have. Every formula range is a slot formula that stays blank
            until its slot is filled.
    """

    name: str
    input_ranges: tuple[InputRange, ...]
    chart_count: int
    chart_label_ranges: tuple[str, ...] = ()
    na_formula_ranges: tuple[str, ...] = ()
    window_rows: int = 0
    percentage_chart_columns: tuple[str, ...] = ()
    percentage_chart_count: int = 0
    bold_heading_cells: tuple[str, ...] = ()
    formula_styles: tuple[tuple[str, str], ...] = ()

    @property
    def formula_ranges(self) -> tuple[str, ...]:
        """The template formula ranges."""
        return tuple(cells for cells, _ in self.formula_styles)


TITLE_CELL = "B3"
HEADING_CELL = "B7"
# Notes are cells, not text boxes: openpyxl drops drawing shapes on save.
CRITERIA_NOTE_CELL = "B14"

# End-of-March baseline plus the 12 months of the financial year.
MONTHLY_SLOTS = 13
# Medium term: baseline plus 24 months, spanning two financial years.
MEDIUM_TERM_SLOTS = 25
# Long term is quarterly from the first import date with data, plus 3 spare
# years. Writer dates are the import date minus one month.
LONG_TERM_SLOTS = 34

FILLED_POSTS = SheetMap(
    name="Filled posts",
    input_ranges=(
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange("B24:B36", note="annual filled posts dates", style="date"),
        InputRange("I24:I36", note="annual filled posts % change dates", style="date"),
        InputRange("E24:G36", note="annual filled posts, 3 spare years", style="count"),
        InputRange("B44:B56", note="annual CQC locations dates", style="date"),
        InputRange("I44:I56", note="annual CQC locations % change dates", style="date"),
        InputRange(
            "E44:G56", note="annual CQC locations, 3 spare years", style="count"
        ),
        InputRange("B65:B77", note="monthly filled posts dates", style="date"),
        InputRange("I65:I77", note="monthly filled posts % change dates", style="date"),
        InputRange("E65:G77", note="monthly filled posts", style="count"),
        InputRange("B84:B96", note="monthly CQC locations dates", style="date"),
        InputRange(
            "I84:I96", note="monthly CQC locations % change dates", style="date"
        ),
        InputRange("E84:G96", note="monthly CQC locations", style="count"),
        InputRange(
            "B99", note="'...since <month year>' chart heading", style="bold_text"
        ),
    ),
    chart_count=3,
    chart_label_ranges=("B61", "B80"),
    na_formula_ranges=("J65:N77", "J84:N96"),
    window_rows=MONTHLY_SLOTS,
    percentage_chart_columns=("J", "K", "N"),
    percentage_chart_count=3,
    bold_heading_cells=("B20", "B40", "B61", "B80"),
    formula_styles=(
        ("C24:D36", "count"),
        ("J25:N36", "percent"),
        ("C44:D56", "count"),
        ("J45:N56", "percent"),
        ("C65:D77", "count"),
        ("J65:N77", "percent"),
        ("C84:D96", "count"),
        ("J84:N96", "percent"),
    ),
)

# Annual rows first, then monthly rows; unused slots stay blank at the end. The
# difference block compares current row i with previous row i, so the writer pads
# the previous block (a blank row where the previous publication had none) to
# line its rows up with the current block's.
COMP_PREV_PUBLICATION = SheetMap(
    name="Comp prev publication",
    input_ranges=(
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange("B23:B59", note="previous publication labels"),
        InputRange("C23:C59", note="previous publication dates", style="date"),
        InputRange("D23:H59", note="previous publication figures", style="count"),
        InputRange("B64:B100", note="current publication labels"),
        InputRange("C64:C100", note="current publication dates", style="date"),
        InputRange("D64:H100", note="current publication figures", style="count"),
    ),
    chart_count=0,
    bold_heading_cells=("B20", "B61", "B103"),
    formula_styles=(
        ("B106:B142", "text"),
        ("C106:C142", "date"),
        ("D106:H142", "percent"),
    ),
)


def _location_count_inputs(
    care_homes_cell: str, non_residential_cell: str, source_column: str
) -> tuple[InputRange, InputRange]:
    """The two "Number of locations" cells on a comparison sheet."""
    return (
        InputRange(
            care_homes_cell,
            source_column,
            "care homes number of locations (unconfirmed source)",
            "count",
        ),
        InputRange(
            non_residential_cell,
            source_column,
            "non-res number of locations (unconfirmed source)",
            "count",
        ),
    )


def _comparison_sheet(
    name: str,
    first_row: int,
    last_row: int,
    filled_posts: str,
    ct_employed: str | None,
    extra_inputs: tuple[InputRange, ...] = (),
    has_criteria_note: bool = True,
) -> SheetMap:
    """Builds the map for a care homes (B:F) and non-residential (K:O) sheet.

    Args:
        name (str): sheet name in the template.
        first_row (int): first date row.
        last_row (int): last date row.
        filled_posts (str): clean dataset column for Skills for Care filled posts.
        ct_employed (str | None): clean dataset column for Capacity Tracker
            employed; None where no column provides it.
        extra_inputs (tuple[InputRange, ...]): sheet-specific input ranges, such
            as the number of locations.
        has_criteria_note (bool): whether the notes carry a dated criteria line.

    Returns:
        SheetMap: the sheet's cell map.
    """
    ranges = [
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange(
            HEADING_CELL, note="heading incl. 'Since <month year>'", style="heading"
        ),
        InputRange(f"E{first_row - 2}", note="care homes % change heading"),
        InputRange(f"N{first_row - 2}", note="non-res % change heading"),
        InputRange(f"B{first_row}:B{last_row}", note="care homes dates", style="date"),
        InputRange(
            f"K{first_row}:K{last_row}", note="non-residential dates", style="date"
        ),
        InputRange(
            f"C{first_row}:C{last_row}", filled_posts, "care homes SfC", "count"
        ),
        InputRange(f"D{first_row}:D{last_row}", ct_employed, "care homes CT", "count"),
        InputRange(f"L{first_row}:L{last_row}", filled_posts, "non-res SfC", "count"),
        InputRange(f"M{first_row}:M{last_row}", ct_employed, "non-res CT", "count"),
        InputRange(
            f"E{first_row}:F{last_row}",
            note="care homes % change, writer-derived",
            style="percent",
        ),
        InputRange(
            f"N{first_row}:O{last_row}",
            note="non-res % change, writer-derived",
            style="percent",
        ),
    ]
    if has_criteria_note:
        ranges.append(
            InputRange(
                CRITERIA_NOTE_CELL, note="'...from end of <month year> onwards' note"
            )
        )
    header_row = first_row - 1
    table_heading_row = first_row - 6
    return SheetMap(
        name,
        tuple(ranges) + extra_inputs,
        chart_count=4,
        chart_label_ranges=(
            f"C{header_row}:F{header_row}",
            f"L{header_row}:O{header_row}",
        ),
        window_rows=last_row - first_row + 1,
        percentage_chart_columns=("E", "F", "N", "O"),
        percentage_chart_count=2,
        bold_heading_cells=(f"B{table_heading_row}", f"K{table_heading_row}"),
    )


COMP_SHORT_TERM = _comparison_sheet(
    "Comp SfC and CT - short term",
    26,
    26 + MONTHLY_SLOTS - 1,
    Pub.assessment_filled_posts_short_term,
    Pub.assessment_ct_total_employed_short_term,
    extra_inputs=_location_count_inputs(
        "D22", "M22", Pub.assessment_locationid_count_short_term
    ),
)
COMP_MEDIUM_TERM = _comparison_sheet(
    "Comp SfC and CT - medium term",
    29,
    29 + MEDIUM_TERM_SLOTS - 1,
    Pub.assessment_filled_posts_medium_term,
    Pub.assessment_ct_total_employed_medium_term,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.assessment_locationid_count_medium_term
    ),
)
COMP_LONG_TERM = _comparison_sheet(
    "Comp SfC and CT - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.assessment_filled_posts_long_term,
    Pub.assessment_ct_total_employed_long_term,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.assessment_locationid_count_long_term
    ),
)

# D25/M25 here are the fixed label "Varies month to month"; location counts are
# per period in G/P instead. The unfiltered Capacity Tracker columns (D, M) have
# no source column in the clean dataset.
NO_FILTERS = _comparison_sheet(
    "No filters - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.publication_filled_posts,
    None,
    has_criteria_note=False,
    extra_inputs=(
        InputRange(
            f"G29:G{29 + LONG_TERM_SLOTS - 1}",
            Pub.publication_locationid_count,
            "care homes number of locations by period (unconfirmed source)",
            "count",
        ),
        InputRange(
            f"P29:P{29 + LONG_TERM_SLOTS - 1}",
            Pub.publication_locationid_count,
            "non-res number of locations by period (unconfirmed source)",
            "count",
        ),
    ),
)

DIAGNOSTICS_SHEETS: tuple[SheetMap, ...] = (
    FILLED_POSTS,
    COMP_PREV_PUBLICATION,
    COMP_SHORT_TERM,
    COMP_MEDIUM_TERM,
    COMP_LONG_TERM,
    NO_FILTERS,
)
