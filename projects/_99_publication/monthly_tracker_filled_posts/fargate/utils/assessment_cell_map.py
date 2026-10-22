"""Cell map for `templates/assessment_template.xlsx`.

Lists the cells the workbook writer fills (`InputRange`) and the charts and
number formats the template tests check. The template has no formulas.

The "Number of locations" cells take the latest row per group, since every row
in a term's period shares the same count.

Open point the map does not settle (needs a pipeline column or ticket):
- the pipeline columns that fill the % change cells on `Filled posts` and `Comp
  prev publication`.
"""

from dataclasses import dataclass
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.template_paths import (
    TEMPLATES_DIR,
)
from utils.column_names.publication_columns import PublicationColumns as Pub


TEMPLATE_PATH = TEMPLATES_DIR / "assessment_template.xlsx"

# Number format each kind of input cell must show in the template: its own, or
# its column's when they match (Excel stores no cell then). The writer gives cells
# it creates their column's style (`write_input_cell`).
NUMBER_FORMATS = {
    "general": "General",
    "date": "mmm-yy",
    "count": "#,##0",
    "percent": "0.0%",
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
        number_format (str): key into `NUMBER_FORMATS` for the format every cell
            in the range must have in the template.
    """

    cells: str
    source_column: str | None = None
    note: str = ""
    number_format: str = "general"


@dataclass(frozen=True)
class SheetMap:
    """Template cells for one sheet; everything not listed is left untouched.

    Attributes:
        name (str): sheet name in the template.
        input_ranges (tuple[InputRange, ...]): cells the writer fills.
        chart_count (int): charts on the sheet.
        series_name_cells (tuple[str, ...]): fixed label cells charts use as
            series names.
        slot_count (int): date rows each chart series must span.
        labels (tuple[tuple[str, str], ...]): fixed (cell, text) pairs the
            template must hold, so a changed layout fails before anything is
            written.
    """

    name: str
    input_ranges: tuple[InputRange, ...]
    chart_count: int
    series_name_cells: tuple[str, ...] = ()
    slot_count: int = 0
    labels: tuple[tuple[str, str], ...] = ()


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

_SERVICE_TYPE_HEADERS = (
    "All CQC locations",
    "CQC care homes",
    "CQC care home with nursing",
    "CQC care home without nursing",
    "CQC non-residential",
)


def _service_type_labels(row: int, first_col: str) -> tuple[tuple[str, str], ...]:
    """Service type header labels across a row, starting at `first_col`."""
    return tuple(
        (f"{chr(ord(first_col) + i)}{row}", text)
        for i, text in enumerate(_SERVICE_TYPE_HEADERS)
    )


_COMPARISON_LABELS = (
    ("B23", "CQC care homes"),
    ("K23", "CQC non-residential"),
    ("B25", "Number of locations:"),
    ("K25", "Number of locations:"),
    ("C27", "Filled posts"),
    ("L27", "Filled posts"),
    ("C28", "Skills for Care"),
    ("D28", "Capacity Tracker"),
    ("L28", "Skills for Care"),
    ("M28", "Capacity Tracker"),
)

FILLED_POSTS = SheetMap(
    name="Filled posts",
    input_ranges=(
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange("B24:B36", note="annual filled posts dates", number_format="date"),
        InputRange(
            "I24:I36", note="annual filled posts % change dates", number_format="date"
        ),
        InputRange(
            "C24:G36",
            note="annual filled posts by primary service type",
            number_format="count",
        ),
        InputRange(
            "J25:N36", note="annual filled posts % change", number_format="percent"
        ),
        InputRange("B44:B56", note="annual CQC locations dates", number_format="date"),
        InputRange(
            "I44:I56", note="annual CQC locations % change dates", number_format="date"
        ),
        InputRange(
            "C44:G56",
            note="annual CQC locations by primary service type",
            number_format="count",
        ),
        InputRange(
            "J45:N56", note="annual CQC locations % change", number_format="percent"
        ),
        InputRange("B65:B77", note="monthly filled posts dates", number_format="date"),
        InputRange(
            "I65:I77", note="monthly filled posts % change dates", number_format="date"
        ),
        InputRange(
            "C65:G77",
            note="monthly filled posts by primary service type",
            number_format="count",
        ),
        InputRange(
            "J65:N77", note="monthly filled posts % change", number_format="percent"
        ),
        InputRange("B84:B96", note="monthly CQC locations dates", number_format="date"),
        InputRange(
            "I84:I96", note="monthly CQC locations % change dates", number_format="date"
        ),
        InputRange(
            "C84:G96",
            note="monthly CQC locations by primary service type",
            number_format="count",
        ),
        InputRange(
            "J84:N96", note="monthly CQC locations % change", number_format="percent"
        ),
        InputRange("B99", note="'...since <month year>' chart heading"),
    ),
    chart_count=3,
    series_name_cells=("B61", "B80"),
    slot_count=MONTHLY_SLOTS,
    labels=(
        ("B7", "Filled posts"),
        ("B20", "Annual filled posts estimates"),
        ("B40", "Annual CQC locations"),
        ("B61", "Monthly filled posts estimates"),
        ("B80", "Monthly CQC locations"),
        *_service_type_labels(23, "C"),
        *_service_type_labels(23, "J"),
        *_service_type_labels(43, "C"),
        *_service_type_labels(43, "J"),
        *_service_type_labels(64, "C"),
        *_service_type_labels(64, "J"),
        *_service_type_labels(83, "C"),
        *_service_type_labels(83, "J"),
    ),
)

# Annual rows first, then monthly rows; unused slots stay blank at the end.
COMP_PREV_PUBLICATION = SheetMap(
    name="Comp prev publication",
    input_ranges=(
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange("B23:B59", note="previous publication labels"),
        InputRange("C23:C59", note="previous publication dates", number_format="date"),
        InputRange(
            "D23:H59", note="previous publication figures", number_format="count"
        ),
        InputRange("B64:B100", note="current publication labels"),
        InputRange("C64:C100", note="current publication dates", number_format="date"),
        InputRange(
            "D64:H100", note="current publication figures", number_format="count"
        ),
        InputRange("B106:B142", note="difference labels"),
        InputRange("C106:C142", note="difference dates", number_format="date"),
        InputRange(
            "D106:H142", note="difference as percentage", number_format="percent"
        ),
    ),
    chart_count=0,
    labels=(
        ("B7", "Comparison of previous publications"),
        ("B20", "Previous publication"),
        ("B61", "Current publication"),
        ("B103", "Difference as percentage"),
        *_service_type_labels(22, "D"),
        *_service_type_labels(63, "D"),
        *_service_type_labels(105, "D"),
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
            "care homes number of locations",
            "count",
        ),
        InputRange(
            non_residential_cell,
            source_column,
            "non-res number of locations",
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
    extra_labels: tuple[tuple[str, str], ...] = (),
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
        extra_labels (tuple[tuple[str, str], ...]): sheet-specific fixed labels.

    Returns:
        SheetMap: the sheet's cell map.
    """
    ranges = [
        InputRange(TITLE_CELL, note="'End of <month year>' title"),
        InputRange(HEADING_CELL, note="heading incl. 'Since <month year>'"),
        InputRange(f"E{first_row - 2}", note="care homes % change heading"),
        InputRange(f"N{first_row - 2}", note="non-res % change heading"),
        InputRange(
            f"B{first_row}:B{last_row}", note="care homes dates", number_format="date"
        ),
        InputRange(
            f"K{first_row}:K{last_row}",
            note="non-residential dates",
            number_format="date",
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
            number_format="percent",
        ),
        InputRange(
            f"N{first_row}:O{last_row}",
            note="non-res % change, writer-derived",
            number_format="percent",
        ),
    ]
    if has_criteria_note:
        ranges.append(
            InputRange(
                CRITERIA_NOTE_CELL, note="'...from end of <month year> onwards' note"
            )
        )
    header_row = first_row - 1
    return SheetMap(
        name,
        tuple(ranges) + extra_inputs,
        chart_count=4,
        series_name_cells=(
            f"C{header_row}:F{header_row}",
            f"L{header_row}:O{header_row}",
        ),
        slot_count=last_row - first_row + 1,
        labels=_COMPARISON_LABELS + extra_labels,
    )


COMP_SHORT_TERM = _comparison_sheet(
    "Comp SfC and CT - short term",
    29,
    29 + MONTHLY_SLOTS - 1,
    Pub.filtered_filled_posts_short,
    Pub.filtered_ct_total_employed_short,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_locationid_count_short
    ),
)
COMP_MEDIUM_TERM = _comparison_sheet(
    "Comp SfC and CT - medium term",
    29,
    29 + MEDIUM_TERM_SLOTS - 1,
    Pub.filtered_filled_posts_medium,
    Pub.filtered_ct_total_employed_medium,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_locationid_count_medium
    ),
)
COMP_LONG_TERM = _comparison_sheet(
    "Comp SfC and CT - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.filtered_filled_posts_long,
    Pub.filtered_ct_total_employed_long,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_locationid_count_long
    ),
)

# D25/M25 here are the fixed label "Varies month to month"; location counts are
# per period in G/P instead.
NO_FILTERS = _comparison_sheet(
    "No filters - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.filled_posts,
    Pub.unfiltered_ct_total_employed_long,
    has_criteria_note=False,
    extra_labels=(("G27", "Number of locations"), ("P27", "Number of locations")),
    extra_inputs=(
        InputRange(
            f"G29:G{29 + LONG_TERM_SLOTS - 1}",
            Pub.locationid_count,
            "care homes number of locations by period",
            "count",
        ),
        InputRange(
            f"P29:P{29 + LONG_TERM_SLOTS - 1}",
            Pub.locationid_count,
            "non-res number of locations by period",
            "count",
        ),
    ),
)

ASSESSMENT_SHEETS: tuple[SheetMap, ...] = (
    FILLED_POSTS,
    COMP_PREV_PUBLICATION,
    COMP_SHORT_TERM,
    COMP_MEDIUM_TERM,
    COMP_LONG_TERM,
    NO_FILTERS,
)
