"""Cell map for `templates/assessment_template.xlsx`.

Lists the cells the workbook writer fills (`InputRange`) and the charts and
number formats the template tests check. The template has no formulas.

The "Number of locations" cells take the latest row per group, since every row
in a term's period shares the same count.
"""

from dataclasses import dataclass

from openpyxl.utils import column_index_from_string, get_column_letter

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.template_paths import (
    TEMPLATES_DIR,
)
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    AssessmentHeadings,
    AssessmentMainService,
    PublishedMainService,
)

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

PERIOD_COL = Pub.period_label_short


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
        key (str): identifier the writer uses to find the range, unique on its
            sheet; empty for ranges the writer does not look up.
    """

    cells: str
    source_column: str | None = None
    note: str = ""
    number_format: str = "general"
    key: str = ""


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
TITLE_NOTE = "'End of <month year> (run N)' title"
HEADING_CELL = "B7"
# Notes are cells, not text boxes: openpyxl drops drawing shapes on save.
CRITERIA_NOTE_CELL = "B14"
FILLED_POSTS_CHART_HEADING_CELL = "B99"

# End-of-March baseline plus the 12 months of the financial year.
MONTHLY_SLOTS = 13
# Medium term: baseline plus 24 months, spanning two financial years.
MEDIUM_TERM_SLOTS = 25
# Long term is quarterly from the first import date with data, plus 3 spare
# years. Writer dates are the import date minus one month.
LONG_TERM_SLOTS = 34

_SERVICE_TYPE_HEADERS = (
    PublishedMainService.all_locations,
    AssessmentMainService.care_homes,
    AssessmentMainService.care_home_with_nursing,
    AssessmentMainService.care_only_home,
    PublishedMainService.non_residential,
)


def _service_type_labels(row: int, first_col: str) -> tuple[tuple[str, str], ...]:
    """Service type header labels across a row, starting at `first_col`."""
    return tuple(
        (f"{get_column_letter(column_index_from_string(first_col) + i)}{row}", text)
        for i, text in enumerate(_SERVICE_TYPE_HEADERS)
    )


_COMPARISON_LABELS = (
    ("B23", AssessmentMainService.care_homes),
    ("K23", PublishedMainService.non_residential),
    ("B25", AssessmentHeadings.number_of_locations_label),
    ("K25", AssessmentHeadings.number_of_locations_label),
    ("C27", AssessmentHeadings.filled_posts),
    ("L27", AssessmentHeadings.filled_posts),
    ("C28", AssessmentHeadings.skills_for_care),
    ("D28", AssessmentHeadings.capacity_tracker),
    ("E27", AssessmentHeadings.cumulative_perc_change),
    ("N27", AssessmentHeadings.cumulative_perc_change),
    ("L28", AssessmentHeadings.skills_for_care),
    ("M28", AssessmentHeadings.capacity_tracker),
)

FILLED_POSTS = SheetMap(
    name="Filled posts",
    input_ranges=(
        InputRange(TITLE_CELL, Pub.period_label_long, note=TITLE_NOTE),
        InputRange(
            "B24:B36",
            PERIOD_COL,
            note="annual filled posts dates",
            key="annual_filled_posts.dates",
            number_format="date",
        ),
        InputRange(
            "I24:I36",
            PERIOD_COL,
            note="annual filled posts % change dates",
            key="annual_filled_posts.change_dates",
            number_format="date",
        ),
        InputRange(
            "C24:G36",
            Pub.unfiltered_filled_posts,
            note="annual filled posts by primary service type",
            key="annual_filled_posts.values",
            number_format="count",
        ),
        InputRange(
            "J25:N36",
            Pub.unfiltered_filled_posts_april_pct_change,
            note="annual filled posts % change",
            key="annual_filled_posts.change",
            number_format="percent",
        ),
        InputRange(
            "B44:B56",
            PERIOD_COL,
            note="annual CQC locations dates",
            key="annual_locations.dates",
            number_format="date",
        ),
        InputRange(
            "I44:I56",
            PERIOD_COL,
            note="annual CQC locations % change dates",
            key="annual_locations.change_dates",
            number_format="date",
        ),
        InputRange(
            "C44:G56",
            Pub.unfiltered_locationid_count,
            note="annual CQC locations by primary service type",
            key="annual_locations.values",
            number_format="count",
        ),
        InputRange(
            "J45:N56",
            Pub.unfiltered_locationid_count_april_pct_change,
            note="annual CQC locations % change",
            key="annual_locations.change",
            number_format="percent",
        ),
        InputRange(
            "B65:B77",
            PERIOD_COL,
            note="monthly filled posts dates",
            key="monthly_filled_posts.dates",
            number_format="date",
        ),
        InputRange(
            "I65:I77",
            PERIOD_COL,
            note="monthly filled posts % change dates",
            key="monthly_filled_posts.change_dates",
            number_format="date",
        ),
        InputRange(
            "C65:G77",
            Pub.unfiltered_filled_posts,
            note="monthly filled posts by primary service type",
            key="monthly_filled_posts.values",
            number_format="count",
        ),
        InputRange(
            "J65:N77",
            Pub.unfiltered_short_sfc_cum_pct_change,
            note="monthly filled posts % change",
            key="monthly_filled_posts.change",
            number_format="percent",
        ),
        InputRange(
            "B84:B96",
            PERIOD_COL,
            note="monthly CQC locations dates",
            key="monthly_locations.dates",
            number_format="date",
        ),
        InputRange(
            "I84:I96",
            PERIOD_COL,
            note="monthly CQC locations % change dates",
            key="monthly_locations.change_dates",
            number_format="date",
        ),
        InputRange(
            "C84:G96",
            Pub.unfiltered_locationid_count,
            note="monthly CQC locations by primary service type",
            key="monthly_locations.values",
            number_format="count",
        ),
        InputRange(
            "J84:N96",
            Pub.unfiltered_locationid_count_cum_pct_change_short,
            note="monthly CQC locations % change",
            key="monthly_locations.change",
            number_format="percent",
        ),
        InputRange(
            FILLED_POSTS_CHART_HEADING_CELL,
            PERIOD_COL,
            note="'...since <month year>' chart heading",
        ),
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
        ("J63", "Cumulative % change"),
        ("J82", "Cumulative % change"),
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
# Ranges of the Annual/Monthly labels: previous publication, current publication,
# difference.
COMP_PREV_LABEL_RANGES = ("B23:B59", "B64:B100", "B106:B142")
COMP_PREV_PUBLICATION = SheetMap(
    name="Comp prev publication",
    input_ranges=(
        InputRange(TITLE_CELL, Pub.period_label_long, note=TITLE_NOTE),
        InputRange(COMP_PREV_LABEL_RANGES[0], note="previous publication labels"),
        InputRange(
            "C23:C59",
            PERIOD_COL,
            note="previous publication dates",
            key="previous.dates",
            number_format="date",
        ),
        InputRange(
            "D23:H59",
            Pub.unfiltered_filled_posts_prev_run,
            note="previous publication figures",
            key="previous.values",
            number_format="count",
        ),
        InputRange(COMP_PREV_LABEL_RANGES[1], note="current publication labels"),
        InputRange(
            "C64:C100",
            PERIOD_COL,
            note="current publication dates",
            key="current.dates",
            number_format="date",
        ),
        InputRange(
            "D64:H100",
            Pub.unfiltered_filled_posts,
            note="current publication figures",
            key="current.values",
            number_format="count",
        ),
        InputRange(COMP_PREV_LABEL_RANGES[2], note="difference labels"),
        InputRange(
            "C106:C142",
            PERIOD_COL,
            note="difference dates",
            key="difference.dates",
            number_format="date",
        ),
        InputRange(
            "D106:H142",
            Pub.unfiltered_filled_posts_pct_change_vs_prev_run,
            note="difference as percentage",
            key="difference.values",
            number_format="percent",
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
            key="care_homes.locations",
        ),
        InputRange(
            non_residential_cell,
            source_column,
            "non-res number of locations",
            "count",
            key="non_res.locations",
        ),
    )


def _comparison_sheet(
    name: str,
    first_row: int,
    last_row: int,
    filled_posts: str,
    ct_employed: str | None,
    filled_posts_perc_change: str,
    ct_perc_change: str,
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
        filled_posts_perc_change (str): clean dataset column for the Skills for Care
            filled posts % change.
        ct_perc_change (str): clean dataset column for the Capacity Tracker
            % change.
        extra_inputs (tuple[InputRange, ...]): sheet-specific input ranges, such
            as the number of locations.
        has_criteria_note (bool): whether the notes carry a dated criteria line.
        extra_labels (tuple[tuple[str, str], ...]): sheet-specific fixed labels.

    Returns:
        SheetMap: the sheet's cell map.
    """
    ranges = [
        InputRange(TITLE_CELL, Pub.period_label_long, note=TITLE_NOTE),
        InputRange(
            HEADING_CELL,
            Pub.period_label_long,
            note="heading incl. 'Since <month year>'",
        ),
        InputRange(
            f"B{first_row}:B{last_row}",
            PERIOD_COL,
            "care homes dates",
            "date",
            key="care_homes.dates",
        ),
        InputRange(
            f"K{first_row}:K{last_row}",
            PERIOD_COL,
            "non-residential dates",
            "date",
            key="non_res.dates",
        ),
        InputRange(
            f"C{first_row}:C{last_row}",
            filled_posts,
            "care homes SfC",
            "count",
            key="care_homes.filled_posts",
        ),
        InputRange(
            f"D{first_row}:D{last_row}",
            ct_employed,
            "care homes CT",
            "count",
            key="care_homes.ct",
        ),
        InputRange(
            f"L{first_row}:L{last_row}",
            filled_posts,
            "non-res SfC",
            "count",
            key="non_res.filled_posts",
        ),
        InputRange(
            f"M{first_row}:M{last_row}",
            ct_employed,
            "non-res CT",
            "count",
            key="non_res.ct",
        ),
        InputRange(
            f"E{first_row}:E{last_row}",
            filled_posts_perc_change,
            "care homes SfC % change",
            "percent",
            key="care_homes.filled_posts_change",
        ),
        InputRange(
            f"F{first_row}:F{last_row}",
            ct_perc_change,
            "care homes CT % change",
            "percent",
            key="care_homes.ct_change",
        ),
        InputRange(
            f"N{first_row}:N{last_row}",
            filled_posts_perc_change,
            "non-res SfC % change",
            "percent",
            key="non_res.filled_posts_change",
        ),
        InputRange(
            f"O{first_row}:O{last_row}",
            ct_perc_change,
            "non-res CT % change",
            "percent",
            key="non_res.ct_change",
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
    Pub.filtered_short_sfc_filled_posts,
    Pub.filtered_short_ct_total_employed,
    Pub.filtered_short_sfc_cum_pct_change,
    Pub.filtered_short_ct_cum_pct_change,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_short_locationid_count
    ),
)
COMP_MEDIUM_TERM = _comparison_sheet(
    "Comp SfC and CT - medium term",
    29,
    29 + MEDIUM_TERM_SLOTS - 1,
    Pub.filtered_medium_sfc_filled_posts,
    Pub.filtered_medium_ct_total_employed,
    Pub.filtered_medium_sfc_cum_pct_change,
    Pub.filtered_medium_ct_cum_pct_change,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_medium_locationid_count
    ),
)
COMP_LONG_TERM = _comparison_sheet(
    "Comp SfC and CT - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.filtered_long_sfc_filled_posts,
    Pub.filtered_long_ct_total_employed,
    Pub.filtered_long_sfc_cum_pct_change,
    Pub.filtered_long_ct_cum_pct_change,
    extra_inputs=_location_count_inputs(
        "D25", "M25", Pub.filtered_long_locationid_count
    ),
)

# D25/M25 here are the fixed label "Varies month to month"; location counts are
# per period in G/P instead.
NO_FILTERS = _comparison_sheet(
    "No filters - long term",
    29,
    29 + LONG_TERM_SLOTS - 1,
    Pub.unfiltered_filled_posts,
    Pub.unfiltered_long_ct_total_employed,
    Pub.unfiltered_long_sfc_cum_pct_change,
    Pub.unfiltered_long_ct_cum_pct_change,
    has_criteria_note=False,
    extra_labels=(("G27", "Number of locations"), ("P27", "Number of locations")),
    extra_inputs=(
        InputRange(
            f"G29:G{29 + LONG_TERM_SLOTS - 1}",
            Pub.unfiltered_locationid_count,
            "care homes number of locations by period",
            "count",
            key="care_homes.locations_by_period",
        ),
        InputRange(
            f"P29:P{29 + LONG_TERM_SLOTS - 1}",
            Pub.unfiltered_locationid_count,
            "non-res number of locations by period",
            "count",
            key="non_res.locations_by_period",
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
