from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._04_direct_payment_recipients.fargate._01_merge as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH: str = "projects._04_direct_payment_recipients.fargate._01_merge"

EXTERNAL_SCHEMA = {
    DP.la_area: pl.String,
    DP.year: pl.String,
    DP.dprs_adass: pl.Float32,
    DP.dprs_employing_staff_adass: pl.Float32,
    DP.service_user_dprs_at_year_end: pl.Float32,
    DP.carer_dprs_at_year_end: pl.Float32,
    DP.service_user_dprs_during_year: pl.Float32,
    DP.proportion_imported: pl.Float32,
    DP.historic_service_users_employing_staff_estimate: pl.Float32,
    DP.filled_posts_per_employer: pl.Float32,
}
SURVEY_SCHEMA = {DP.year: pl.Int32, DP.total_staff_recoded: pl.Float32}

MERGED_COLUMNS = [
    DP.year_as_integer,
    DP.la_area,
    DP.year,
    DP.service_user_dprs_during_year,
    DP.proportion_employing_staff,
    DP.historic_service_users_employing_staff_estimate,
    DP.total_dprs_during_year,
    DP.filled_posts_per_employer,
]


def external_row(la="Leeds", year="2021", su_during=100.0, filled_posts=9.9) -> tuple:
    return (la, year, 150.0, 75.0, 100.0, 50.0, su_during, None, None, filled_posts)


def run_main(external_rows: list[tuple], survey_rows=((2022, 2.0),)) -> pl.DataFrame:
    """Runs main with mocked scan and sink, returning the collected sunk output."""
    external_lf = pl.LazyFrame(external_rows, schema=EXTERNAL_SCHEMA, orient="row")
    survey_lf = pl.LazyFrame(list(survey_rows), schema=SURVEY_SCHEMA, orient="row")

    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet", side_effect=[survey_lf, external_lf]),
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
    ):
        job.main("survey/source", "external/source", "destination")

    return sink_mock.call_args.args[0].collect()


@pytest.mark.parametrize(
    "la, year, su_during, expected",
    [
        ("Hackney", "2022", None, 580.5),
        ("Hackney", "2023", None, 629.2),
        ("Hackney", "2022", 10.0, 10.0),
        ("Hackney", "2021", None, None),
        ("Leeds", "2022", None, None),
    ],
)
def test_hackney_2022_and_2023_service_user_dprs_filled_when_null(
    la, year, su_during, expected
):
    returned_df = run_main([external_row(la=la, year=year, su_during=su_during)])

    assert returned_df[DP.service_user_dprs_during_year].to_list() == [
        pytest.approx(expected)
    ]


def test_total_dprs_equals_filled_service_user_dprs():
    returned_df = run_main(
        [external_row("Hackney", "2022", None), external_row("Leeds", "2022", 50.0)]
    )

    assert returned_df[DP.total_dprs_during_year].to_list() == [580.5, 50.0]


def test_survey_ratio_is_left_joined_on_year_and_renamed():
    # Survey year 2022 gives a ratio for external year 2021; 2020 has no ratio
    # and the imported filled posts per employer is discarded.
    returned_df = run_main(
        [external_row(year="2021"), external_row(year="2020")],
        survey_rows=((2022, 2.0),),
    )

    ratios = dict(
        zip(
            returned_df[DP.year_as_integer].to_list(),
            returned_df[DP.filled_posts_per_employer].to_list(),
        )
    )
    assert ratios[2021] == pytest.approx(2.0)
    assert ratios[2020] is None
    assert DP.ratio_rolling_average not in returned_df.columns


def test_main_output_columns_match_merged_dataset_contract():
    returned_df = run_main([external_row()])

    assert returned_df.columns == MERGED_COLUMNS


def test_main_runs_full_pipeline():
    returned_df = run_main(
        [external_row("Hackney", "2022", None), external_row("Leeds", "2021", 80.0)]
    )

    assert returned_df.height == 2
    assert returned_df[DP.proportion_employing_staff].null_count() < 2


def test_main_scans_both_sources_and_sinks_once():
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_mock,
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
    ):
        scan_mock.side_effect = [
            pl.LazyFrame(schema=SURVEY_SCHEMA),
            pl.LazyFrame(schema=EXTERNAL_SCHEMA),
        ]
        job.main("survey/source", "external/source", "destination")

    assert scan_mock.call_count == 2
    sink_mock.assert_called_once()
    assert sink_mock.call_args.args[1] == "destination"


def test_output_float_columns_are_float32():
    returned_df = run_main([external_row("Hackney", "2022", None)])

    float_dtypes = {dtype for dtype in returned_df.schema.values() if dtype.is_float()}
    assert float_dtypes == {pl.Float32}
