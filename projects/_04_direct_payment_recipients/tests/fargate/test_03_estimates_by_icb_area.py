from datetime import date
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate._03_estimates_by_icb_area as job
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH: str = (
    "projects._04_direct_payment_recipients.fargate._03_estimates_by_icb_area"
)

POSTCODE_SCHEMA = {
    ONSClean.contemporary_ons_import_date: pl.Date,
    ONSClean.postcode: pl.String,
    ONSClean.contemporary_cssr: pl.String,
    ONSClean.contemporary_icb: pl.String,
}
PA_SCHEMA = {
    DP.LA_AREA: pl.String,
    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: pl.Float32,
    DP.YEAR_AS_INTEGER: pl.Int64,
}

OUTPUT_SCHEMA = {
    ONSClean.contemporary_ons_import_date: pl.Date,
    ONSClean.contemporary_cssr: pl.String,
    ONSClean.contemporary_icb: pl.String,
    DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA: pl.Float32,
    DP.YEAR_AS_INTEGER: pl.Int64,
    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA: pl.Float32,
}

# cssr1 has 3 postcodes in icb1; cssr2 has 1 in icb2 and 3 in icb3.
POSTCODE_ROWS = [
    (date(2024, 1, 1), "AB10AA", "cssr1", "icb1"),
    (date(2024, 1, 1), "AB10AB", "cssr1", "icb1"),
    (date(2024, 1, 1), "AB10AC", "cssr1", "icb1"),
    (date(2024, 1, 1), "CD10AA", "cssr2", "icb2"),
    (date(2024, 1, 1), "CD10AB", "cssr2", "icb3"),
    (date(2024, 1, 1), "CD10AC", "cssr2", "icb3"),
    (date(2024, 1, 1), "CD10AD", "cssr2", "icb3"),
    (date(2023, 1, 1), "AB10AA", "cssr1", "icb1"),
    (date(2023, 1, 1), "AB10AB", "cssr1", "icb1"),
    (date(2023, 1, 1), "CD10AA", "cssr2", "icb2"),
    (date(2023, 1, 1), "CD10AB", "cssr2", "icb3"),
]


def postcode_lf(rows=POSTCODE_ROWS) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=POSTCODE_SCHEMA, orient="row")


def run_main(postcodes: pl.LazyFrame, pa_lf: pl.LazyFrame) -> pl.DataFrame:
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_mock,
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
    ):
        scan_mock.side_effect = [postcodes, pa_lf]
        job.main("postcode_source", "pa_source", "destination")
    return sink_mock.call_args.args[0].collect()


class TestCheckForDuplicatePostcodes:
    def test_raises_when_postcode_directory_has_duplicate_postcodes(self):
        rows = POSTCODE_ROWS + [POSTCODE_ROWS[0]]

        with pytest.raises(ValueError, match="duplicate"):
            job.check_for_duplicate_postcodes(postcode_lf(rows))

    def test_passes_when_same_postcode_appears_on_different_import_dates(self):
        job.check_for_duplicate_postcodes(postcode_lf())


class TestCalculateIcbProportions:
    def test_icb_proportion_is_icb_postcodes_over_la_postcodes(self):
        returned_lf = job.calculate_icb_proportions(postcode_lf())

        expected_lf = pl.LazyFrame(
            [
                (date(2024, 1, 1), "cssr1", "icb1", 1.0),
                (date(2024, 1, 1), "cssr2", "icb2", 0.25),
                (date(2024, 1, 1), "cssr2", "icb3", 0.75),
                (date(2023, 1, 1), "cssr1", "icb1", 1.0),
                (date(2023, 1, 1), "cssr2", "icb2", 0.5),
                (date(2023, 1, 1), "cssr2", "icb3", 0.5),
            ],
            schema={
                ONSClean.contemporary_ons_import_date: pl.Date,
                ONSClean.contemporary_cssr: pl.String,
                ONSClean.contemporary_icb: pl.String,
                DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA: pl.Float32,
            },
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_proportions_sum_to_one_per_la_and_date(self):
        returned_df = job.calculate_icb_proportions(postcode_lf()).collect()

        sums_df = returned_df.group_by(
            ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr
        ).agg(pl.col(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA).sum())

        assert (
            sums_df[DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA].to_list()
            == [1.0] * sums_df.height
        )


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_writes_expected_output(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        pa_lf = pl.LazyFrame(
            [
                # 2022 -> 2023-03-31, aligns back to 2023-01-01
                ("cssr1", 100.0, 2022),
                # 2023 -> 2024-03-31, aligns back to 2024-01-01
                ("cssr1", 110.0, 2023),
            ],
            schema=PA_SCHEMA,
            orient="row",
        )
        scan_parquet_mock.side_effect = [postcode_lf(), pa_lf]

        job.main("postcode_source", "pa_source", "destination")

        sink_to_parquet_mock.assert_called_once_with(ANY, "destination")
        returned_df = sink_to_parquet_mock.call_args.args[0].collect()
        expected_df = pl.DataFrame(
            [
                (date(2024, 1, 1), "cssr1", "icb1", 1.0, 2023, 110.0),
                (date(2024, 1, 1), "cssr2", "icb2", 0.25, None, None),
                (date(2024, 1, 1), "cssr2", "icb3", 0.75, None, None),
                (date(2023, 1, 1), "cssr1", "icb1", 1.0, 2022, 100.0),
                (date(2023, 1, 1), "cssr2", "icb2", 0.5, None, None),
                (date(2023, 1, 1), "cssr2", "icb3", 0.5, None, None),
            ],
            schema=OUTPUT_SCHEMA,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_df, expected_df, check_row_order=False)

    def test_estimate_date_is_march_31_of_following_year(self):
        pa_lf = pl.LazyFrame([("cssr1", 100.0, 2022)], schema=PA_SCHEMA, orient="row")
        # Dates either side of 2023-03-31: only an exact 2023-03-31 estimate date
        # aligns to the 2023-03-31 import date.
        postcodes = postcode_lf(
            [
                (date(2023, 3, 30), "AB10AA", "cssr1", "icb1"),
                (date(2023, 3, 31), "AB10AA", "cssr1", "icb1"),
                (date(2023, 4, 1), "AB10AA", "cssr1", "icb1"),
            ]
        )

        returned_df = run_main(postcodes, pa_lf)

        matched_dates = returned_df.filter(pl.col(DP.YEAR_AS_INTEGER).is_not_null())[
            ONSClean.contemporary_ons_import_date
        ].to_list()
        assert matched_dates == [date(2023, 3, 31)]

    def test_filled_posts_are_split_by_proportion_and_unmatched_dates_are_null(self):
        pa_lf = pl.LazyFrame([("cssr2", 200.0, 2023)], schema=PA_SCHEMA, orient="row")

        returned_df = run_main(postcode_lf(), pa_lf)

        expected_df = pl.DataFrame(
            [
                (date(2024, 1, 1), "cssr1", "icb1", 1.0, None, None),
                (date(2024, 1, 1), "cssr2", "icb2", 0.25, 2023, 50.0),
                (date(2024, 1, 1), "cssr2", "icb3", 0.75, 2023, 150.0),
                (date(2023, 1, 1), "cssr1", "icb1", 1.0, None, None),
                (date(2023, 1, 1), "cssr2", "icb2", 0.5, None, None),
                (date(2023, 1, 1), "cssr2", "icb3", 0.5, None, None),
            ],
            schema=OUTPUT_SCHEMA,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_df, expected_df, check_row_order=False)

    def test_output_float_columns_are_float32(self):
        pa_lf = pl.LazyFrame([("cssr2", 200.0, 2022)], schema=PA_SCHEMA, orient="row")

        returned_df = run_main(postcode_lf(), pa_lf)

        float_dtypes = {
            dtype for dtype in returned_df.schema.values() if dtype.is_float()
        }
        assert float_dtypes == {pl.Float32}
