from datetime import date

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.bakeoff_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType

INPUT_SCHEMA = {
    IndCQC.location_id: pl.String,
    IndCQC.main_job_role_clean_labelled: pl.String,
    IndCQC.cqc_location_import_date: pl.Date,
    job.BakeoffCols.ratio: pl.Float32,
}


def build_variants(rows: list[tuple]) -> pl.DataFrame:
    """Run the location level steps over a small hand-built ratio series."""
    lf = pl.LazyFrame(rows, schema=INPUT_SCHEMA, orient="row")
    lf = job.add_fill_boundaries(lf)
    lf = job.add_capped_interpolation(lf, job.INTERPOLATION_CAP_DAYS)
    lf = job.add_variant_ratios(lf)
    return lf.sort(
        IndCQC.location_id,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.cqc_location_import_date,
    ).collect()


def values_for(df: pl.DataFrame, variant_name: str) -> list:
    """Pull one variant's filled ratio column out, rounded past Float32 noise."""
    variant = next(v for v in job.VARIANTS if v.name == variant_name)
    values = df.get_column(job.measure_col(variant, job.BakeoffCols.ratio)).to_list()
    return [None if value is None else round(value, 4) for value in values]


class TestCappedInterpolation:
    def test_interpolates_across_a_gap_within_the_cap(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2024, 1, 1), 0.4),
                ("1", "care_worker", date(2024, 2, 1), None),
                ("1", "care_worker", date(2024, 3, 1), 0.6),
            ]
        )
        # 2024-02-01 sits 31 days into a 60 day gap, so by date rather than by row position.
        assert values_for(df, "none") == [0.4, round(0.4 + 0.2 * 31 / 60, 4), 0.6]

    def test_leaves_a_gap_beyond_the_cap_null(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2020, 1, 1), 0.4),
                ("1", "care_worker", date(2022, 1, 1), None),
                ("1", "care_worker", date(2024, 1, 1), 0.6),
            ]
        )
        assert values_for(df, "none") == [0.4, None, 0.6]

    def test_edge_fill_does_not_reach_into_an_interior_gap(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2020, 1, 1), 0.4),
                ("1", "care_worker", date(2022, 1, 1), None),
                ("1", "care_worker", date(2024, 1, 1), 0.6),
            ]
        )
        assert values_for(df, "indefinite") == [0.4, None, 0.6]


class TestEdgeFillLimit:
    def test_fills_on_the_limit_day_and_not_the_day_after(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2024, 1, 1), 0.5),
                ("1", "care_worker", date(2024, 7, 2), None),  # 183 days later
                ("1", "care_worker", date(2024, 7, 3), None),  # 184 days later
            ]
        )
        assert values_for(df, "fill_6m") == [0.5, 0.5, None]

    def test_backward_fill_uses_the_same_limit(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2023, 7, 2), None),  # 183 days before
                ("1", "care_worker", date(2023, 7, 1), None),  # 184 days before
                ("1", "care_worker", date(2024, 1, 1), 0.5),
            ]
        )
        assert values_for(df, "fill_6m") == [None, 0.5, 0.5]

    def test_none_variant_leaves_both_edges_null(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2023, 12, 1), None),
                ("1", "care_worker", date(2024, 1, 1), 0.5),
                ("1", "care_worker", date(2024, 2, 1), None),
            ]
        )
        assert values_for(df, "none") == [None, 0.5, None]

    def test_indefinite_variant_fills_both_edges(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2014, 1, 1), None),
                ("1", "care_worker", date(2024, 1, 1), 0.5),
                ("1", "care_worker", date(2034, 1, 1), None),
            ]
        )
        assert values_for(df, "indefinite") == [0.5, 0.5, 0.5]


class TestEdgeFillAcrossDateAxisChange:
    def test_six_months_reaches_two_quarterly_rows(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2015, 1, 1), 0.5),
                ("1", "care_worker", date(2015, 4, 1), None),  # 90 days
                ("1", "care_worker", date(2015, 7, 1), None),  # 181 days
                ("1", "care_worker", date(2015, 10, 1), None),  # 273 days
            ]
        )
        assert values_for(df, "fill_6m") == [0.5, 0.5, 0.5, None]

    def test_six_months_reaches_six_monthly_rows(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2024, 1, 1), 0.5),
                ("1", "care_worker", date(2024, 2, 1), None),
                ("1", "care_worker", date(2024, 3, 1), None),
                ("1", "care_worker", date(2024, 4, 1), None),
                ("1", "care_worker", date(2024, 5, 1), None),
                ("1", "care_worker", date(2024, 6, 1), None),
                ("1", "care_worker", date(2024, 7, 1), None),  # 182 days
                ("1", "care_worker", date(2024, 8, 1), None),  # 213 days
            ]
        )
        assert values_for(df, "fill_6m") == [0.5] * 7 + [None]


class TestBaseVariant:
    def test_fills_every_row_regardless_of_gap_length(self):
        df = build_variants(
            [
                ("1", "care_worker", date(2013, 1, 1), None),
                ("1", "care_worker", date(2015, 1, 1), 0.4),
                ("1", "care_worker", date(2020, 1, 1), None),
                ("1", "care_worker", date(2024, 1, 1), 0.6),
                ("1", "care_worker", date(2026, 1, 1), None),
            ]
        )
        assert None not in values_for(df, "base")


class TestAllRolesOrNoneInvariant:
    def test_roles_are_populated_and_null_together(self):
        rows = []
        for role, first, second in (
            ("care_worker", 0.6, 0.8),
            ("supervisor", 0.4, 0.2),
        ):
            rows.extend(
                [
                    ("1", role, date(2023, 12, 1), None),
                    ("1", role, date(2024, 1, 1), first),
                    ("1", role, date(2024, 2, 1), None),
                    ("1", role, date(2024, 3, 1), second),
                    ("1", role, date(2029, 1, 1), None),
                ]
            )
        df = build_variants(rows)

        for variant in job.VARIANTS:
            column = job.measure_col(variant, job.BakeoffCols.ratio)
            populated = df.select(
                pl.col(column).is_not_null().alias("populated"),
                pl.col(IndCQC.main_job_role_clean_labelled),
                pl.col(IndCQC.cqc_location_import_date),
            )
            per_date = populated.group_by(IndCQC.cqc_location_import_date).agg(
                pl.col("populated").n_unique()
            )
            assert per_date.get_column("populated").to_list() == [
                1,
                1,
                1,
                1,
                1,
            ], f"{variant.name} populated one role but not the other"

    def test_populated_rows_still_sum_to_one_across_roles(self):
        rows = []
        for role, first, second in (
            ("care_worker", 0.6, 0.8),
            ("supervisor", 0.4, 0.2),
        ):
            rows.extend(
                [
                    ("1", role, date(2023, 12, 1), None),
                    ("1", role, date(2024, 1, 1), first),
                    ("1", role, date(2024, 2, 1), None),
                    ("1", role, date(2024, 3, 1), second),
                ]
            )
        df = build_variants(rows)

        column = job.measure_col(
            next(v for v in job.VARIANTS if v.name == "fill_6m"),
            job.BakeoffCols.ratio,
        )
        totals = (
            df.group_by(IndCQC.cqc_location_import_date)
            .agg(pl.col(column).sum())
            .get_column(column)
            .to_list()
        )
        assert [round(total, 4) for total in totals] == [1.0, 1.0, 1.0, 1.0]


SOURCE_SCHEMA = {
    IndCQC.location_id: pl.String,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.primary_service_type: pl.String,
    IndCQC.main_job_role_clean_labelled: pl.String,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}


def build_source(locations: tuple) -> pl.LazyFrame:
    """Build a source frame from (location, posts, count) tuples over two months."""
    rows = [
        (loc, d, posts, PrimaryServiceType.non_residential, role, count)
        for loc, posts, count in locations
        for d in (date(2024, 1, 1), date(2024, 2, 1))
        for role in ("care_worker", "supervisor")
    ]
    return pl.LazyFrame(rows, schema=SOURCE_SCHEMA, orient="row")


class TestEmptyStratum:
    def test_rolling_ratio_is_null_not_nan(self):
        # Location 2 is in a different size group and never submits, so its stratum has
        # nothing in it and would otherwise divide zero by zero.
        lf = build_source((("1", 120.0, 5), ("2", 30.0, None)))
        pre_aggregate = (
            job.build_pre_aggregate(job.prepare_variants(lf)).collect().lazy()
        )
        result = job.build_bakeoff(
            pre_aggregate,
            [IndCQC.primary_service_type, IndCQC.estimate_filled_posts_size_group],
        ).collect()

        empty = result.filter(
            pl.col(IndCQC.estimate_filled_posts_size_group) == "NR 25 to 49"
        ).get_column(job.BakeoffCols.rolling_ratio)
        assert empty.len() > 0
        assert empty.is_null().all()


class TestProvenanceCounts:
    def test_categories_account_for_every_row(self):
        # Location 2 never submits, which is the case that falls outside both the interior
        # and the edge counts and so has to be counted separately.
        lf = build_source((("1", 120.0, 5), ("2", 130.0, None)))
        pre_aggregate = job.build_pre_aggregate(job.prepare_variants(lf)).collect()

        for variant in job.VARIANTS:
            categories = [
                job.BakeoffCols.known_rows,
                job.BakeoffCols.never_submitted_rows,
            ] + [
                job.measure_col(variant, measure)
                for measure in (
                    job.BakeoffCols.interpolated_rows,
                    job.BakeoffCols.filled_rows,
                    job.BakeoffCols.null_interior_rows,
                    job.BakeoffCols.null_edge_rows,
                )
            ]
            accounted = pre_aggregate.select(
                pl.sum_horizontal(categories).alias("accounted")
            ).get_column("accounted")
            # Two locations contribute one row each per stratum, role and month.
            assert (
                accounted.to_list() == [2] * pre_aggregate.height
            ), f"{variant.name} does not account for every row"
