from datetime import date

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.impute_utils as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

RATE = SLVCols.ascwds_turnover_rate_imputed_trendline
ROLLING_AVG = SLVCols.ascwds_turnover_rate_rolling_avg
ROLE = IndCQC.published_job_role_label
REGION = IndCQC.current_region
JAN = date(2024, 1, 1)


def build_lf(rows: list[dict]) -> pl.LazyFrame:
    """Each row needs a rate and employees; other columns default to a single group."""
    defaults = {
        IndCQC.cqc_location_import_date: JAN,
        ROLE: "role",
        REGION: "region",
    }
    rows = [{**defaults, **row} for row in rows]
    return pl.LazyFrame(
        {
            IndCQC.location_id: [str(i) for i in range(len(rows))],
            **{key: [row[key] for row in rows] for key in defaults},
            RATE: [row[RATE] for row in rows],
            EmpStatus.estimated_employees: [
                row[EmpStatus.estimated_employees] for row in rows
            ],
        },
        schema_overrides={
            RATE: pl.Float32,
            EmpStatus.estimated_employees: pl.Float32,
            ROLE: pl.String,
            REGION: pl.String,
        },
    )


def workplace(rate: float | None, employees: float | None, **extra) -> dict:
    return {RATE: rate, EmpStatus.estimated_employees: employees, **extra}


def add_rolling_avg(
    lf: pl.LazyFrame,
    group_levels: list[list[str]] | None = None,
    min_contributors: int = 1,
    rolling_period: str = "3mo",
) -> pl.DataFrame:
    return job.add_rolling_avg_rate(
        lf,
        rolling_period=rolling_period,
        group_levels=group_levels or [[ROLE]],
        min_contributors=min_contributors,
    ).collect()


class TestAddRollingAvgRate:
    def test_rolling_avg_is_sum_leavers_over_sum_employees(self):
        lf = build_lf([workplace(5, 1), workplace(0.1, 100)])

        returned_df = add_rolling_avg(lf)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx(
            [15 / 101, 15 / 101], abs=1e-6
        )

    def test_rolling_avg_ignores_null_and_zero_employees(self):
        lf = build_lf(
            [
                workplace(0.1, 100),
                workplace(9, 0),
                workplace(9, None),
            ]
        )

        returned_df = add_rolling_avg(lf)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx([0.1] * 3, abs=1e-6)

    def test_rolling_avg_can_exceed_one(self):
        lf = build_lf([workplace(5, 1)])

        returned_df = add_rolling_avg(lf)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx([5.0])

    def test_rolling_avg_falls_back_to_coarser_group_below_min_contributors(self):
        lf = build_lf(
            [
                workplace(0.5, 10, **{REGION: "alone"}),
                workplace(0.1, 10, **{REGION: "pair"}),
                workplace(0.1, 10, **{REGION: "pair"}),
            ]
        )

        returned_df = add_rolling_avg(
            lf, group_levels=[[REGION, ROLE], [ROLE]], min_contributors=2
        ).sort(REGION)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx(
            [7 / 30, 0.1, 0.1], abs=1e-6
        )

    def test_rolling_avg_pools_dates_within_window(self):
        lf = build_lf(
            [
                workplace(0.1, 10),
                workplace(
                    0.3, 10, **{IndCQC.cqc_location_import_date: date(2024, 2, 1)}
                ),
            ]
        )

        returned_df = add_rolling_avg(lf).sort(IndCQC.cqc_location_import_date)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx([0.1, 0.2], abs=1e-6)

    def test_rolling_avg_excludes_dates_outside_window(self):
        lf = build_lf(
            [
                workplace(0.1, 10),
                workplace(
                    0.3, 10, **{IndCQC.cqc_location_import_date: date(2024, 6, 1)}
                ),
            ]
        )

        returned_df = add_rolling_avg(lf).sort(IndCQC.cqc_location_import_date)

        assert returned_df[ROLLING_AVG].to_list() == pytest.approx([0.1, 0.3], abs=1e-6)

    def test_rolling_avg_is_null_when_group_has_no_employees(self):
        lf = build_lf([workplace(0.1, 0)])

        returned_df = add_rolling_avg(lf)

        assert returned_df[ROLLING_AVG].to_list() == [None]

    def test_rolling_avg_keeps_row_count_and_adds_float32_column(self):
        lf = build_lf([workplace(0.1, 10), workplace(0.2, 20), workplace(None, 5)])

        returned_lf = job.add_rolling_avg_rate(
            lf, rolling_period="3mo", group_levels=[[ROLE]], min_contributors=1
        )

        schema = returned_lf.collect_schema()
        assert schema.names() == [*lf.collect_schema().names(), ROLLING_AVG]
        assert schema[ROLLING_AVG] == pl.Float32
        assert returned_lf.collect().height == 3


class TestAddImputedRate:
    KNOWN = SLVCols.ascwds_turnover_rate
    IMPUTED = SLVCols.ascwds_turnover_rate_imputed

    def impute(
        self,
        rows: list[tuple[date, float | None, float | None]],
        method: str = "ratio",
    ):
        lf = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001"] * len(rows),
                ROLE: ["role"] * len(rows),
                IndCQC.cqc_location_import_date: [row[0] for row in rows],
                self.KNOWN: [row[1] for row in rows],
                ROLLING_AVG: [row[2] for row in rows],
            },
            schema_overrides={self.KNOWN: pl.Float32, ROLLING_AVG: pl.Float32},
        )
        return (
            job.add_imputed_rate(lf, extrapolation_method=method)
            .collect()
            .sort(IndCQC.cqc_location_import_date)[self.IMPUTED]
            .to_list()
        )

    def test_imputed_keeps_known_rate(self):
        returned = self.impute([(JAN, 0.2, 0.1), (date(2024, 2, 1), 0.4, 0.1)])

        assert returned == pytest.approx([0.2, 0.4])

    def test_imputed_extrapolates_by_ratio_of_rolling_avg(self):
        returned = self.impute(
            [
                (JAN, 0.2, 0.1),
                (date(2024, 2, 1), None, 0.15),
                (date(2024, 3, 1), None, 0.05),
            ]
        )

        assert returned == pytest.approx([0.2, 0.3, 0.1], abs=1e-6)

    def test_imputed_extrapolates_by_difference_of_rolling_avg_for_nominal(self):
        returned = self.impute(
            [(JAN, 0.2, 0.1), (date(2024, 2, 1), None, 0.15)], method="nominal"
        )

        assert returned == pytest.approx([0.2, 0.25], abs=1e-6)

    def test_imputed_is_floored_at_zero(self):
        returned = self.impute(
            [(JAN, 0.1, 0.3), (date(2024, 2, 1), None, 0.1)], method="nominal"
        )

        assert returned == pytest.approx([0.1, 0.0], abs=1e-6)

    def test_imputed_extrapolates_backwards_by_ratio_of_rolling_avg(self):
        returned = self.impute([(JAN, None, 0.05), (date(2024, 2, 1), 0.2, 0.1)])

        assert returned == pytest.approx([0.1, 0.2], abs=1e-6)

    def test_imputed_is_null_when_location_has_no_known_rate(self):
        returned = self.impute([(JAN, None, 0.1), (date(2024, 2, 1), None, 0.1)])

        assert returned == [None, None]

    def test_imputed_is_null_when_ratio_is_undefined(self):
        returned = self.impute([(JAN, 0.0, 0.0), (date(2024, 2, 1), None, 0.1)])

        assert returned == [0.0, None]
