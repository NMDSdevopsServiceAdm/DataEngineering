import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
import projects._98_hackathon.fargate.validate_01_worker_turnover as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_names.hackathon_columns import HackathonOutputColumns as OutCols
from utils.column_values.ascwds_labelled_vocab import (
    TRAIN_FLAG,
    CountryOfBirthGroupLabels,
)
from utils.column_values.ascwds_labelled_vocab import NationalityGroupLabels
from utils.column_values.categorical_column_values import (
    CurrentCSSR,
    PrimaryServiceType,
    Region,
)

PATCH_PATH = "projects._98_hackathon.fargate.validate_01_worker_turnover"

GROUP_RECODES = {
    OutCols.ethnicity_group: wUtils.GROUP_LABEL_RECODES[
        HackCols.ethnicity_group_labels
    ],
    OutCols.source_of_recruitment_group: wUtils.GROUP_LABEL_RECODES[
        HackCols.scerec_wo_group_labels
    ],
}


def build_source_df() -> pl.DataFrame:
    """Two snapshots of a worker 12 months apart. The later snapshot has no
    snapshot 12 months after it, so its turnover rate is null."""
    return pl.DataFrame(
        data={
            OutCols.establishmentid: ["100", "100"],
            OutCols.workerid: ["1", "1"],
            OutCols.snapshot_date: [date(2023, 1, 1), date(2024, 1, 1)],
            OutCols.job_role: ["Care worker"] * 2,
            OutCols.orgid: ["1-ORG"] * 2,
            OutCols.service_type: [PrimaryServiceType.non_residential] * 2,
            OutCols.is_leaver: [False, None],
            OutCols.turnover_rate: [0.5, None],
            OutCols.hours_worked: [30.0, None],
            OutCols.hourly_pay_minus_nlw: [1.0, None],
            OutCols.nationality_group: [NationalityGroupLabels.non_eea] * 2,
            OutCols.country_of_birth_group: [CountryOfBirthGroupLabels.non_eea] * 2,
            OutCols.local_authority: [CurrentCSSR.barnet] * 2,
            OutCols.region: [Region.london] * 2,
            OutCols.years_in_role: [3.0, 4.0],
            OutCols.years_in_sector: [5.0, 6.0],
            **{column: [1.0] * 2 for column in job.vUtils.NUMERIC_COLUMNS},
            OutCols.training: [
                TRAIN_FLAG.label_cls(OutCols.training).categorical_values[0]
            ]
            * 2,
            **{
                label_column: [vocabulary.label_cls(label_column).categorical_values[0]]
                * 2
                for label_column, vocabulary in job.vUtils.LABEL_VOCABULARIES.items()
            },
            **{
                group_column: [sorted(set(mapping.values()))[0]] * 2
                for group_column, (_, mapping) in GROUP_RECODES.items()
            },
        },
        schema={
            OutCols.establishmentid: CatColType.EstablishmentCatType,
            OutCols.workerid: pl.String,
            OutCols.snapshot_date: pl.Date,
            OutCols.job_role: pl.Categorical,
            OutCols.orgid: pl.Categorical,
            OutCols.service_type: CatColType.PrimaryServiceEnumType,
            OutCols.is_leaver: pl.Boolean,
            OutCols.turnover_rate: pl.Float32,
            OutCols.hours_worked: pl.Float32,
            OutCols.hourly_pay_minus_nlw: pl.Float32,
            OutCols.nationality_group: pl.Categorical,
            OutCols.country_of_birth_group: pl.Categorical,
            OutCols.local_authority: pl.Categorical,
            OutCols.region: pl.Categorical,
            OutCols.years_in_role: pl.Float32,
            OutCols.years_in_sector: pl.Float32,
            **{column: pl.Float32 for column in job.vUtils.NUMERIC_COLUMNS},
            OutCols.training: pl.Categorical,
            **{column: pl.Categorical for column in job.vUtils.LABEL_VOCABULARIES},
            **{column: pl.Categorical for column in GROUP_RECODES},
        },
    )


def get_report(mock_write_reports: Mock) -> list[dict]:
    validation_arg = mock_write_reports.call_args[0][0]
    return json.loads(validation_arg.get_json_report())


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.return_value = build_source_df()

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/", selected_columns=job.SOURCE_COLUMNS
        )
        mock_write_reports.assert_called_once()

        assertion_types_present = {
            item["assertion_type"] for item in get_report(mock_write_reports)
        }

        expected_assertions = {
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
            "col_vals_ge",
            "col_vals_gt",
            "col_vals_in_set",
            "col_vals_expr",
            "specially",
        }
        assert assertion_types_present == expected_assertions

    def test_source_columns_are_all_output_columns(self):
        assert set(job.SOURCE_COLUMNS) <= set(wUtils.OUTPUT_COLUMN_SOURCES)
