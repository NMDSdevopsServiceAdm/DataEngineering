import dataclasses
import json
import re
from pathlib import Path

import polars as pl
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns,
    ModelEvaluationLabels,
)

REPO_ROOT = Path(__file__).parents[2]
SKILL_DIR = REPO_ROOT / ".claude" / "skills" / "sagemaker"
TEMPLATES_DIR = REPO_ROOT / "notebooks" / "sagemaker"

EXPECTED_TEMPLATES = [
    "compare_outputs",
    "compare_before_after_filter",
    "interpolation_durations",
    "model_metrics_history",
    "sagemaker_setup",
]
TEMPLATE_CASES = [
    pytest.param(name, id=name)
    for name in sorted(
        set(EXPECTED_TEMPLATES) | {path.stem for path in TEMPLATES_DIR.glob("*.ipynb")}
    )
]

EAGER_READ = re.compile(r"\bread_(parquet|csv|ndjson|json|excel)\s*\(")
AWS_IDENTIFIER = re.compile(r"\b\d{12}\b|\b(AKIA|ASIA)[0-9A-Z]{16}\b")


def template_path(name: str) -> Path:
    """Returns the path of a template notebook in the sagemaker skill."""
    return TEMPLATES_DIR / f"{name}.ipynb"


def load_cells(name: str) -> list[dict]:
    """Returns the cells of a template notebook."""
    return json.loads(template_path(name).read_text(encoding="utf-8"))["cells"]


class TestNotebookTemplates:
    """Structural checks on the sagemaker skill's committed notebook templates."""

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_file_exists(self, name):
        assert template_path(name).is_file(), f"Missing template: {template_path(name)}"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_is_valid_notebook_json(self, name):
        notebook = json.loads(template_path(name).read_text(encoding="utf-8"))

        assert notebook.get("nbformat") == 4, "Expected nbformat 4"
        assert notebook.get("cells"), "Notebook has no cells"
        for cell in notebook["cells"]:
            assert cell["cell_type"] in ("markdown", "code")
            assert "source" in cell

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_starts_with_title_and_stop_instance_reminder(self, name):
        first_cell = load_cells(name)[0]
        text = "".join(first_cell["source"])

        assert first_cell["cell_type"] == "markdown"
        assert text.startswith("# "), "First cell must open with a '# ' title"
        assert (
            "stop the notebook instance" in text.lower()
        ), "First cell must remind the user to stop the notebook instance"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_has_exactly_one_parameters_cell(self, name):
        parameters_cells = [
            cell
            for cell in load_cells(name)
            if cell["cell_type"] == "code"
            and "parameters" in cell["metadata"].get("tags", [])
        ]

        assert (
            len(parameters_cells) == 1
        ), f"Expected one code cell tagged 'parameters', found {len(parameters_cells)}"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_code_cells_are_each_preceded_by_a_markdown_cell(self, name):
        cells = load_cells(name)

        undocumented = [
            index
            for index, cell in enumerate(cells)
            if cell["cell_type"] == "code"
            and (index == 0 or cells[index - 1]["cell_type"] != "markdown")
        ]

        assert (
            not undocumented
        ), f"Code cells at index {undocumented} have no markdown cell before them"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_has_no_saved_outputs(self, name):
        saved = [
            index
            for index, cell in enumerate(load_cells(name))
            if cell["cell_type"] == "code"
            and (cell["outputs"] or cell["execution_count"] is not None)
        ]

        assert not saved, f"Code cells at index {saved} have saved outputs"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_uses_scan_not_read(self, name):
        eager_reads = [
            index
            for index, cell in enumerate(load_cells(name))
            if cell["cell_type"] == "code"
            and EAGER_READ.search("".join(cell["source"]))
        ]

        assert (
            not eager_reads
        ), f"Code cells at index {eager_reads} use read_* instead of scan_*"

    @pytest.mark.parametrize("name", TEMPLATE_CASES)
    def test_template_has_no_aws_account_ids_or_keys(self, name):
        text = "".join("".join(cell["source"]) for cell in load_cells(name))

        assert not AWS_IDENTIFIER.search(
            text
        ), "Found what looks like an AWS account id or access key"


SKILL_MD = SKILL_DIR / "SKILL.md"
TEMPLATE_LINK = re.compile(r"notebooks/sagemaker/([\w-]+)\.ipynb")


def read_frontmatter(path: Path) -> dict[str, str]:
    """Returns the top-level `key: value` pairs from a SKILL.md frontmatter block."""
    block = re.match(r"---\n(.*?)\n---\n", path.read_text(encoding="utf-8"), re.S)
    assert block, f"{path} has no frontmatter block"
    return dict(re.findall(r"^(\w+):\s*(.+)$", block.group(1), re.M))


class TestSkillMd:
    """The skill's SKILL.md must be discoverable and in step with its templates."""

    def test_name_matches_directory_and_has_description(self):
        frontmatter = read_frontmatter(SKILL_MD)

        assert frontmatter.get("name") == SKILL_DIR.name
        assert frontmatter.get("description", "").strip()

    def test_lists_every_template_and_no_others(self):
        template_files = {path.stem for path in TEMPLATES_DIR.glob("*.ipynb")}
        listed = set(TEMPLATE_LINK.findall(SKILL_MD.read_text(encoding="utf-8")))

        assert listed == template_files


def field_values(dataclass: type) -> set[str]:
    """Returns the default string value of every field of a column-name dataclass."""
    return {field.default for field in dataclasses.fields(dataclass)}


def run_cells(name: str, markers: tuple[str, ...] = (), namespace=None) -> dict:
    """Runs a template's parameters cell and the code cells containing a marker, in order."""
    namespace = {} if namespace is None else namespace
    for cell in load_cells(name):
        source = "".join(cell["source"])
        if cell["cell_type"] == "code" and (
            "parameters" in cell["metadata"].get("tags", [])
            or any(marker in source for marker in markers)
        ):
            exec(source, namespace)
    return namespace


ROW_SCORES = {
    ModelEvaluationColumns.number_of_rows,
    ModelEvaluationColumns.bias,
    ModelEvaluationColumns.weighted_absolute_percentage_error,
    IndCQC.r2,
    IndCQC.rmse,
    IndCQC.proportion_of_model_predictions_within_ten,
    IndCQC.proportion_of_model_predictions_within_twenty_five,
}
METADATA_SCALE_SCORES = {
    IndCQC.r2,
    IndCQC.rmse,
    IndCQC.proportion_of_model_predictions_within_ten,
    IndCQC.proportion_of_model_predictions_within_twenty_five,
}
EMITTED_SCORES = {
    (ModelEvaluationLabels.row_level_posts, ModelEvaluationLabels.pooled): ROW_SCORES,
    (ModelEvaluationLabels.headline_group_totals, ModelEvaluationLabels.pooled): {
        IndCQC.r2,
        ModelEvaluationColumns.weighted_absolute_percentage_error,
        ModelEvaluationColumns.number_of_groups,
    },
    (ModelEvaluationLabels.coverage, ModelEvaluationLabels.pooled): {
        ModelEvaluationColumns.known_rows_scored,
        ModelEvaluationColumns.known_rows,
        ModelEvaluationColumns.coverage,
    },
    (ModelEvaluationLabels.jumpiness, ModelEvaluationLabels.pooled): {
        ModelEvaluationColumns.mean_period_to_period_change,
        ModelEvaluationColumns.locations,
        ModelEvaluationColumns.rows,
    },
    (
        ModelEvaluationLabels.row_level_metadata_scale,
        ModelEvaluationLabels.pooled,
    ): METADATA_SCALE_SCORES,
    (ModelEvaluationLabels.fit_diagnostics, ModelEvaluationLabels.all_rows): {
        ModelEvaluationColumns.n_iter,
        ModelEvaluationColumns.max_iter,
        ModelEvaluationColumns.hit_max_iter,
        ModelEvaluationColumns.zero_coefficients,
        ModelEvaluationColumns.number_of_features,
    },
}


class TestModelMetricsHistoryMatchesTraining:
    """The notebook can't import the repo, so check its names against training output."""

    @pytest.fixture
    def parameters(self) -> dict:
        return run_cells("model_metrics_history")

    def test_key_metrics_are_scores_training_writes(self, parameters):
        unknown = [
            name
            for name, level, metric, fold, _ in parameters["KEY_METRICS"]
            if metric not in EMITTED_SCORES.get((level, fold), set())
        ]

        assert not unknown, f"Tracked scores training does not write: {unknown}"

    def test_metric_lists_only_name_tracked_scores(self, parameters):
        tracked = {name for name, *_ in parameters["KEY_METRICS"]}
        listed = (
            set(parameters["ABSOLUTE_VALUE_METRICS"])
            | set(parameters["ABSOLUTE_CHANGE_METRICS"])
            | set(parameters["FLAG_METRICS"])
        )

        assert not listed - tracked, f"Not in KEY_METRICS: {listed - tracked}"

    def test_metadata_metrics_are_scores_training_writes(self, parameters):
        assert set(parameters["METADATA_METRICS"]) == METADATA_SCALE_SCORES

    def test_labels_and_columns_match_training_output(self, parameters):
        expected = {
            "POOLED": ModelEvaluationLabels.pooled,
            "ALL_GROUPS": ModelEvaluationLabels.all_groups,
            "ALL_ROWS": ModelEvaluationLabels.all_rows,
            "MODEL": ModelEvaluationColumns.model,
            "LEVEL": ModelEvaluationColumns.level,
            "GROUP": ModelEvaluationColumns.group,
            "FOLD": ModelEvaluationColumns.fold,
            "METRIC": ModelEvaluationColumns.metric,
            "VALUE": ModelEvaluationColumns.value,
            "R2": IndCQC.r2,
            "RMSE": IndCQC.rmse,
            "WAPE": ModelEvaluationColumns.weighted_absolute_percentage_error,
        }

        assert {key: parameters[key] for key in expected} == expected


CHECK_COLUMNS = ["model", "version", "run_number", "name", "higher_is_better", "value"]


def run_checks(name: str, higher_is_better: bool, values: list[float | None]) -> list:
    """Runs the notebook's change checks on one score's values, oldest run first."""
    cv_metrics_df = pl.DataFrame(
        [
            ("m", "1.0.0", run, name, higher_is_better, v)
            for run, v in enumerate(values)
        ],
        schema=CHECK_COLUMNS,
        orient="row",
    )
    namespace = run_cells(
        "model_metrics_history",
        ("checks_df =",),
        {"pl": pl, "cv_metrics_df": cv_metrics_df},
    )
    return namespace["checks_df"].sort("run_number").to_dicts()


class TestModelMetricsHistoryChecks:
    """The notebook's change checks flag a worse score, and only that."""

    @pytest.mark.parametrize(
        "higher_is_better, values",
        [
            pytest.param(True, [0.8, 0.7], id="drop_in_higher_is_better_score"),
            pytest.param(False, [10.0, 12.0], id="rise_in_lower_is_better_score"),
        ],
    )
    def test_flags_a_step_in_the_wrong_direction(self, higher_is_better, values):
        last = run_checks("score", higher_is_better, values)[-1]

        assert last["step_breach"] is True
        assert last["step_adverse"] > 0

    @pytest.mark.parametrize(
        "higher_is_better, values",
        [
            pytest.param(True, [0.7, 0.8], id="rise_in_higher_is_better_score"),
            pytest.param(False, [12.0, 10.0], id="drop_in_lower_is_better_score"),
            pytest.param(True, [0.8, 0.79], id="change_within_tolerance"),
        ],
    )
    def test_does_not_flag_other_steps(self, higher_is_better, values):
        last = run_checks("score", higher_is_better, values)[-1]

        assert last["step_breach"] is False

    def test_first_run_has_no_step_check(self):
        first = run_checks("score", True, [0.8, 0.7])[0]

        assert first["step_adverse"] is None
        assert not first["step_breach"]

    def test_does_not_flag_a_change_from_zero(self):
        last = run_checks("score", False, [0.0, 0.0])[-1]

        assert last["step_adverse"] is None
        assert not last["step_breach"]

    def test_absolute_change_scores_use_the_absolute_tolerance(self):
        small, large = (
            run_checks("jumpiness", False, [0.05, rise])[-1] for rise in (0.056, 0.07)
        )

        assert small["step_breach"] is False
        assert large["step_breach"] is True

    def test_baseline_starts_after_enough_earlier_runs(self):
        rows = run_checks("score", True, [0.8] * 5 + [0.5])

        assert [row["drift_adverse"] is None for row in rows] == [True] * 5 + [False]
        assert rows[-1]["drift_breach"] is True

    def test_a_missing_score_leaves_the_next_step_check_empty(self):
        rows = run_checks("score", True, [0.8, None, 0.5])

        assert rows[2]["step_adverse"] is None


class TestModelMetricsHistoryTrend:
    """The notebook's text trend and version order."""

    @pytest.fixture
    def namespace(self) -> dict:
        return run_cells("model_metrics_history", ("def summarise_trend",), {"pl": pl})

    @staticmethod
    def trend(namespace: dict, values: list[float | None]) -> str:
        scores_df = pl.DataFrame(
            {
                "model": "m",
                "version": "1.0.0",
                "name": "score",
                "run_number": range(len(values)),
                "value": values,
            },
            schema_overrides={"value": pl.Float64},
        )
        return namespace["summarise_trend"](scores_df)["trend"].item()

    def test_draws_each_run_from_the_lowest_block_to_the_highest(self, namespace):
        assert self.trend(namespace, [0.0, 0.5, 1.0]) == "▁▅█"

    def test_a_score_that_never_changes_draws_the_lowest_block(self, namespace):
        assert self.trend(namespace, [0.5, 0.5]) == "▁▁"

    def test_ignores_missing_and_non_finite_scores(self, namespace):
        values = [0.0, None, float("nan"), float("inf"), 1.0]

        assert self.trend(namespace, values) == "▁█"

    def test_sorts_versions_as_numbers(self, namespace):
        versions_df = pl.DataFrame({"version": ["10.0.0", "7.0.1", "7.0.10"]})

        sorted_df = versions_df.sort(namespace["VERSION_ORDER"])

        assert sorted_df["version"].to_list() == ["7.0.1", "7.0.10", "10.0.0"]
