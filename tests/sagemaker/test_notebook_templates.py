import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
SKILL_DIR = REPO_ROOT / ".claude" / "skills" / "sagemaker"
TEMPLATES_DIR = REPO_ROOT / "notebooks" / "sagemaker"

EXPECTED_TEMPLATES = [
    "compare_outputs",
    "compare_before_after_filter",
    "interpolation_durations",
    "sagemaker_setup",
]
TEMPLATE_CASES = [pytest.param(name, id=name) for name in EXPECTED_TEMPLATES]

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
