import re
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
DOCKER_REQUIREMENTS = REPO_ROOT / "docker_requirements" / "requirements.txt"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def normalise_name(name: str) -> str:
    """
    Normalises a package name so spelling variants compare equal (PEP 503).

    Args:
        name (str): A package name, e.g. "css_inline".

    Returns:
        str: The normalised name, e.g. "css-inline".
    """
    return re.sub(r"[-_.]+", "-", name).lower()


def get_pins(requirement_lines: list[str]) -> dict[str, str]:
    """
    Extracts the `name==version` pins from a list of requirement lines.

    Args:
        requirement_lines (list[str]): Requirement specifiers, one per entry.
            Comments and anything other than an exact `==` pin are ignored.

    Returns:
        dict[str, str]: Normalised package name mapped to its pinned version.
    """
    pins = {}
    for line in requirement_lines:
        pin_match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*([^\s;#]+)", line)
        if pin_match:
            pins[normalise_name(pin_match.group(1))] = pin_match.group(2)
    return pins


DOCKER_PINS = get_pins(DOCKER_REQUIREMENTS.read_text().splitlines())
PYPROJECT_PINS = get_pins(
    tomllib.loads(PYPROJECT.read_text())["project"]["dependencies"]
)

assert DOCKER_PINS, f"No '==' pins found in {DOCKER_REQUIREMENTS}"

PUBLICATION_EXTRAS = (
    REPO_ROOT
    / "projects"
    / "_99_publication"
    / "Dockerfile_and_requirements"
    / "requirements-extra.txt"
)
PUBLICATION_EXTRAS_PINS = (
    get_pins(PUBLICATION_EXTRAS.read_text().splitlines())
    if PUBLICATION_EXTRAS.exists()
    else {}
)
EXCEL_PACKAGES = ["gptables", "xlsxwriter", "pandas", "numpy", "openpyxl", "pyarrow"]

PYPROJECT_ALL_PINS = {
    **get_pins(tomllib.loads(PYPROJECT.read_text())["dependency-groups"]["dev"]),
    **PYPROJECT_PINS,
}
DOCKERFILES = sorted(REPO_ROOT.glob("projects/**/Dockerfile"))
EXTRAS_FILES = sorted(REPO_ROOT.glob("projects/**/requirements-extra.txt"))
EXTRAS_FILE_CASES = [
    pytest.param(extras_file, id=extras_file.parent.parent.name)
    for extras_file in EXTRAS_FILES
]
EXTRAS_PIN_CASES = [
    pytest.param(
        extras_file, package, version, id=f"{extras_file.parent.parent.name}-{package}"
    )
    for extras_file in EXTRAS_FILES
    for package, version in sorted(
        get_pins(extras_file.read_text().splitlines()).items()
    )
]


class TestDockerRequirementsPins:
    """
    Guards against the Fargate images installing different versions from the ones
    uv.lock (and so CI) tests.

    The images install docker_requirements/requirements.txt with plain pip, so only
    what is pinned there is fixed. An unpinned great-tables once floated to 1.0.0,
    which HTML-escapes pointblank's report icons, while CI stayed green on 0.23.0.
    """

    @pytest.mark.parametrize("package,version", sorted(DOCKER_PINS.items()))
    def test_docker_pin_matches_pyproject_pin(self, package, version):
        pyproject_version = PYPROJECT_PINS.get(package)

        assert pyproject_version == version, (
            f"{package}=={version} is pinned in {DOCKER_REQUIREMENTS.name} but "
            f"pyproject.toml pins {pyproject_version}. Keep the two in step so the "
            "Docker images and uv.lock resolve the same version."
        )


class TestExtrasPins:
    """
    Guards each project's requirements-extra.txt against drifting from the
    versions uv.lock (and so CI) tests, as the shared requirements are guarded.
    """

    @pytest.mark.parametrize("extras_file,package,version", EXTRAS_PIN_CASES)
    def test_extras_pin_matches_pyproject_pin(self, extras_file, package, version):
        pyproject_version = PYPROJECT_ALL_PINS.get(package)

        assert pyproject_version == version, (
            f"{package}=={version} is pinned in {extras_file} but pyproject.toml "
            f"pins {pyproject_version}. Keep the two in step so the Docker image "
            "and uv.lock resolve the same version."
        )


class TestExtrasFiles:
    """
    Guards the shape of each requirements-extra.txt.

    get_pins() ignores anything that isn't an exact `==` pin, so a loose line
    would otherwise escape the pin checks and float to the latest version.
    """

    @pytest.mark.parametrize("extras_file", EXTRAS_FILE_CASES)
    def test_extras_requirement_is_exact_pin(self, extras_file):
        requirement_lines = [
            line
            for line in extras_file.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        loose_lines = [line for line in requirement_lines if not get_pins([line])]

        assert not loose_lines, (
            f"{extras_file} has requirements that are not exact `==` pins: "
            f"{loose_lines}. Pin them so the Docker image can't float."
        )

    @pytest.mark.parametrize("extras_file", EXTRAS_FILE_CASES)
    def test_dockerfile_installs_extras_file(self, extras_file):
        extras_path = extras_file.relative_to(REPO_ROOT).as_posix()
        copy_line = re.compile(rf"^\s*COPY\s+{re.escape(extras_path)}\s+\.\s*$", re.M)
        copying_dockerfiles = [
            dockerfile
            for dockerfile in DOCKERFILES
            if copy_line.search(dockerfile.read_text())
        ]

        assert len(copying_dockerfiles) == 1, (
            f"Expected exactly one Dockerfile to `COPY {extras_path} .`, found "
            f"{len(copying_dockerfiles)}."
        )
        assert re.search(
            r"pip install[^\n]*-r requirements-extra\.txt",
            copying_dockerfiles[0].read_text(),
        ), (
            f"{copying_dockerfiles[0]} copies {extras_file.name} but never runs "
            "`pip install -r requirements-extra.txt`, so its packages are missing "
            "from the image."
        )


class TestPublicationExtrasPins:
    """
    Guards the Excel packages the publication image needs but the shared
    requirements don't provide.

    pyarrow is included because Polars' `to_pandas()` (used to hand tables to
    gptables) needs it, and pytest can't see that the slim image lacks it.
    """

    @pytest.mark.parametrize("package", EXCEL_PACKAGES)
    def test_excel_package_is_pinned(self, package):
        assert package in PUBLICATION_EXTRAS_PINS, (
            f"{package} is not pinned in {PUBLICATION_EXTRAS.name} for the "
            "publication image, so the Excel jobs would fail at import in the "
            "container despite passing locally."
        )
