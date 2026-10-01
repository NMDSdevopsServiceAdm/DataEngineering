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
