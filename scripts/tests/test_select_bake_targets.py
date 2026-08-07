from dataclasses import dataclass
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

import scripts.select_bake_targets as job

# Two targets in the "all" group, one defined but excluded from it, and one
# commented out -- the selector should only ever schedule alpha and beta.
BAKE_FILE_CONTENTS = """
group "all" {
  targets = ["alpha", "beta"]
}

# group "disabled" {
#   targets = ["gamma"]
# }

target "alpha" {
  context = "."
  dockerfile = "./projects/alpha/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/alpha:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
}

target "beta" {
  context = "."
  dockerfile = "./projects/beta/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/beta:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
}

target "gamma" {
  context = "."
  dockerfile = "./projects/gamma/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/gamma:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
}
"""

# Copies a whole directory, a non-recursive glob, and a shared requirements file.
ALPHA_DOCKERFILE_CONTENTS = """
FROM python:3.11-slim

# Dependencies
COPY shared_utils shared_utils

# Copy all python jobs to WORKDIR
COPY projects/alpha/src/*.py .

COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
"""

# Copies one named file rather than a glob, so a sibling file must not trigger it.
BETA_DOCKERFILE_CONTENTS = """
FROM python:3.11-slim

COPY shared_utils shared_utils
COPY projects/beta/src/job.py /app/job.py

COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
"""

GAMMA_DOCKERFILE_CONTENTS = """
FROM python:3.11-slim

COPY projects/gamma/src/job.py /app/job.py
"""


@pytest.fixture
def fake_repo(tmp_path: Path) -> Path:
    """
    Write a miniature repo with a bake file and three Dockerfiles.

    Args:
        tmp_path (Path): pytest's per-test temporary directory.

    Returns:
        Path: Root of the fake repo.
    """
    (tmp_path / job.BAKE_FILE).write_text(BAKE_FILE_CONTENTS)
    for name, contents in [
        ("alpha", ALPHA_DOCKERFILE_CONTENTS),
        ("beta", BETA_DOCKERFILE_CONTENTS),
        ("gamma", GAMMA_DOCKERFILE_CONTENTS),
    ]:
        dockerfile = tmp_path / "projects" / name / "Dockerfile"
        dockerfile.parent.mkdir(parents=True)
        dockerfile.write_text(contents)

    return tmp_path


def _ecr_client(repositories_with_image: set[str]) -> Mock:
    """
    Build a stub ECR client that only knows about the given repositories.

    Args:
        repositories_with_image (set[str]): Repositories holding the image tag.

    Returns:
        Mock: Stand-in for a boto3 ECR client.
    """
    error = ClientError(
        {"Error": {"Code": "ImageNotFoundException", "Message": "not found"}},
        "DescribeImages",
    )

    def describe_images(repositoryName: str, imageIds: list[dict]) -> dict:
        if repositoryName not in repositories_with_image:
            raise error
        return {"images": [{"imageId": imageIds[0]}]}

    return Mock(describe_images=Mock(side_effect=describe_images))


@dataclass
class TargetSelectionCase:
    id: str
    changed_paths: list[str]
    expected_targets: list[str]

    def as_pytest_param(self):
        return pytest.param(self.changed_paths, self.expected_targets, id=self.id)


target_selection_cases = [
    TargetSelectionCase(
        id="job_source_change_selects_only_its_own_target",
        changed_paths=["projects/alpha/src/prepare.py"],
        expected_targets=["alpha"],
    ),
    TargetSelectionCase(
        id="shared_library_change_selects_every_dependent_target",
        changed_paths=["shared_utils/column_names.py"],
        expected_targets=["alpha", "beta"],
    ),
    TargetSelectionCase(
        id="shared_requirements_change_selects_every_dependent_target",
        changed_paths=["docker_requirements/requirements.txt"],
        expected_targets=["alpha", "beta"],
    ),
    TargetSelectionCase(
        id="bake_file_change_selects_every_target",
        changed_paths=[job.BAKE_FILE],
        expected_targets=["alpha", "beta"],
    ),
    TargetSelectionCase(
        id="dockerfile_change_selects_only_its_own_target",
        changed_paths=["projects/beta/Dockerfile"],
        expected_targets=["beta"],
    ),
    TargetSelectionCase(
        id="unrelated_change_selects_nothing",
        changed_paths=["README.md", "CHANGELOG.md"],
        expected_targets=[],
    ),
    # uv.lock and pyproject.toml govern the CI/test environment only -- the
    # images install from docker_requirements/requirements.txt, so neither is a
    # build input. The intuition runs the other way, hence the explicit case.
    TargetSelectionCase(
        id="ci_dependency_change_selects_nothing",
        changed_paths=["uv.lock", "pyproject.toml"],
        expected_targets=[],
    ),
    TargetSelectionCase(
        id="change_below_a_non_recursive_glob_selects_nothing",
        changed_paths=["projects/alpha/src/nested/deep.py"],
        expected_targets=[],
    ),
    TargetSelectionCase(
        id="change_beside_a_named_copy_selects_nothing",
        changed_paths=["projects/beta/src/not_copied.py"],
        expected_targets=[],
    ),
    TargetSelectionCase(
        id="change_to_a_target_outside_the_all_group_selects_nothing",
        changed_paths=["projects/gamma/src/job.py"],
        expected_targets=[],
    ),
    TargetSelectionCase(
        id="several_changes_select_the_union_of_their_targets",
        changed_paths=["projects/alpha/src/prepare.py", "projects/beta/src/job.py"],
        expected_targets=["alpha", "beta"],
    ),
]


class TestParseCopySources:
    def test_returns_every_source_in_order(self):
        sources = job.parse_copy_sources(ALPHA_DOCKERFILE_CONTENTS)

        assert sources == [
            "shared_utils",
            "projects/alpha/src/*.py",
            "docker_requirements/requirements.txt",
        ]

    def test_returns_every_source_when_a_copy_has_several(self):
        sources = job.parse_copy_sources("COPY first.py second.py /app/\n")

        assert sources == ["first.py", "second.py"]

    def test_ignores_copy_flags(self):
        sources = job.parse_copy_sources("COPY --from=builder /out /app/out\n")

        assert sources == ["/out"]

    def test_ignores_commented_out_copy_instructions(self):
        sources = job.parse_copy_sources("# COPY secrets/ /app/secrets\n")

        assert sources == []


class TestParseBakeTargets:
    def test_returns_targets_in_group_order(self):
        definitions = job.parse_bake_targets(BAKE_FILE_CONTENTS)

        assert list(definitions) == ["alpha", "beta"]

    def test_reads_dockerfile_and_repository_past_a_tag_interpolation(self):
        definitions = job.parse_bake_targets(BAKE_FILE_CONTENTS)

        assert definitions["alpha"] == ("projects/alpha/Dockerfile", "fargate/alpha")

    def test_raises_when_the_all_group_is_missing(self):
        with pytest.raises(ValueError, match='No group "all"'):
            job.parse_bake_targets('target "alpha" {\n  context = "."\n}\n')

    def test_raises_when_a_grouped_target_has_no_definition(self):
        contents = 'group "all" {\n  targets = ["alpha"]\n}\n'

        with pytest.raises(ValueError, match="alpha"):
            job.parse_bake_targets(contents)


class TestPathTriggersRebuild:
    @pytest.mark.parametrize(
        ("changed_path", "trigger_path", "expected"),
        [
            pytest.param("utils/names.py", "utils", True, id="file_inside_directory"),
            pytest.param("utils", "utils", True, id="exact_match"),
            pytest.param("utils_extra/names.py", "utils", False, id="prefix_only"),
            pytest.param("src/job.py", "src/*.py", True, id="glob_in_directory"),
            pytest.param(
                "src/nested/job.py", "src/*.py", False, id="glob_no_recursion"
            ),
            pytest.param("src/job.txt", "src/*.py", False, id="glob_wrong_extension"),
        ],
    )
    def test_returns_expected_result(
        self, changed_path: str, trigger_path: str, expected: bool
    ):
        assert job.path_triggers_rebuild(changed_path, trigger_path) is expected


class TestLoadBakeTargets:
    def test_derives_trigger_paths_from_the_dockerfile(self, fake_repo: Path):
        targets = {target.name: target for target in job.load_bake_targets(fake_repo)}

        assert targets["alpha"].trigger_paths == (
            "shared_utils",
            "projects/alpha/src/*.py",
            "docker_requirements/requirements.txt",
            "projects/alpha/Dockerfile",
            job.BAKE_FILE,
        )

    def test_excludes_targets_outside_the_all_group(self, fake_repo: Path):
        target_names = [target.name for target in job.load_bake_targets(fake_repo)]

        assert target_names == ["alpha", "beta"]

    def test_parses_this_repos_real_bake_file_and_dockerfiles(self):
        repo_root = Path(job.__file__).resolve().parent.parent

        targets = job.load_bake_targets(repo_root)

        assert targets, "no targets found in the repo's bake file"
        assert all(target.ecr_repository for target in targets)
        assert all(target.trigger_paths for target in targets)

    def test_every_real_target_depends_on_the_shared_requirements_file(self):
        # Every image installs docker_requirements/requirements.txt, so a change
        # to it must rebuild all of them. A parser that silently stopped reading
        # one Dockerfile's COPY lines would show up here as a skipped rebuild.
        repo_root = Path(job.__file__).resolve().parent.parent
        targets = job.load_bake_targets(repo_root)

        selected = job.select_targets(targets, ["docker_requirements/requirements.txt"])

        assert selected == [target.name for target in targets]


class TestTargetsMissingFromEcr:
    def test_returns_targets_without_an_image_for_this_tag(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)

        missing = job.targets_missing_from_ecr(
            targets, "my-branch", _ecr_client({"fargate/alpha"})
        )

        assert missing == {"beta"}

    def test_returns_nothing_when_every_image_exists(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)

        missing = job.targets_missing_from_ecr(
            targets, "my-branch", _ecr_client({"fargate/alpha", "fargate/beta"})
        )

        assert missing == set()

    def test_reraises_errors_other_than_a_missing_image(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)
        ecr_client = Mock(
            describe_images=Mock(
                side_effect=ClientError(
                    {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                    "DescribeImages",
                )
            )
        )

        with pytest.raises(ClientError):
            job.targets_missing_from_ecr(targets, "my-branch", ecr_client)


class TestSelectTargets:
    @pytest.mark.parametrize(
        ("changed_paths", "expected_targets"),
        [case.as_pytest_param() for case in target_selection_cases],
    )
    def test_selects_targets_whose_build_context_changed(
        self, fake_repo: Path, changed_paths: list[str], expected_targets: list[str]
    ):
        targets = job.load_bake_targets(fake_repo)

        selected = job.select_targets(targets, changed_paths)

        assert selected == expected_targets

    def test_selects_targets_missing_from_ecr_despite_no_changes(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)

        selected = job.select_targets(
            targets,
            ["README.md"],
            image_tag="new-branch",
            ecr_client=_ecr_client(set()),
        )

        assert selected == ["alpha", "beta"]

    def test_selects_the_union_of_changed_and_missing_targets(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)

        selected = job.select_targets(
            targets,
            ["projects/alpha/src/prepare.py"],
            image_tag="my-branch",
            ecr_client=_ecr_client({"fargate/alpha"}),
        )

        assert selected == ["alpha", "beta"]

    def test_returns_targets_in_bake_file_order(self, fake_repo: Path):
        targets = job.load_bake_targets(fake_repo)

        selected = job.select_targets(
            targets, ["projects/beta/src/job.py", "shared_utils/x.py"]
        )

        assert selected == ["alpha", "beta"]
