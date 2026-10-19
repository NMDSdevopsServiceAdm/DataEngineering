import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.template_paths as job


class TestTemplatesDir:
    @pytest.mark.parametrize(
        "file_name",
        [
            pytest.param("output_workbook_pages.yaml", id="page_config"),
            pytest.param("assessment_template.xlsx", id="assessment_template"),
        ],
    )
    def test_templates_dir_holds_the_workbook_templates(self, file_name):
        assert (job.TEMPLATES_DIR / file_name).is_file()
