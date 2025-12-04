import unittest

from projects._03_independent_cqc._04_model.utils import (
    validate_model_definitions as job,
)


class ValidateModelDefinitionsTests(unittest.TestCase):
    def setUp(self):
        self.model_registry = {
            "model_1": {
                "version": "7.0.0",
                "model_type": "linear_regression",
                "dependent": "dependent_col",
            },
            "model_2": {
                "version": "7.0.0",
                "model_type": "linear_regression",
                "features": ["a", "b"],
            },
        }

    def test_valid_model_definition(self):
        """Test that validation passes when all required keys exist."""
        required = ["version", "dependent"]
        # Should not raise
        job.validate_model_definition("model_1", required, self.model_registry)

    def test_missing_model_name(self):
        """Test that ValueError is raised when model_id is not present."""

        with self.assertRaises(ValueError) as cm:
            job.validate_model_definition(
                "unknown_model", ["dependent"], self.model_registry
            )

        self.assertIn("unknown_model not found", str(cm.exception))

    def test_one_missing_key(self):
        """Test that a missing required key triggers an error."""

        required = ["dependent"]

        with self.assertRaises(ValueError) as cm:
            job.validate_model_definition("model_2", required, self.model_registry)

        msg = str(cm.exception)

        self.assertEqual("model_2 is missing required keys: dependent", msg)

    def test_multiple_missing_keys(self):
        """Test that multiple missing keys are reported."""

        required = ["missing_key", "another_missing_key"]

        with self.assertRaises(ValueError) as cm:
            job.validate_model_definition("model_1", required, self.model_registry)

        msg = str(cm.exception)

        self.assertEqual(
            "model_1 is missing required keys: missing_key, another_missing_key", msg
        )

    def test_mix_of_present_and_missing_keys(self):
        """Test that multiple missing keys are reported."""

        required = ["dependent", "features"]

        with self.assertRaises(ValueError) as cm:
            job.validate_model_definition("model_1", required, self.model_registry)

        msg = str(cm.exception)

        self.assertEqual("model_1 is missing required keys: features", msg)
