def validate_model_definition(
    model_name: str, required_keys: list[str], model_registry: dict
) -> None:
    """
    Validate that a model definition exists and contains required keys.

    This function checks that a given model ID is present in the model registry
    and that all keys required for the current job are included in that model's
    definition. It raises a ValueError when validation fails.

    Args:
        model_name (str): The name of the model definition to validate.
        required_keys (list[str]): Keys that must be present in the model's definition.
        model_registry (dict): Dictionary of  model definitions.

    Raises:
        ValueError: If the model_name does not exist in the model registry.
                    If any of the required_keys are missing in the model definition.
    """
    # Check the model exists
    if model_name not in model_registry:
        raise ValueError(f"{model_name} not found in model registry")

    model_def = model_registry[model_name]

    # Check required keys
    missing = [key for key in required_keys if key not in model_def]

    if missing:
        raise ValueError(f"{model_name} is missing required keys: {', '.join(missing)}")
