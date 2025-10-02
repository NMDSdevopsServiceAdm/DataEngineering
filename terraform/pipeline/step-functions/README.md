# StepFunctions Terraform Guide

This document explains how to add new create StepFunctions dynamically `sf_pipelines` resource in [step-function.tf](../step-function.tf).

## Adding a New Step Function

1. **Create a Definition File**
    - Add a new JSON definition in the [dynamic](./dynamic/) folder.
    - The file name should match the desired name of the StepFunction.
    - If the StepFunction requires parameters, add them to the `aws_sfn_state_machine.sf_pipelines` _templatefile_ definition in [step-function.tf](../step-function.tf).

## Handling Dependencies

- **Dependent Step Functions**
  - If the StepFunction depends on another, do **not** add it to the `dynamic` folder.
  - Instead, add the JSON desifintion to this [step-functions](./) folder.
  - Then, explicitly add its resource block in [step-function.tf](../step-function.tf) and add paramaeters as needed (see `run_crawler` as an example).

## Example Structure

```
step-functions/
├── dynamic/
│   ├── my-step-function.json
│   └── another-step-function.yaml
├── dependent-step-function.json
step-function.tf
```

