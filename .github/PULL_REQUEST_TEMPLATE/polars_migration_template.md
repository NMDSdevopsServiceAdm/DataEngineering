## Description
Trello ticket [#number](add link)

[State which jobs/functions are being migrated to polars]

## Testing
- [ ] Unit tests passing
- [ ] Successful [Job / Step Function run](add link)
- [ ] Outputs checked in Athena
- [ ] Outputs compared using [SageMaker script](https://eu-west-2.console.aws.amazon.com/sagemaker/home?region=eu-west-2#/notebook-instances/data-engineering-notebook)
      - Notebook name: compare_outputs.ipynb
- [ ] Output Screenshots added to Trello ticket

[Add any additional testing information here - add screenshot if relevant]

## Migration to polars checklist
- [ ] Check that similar utils functions don't already exist, or if one can be created
- [ ] Check that utils are being saved in the appropriate place
- [ ] Used LazyFrame option for test data
- [ ] Unit tests added
- [ ] Docstrings added
- [ ] Added comment to pyspark functions which have been migrated with the link to the new location
      `# converted to polars -> filepath.py`
- [ ] Use new file structure when saving to S3
      `{dataset}_{sub_dataset_if_relevant}_{order_of_process_if_relevant}_{very_brief_description}`
- [ ] Updated CHANGELOG
- [ ] Code reviewed by AI
- [ ] Moved Trello ticket to PR column

## Reviewer checklist
- [ ] PR solves the Trello ticket requirements
- [ ] Outputs appear reasonable and understandable
- [ ] The overall approach is appropriate and not over-engineered
- [ ] No obvious scalability, performance or interdependency concerns
- [ ] Tests appropriately cover the main behaviour/edge cases
- [ ] Naming and structure are broadly understandable and consistent
- [ ] Docstrings are sufficient for future maintenance
- [ ] Docstrings cover non-obvious behaviour
