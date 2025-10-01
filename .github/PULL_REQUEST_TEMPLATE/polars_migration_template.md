## Description
Trello ticket [#number](add link)

[State which jobs/functions are being migrated to polars]

## Testing
- [ ] Unit tests passing
- [ ] Successful [Job / Step Function run](add link)
- [ ] Outputs checked in Athena

[Add any additional testing information here - add screenshot if relevant]

## Migration to polars checklist
- [ ] Check that similar utils functions don't already exist, or if one can be created
- [ ] Check that utils are being saved in the appropriate place
- [ ] Used LazyFrame option for test data
- [ ] Unit tests added
- [ ] Docstrings added
- [ ] Added comment to pyspark functions which have been migrated with the link to the new location
      # converted to polars -> filepath.py
- [ ] Updated CHANGELOG
- [ ] Moved Trello ticket to PR column
