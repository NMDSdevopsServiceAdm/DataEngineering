# Welcome to Skills for Care Data Engineering's documentation!

```{warning}
This library is under heavy development
```

To open this library locally, run `sphinx-autobuild docs/source/ docs/build/` at the command line.

```{toctree}
:caption: 'Starting points:'
:maxdepth: 1

usage
currentReadme
```

```{toctree}
:caption: 'Pipelines:'
:maxdepth: 1
:glob:
_01_data_ingestion_pipelines/*
_03_ind_cqc_pipelines/*
```

```{toctree}
:caption: 'Jobs:'
:maxdepth: 1
:glob:

_03_ind_cqc_jobs/*
```
