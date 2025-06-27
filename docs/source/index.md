% Skills for Care Data Engineering documentation master file, created by
% sphinx-quickstart on Thu Feb 15 12:56:16 2024.
% You can adapt this file completely to your liking, but it should at least
% contain the root `toctree` directive.

# Welcome to Skills for Care Data Engineering's documentation!

```{warning}
This library is under heavy development
```

To open this library locally, run `sphinx-autobuild docs/source/ docs/build/` at the command line.

```{toctree}
:caption: 'Starting points:'
:maxdepth: 1

currentReadme
ind_cqc_pipelines
```

```{toctree}
:caption: 'Pipelines:'
:maxdepth: 1
:glob:
data_ingestion_pipelines/*
ind_cqc_pipelines/*
```

```{toctree}
:caption: 'Jobs:'
:maxdepth: 1
:glob:

ind_cqc_jobs/*
```
