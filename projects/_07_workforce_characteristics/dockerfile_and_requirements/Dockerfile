FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/utils /app/projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/utils

# Copy python jobs to WORKDIR
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_00_prepare.py /app/_01_starters_leavers_vacancies/fargate/_00_prepare.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_01_merge.py /app/_01_starters_leavers_vacancies/fargate/_01_merge.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_02_clean.py /app/_01_starters_leavers_vacancies/fargate/_02_clean.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_03_impute.py /app/_01_starters_leavers_vacancies/fargate/_03_impute.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_04_estimate.py /app/_01_starters_leavers_vacancies/fargate/_04_estimate.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_00_prepare.py /app/_01_starters_leavers_vacancies/fargate/validate_00_prepare.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_01_merge.py /app/_01_starters_leavers_vacancies/fargate/validate_01_merge.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_02_clean.py /app/_01_starters_leavers_vacancies/fargate/validate_02_clean.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_03_impute.py /app/_01_starters_leavers_vacancies/fargate/validate_03_impute.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_04_estimate.py /app/_01_starters_leavers_vacancies/fargate/validate_04_estimate.py

# TEMPORARY - ticket 1814 validate_00_prepare OOM isolation diagnostics.
# Delete these 4 lines, the diag_0N_*.py scripts, fargate/utils/diag_helpers.py
# and step-functions/dynamic/Ind-CQC-SLV-1814-oom-diag.json once the OOM root
# cause has been isolated - none of this is meant to reach main.
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/diag_01_read_parquet_only.py /app/_01_starters_leavers_vacancies/fargate/diag_01_read_parquet_only.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/diag_02_rows_distinct_only.py /app/_01_starters_leavers_vacancies/fargate/diag_02_rows_distinct_only.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/diag_03_cheap_checks_only.py /app/_01_starters_leavers_vacancies/fargate/diag_03_cheap_checks_only.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/diag_04_full_instrumented.py /app/_01_starters_leavers_vacancies/fargate/diag_04_full_instrumented.py

COPY projects/_07_workforce_characteristics/dockerfile_and_requirements/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
