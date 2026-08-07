FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies. Installed before any source is copied so that a source-only
# change doesn't invalidate the pip layer.
COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared libraries
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY utils/value_labels/ascwds_worker /app/utils/value_labels/ascwds_worker/
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

ENTRYPOINT ["python"]
