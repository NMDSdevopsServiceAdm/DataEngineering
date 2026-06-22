FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/

# Copy python jobs to WORKDIR
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_01_merge.py /app/_01_starters_leavers_vacancies/fargate/_01_merge.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_02_clean.py /app/_01_starters_leavers_vacancies/fargate/_02_clean.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_01_merge.py /app/_01_starters_leavers_vacancies/fargate/validate_01_merge.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_02_clean.py /app/_01_starters_leavers_vacancies/fargate/validate_02_clean.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_03_impute.py /app/_01_starters_leavers_vacancies/fargate/_03_impute.py
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_03_impute.py /app/_01_starters_leavers_vacancies/fargate/validate_03_impute.py
COPY projects/_07_workforce_characteristics/dockerfile_and_requirements/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
