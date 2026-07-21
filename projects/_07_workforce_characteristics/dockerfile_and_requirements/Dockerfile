FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/utils /app/projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/utils

# Copy python jobs to WORKDIR
COPY projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/*.py /app/_01_starters_leavers_vacancies/fargate/*.py

COPY projects/_07_workforce_characteristics/dockerfile_and_requirements/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
