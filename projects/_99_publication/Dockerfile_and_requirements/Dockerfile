FROM python:3.11-slim
WORKDIR /app
RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared libraries
COPY polars_utils /app/polars_utils
COPY utils/file_utils.py /app/utils/file_utils.py
COPY utils/column_names /app/utils/column_names/
COPY projects/_99_publication/monthly_tracker_filled_posts/fargate/utils /app/projects/_99_publication/monthly_tracker_filled_posts/fargate/utils

# Copy python jobs to WORKDIR
COPY projects/_99_publication/monthly_tracker_filled_posts/fargate/_01_merge.py /app/_01_merge.py
COPY projects/_99_publication/monthly_tracker_filled_posts/fargate/_02_clean.py /app/_02_clean.py

ENTRYPOINT ["python"]
