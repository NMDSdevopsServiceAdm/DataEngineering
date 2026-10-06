FROM python:3.11-slim
WORKDIR /app
RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies. Installed before any source is copied so that a source-only
# change doesn't invalidate the pip layer.
COPY docker_requirements/requirements.txt .
COPY projects/_99_publication/Dockerfile_and_requirements/requirements-extra.txt .
RUN pip install --no-cache-dir -r requirements.txt -r requirements-extra.txt

# Shared libraries
COPY polars_utils /app/polars_utils
COPY utils/file_utils.py /app/utils/file_utils.py
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY projects/_99_publication/monthly_tracker_filled_posts/fargate/utils /app/projects/_99_publication/monthly_tracker_filled_posts/fargate/utils
COPY projects/_99_publication/monthly_tracker_filled_posts/templates /app/projects/_99_publication/monthly_tracker_filled_posts/templates

# Copy all python jobs to WORKDIR
COPY projects/_99_publication/monthly_tracker_filled_posts/fargate/*.py .

ENTRYPOINT ["python"]
