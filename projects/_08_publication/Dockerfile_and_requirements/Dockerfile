FROM python:3.11-slim
WORKDIR /app
RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared libraries
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/

# Copy python jobs to WORKDIR
COPY projects/_08_publication/_01_job_role_estimates/fargate/_01_merge_pub_data.py /app/_01_merge_pub_data.py
COPY projects/_08_publication/_01_job_role_estimates/fargate/validate_01_merge_pub_data.py /app/validate_01_merge_pub_data.py
COPY projects/_08_publication/_01_job_role_estimates/fargate/_02_clean_pub_data.py /app/_02_clean_pub_data.py
COPY projects/_08_publication/_01_job_role_estimates/fargate/validate_02_clean_pub_data.py /app/validate_02_clean_pub_data.py

ENTRYPOINT ["python"]
