FROM public.ecr.aws/lambda/python:3.11

# Install the function's dependencies using file requirements.txt
COPY lambdas/create_dataset_snapshot/requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

# Copy function code
COPY projects/tools/delta_data_remodel/jobs/utils.py ${LAMBDA_TASK_ROOT}
COPY utils/column_names/ind_cqc_pipeline_columns.py ${LAMBDA_TASK_ROOT}
COPY lambdas/create_dataset_snapshot/lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]