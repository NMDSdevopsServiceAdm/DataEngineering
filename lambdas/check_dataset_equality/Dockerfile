FROM public.ecr.aws/lambda/python:3.11

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN dir -s
#WORKDIR /check_datasets_equal

COPY lambdas/check_dataset_equality/requirements.txt ${LAMBDA_TASK_ROOT}
RUN  pip install -r requirements.txt

# Copy function code
COPY utils/column_names/raw_data_files/cqc_location_api_columns.py ${LAMBDA_TASK_ROOT}
COPY lambdas/check_dataset_equality/lambda_function.py  ${LAMBDA_TASK_ROOT}
COPY utils/column_names/raw_data_files/cqc_provider_api_columns.py ${LAMBDA_TASK_ROOT}
COPY utils/column_names/raw_data_files/cqc_location_api_columns.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]