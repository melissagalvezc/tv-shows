FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_tv_shows_data.py ${LAMBDA_TASK_ROOT}

CMD [ "sync_tv_shows_data.lambda_handler" ]

