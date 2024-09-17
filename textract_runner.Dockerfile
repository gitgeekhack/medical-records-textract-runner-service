FROM python:3.9-slim

WORKDIR /textract-runner-service

COPY ./requirements.txt ./requirements.txt
RUN pip install --upgrade pip==24.2 && \
    pip install -r requirements.txt

COPY ./app ./app
COPY ./.env ./.env
CMD ["source", ".env"]

ENV PYTHONPATH=/textract-runner-service
CMD ["python", "app/textract/textract_runner.py"]