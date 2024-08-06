FROM python:3.9-slim

WORKDIR /runner-service

COPY ./requirements.txt ./requirements.txt
RUN pip install --upgrade pip==23.3 && \
    pip install -r requirements.txt

COPY ./app ./app

ENV PYTHONPATH=/runner-service
CMD ["python", "app/llm/llm_runner.py"]