FROM python:3.9-slim

WORKDIR /llm-runner-service

COPY ./requirements.txt ./requirements.txt
RUN pip install --upgrade pip==24.2 && \
    pip install -r requirements.txt

COPY ./app ./app

ENV PYTHONPATH=/runner-service
CMD ["python", "app/llm/llm_runner.py"]