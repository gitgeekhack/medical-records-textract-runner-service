import os


class AWS:
    class S3:
        S3_BUCKET = 'ds-medical-insights-extractor'

    class BotoClient:
        AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')

    class CloudWatch:
        LOG_GROUP = os.getenv('LOG_GROUP', 'ds-mrs-logs')
        TEXTRACT_RUNNER_STREAM = os.getenv('TEXTRACT_RUNNER_STREAM', 'textract-runner-service')
        LLM_RUNNER_STREAM = os.getenv('LLM_RUNNER_STREAM', 'llm-runner-service')


class MedicalInsights:
    MAX_PAGE_LIMIT = 3000
    MAX_SIZE_MB = 500
    TEXTRACT_FOLDER_NAME = "textract_response"
    SPLIT_DOCUMENT_JSON_FOLDER = "split_documents_json"
    LOCAL_JSON_PATH = 'static/json_files/'
    STATIC_FOLDER_PATH = 'static/'
    SPLIT_FILES_LOCAL_PATH = 'static/split_files'
    JSON_PATH_REPLACE_OLD = '/request/split_documents/'
    JSON_PATH_REPLACE_NEW = '/textract_response/split_documents_json/'


class ExceptionMessage:
    TEXTRACT_FAILED_MESSAGE = 'Text extraction using Textract Async failed'
