import os


class AWS:
    class BotoClient:
        AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')

    class CloudWatch:
        LOG_GROUP = os.getenv('LOG_GROUP', 'ds-mrs-logs')
        TEXTRACT_RUNNER_STREAM = os.getenv('TEXTRACT_RUNNER_STREAM', 'textract-runner-service')
        LLM_RUNNER_STREAM = os.getenv('LLM_RUNNER_STREAM', 'llm-runner-service')
