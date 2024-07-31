import os


class AWS:
    class BotoClient:
        AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')

    class CloudWatch:
        LOG_GROUP = os.getenv('LOG_GROUP', 'ds-mrs-logs')
        TEXTRACT_RUNNER_STREAM = 'textract-runner-service'

    class SQS:
        TEXTRACT_QUEUE = 'https://sqs.ap-south-1.amazonaws.com/851725323009/start-textract-async-sqs'
