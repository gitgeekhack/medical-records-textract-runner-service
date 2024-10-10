import asyncio
import json
import os.path
import sys
import time
import uuid
import traceback

from kubernetes import client, config

from app.common.cloudwatch_helper import get_cloudwatch_logger
from app.common.sqs_helper import SQSHelper
from app.constant import AWS, MedicalInsights
from app.common.s3_utils import S3Utils
from app.common.utils import get_page_count
from app.service.helper.pdf_splitter import split_pdf

# config.load_kube_config()   # Uncomment this line while testing in local
config.load_incluster_config()


class TextractRunner:
    def __init__(self):
        self.s3_utils = S3Utils()
        self.START_TEXTRACT_QUEUE_URL = os.getenv('START_TEXTRACT_QUEUE_URL')
        self.NAMESPACE = os.getenv('ENVIRONMENT')
        self.TEXTRACT_IMAGE_NAME = os.getenv('TEXTRACT_IMAGE_NAME')
        self.logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
        self.sqs_helper = SQSHelper()

    async def create_job(self, message_body):
        batch_v1 = client.BatchV1Api()

        job_manifest = json.load(open('app/textract/job_manifest.json', 'r'))

        job_name = f"textract-job-{uuid.uuid1()}"
        job_manifest['metadata']['name'] = job_name
        job_manifest['spec']['template']['spec']['containers'][0]['image'] = self.TEXTRACT_IMAGE_NAME

        for env_variable in job_manifest['spec']['template']['spec']['containers'][0]['env']:
            if env_variable['name'] == 'INPUT_MESSAGE':
                env_variable['value'] = json.dumps(message_body)
            elif env_variable['name'] == 'LLM_OUTPUT_QUEUE_URL':
                env_variable['value'] = os.getenv('LLM_OUTPUT_QUEUE_URL')
            elif env_variable['name'] == 'SNS_TOPIC_ARN':
                env_variable['value'] = os.getenv('SNS_TOPIC_ARN')
            elif env_variable['name'] == 'ROLE_ARN':
                env_variable['value'] = os.getenv('ROLE_ARN')

        batch_v1.create_namespaced_job(namespace=self.NAMESPACE, body=job_manifest)
        self.logger.info(f'Job {job_name} created in namespace: {self.NAMESPACE}')

    async def process_split_pdf(self, document_path, document_size, document_pages):
        """Process and split large PDFs if they exceed the size or page limits."""
        start_time = time.time()
        self.logger.info("PDF Splitting is started...")

        pdf_splitter = await split_pdf(document_path, document_size, document_pages)
        self.logger.info(f"PDF Splitting is completed in {time.time() - start_time} seconds.")

        for file_path in pdf_splitter:
            self.logger = get_cloudwatch_logger(project_id=file_path.split('/')[2],
                                                document_name=os.path.basename(file_path),
                                                log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
            message_body = {"document_path": file_path}
            self.logger.info(f"Create Job with Split PDF: {message_body}")
            await self.create_job(message_body)

    async def process_single_pdf(self, document_path, message_body):
        """Process single PDFs without splitting."""
        self.logger = get_cloudwatch_logger(project_id=document_path.split('/')[2],
                                            document_name=os.path.basename(document_path),
                                            log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)

        self.logger.info(f"Create Job without Split PDF: {message_body}")
        await self.create_job(message_body)

    async def process_message(self, message_body, receipt_handle):
        document_path = message_body.get('document_path', '')
        document_size = await self.s3_utils.get_file_size(AWS.S3.S3_BUCKET, document_path)
        document_pages = await get_page_count(document_path)
        try:
            if document_size >= MedicalInsights.MAX_SIZE_MB or document_pages >= MedicalInsights.MAX_PAGE_LIMIT:
                await self.process_split_pdf(document_path, document_size, document_pages)
            else:
                await self.process_single_pdf(document_path, message_body)
        except Exception as e:
            self.logger.error('%s -> %s' % (e, traceback.format_exc()))
        finally:
            if receipt_handle:
                await self.sqs_helper.delete_message(self.START_TEXTRACT_QUEUE_URL, receipt_handle)
            sys.stdout.flush()

    async def runner(self):
        try:
            if not self.NAMESPACE:
                self.logger.info('Configuration incomplete. Please configure ENVIRONMENT variable.')
                exit(0)
            if not self.TEXTRACT_IMAGE_NAME:
                self.logger.info('Configuration incomplete. Please configure TEXTRACT_IMAGE_NAME variable.')
                exit(0)
            if not self.START_TEXTRACT_QUEUE_URL:
                self.logger.info('Configuration incomplete. Please configure START_TEXTRACT_QUEUE_URL variable.')
                exit(0)

            self.logger.info(f'Reading messages from queue: {self.START_TEXTRACT_QUEUE_URL.split("/")[-1]}')
            while True:
                message_body, receipt_handle = await self.sqs_helper.consume_message(self.START_TEXTRACT_QUEUE_URL)
                if not (message_body and receipt_handle):
                    time.sleep(10)
                    continue

                self.logger.info(f'Message received from queue: {self.START_TEXTRACT_QUEUE_URL.split("/")[-1]}')
                self.logger.info(f"Message body from Queue: {message_body}")
                await self.process_message(message_body, receipt_handle)

        except Exception as e:
            self.logger.error('%s -> %s' % (e, traceback.format_exc()))


if __name__ == '__main__':
    textract_runner = TextractRunner()
    asyncio.run(textract_runner.runner())
