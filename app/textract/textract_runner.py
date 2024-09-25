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
from app.service.helper.pdf_splitter import split_pdf_by_size

s3_utils = S3Utils()
# config.load_kube_config()   # Uncomment this line while testing in local
config.load_incluster_config()
START_TEXTRACT_QUEUE_URL = os.getenv('START_TEXTRACT_QUEUE_URL')

# Get namespace from environment variable
NAMESPACE = os.getenv('ENVIRONMENT')
TEXTRACT_IMAGE_NAME = os.getenv('TEXTRACT_IMAGE_NAME')


async def create_job(message_body, logger):
    batch_v1 = client.BatchV1Api()

    job_manifest = json.load(open('app/textract/job_manifest.json', 'r'))

    job_name = f"textract-job-{uuid.uuid1()}"
    job_manifest['metadata']['name'] = job_name

    job_manifest['spec']['template']['spec']['containers'][0]['image'] = TEXTRACT_IMAGE_NAME

    for env_variable in job_manifest['spec']['template']['spec']['containers'][0]['env']:
        if env_variable['name'] == 'INPUT_MESSAGE':
            env_variable['value'] = json.dumps(message_body)
        elif env_variable['name'] == 'LLM_OUTPUT_QUEUE_URL':
            env_variable['value'] = os.getenv('LLM_OUTPUT_QUEUE_URL')
        elif env_variable['name'] == 'SNS_TOPIC_ARN':
            env_variable['value'] = os.getenv('SNS_TOPIC_ARN')
        elif env_variable['name'] == 'ROLE_ARN':
            env_variable['value'] = os.getenv('ROLE_ARN')

    batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
    logger.info(f'Job {job_name} created in namespace: {NAMESPACE}')


async def runner():
    logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
    try:
        sqs_helper = SQSHelper()

        if not NAMESPACE:
            logger.info('Configuration incomplete. Please configure ENVIRONMENT variable.')
            exit(0)
        if not TEXTRACT_IMAGE_NAME:
            logger.info('Configuration incomplete. Please configure TEXTRACT_IMAGE_NAME variable.')
            exit(0)
        if not START_TEXTRACT_QUEUE_URL:
            logger.info('Configuration incomplete. Please configure START_TEXTRACT_QUEUE_URL variable.')
            exit(0)

        logger.info(f'Reading messages from queue: {START_TEXTRACT_QUEUE_URL.split("/")[-1]}')
        while True:
            message_body, receipt_handle = await sqs_helper.consume_message(START_TEXTRACT_QUEUE_URL)
            document_path = message_body.get('document_path', '')
            document_size = await s3_utils.get_file_size(AWS.S3.S3_BUCKET, document_path)
            document_pages = await get_page_count(document_path)
            try:
                if not (message_body and receipt_handle):
                    time.sleep(10)
                    continue

                if document_size > MedicalInsights.MAX_SIZE_MB or document_pages > MedicalInsights.MAX_PAGE_LIMIT:
                    start_time = time.time()
                    logger.info("PDF Splitting is started...")
                    pdf_splitter = await split_pdf_by_size(document_path, document_size, document_pages)
                    logger.info(f"PDF Splitting is completed in {time.time() - start_time} seconds.")

                    for file_path in pdf_splitter:
                        logger = get_cloudwatch_logger(project_id=file_path.split('/')[2],
                                                       document_name=os.path.basename(file_path),
                                                       log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
                        logger.info(f'Message received from queue: {START_TEXTRACT_QUEUE_URL.split("/")[-1]}')
                        await create_job(file_path, logger)
                else:
                    logger = get_cloudwatch_logger(project_id=document_path.split('/')[2],
                                                   document_name=os.path.basename(document_path),
                                                   log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
                    logger.info(f'Message received from queue: {START_TEXTRACT_QUEUE_URL.split("/")[-1]}')
                    await create_job(message_body, logger)
            except Exception as e:
                logger.error('%s -> %s' % (e, traceback.format_exc()))
            finally:
                if receipt_handle:
                    await sqs_helper.delete_message(START_TEXTRACT_QUEUE_URL, receipt_handle)
                sys.stdout.flush()
    except Exception as e:
        logger.error('%s -> %s' % (e, traceback.format_exc()))

if __name__ == '__main__':
    asyncio.run(runner())
