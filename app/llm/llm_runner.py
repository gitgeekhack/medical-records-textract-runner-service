import asyncio
import json
import os
import sys
import time
import traceback
import uuid

from kubernetes import client, config

from app.common.cloudwatch_helper import get_cloudwatch_logger
from app.common.sqs_helper import SQSHelper
from app.common.utils import get_project_id_and_document
from app.constant import AWS

# config.load_kube_config()   # Uncomment this line while testing in local
config.load_incluster_config()
queue_url = AWS.SQS.COMPLETED_TEXTRACT_QUEUE

# Get namespace from environment variable
NAMESPACE = os.getenv('ENVIRONMENT')


async def create_job(message_body, logger):
    batch_v1 = client.BatchV1Api()

    # TODO: Update job manifest file as per the requirement
    job_manifest = json.load(open('app/llm/job_manifest.json', 'r'))

    job_name = f"llm-job-{uuid.uuid1()}"
    job_manifest['metadata']['name'] = job_name

    for env_variable in job_manifest['spec']['template']['spec']['containers'][0]['env']:
        if env_variable['name'] == 'INPUT_MESSAGE':
            env_variable['value'] = json.dumps(message_body)

    batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
    logger.info(f'Job {job_name} created in namespace: {NAMESPACE}')


async def runner():
    logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
    try:
        sqs_helper = SQSHelper()

        if not NAMESPACE:
            logger.info('Configuration incomplete. Please configure ENVIRONMENT variable.')
            exit(0)

        logger.info(f'Reading messages from queue: {queue_url.split("/")[-1]}')
        while True:
            message_body, receipt_handle = await sqs_helper.consume_message(queue_url)
            if not (message_body and receipt_handle):
                time.sleep(10)
                continue

            project_id, document_name = await get_project_id_and_document(
                message_body['DocumentLocation']['S3ObjectName'])
            logger = get_cloudwatch_logger(project_id=project_id, document_name=document_name,
                                           log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
            logger.info(f'Message received from queue: {queue_url.split("/")[-1]}')
            await create_job(message_body, logger)
            await sqs_helper.delete_message(queue_url, receipt_handle)
            sys.stdout.flush()
    except Exception as e:
        logger.error('%s -> %s' % (e, traceback.format_exc()))


if __name__ == '__main__':
    asyncio.run(runner())
