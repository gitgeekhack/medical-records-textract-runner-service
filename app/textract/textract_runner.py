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
from app.constant import AWS

# config.load_kube_config()   # Uncomment this line while testing in local
queue_url = AWS.SQS.START_TEXTRACT_QUEUE

# Get namespace from environment variable
NAMESPACE = os.getenv('ENVIRONMENT')

async def create_job(message_body, logger):
    batch_v1 = client.BatchV1Api()

    # TODO: Update job manifest file as per the requirement
    job_manifest = json.load(open('app/textract/job_manifest.json', 'r'))

    job_name = f"textract-job-{uuid.uuid1()}"
    job_manifest['metadata']['name'] = job_name

    for env_variable in job_manifest['spec']['template']['spec']['containers'][0]['env']:
        if env_variable['name'] == 'INPUT_MESSAGE':
            env_variable['value'] = json.dumps(message_body)

    batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
    logger.info(f'Job {job_name} created in namespace: {NAMESPACE}')


async def runner():
    logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
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

            logger = get_cloudwatch_logger(project_id=message_body['document_path'].split('/')[2],
                                           document_name=os.path.basename(message_body['document_path']),
                                           log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
            logger.info(f'Message received from queue: {queue_url.split("/")[-1]}')
            await create_job(message_body, logger)
            await sqs_helper.delete_message(queue_url, receipt_handle)
            sys.stdout.flush()
    except Exception as e:
        logger.error('%s -> %s' % (e, traceback.format_exc()))

if __name__ == '__main__':
    asyncio.run(runner())
