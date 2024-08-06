import asyncio
import json
import os.path
import sys
import time

from kubernetes import client, config

from app.common.cloudwatch_helper import get_cloudwatch_logger
from app.common.sqs_helper import SQSHelper
from app.common.utils import get_project_id_and_document
from app.constant import AWS

config.load_kube_config()
queue_url = AWS.SQS.COMPLETED_TEXTRACT_QUEUE


async def create_pod(message_body, logger):
    v1 = client.CoreV1Api()

    # TODO: Update pod manifest file as per the requirement
    pod_manifest = json.load(open('/home/darshan/Music/mrs-runner-service/app/llm/pod_manifest.json', 'r'))

    for env_variable in pod_manifest['spec']['containers'][0]['env']:
        if env_variable['name'] == 'INPUT_MESSAGE':
            env_variable['value'] = json.dumps(message_body)

    namespace = 'default'  # TODO: Update namespace as per the requirement
    v1.create_namespaced_pod(namespace=namespace, body=pod_manifest)
    logger.info(f'Pod created in namespace: {namespace}')


async def runner():
    sqs_helper = SQSHelper()
    logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
    logger.info(f'Reading messages from queue: {queue_url.split("/")[-1]}')
    while True:
        message_body, receipt_handle = await sqs_helper.consume_message(queue_url)
        if not (message_body and receipt_handle):
            time.sleep(10)
            continue

        project_id, document_name = await get_project_id_and_document(message_body['DocumentLocation']['S3ObjectName'])
        logger = get_cloudwatch_logger(project_id=project_id, document_name=document_name,
                                       log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
        logger.info(f'Message received from queue: {queue_url.split("/")[-1]}')
        await create_pod(message_body, logger)
        await sqs_helper.delete_message(queue_url, receipt_handle)
        sys.stdout.flush()


if __name__ == '__main__':
    asyncio.run(runner())
