import asyncio
import json
import os
import sys
import time
import re
import traceback
import uuid
import shutil
from kubernetes import client, config
from app.business_rule_exception import TextExtractionFailed
from app.common.cloudwatch_helper import get_cloudwatch_logger
from app.common.sqs_helper import SQSHelper
from app.common.utils import get_project_id_and_document
from app.constant import AWS, MedicalInsights
from app.service.helper.textract_helper import TextractHelper
from app.common.s3_utils import S3Utils
from app.service.helper.json_merger import merged_json_file

# config.load_kube_config()   # Uncomment this line while testing in local
config.load_incluster_config()


class LlmRunner:
    def __init__(self):
        self.s3_utils = S3Utils()
        self.COMPLETED_TEXTRACT_QUEUE_URL = os.getenv('COMPLETED_TEXTRACT_QUEUE_URL')
        self.NAMESPACE = os.getenv('ENVIRONMENT')
        self.LLM_IMAGE_NAME = os.getenv('LLM_IMAGE_NAME')
        self.logger = get_cloudwatch_logger(log_stream_name=AWS.CloudWatch.TEXTRACT_RUNNER_STREAM)
        self.textract_helper = TextractHelper(self.logger)
        self.sqs_helper = SQSHelper()

    async def create_job(self, s3_json_path):
        batch_v1 = client.BatchV1Api()

        job_manifest = json.load(open('app/llm/job_manifest.json', 'r'))

        job_name = f"llm-job-{uuid.uuid1()}"
        job_manifest['metadata']['name'] = job_name

        job_manifest['spec']['template']['spec']['containers'][0]['image'] = self.LLM_IMAGE_NAME

        for env_variable in job_manifest['spec']['template']['spec']['containers'][0]['env']:
            if env_variable['name'] == 'LLM_OUTPUT_QUEUE_URL':
                env_variable['value'] = os.getenv('LLM_OUTPUT_QUEUE_URL')
            elif env_variable['name'] == 'INPUT_MESSAGE':
                env_variable['value'] = s3_json_path

        batch_v1.create_namespaced_job(namespace=self.NAMESPACE, body=job_manifest)
        self.logger.info(f'Job {job_name} created in namespace: {self.NAMESPACE}')

    async def process_splitted_files(self, message_body, file_path):
        project_id_path = os.path.join(*file_path.split('/')[:3])
        pdf_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]
        pdf_name = re.sub(r'_\d+_to_\d+$', '', pdf_name_without_extension)
        s3_textract_path = os.path.join(project_id_path, MedicalInsights.TEXTRACT_FOLDER_NAME, MedicalInsights.SPLIT_DOCUMENT_JSON_FOLDER, pdf_name, f'{pdf_name_without_extension}_text.json')
        await self.textract_helper.get_page_wise_text(message_body, s3_textract_path)

        pdf_path = os.path.dirname(message_body['DocumentLocation']['S3ObjectName'])
        json_path = pdf_path.replace(MedicalInsights.JSON_PATH_REPLACE_OLD, MedicalInsights.JSON_PATH_REPLACE_NEW)
        local_json_path = MedicalInsights.LOCAL_JSON_PATH

        pdf_count = await self.s3_utils.get_s3_path_object_count(AWS.S3.S3_BUCKET, pdf_path)
        json_count = await self.s3_utils.get_s3_path_object_count(AWS.S3.S3_BUCKET, json_path)

        if pdf_count == json_count:
            os.makedirs(local_json_path, exist_ok=True)
            await self.s3_utils.download_multiple_files(AWS.S3.S3_BUCKET, json_path)

            start_time = time.time()
            self.logger.info("Merging JSON file is started...")

            merged_s3_json_path = await merged_json_file(local_json_path, json_path)
            self.logger.info(f"Merging JSON file is completed in {time.time() - start_time} seconds.")

            if os.path.exists(local_json_path):
                shutil.rmtree(local_json_path)

            project_id, document_name = await get_project_id_and_document(message_body['DocumentLocation']['S3ObjectName'])
            self.logger = get_cloudwatch_logger(project_id=project_id, document_name=document_name, log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
            textract_json_path = {"textract_json_path": merged_s3_json_path, "document_path": file_path}
            json_data = json.dumps(textract_json_path)
            await self.create_job(json_data)
        else:
            self.logger.error(f"PDF count ({pdf_count}) does not match JSON count ({json_count}) for {file_path}.")

    async def process_normal_file(self, message_body, file_path, pdf_name):
        s3_textract_path = os.path.join(os.path.dirname(os.path.dirname(file_path)), MedicalInsights.TEXTRACT_FOLDER_NAME, f'{pdf_name}_text.json')
        await self.textract_helper.get_page_wise_text(message_body, s3_textract_path)

        project_id, document_name = await get_project_id_and_document(message_body['DocumentLocation']['S3ObjectName'])
        self.logger = get_cloudwatch_logger(project_id=project_id, document_name=document_name, log_stream_name=AWS.CloudWatch.LLM_RUNNER_STREAM)
        textract_json_path = {"textract_json_path": s3_textract_path, "document_path": file_path}
        json_data = json.dumps(textract_json_path)
        await self.create_job(json_data)

    async def runner(self):
        try:
            if not self.NAMESPACE:
                self.logger.info('Configuration incomplete. Please configure ENVIRONMENT variable.')
                exit(0)
            if not self.LLM_IMAGE_NAME:
                self.logger.info('Configuration incomplete. Please configure LLM_IMAGE_NAME variable.')
                exit(0)
            if not self.COMPLETED_TEXTRACT_QUEUE_URL:
                self.logger.info('Configuration incomplete. Please configure COMPLETED_TEXTRACT_QUEUE_URL variable.')
                exit(0)

            self.logger.info(f'Reading messages from queue: {self.COMPLETED_TEXTRACT_QUEUE_URL.split("/")[-1]}')
            while True:
                message_body, receipt_handle = await self.sqs_helper.consume_message(self.COMPLETED_TEXTRACT_QUEUE_URL)
                self.logger.info(f'Message received from queue: {self.COMPLETED_TEXTRACT_QUEUE_URL.split("/")[-1]}')
                try:
                    if not (message_body and receipt_handle):
                        time.sleep(10)
                        continue

                    if message_body['Status'] != "SUCCEEDED":
                        raise TextExtractionFailed

                    file_path = message_body['DocumentLocation']['S3ObjectName']
                    pdf_name = os.path.basename(file_path)

                    if '/split_documents/' in file_path:
                        await self.process_splitted_files(message_body, file_path)
                    else:
                        await self.process_normal_file(message_body, file_path, pdf_name)
                except Exception as e:
                    self.logger.error('%s -> %s' % (e, traceback.format_exc()))
                finally:
                    if receipt_handle:
                        await self.sqs_helper.delete_message(self.COMPLETED_TEXTRACT_QUEUE_URL, receipt_handle)
                    sys.stdout.flush()
        except Exception as e:
            self.logger.error('%s -> %s' % (e, traceback.format_exc()))


if __name__ == '__main__':
    runner = LlmRunner()
    asyncio.run(runner.runner())
