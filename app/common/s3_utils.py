import io
import boto3
import os
from app.constant import MedicalInsights
from app.constant import AWS


class S3Utils:
    def __init__(self):
        self.client = boto3.client(service_name='s3', region_name=AWS.BotoClient.AWS_DEFAULT_REGION)

    async def download_object(self, bucket, key, download_path):
        bytes_buffer = io.BytesIO()
        self.client.download_fileobj(Bucket=bucket, Key=key, Fileobj=bytes_buffer)
        file_object = bytes_buffer.getvalue()

        with open(download_path, 'wb') as file:
            file.write(file_object)

    async def download_multiple_files(self, bucket, key):
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=key)
        local_json_path = MedicalInsights.LOCAL_JSON_PATH

        if 'Contents' not in response:
            return

        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = os.path.basename(file_key)

            if not file_name:
                continue
            local_file_path = os.path.join(local_json_path, file_name)

            bytes_buffer = io.BytesIO()
            self.client.download_fileobj(Bucket=bucket, Key=file_key, Fileobj=bytes_buffer)
            bytes_buffer.seek(0)

            with open(local_file_path, 'wb') as file:
                file.write(bytes_buffer.read())

    async def upload_object(self, bucket, key, file_object):
        file_object = io.BytesIO(file_object)
        self.client.upload_fileobj(file_object, bucket, key)
        url = f's3://{bucket}/{key}'
        return url

    async def delete_object(self, bucket, key):
        self.client.delete_object(Bucket=bucket, Key=key)

    async def check_s3_path_exists(self, bucket, key):
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=key)
        if 'Contents' not in response or len(response['Contents']) < 1:
            return []
        else:
            return response

    async def get_s3_path_object_count(self, bucket, key):
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=key)
        if 'Contents' in response:
            object_count = len(response['Contents'])
            return object_count
        else:
            return []

    async def get_file_size(self, bucket, key):
        response = self.client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        file_size_mb = file_size / (1024 * 1024)
        return file_size_mb
