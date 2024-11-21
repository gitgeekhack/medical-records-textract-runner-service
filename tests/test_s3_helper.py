import os
import io
import pytest
from app.common.s3_utils import S3Utils

from botocore.exceptions import ClientError, ParamValidationError

pytest_plugins = ('pytest_asyncio',)


class TestS3Utils:
    @pytest.mark.asyncio
    async def test_download_object_with_valid_parameters(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "ds-medical-insights-extractor"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        download_path = str(target_path / "Fugarino Dictation_ 06-27-2023.pdf")

        await s3.download_object(bucket, key, download_path)
        assert sum(len(files) for _, _, files in os.walk(target_path)) == 1

    @pytest.mark.asyncio
    async def test_download_object_with_invalid_download_path(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "ds-medical-insights-extractor"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        download_path = str(target_path / "pdf" / "Fugarino Dictation_ 06-27-2023.pdf")

        try:
            await s3.download_object(bucket, key, download_path)
        except FileNotFoundError:
            assert True

    @pytest.mark.asyncio
    async def test_download_object_with_invalid_key(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "ds-medical-insights-extractor"
        key = "tests/Fugarino Dictation_ 06-27-2023.pdf"
        download_path = str(target_path / "Fugarino Dictation_ 06-27-2023.pdf")

        try:
            await s3.download_object(bucket, key, download_path)
        except ClientError:
            assert True

    @pytest.mark.asyncio
    async def test_download_object_with_invalid_bucket(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "medical-insights-extractor-ds"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        download_path = str(target_path / "Fugarino Dictation_ 06-27-2023.pdf")

        try:
            await s3.download_object(bucket, key, download_path)
        except ClientError:
            assert True

    @pytest.mark.asyncio
    async def test_upload_object_with_valid_parameters(self):
        s3 = S3Utils()
        bytes_buffer = io.BytesIO()
        file_path = "data/pdf/operative_report.pdf"
        with open(file_path, mode='rb') as file:
            bytes_buffer.write(file.read())

        bucket = "ds-medical-insights-extractor"
        key = "tests/operative_report.pdf"
        file_object = bytes_buffer.getvalue()

        await s3.upload_object(bucket, key, file_object)
        if await s3.check_s3_path_exists(bucket, key):
            assert True
            await s3.delete_object(bucket, key)
        else:
            assert False

    @pytest.mark.asyncio
    async def test_upload_object_with_invalid_bucket(self):
        s3 = S3Utils()
        bytes_buffer = io.BytesIO()
        file_path = "data/pdf/operative_report.pdf"
        with open(file_path, mode='rb') as file:
            bytes_buffer.write(file.read())

        bucket = "medical-insights-ds"
        key = "tests-data/operative_report.pdf"
        file_object = bytes_buffer.getvalue()

        try:
            await s3.upload_object(bucket, key, file_object)
            if await s3.check_s3_path_exists(bucket, key):
                assert True
                await s3.delete_object(bucket, key)
            else:
                assert False
        except s3.client.exceptions.NoSuchBucket:
            assert True

    @pytest.mark.asyncio
    async def test_delete_object_with_valid_parameters(self):
        s3 = S3Utils()
        bytes_buffer = io.BytesIO()
        file_path = "data/pdf/operative_report.pdf"
        with open(file_path, mode='rb') as file:
            bytes_buffer.write(file.read())

        bucket = "ds-medical-insights-extractor"
        key = "tests/operative_report.pdf"
        file_object = bytes_buffer.getvalue()

        await s3.upload_object(bucket, key, file_object)

        await s3.delete_object(bucket, key)
        if not await s3.check_s3_path_exists(bucket, key):
            assert True
        else:
            assert False

    @pytest.mark.asyncio
    async def test_delete_object_with_invalid_key(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = ""
        try:
            await s3.delete_object(bucket, key)
        except ParamValidationError:
            assert True

    @pytest.mark.asyncio
    async def test_delete_object_with_invalid_bucket(self):
        s3 = S3Utils()

        bucket = "medical-insights-ds"
        key = "tests/operative_report.pdf"
        try:
            await s3.delete_object(bucket, key)
        except s3.client.exceptions.NoSuchBucket:
            assert True

    @pytest.mark.asyncio
    async def test_download_multiple_files_with_valid_parameters(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "ds-medical-insights-extractor"
        key = "test-data/"
        download_path = str(target_path)

        await s3.download_multiple_files(bucket, key, download_path)
        assert sum(len(files) for _, _, files in os.walk(target_path)) == 2

    @pytest.mark.asyncio
    async def test_download_multiple_files_with_invalid_key(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "ds-medical-insights-extractor"
        key = "test/"
        download_path = str(target_path)

        try:
            await s3.download_multiple_files(bucket, key, download_path)
        except ClientError:
            assert True

    @pytest.mark.asyncio
    async def test_download_multiple_files_with_invalid_bucket(self, tmp_path):
        s3 = S3Utils()
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        bucket = "medical-insights-extractor-ds"
        key = "test-data/"
        download_path = str(target_path)

        try:
            await s3.download_multiple_files(bucket, key, download_path)
        except ClientError:
            assert True

    @pytest.mark.asyncio
    async def test_check_s3_path_exists_with_valid_parameters(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        if await s3.check_s3_path_exists(bucket, key):
            assert True
        else:
            assert False

    @pytest.mark.asyncio
    async def test_check_s3_path_exists_with_invalid_key(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "tests/Fugarino Dictation_ 06-27-2023.pdf"
        if not await s3.check_s3_path_exists(bucket, key):
            assert True
        else:
            assert False

    @pytest.mark.asyncio
    async def test_check_s3_path_exists_with_invalid_bucket(self):
        s3 = S3Utils()

        bucket = "medical-insights-ds"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        try:
            await s3.check_s3_path_exists(bucket, key)
        except s3.client.exceptions.NoSuchBucket:
            assert True

    @pytest.mark.asyncio
    async def test_get_s3_path_object_count_with_valid_parameters(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "test-data/"
        count = await s3.get_s3_path_object_count(bucket, key)
        assert count == 3

    @pytest.mark.asyncio
    async def test_get_s3_path_object_count_with_invalid_key(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "tests-data/"
        count = await s3.get_s3_path_object_count(bucket, key)
        assert count == []

    @pytest.mark.asyncio
    async def test_get_s3_path_object_count_with_invalid_bucket(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "test-data/"
        try:
            await s3.get_s3_path_object_count(bucket, key)
        except s3.client.exceptions.NoSuchBucket:
            assert True

    @pytest.mark.asyncio
    async def test_get_file_size_with_valid_parameters(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"
        size = await s3.get_file_size(bucket, key)
        assert size == 0.33324337005615234

    @pytest.mark.asyncio
    async def test_get_file_size_with_invalid_key(self):
        s3 = S3Utils()

        bucket = "ds-medical-insights-extractor"
        key = "tests/Fugarino Dictation_ 06-27-2023.pdf"

        try:
            await s3.get_file_size(bucket, key)
        except ClientError:
            assert True

    @pytest.mark.asyncio
    async def test_get_file_size_with_invalid_bucket(self):
        s3 = S3Utils()

        bucket = "medical-insights-extractor-ds"
        key = "test/Fugarino Dictation_ 06-27-2023.pdf"

        try:
            await s3.get_file_size(bucket, key)
        except ClientError:
            assert True
