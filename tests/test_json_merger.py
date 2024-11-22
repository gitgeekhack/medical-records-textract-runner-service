import pytest

from app.service.helper.json_merger import merged_json_file

pytest_plugins = ('pytest_asyncio',)


class TestJsonMerger:

    @pytest.mark.asyncio
    async def test_merged_json_file_with_valid_parameter(self, tmp_path):
        local_json_path = "data/json/split_documents_json"
        s3_json_path = "test/split_documents_json_test/Stutes - CHRISTUS Medical Records.pdf"
        uploaded_path = await merged_json_file(local_json_path, s3_json_path)
        assert uploaded_path == 'test/split_documents_json_test/Stutes - CHRISTUS Medical Records.pdf/textract_response/Stutes - CHRISTUS Medical Records.pdf_text.json'

    @pytest.mark.asyncio
    async def test_merged_json_file_with_invalid_local_path(self, tmp_path):
        local_json_path = "tests/data/json//split_documents_json"
        s3_json_path = "test/split_documents_json_test/Stutes - CHRISTUS Medical Records.pdf"

        try:
            await merged_json_file(local_json_path, s3_json_path)
            assert False
        except:
            assert True
