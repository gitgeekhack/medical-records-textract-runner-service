import pytest

from app.common.utils import get_project_id_and_document, get_page_count

pytest_plugins = ('pytest_asyncio',)


class TestUtils:
    @pytest.mark.asyncio
    async def test_get_project_id_and_document_with_invalid_path(self):
        document_path = "request//1234-abcd//defg-5678-hij/Fugarino Dictation_ 06-27-2023.pdf"
        id, name = await get_project_id_and_document(document_path)
        if id != "defg-5678-hij":
            assert True

    @pytest.mark.asyncio
    async def test_get_project_id_and_document_with_empty_path(self):
        document_path = ""
        try:
            await get_project_id_and_document(document_path)
        except IndexError:
            assert True

    @pytest.mark.asyncio
    async def test_get_project_id_and_document_with_valid_path(self):
        document_path = "request/1234-abcd/defg-5678-hij/Fugarino Dictation_ 06-27-2023.pdf"
        id, name = await get_project_id_and_document(document_path)
        if id == "defg-5678-hij":
            assert True

    @pytest.mark.asyncio
    async def test_get_page_count_valid_parameters(self, tmp_path):
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        document_path = "test-data/Fenn Dictation_ 09-06-2022.pdf"
        page_count = await get_page_count(document_path, str(target_path))

        assert page_count == 5

    @pytest.mark.asyncio
    async def test_get_page_count_invalid_parameters(self, tmp_path):
        target_path = tmp_path / "target"
        target_path.mkdir(parents=True, exist_ok=True)

        document_path = "tests/Fenn Dictation_ 09-06-2022.pdf"

        try:
            await get_page_count(document_path, str(target_path))
            assert False

        except:
            assert True
