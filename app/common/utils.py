import os
import fitz
from app.common.s3_utils import S3Utils
from app.constant import AWS, MedicalInsights


async def get_project_id_and_document(document_path):
    document_name = os.path.basename(document_path)
    project_id = document_path.split('/')[2]
    return project_id, document_name


async def get_page_count(document_path):
    s3_utils = S3Utils()
    document_name = os.path.splitext(os.path.basename(document_path))[0]
    local_pdf_path = os.path.join(MedicalInsights.STATIC_FOLDER_PATH, f"{document_name}.pdf")
    directory = os.path.dirname(local_pdf_path)

    if not os.path.exists(directory):
        os.makedirs(directory)

    await s3_utils.download_object(AWS.S3.S3_BUCKET, document_path, local_pdf_path)

    with fitz.open(local_pdf_path) as pdf_document:
        page_count = pdf_document.page_count

    if os.path.exists(local_pdf_path):
        os.remove(local_pdf_path)

    return page_count
