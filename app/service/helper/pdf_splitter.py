import os
import fitz
from app.constant import AWS, MedicalInsights
from app.common.s3_utils import S3Utils

s3_utils = S3Utils()


async def split_pdf_by_size(document_path, document_size, document_pages):
    document_name = os.path.basename(document_path)
    local_pdf_path = os.path.join(MedicalInsights.STATIC_FOLDER_PATH, document_name)
    await s3_utils.download_object(AWS.S3.S3_BUCKET, document_path, local_pdf_path)

    with fitz.open(local_pdf_path) as pdf_document:
        pdf_document = fitz.open(pdf_document)

    avg_page_size = document_size / document_pages
    max_pages_per_part = document_pages

    while max_pages_per_part > MedicalInsights.MAX_PAGE_LIMIT or (max_pages_per_part * avg_page_size) > MedicalInsights.MAX_SIZE_MB:
        max_pages_per_part = max_pages_per_part // 2

    part_num = 1
    start_page = 0
    output_dir = MedicalInsights.SPLIT_FILES_LOCAL_PATH

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    temp_files = []
    s3_paths = []

    while start_page < document_pages:
        remaining_pages = document_pages - start_page

        if remaining_pages <= max_pages_per_part + 1:
            max_pages_per_part = remaining_pages

        writer = fitz.open()

        end_page = min(start_page + max_pages_per_part, document_pages)

        writer.insert_pdf(pdf_document, from_page=start_page, to_page=end_page - 1)

        temp_output = os.path.join(output_dir, f"{document_name}_{start_page + 1}_to_{end_page}.pdf")
        writer.save(temp_output)
        temp_files.append(temp_output)

        part_num += 1
        start_page = end_page

    pdf_document.close()

    for temp_file in temp_files:
        part_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
        print(f"Temporary part size: {part_size_mb:.2f} MB")

        if part_size_mb > MedicalInsights.MAX_SIZE_MB:
            reduce_pdf_size(temp_file, output_dir, MedicalInsights.MAX_SIZE_MB)

        # Upload files to s3
        directory_path = os.path.dirname(document_path)
        document_name = os.path.splitext(document_name)[0]
        s3_folder_path = os.path.join(directory_path, 'split_documents', document_name)
        s3_path = os.path.join(s3_folder_path, os.path.basename(temp_file))
        with open(temp_file, 'rb') as f:
            file_object = f.read()
        await s3_utils.upload_object(AWS.S3.S3_BUCKET, s3_path, file_object)
        s3_paths.append(s3_path)

        print(f"Uploaded {temp_file} to S3 at {s3_path}")

    print("PDF splitting and upload complete.")
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"Removed temporary file: {temp_file}")
    return s3_paths


def reduce_pdf_size(temp_file, output_dir, max_size_mb):
    """ Reduces the size of a PDF by splitting it further until it's under max_size_mb """
    pdf_document = fitz.open(temp_file)
    total_pages = pdf_document.page_count
    part_num = 1
    start_page = 0
    temp_files = []

    avg_page_size = os.path.getsize(temp_file) / (1024 * 1024) / total_pages
    max_pages_per_part = int((max_size_mb / avg_page_size))

    while max_pages_per_part > MedicalInsights.MAX_PAGE_LIMIT or (max_pages_per_part * avg_page_size) > max_size_mb:
        max_pages_per_part = max_pages_per_part // 2
        print(f"Further reducing pages per part: {max_pages_per_part}")

    while start_page < total_pages:
        remaining_pages = total_pages - start_page

        if remaining_pages <= max_pages_per_part + 1:
            max_pages_per_part = remaining_pages

        writer = fitz.open()

        end_page = min(start_page + MedicalInsights.MAX_PAGE_LIMIT, total_pages)

        writer.insert_pdf(pdf_document, from_page=start_page, to_page=end_page - 1)

        temp_output = os.path.join(output_dir, f"reduced_part_{part_num}.pdf")
        writer.save(temp_output)
        temp_files.append(temp_output)

        part_size_mb = os.path.getsize(temp_output) / (1024 * 1024)

        if part_size_mb > max_size_mb:
            print(f"Part {temp_output} exceeds size limit. Further splitting required.")
            reduce_pdf_size(temp_output, output_dir, max_size_mb)
        else:
            final_output = os.path.join(output_dir, temp_file.replace("temp_", "part_"))
            os.rename(temp_output, final_output)
            print(f"Created {final_output} with size: {os.path.getsize(final_output) / (1024 * 1024):.2f} MB")

        start_page = end_page
        part_num += 1

    pdf_document.close()

    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"Removed temporary file: {temp_file}")
