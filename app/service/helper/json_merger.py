import os
import json
from app.constant import AWS
from app.common.s3_utils import S3Utils

s3_utils = S3Utils()


async def merged_json_file(local_json_path, json_path):
    sorted_paths = sorted(os.listdir(local_json_path))

    file_path = os.path.join(local_json_path, sorted_paths[0])
    with open(file_path, 'r') as file:
        merged_json = json.load(file)

    page_counter = len(merged_json)

    for file_idx in range(1, len(sorted_paths)):
        filename = sorted_paths[file_idx]

        if filename.endswith('.json'):
            file_path = os.path.join(local_json_path, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)

            for key in data.keys():
                if key.startswith('page_'):
                    page_counter += 1
                    new_page_key = f'page_{page_counter}'
                    merged_json[new_page_key] = data[key]

    document_name = os.path.basename(json_path)
    output_json_filename = f'{document_name}_text.json'
    output_json_path = os.path.join(local_json_path, output_json_filename)

    with open(output_json_path, 'w') as outfile:
        json.dump(merged_json, outfile)

    with open(output_json_path, 'rb') as f:
        file_data = f.read()

    upload_json_path = os.path.join(json_path.split('textract_response')[0], 'textract_response', output_json_filename)
    await s3_utils.upload_object(AWS.S3.S3_BUCKET, upload_json_path, file_data)

    return upload_json_path
