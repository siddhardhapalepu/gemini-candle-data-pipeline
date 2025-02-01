import boto3
from botocore.exceptions import NoCredentialsError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    
    upload_file_to_s3('example.txt', 'your-bucket-name', 'folder/subfolder/example.txt')
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        logging.info(f"File {file_name} uploaded to {bucket}/{object_name}")
        return True
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False
