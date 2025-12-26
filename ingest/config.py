import boto3
from dotenv import load_dotenv
import os

load_dotenv()

R2_BUCKET = os.getenv("R2_BUCKET_NAME")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    )