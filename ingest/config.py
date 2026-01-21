import os
import boto3

# Get env vars directly at module load
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL")
R2_BUCKET = os.environ.get("R2_BUCKET_NAME")

print(f"CONFIG: R2_BUCKET = {R2_BUCKET}")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )