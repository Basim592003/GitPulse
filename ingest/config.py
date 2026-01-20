import os
import boto3

try:
    import streamlit as st
    R2_ACCESS_KEY_ID = st.secrets["R2_ACCESS_KEY_ID"]
    R2_SECRET_ACCESS_KEY = st.secrets["R2_SECRET_ACCESS_KEY"]
    R2_ENDPOINT_URL = st.secrets["R2_ENDPOINT_URL"]
    R2_BUCKET = st.secrets["R2_BUCKET_NAME"]
except:
    from dotenv import load_dotenv
    load_dotenv()
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
    R2_BUCKET = os.getenv("R2_BUCKET_NAME")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )