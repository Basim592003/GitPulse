import os
import boto3

def load_config():
    try:
        import streamlit as st
        if "R2_ACCESS_KEY_ID" in st.secrets:
            return (
                st.secrets["R2_ACCESS_KEY_ID"],
                st.secrets["R2_SECRET_ACCESS_KEY"],
                st.secrets["R2_ENDPOINT_URL"],
                st.secrets["R2_BUCKET_NAME"]
            )
    except:
        pass
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass
    
    bucket = os.getenv("R2_BUCKET_NAME")
    
    return (
        os.getenv("R2_ACCESS_KEY_ID"),
        os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_ENDPOINT_URL"),
        bucket
    )

R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET = load_config()

print(f"CONFIG LOADED: R2_BUCKET = {R2_BUCKET}")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )