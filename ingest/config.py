import os
import boto3

def load_config():
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and len(st.secrets) > 0:
            return (
                st.secrets["R2_ACCESS_KEY_ID"],
                st.secrets["R2_SECRET_ACCESS_KEY"],
                st.secrets["R2_ENDPOINT_URL"],
                st.secrets["R2_BUCKET_NAME"]
            )
    except ImportError:
        pass
    except Exception:
        pass
    
    # Fall back to environment variables (for local/.env and GitHub Actions)
    from dotenv import load_dotenv
    load_dotenv()  # Does nothing if no .env file, that's fine
    return (
        os.getenv("R2_ACCESS_KEY_ID"),
        os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_ENDPOINT_URL"),
        os.getenv("R2_BUCKET_NAME")
    )

R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET = load_config()

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )