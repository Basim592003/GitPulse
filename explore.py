import sys
sys.path.append(".")

import pandas as pd
from io import BytesIO
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
from ingest.config import get_s3_client, R2_BUCKET

s3 = get_s3_client()

response = s3.get_object(Bucket=R2_BUCKET, Key="ml/training_data.parquet")
df = pd.read_parquet(BytesIO(response["Body"].read()))
print(f"Loaded {len(df)} rows")
