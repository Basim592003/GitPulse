import sys
sys.path.append(".")

import pandas as pd
from io import BytesIO
from datetime import timedelta
from ingest.config import get_s3_client, R2_BUCKET
from features import load_gold_day

def add_labels_vectorized(features_df, s3):
    future_days = []
    for day in range(9, 19):
        date_str = f"2025-12-{day:02d}"
        try:
            df = load_gold_day(s3, date_str)
            df["future_date"] = date_str
            future_days.append(df[["repo_id", "stars", "future_date"]])
        except:
            pass
    print(f"Loaded {len(future_days)} future days")
    
    all_future = pd.concat(future_days)
    all_future = all_future.rename(columns={"stars": "future_stars"})
    
    features_df["day1"] = pd.to_datetime(features_df["date"]) + timedelta(days=1)
    features_df["day2"] = pd.to_datetime(features_df["date"]) + timedelta(days=2)
    features_df["day1"] = features_df["day1"].dt.strftime("%Y-%m-%d")
    features_df["day2"] = features_df["day2"].dt.strftime("%Y-%m-%d")
    
    day1_stars = features_df.merge(
        all_future, 
        left_on=["repo_id", "day1"], 
        right_on=["repo_id", "future_date"], 
        how="left"
    )[["future_stars"]].fillna(0)
    
    day2_stars = features_df.merge(
        all_future, 
        left_on=["repo_id", "day2"], 
        right_on=["repo_id", "future_date"], 
        how="left"
    )[["future_stars"]].fillna(0)
    
    features_df["future_stars"] = day1_stars["future_stars"].values + day2_stars["future_stars"].values
    features_df["viral"] = (features_df["future_stars"] >= 20).astype(int)
    features_df["trending"] = (
        (features_df["future_stars"] >= features_df["avg_stars_7d"] * 3) & 
        (features_df["future_stars"] >= 5)
    ).astype(int)
    features_df = features_df.drop(columns=["future_stars", "day1", "day2"])
    
    return features_df

if __name__ == "__main__":
    s3 = get_s3_client()
    
    response = s3.get_object(Bucket=R2_BUCKET, Key="ml/features.parquet")
    features = pd.read_parquet(BytesIO(response["Body"].read()))
    print(f"Loaded {len(features)} rows")
    
    labeled = add_labels_vectorized(features, s3)
    
    print(f"Viral repos: {labeled['viral'].sum()}")
    print(f"Trending repos: {labeled['trending'].sum()}")
    
    buffer = BytesIO()
    labeled.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3.put_object(Bucket=R2_BUCKET, Key="ml/training_data.parquet", Body=buffer.getvalue())
    print("Saved: ml/Training_data.parquet")