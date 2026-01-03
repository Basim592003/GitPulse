import sys
sys.path.append(".")

import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from ingest.config import get_s3_client, R2_BUCKET

def load_gold_day(s3, date):
    year, month, day = date.split("-")
    key = f"gold/year={year}/month={month}/day={day}/metrics.parquet"
    response = s3.get_object(Bucket=R2_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))

def build_features(target_date):
    s3 = get_s3_client()
    target = datetime.strptime(target_date, "%Y-%m-%d")
    
    today = load_gold_day(s3, target_date)
    
    past_7_days = []
    for i in range(1, 8):
        past_date = (target - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            past_7_days.append(load_gold_day(s3, past_date))
        except:
            pass
    
    if not past_7_days:
        raise ValueError("No historical data available")
    
    history = pd.concat(past_7_days)
    avg_stats = history.groupby("repo_id").agg({
        "stars": "mean",
        "forks": "mean",
        "pushes": "mean"
    }).rename(columns={
        "stars": "avg_stars_7d",
        "forks": "avg_forks_7d",
        "pushes": "avg_pushes_7d"
    })
    
    features = today.merge(avg_stats, on="repo_id", how="left").fillna(0)
    
    features["star_velocity"] = features["stars"] / (features["avg_stars_7d"] + 1)
    features["fork_ratio"] = features["forks"] / (features["stars"] + 1)
    features["activity_score"] = features["pushes"] + features["prs"] + features["issues"]
    
    return features

if __name__ == "__main__":
    s3 = get_s3_client()
    all_features = []
    
    start_day = 8
    end_day = 16
    
    for day in range(start_day, end_day + 1):
        date_str = f"2025-12-{day:02d}"
        df = build_features(date_str)
        all_features.append(df)
        print(f"{date_str}: {len(df)} repos")
    
    full_dataset = pd.concat(all_features)
    
    buffer = BytesIO()
    full_dataset.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3.put_object(Bucket=R2_BUCKET, Key="ml/features.parquet", Body=buffer.getvalue())
    print(f"Saved: {len(full_dataset)} rows to ml/features.parquet")
