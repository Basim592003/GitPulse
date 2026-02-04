import os
import sys
sys.path.append(".")

import pandas as pd
import joblib
import glob
from io import BytesIO
from datetime import datetime, timedelta, timezone
from ingest.config import get_s3_client, R2_BUCKET
from features import load_gold_day

feature_cols = ["stars", "forks", "pushes", "prs", "issues",
                "avg_stars_7d", "avg_forks_7d", "avg_pushes_7d",
                "star_velocity", "fork_ratio"]

def get_latest_model(script_dir):
    model_files = glob.glob(os.path.join(script_dir, "model_viral_*.pkl"))
    
    if not model_files:
        return (
            os.path.join(script_dir, "model_viral.pkl"),
            os.path.join(script_dir, "scaler_viral.pkl")
        )
    
    latest_model = max(model_files)
    timestamp = latest_model.split("model_viral_")[1].replace(".pkl", "")
    latest_scaler = os.path.join(script_dir, f"scaler_viral_{timestamp}.pkl")
    
    return latest_model, latest_scaler

def build_features(s3, target_date):
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
    
    return features

def make_predictions():
    s3 = get_s3_client()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    model_path, scaler_path = get_latest_model(script_dir)
    
    print(f"Using model: {model_path}")
    print(f"Using scaler: {scaler_path}")
    
    model_viral = joblib.load(model_path)
    scaler_viral = joblib.load(scaler_path)
    
    today = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Building features for {today}")
    
    features = build_features(s3, today)
    print(f"Features built for {len(features)} repos")
    
    X = features[feature_cols]
    
    X_viral = scaler_viral.transform(X)
    features["viral_prob"] = model_viral.predict_proba(X_viral)[:, 1]
    features["viral_pred"] = (features["viral_prob"] >= 0.7).astype(int)
    
    features["github_url"] = "https://github.com/" + features["repo_name"]
    
    top_viral = features.sort_values("viral_prob", ascending=False)
    
    buffer = BytesIO()
    top_viral.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=R2_BUCKET, Key="predictions/latest.parquet", Body=buffer.getvalue())
    
    print(f"Saved {len(top_viral)} repos to predictions/latest.parquet")
    print(f"Viral (prob >= 0.7): {features['viral_pred'].sum()}")

if __name__ == "__main__":
    make_predictions()