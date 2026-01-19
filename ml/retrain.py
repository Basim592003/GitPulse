import sys
sys.path.append(".")

import pandas as pd
import joblib
import os
from io import BytesIO
from datetime import datetime, timedelta, timezone
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
from ingest.config import get_s3_client, R2_BUCKET
from features import load_gold_day, build_features

feature_cols = ["stars", "forks", "pushes", "prs", "issues",
                "avg_stars_7d", "avg_forks_7d", "avg_pushes_7d",
                "star_velocity", "fork_ratio"]

def get_available_dates(s3):
    """Get all available gold dates"""
    response = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix="gold/")
    dates = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if "metrics.parquet" in key:
            parts = key.split("/")
            year = parts[1].split("=")[1]
            month = parts[2].split("=")[1]
            day = parts[3].split("=")[1]
            dates.append(f"{year}-{month}-{day}")
    return sorted(dates)

def add_labels_vectorized(features_df, s3, available_dates):
    """Reused from labels.py but with dynamic dates"""
    future_days = []
    for date_str in available_dates:
        try:
            df = load_gold_day(s3, date_str)
            df["future_date"] = date_str
            future_days.append(df[["repo_id", "stars", "future_date"]])
        except:
            pass
    
    if not future_days:
        raise ValueError("No future days available for labeling")
    
    print(f"Loaded {len(future_days)} days for labeling")
    
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
    features_df = features_df.drop(columns=["future_stars", "day1", "day2"])
    
    return features_df

def delete_old_month(s3, dates):
    """Delete gold files from previous month"""
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    
    for date_str in dates:
        if not date_str.startswith(current_month):
            year, month, day = date_str.split("-")
            key = f"gold/year={year}/month={month}/day={day}/metrics.parquet"
            try:
                s3.delete_object(Bucket=R2_BUCKET, Key=key)
                print(f"Deleted: {date_str}")
            except:
                pass

def retrain():
    s3 = get_s3_client()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("Finding available dates...")
    available_dates = get_available_dates(s3)
    print(f"Found {len(available_dates)} days: {available_dates[0]} to {available_dates[-1]}")
    
    if len(available_dates) < 10:
        raise ValueError("Need at least 10 days for retraining (7 history + 2 future + 1 target)")
    
    print("\nBuilding features...")
    all_features = []
    
    for i, date_str in enumerate(available_dates):
        if i < 7: 
            continue
        if i >= len(available_dates) - 2: 
            continue
        
        try:
            df = build_features(date_str)
            df["date"] = date_str
            all_features.append(df)
            print(f"{date_str}: {len(df)} repos")
        except Exception as e:
            print(f"{date_str}: Failed - {e}")
    
    if not all_features:
        raise ValueError("No features could be built")
    
    features_df = pd.concat(all_features)
    print(f"\nTotal features: {len(features_df)} rows")
    
    print("\nAdding labels...")
    df = add_labels_vectorized(features_df, s3, available_dates)
    print(f"Viral repos: {df['viral'].sum()}")
    
    viral = df[df["viral"] == 1]
    non_viral = df[df["viral"] == 0].sample(frac=0.02, random_state=42)
    balanced_df = pd.concat([viral, non_viral]).sample(frac=1, random_state=42)
    
    print(f"Balanced: {len(viral)} viral, {len(non_viral)} non-viral")
    
    X = balanced_df[feature_cols]
    y = balanced_df["viral"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    scaler_new = MinMaxScaler()
    X_train_scaled = scaler_new.fit_transform(X_train)
    X_test_scaled = scaler_new.transform(X_test)
    
    model_new = MLPClassifier(max_iter=500, random_state=42)
    model_new.fit(X_train_scaled, y_train)
    
    new_f1 = f1_score(y_test, model_new.predict(X_test_scaled))
    print(f"\nNew model F1: {new_f1:.4f}")
    
    try:
        model_old = joblib.load(os.path.join(script_dir, "model_viral.pkl"))
        scaler_old = joblib.load(os.path.join(script_dir, "scaler_viral.pkl"))
        X_test_old_scaled = scaler_old.transform(X_test)
        old_f1 = f1_score(y_test, model_old.predict(X_test_old_scaled))
        print(f"Old model F1: {old_f1:.4f}")
    except:
        old_f1 = 0
        print("No old model found")
    
    if new_f1 > old_f1:
        joblib.dump(model_new, os.path.join(script_dir, "model_viral.pkl"))
        joblib.dump(scaler_new, os.path.join(script_dir, "scaler_viral.pkl"))
        print("New model is better — saved!")
    else:
        print("Old model is better — keeping it")
    
    print("\nCleaning up old data...")
    delete_old_month(s3, available_dates)
    
    print("\nRetrain complete!")

if __name__ == "__main__":
    retrain()
