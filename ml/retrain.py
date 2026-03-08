import sys
sys.path.append(".")

import os
import pandas as pd
import joblib
import mlflow
from mlflow.tracking import MlflowClient
from io import BytesIO
from datetime import datetime, timedelta, timezone
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
from ingest.config import get_s3_client, R2_BUCKET
from features import load_gold_day, build_features

os.environ["MLFLOW_TRACKING_URI"] = "https://dagshub.com/Basim592003/GitPulse.mlflow"
os.environ["MLFLOW_TRACKING_USERNAME"] = "Basim592003"
os.environ["MLFLOW_TRACKING_PASSWORD"] = os.environ.get("DAGSHUB_TOKEN", "")

feature_cols = ["stars", "forks", "pushes", "prs", "issues",
                "avg_stars_7d", "avg_forks_7d", "avg_pushes_7d",
                "star_velocity", "fork_ratio"]

MODEL_NAME = "gitpulse-viral"

def get_available_dates(s3):
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

def delete_old_data(s3, dates):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%d")
    
    for date_str in dates:
        if date_str < cutoff:
            year, month, day = date_str.split("-")
            key = f"gold/year={year}/month={month}/day={day}/metrics.parquet"
            try:
                s3.delete_object(Bucket=R2_BUCKET, Key=key)
                print(f"Deleted: {date_str}")
            except:
                pass

def retrain():
    s3 = get_s3_client()
    client = MlflowClient()
    
    mlflow.set_experiment("gitpulse-viral-prediction")
    
    with mlflow.start_run() as run:
        print("Finding available dates...")
        available_dates = get_available_dates(s3)
        print(f"Found {len(available_dates)} days: {available_dates[0]} to {available_dates[-1]}")
        
        mlflow.log_param("training_days", len(available_dates))
        mlflow.log_param("date_range", f"{available_dates[0]} to {available_dates[-1]}")
        
        if len(available_dates) < 10:
            raise ValueError("Need at least 10 days for retraining")
        
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
        mlflow.log_param("total_samples", len(features_df))
        
        print("\nAdding labels...")
        df = add_labels_vectorized(features_df, s3, available_dates)
        print(f"Viral repos: {df['viral'].sum()}")
        
        viral = df[df["viral"] == 1]
        non_viral = df[df["viral"] == 0].sample(frac=0.02, random_state=42)
        balanced_df = pd.concat([viral, non_viral]).sample(frac=1, random_state=42)
        
        mlflow.log_param("viral_count", len(viral))
        mlflow.log_param("non_viral_count", len(non_viral))
        mlflow.log_param("balance_ratio", round(len(viral) / len(non_viral), 2))
        
        print(f"Balanced: {len(viral)} viral, {len(non_viral)} non-viral")
        
        X = balanced_df[feature_cols]
        y = balanced_df["viral"]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        mlflow.log_param("test_size", 0.2)
        mlflow.log_param("features", str(feature_cols))
        
        scaler_new = MinMaxScaler()
        X_train_scaled = scaler_new.fit_transform(X_train)
        X_test_scaled = scaler_new.transform(X_test)
        
        mlflow.log_param("model_type", "MLPClassifier")
        mlflow.log_param("max_iter", 500)
        
        model_new = MLPClassifier(max_iter=500, random_state=42)
        model_new.fit(X_train_scaled, y_train)
        
        preds = model_new.predict(X_test_scaled)
        
        f1 = f1_score(y_test, preds)
        precision = precision_score(y_test, preds)
        recall = recall_score(y_test, preds)
        accuracy = accuracy_score(y_test, preds)
        
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("accuracy", accuracy)
        
        print(f"\nMetrics:")
        print(f"  F1: {f1:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall: {recall:.4f}")
        print(f"  Accuracy: {accuracy:.4f}")
        
        mlflow.sklearn.log_model(model_new, name="model")
        
        scaler_path = "/tmp/scaler.pkl"
        joblib.dump(scaler_new, scaler_path)
        mlflow.log_artifact(scaler_path, artifact_path="scaler")
        
        model_uri = f"runs:/{run.info.run_id}/model"
        result = mlflow.register_model(model_uri, MODEL_NAME)
        print(f"\nRegistered model version: {result.version}")
        
        try:
            prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
            if prod_versions:
                prod_run = client.get_run(prod_versions[0].run_id)
                current_f1 = prod_run.data.metrics.get("f1", 0)
            else:
                current_f1 = 0
        except:
            current_f1 = 0
        
        if f1 > current_f1:
            client.set_registered_model_alias(MODEL_NAME, "Production", result.version)
            print(f"Model promoted to Production! F1: {f1:.4f} > {current_f1:.4f}")
        else:
            print(f"Model NOT promoted. New F1: {f1:.4f} <= Current: {current_f1:.4f}")
            if current_f1 == 0:
                client.set_registered_model_alias(MODEL_NAME, "Production", result.version)
                print("No existing Production model, setting this as Production")
        
        print("\nCleaning up old data...")
        delete_old_data(s3, available_dates)
        
        print("\nRetrain complete!")

if __name__ == "__main__":
    retrain()
    