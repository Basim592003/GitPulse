import pandas as pd
from io import BytesIO
from ingest.config import get_s3_client, R2_BUCKET

def load_silver_day(s3, date):
    year, month, day = date.split("-")
    key = f"silver/year={year}/month={month}/day={day}/events.parquet"
    response = s3.get_object(Bucket=R2_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response["Body"].read()))

def build_daily_metrics(df):
    watch = df[df["event_type"] == "WatchEvent"].groupby("repo_id").size().rename("stars")
    fork = df[df["event_type"] == "ForkEvent"].groupby("repo_id").size().rename("forks")
    push = df[df["event_type"] == "PushEvent"].groupby("repo_id").size().rename("pushes")
    pr = df[df["event_type"] == "PullRequestEvent"].groupby("repo_id").size().rename("prs")
    issues = df[df["event_type"] == "IssuesEvent"].groupby("repo_id").size().rename("issues")
    
    repo_names = df.groupby("repo_id")["repo_name"].first()
    
    metrics = pd.concat([repo_names, watch, fork, push, pr, issues], axis=1).fillna(0)
    metrics[["stars", "forks", "pushes", "prs", "issues"]] = metrics[["stars", "forks", "pushes", "prs", "issues"]].astype(int)
    
    return metrics.reset_index()

def process_day_to_gold(date):
    s3 = get_s3_client()
    year, month, day = date.split("-")
    
    df = load_silver_day(s3, date)
    metrics = build_daily_metrics(df)
    metrics["date"] = date
    
    buffer = BytesIO()
    metrics.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    gold_key = f"gold/year={year}/month={month}/day={day}/metrics.parquet"
    s3.put_object(Bucket=R2_BUCKET, Key=gold_key, Body=buffer.getvalue())
    print(f"Uploaded: {gold_key} ({len(metrics)} repos)")
    return gold_key

if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    start_date = datetime(2025, 12, 1)
    end_date = datetime(2025, 12, 18)
    
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        try:
            process_day_to_gold(date_str)
        except Exception as e:
            print(f"Failed {date_str}: {e}")
        current += timedelta(days=1)
