import gzip
import json
import pandas as pd
from io import BytesIO
from config import get_s3_client, R2_BUCKET

KEEP_EVENTS = ["WatchEvent", "ForkEvent", "PushEvent", "PullRequestEvent", "IssuesEvent", "CreateEvent"]

def process_hour_to_records(s3, date, hour):
    year, month, day = date.split("-")
    key = f"bronze/year={year}/month={month}/day={day}/hour={hour:02d}/events.json.gz"
    print(f"R2_BUCKET = {R2_BUCKET}")
    response = s3.get_object(Bucket=R2_BUCKET, Key=key)
    compressed = response["Body"].read()
    decompressed = gzip.decompress(compressed)
    
    records = []
    for line in decompressed.split(b"\n"):
        if not line:
            continue
        try:
            event = json.loads(line)
            if event["type"] not in KEEP_EVENTS:
                continue
            records.append({
                "event_type": event["type"],
                "repo_id": event["repo"]["id"],
                "repo_name": event["repo"]["name"],
                "actor_id": event["actor"]["id"],
                "created_at": event["created_at"]
            })
        
        except (KeyError, json.JSONDecodeError):
            continue
    return records

def process_day_to_silver(date):
    s3 = get_s3_client()
    year, month, day = date.split("-")
    
    all_records = []
    for hour in range(24):
        try:
            records = process_hour_to_records(s3, date, hour)
            all_records.extend(records)
            print(f"Processed hour {hour}: {len(records)} events")
        except Exception as e:
            print(f"Failed hour {hour}: {e}")
    
    df = pd.DataFrame(all_records)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    print(f"R2_BUCKET = {R2_BUCKET}")
    silver_key = f"silver/year={year}/month={month}/day={day}/events.parquet"
    s3.put_object(Bucket=R2_BUCKET, Key=silver_key, Body=buffer.getvalue())
    print(f"Uploaded: {silver_key} ({len(df)} total events)")
    return silver_key

def delete_silver_day(date):
    s3 = get_s3_client()
    year, month, day = date.split("-")
    key = f"silver/year={year}/month={month}/day={day}/events.parquet"
    try:
        s3.delete_object(Bucket=R2_BUCKET, Key=key)
        print(f"Deleted silver for {date}")
    except:
        pass