import requests
import gzip
from datetime import datetime, timedelta
from config import get_s3_client, R2_BUCKET

def download_hour(date, hour):
    url = f"https://data.gharchive.org/{date}-{hour}.json.gz"
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def upload_to_bronze(data, date, hour):
    s3 = get_s3_client()
    year, month, day = date.split("-")
    key = f"bronze/year={year}/month={month}/day={day}/hour={hour:02d}/events.json.gz"
    s3.put_object(Bucket=R2_BUCKET, Key=key, Body=data)
    return key

def ingest_hour(date, hour):
    data = download_hour(date, hour)
    key = upload_to_bronze(data, date, hour)
    return key

if __name__ == "__main__":
    yesterday = datetime.now() - timedelta(days=0)
    date = yesterday.strftime("%Y-%m-%d")
    hour = 12
    
    key = ingest_hour(date, hour)
    print(f"Uploaded: {key}")
