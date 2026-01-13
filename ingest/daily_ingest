import sys
sys.path.append(".")

from datetime import datetime, timedelta, timezone
from ingest.bronze import ingest_hour, delete_bronze_day
from ingest.silver import process_day_to_silver, delete_silver_day
from ingest.gold import process_day_to_gold
from ingest.config import get_s3_client, R2_BUCKET

s3 = get_s3_client()

today = datetime.now(timezone.utc)
today_str = today.strftime("%Y-%m-%d")

old_date = today - timedelta(days=8)
old_str = old_date.strftime("%Y-%m-%d")
year, month, day = old_str.split("-")

try:
    s3.delete_object(Bucket=R2_BUCKET, Key=f"gold/year={year}/month={month}/day={day}/metrics.parquet")
    print(f"Deleted old gold: {old_str}")
except:
    print(f"No old data to delete: {old_str}")

print(f"\n=== Processing {today_str} ===")

for hour in range(24):
    try:
        ingest_hour(today_str, hour)
        print(f"Downloaded: hour {hour}")
    except Exception as e:
        print(f"Failed hour {hour}: {e}")

process_day_to_silver(today_str)
process_day_to_gold(today_str)
delete_bronze_day(today_str)
delete_silver_day(today_str)

print("\nDaily ingest complete!")