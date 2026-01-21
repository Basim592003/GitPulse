import sys
sys.path.append(".")

from datetime import datetime, timedelta, timezone
from ingest.bronze import ingest_hour, delete_bronze_day
from ingest.silver import process_day_to_silver, delete_silver_day
from ingest.gold import process_day_to_gold
from ingest.config import get_s3_client, R2_BUCKET

s3 = get_s3_client()

yesterday = datetime.now(timezone.utc) - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

print(f"\n=== Processing {yesterday_str} ===")

for hour in range(24):
    try:
        ingest_hour(yesterday_str, hour)
        print(f"Downloaded: hour {hour}")
    except Exception as e:
        print(f"Failed hour {hour}: {e}")
process_day_to_silver(yesterday_str)
process_day_to_gold(yesterday_str)
delete_bronze_day(yesterday_str)
delete_silver_day(yesterday_str)

print("\nDaily ingest complete!")