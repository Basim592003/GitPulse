import sys
sys.path.append(".")

from datetime import datetime, timedelta, timezone
from ingest.bronze import ingest_hour, delete_bronze_day
from ingest.silver import process_day_to_silver, delete_silver_day
from ingest.gold import process_day_to_gold

start_date = datetime(2026, 1, 1)
end_date = datetime(2026, 1, 10)

current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    print(f"\n=== Processing {date_str} ===")
    
    for hour in range(24):
        try:
            ingest_hour(date_str, hour)
            print(f"Downloaded: hour {hour}")
        except Exception as e:
            print(f"Failed hour {hour}: {e}")
    
    process_day_to_silver(date_str)
    process_day_to_gold(date_str)
    delete_bronze_day(date_str)
    delete_silver_day(date_str)
    
    current += timedelta(days=1)

print("\nDone!")
