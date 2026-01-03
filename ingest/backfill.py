from datetime import datetime, timedelta
from bronze import ingest_hour
from silver import process_day_to_silver, delete_bronze_day

start_date = datetime(2025, 12, 3)
end_date = datetime(2025, 12, 18)

current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    print(f"\n=== Processing {date_str} ===")
    
    for hour in range(24):
        try:
            key = ingest_hour(date_str, hour)
            print(f"Downloaded: hour {hour}")
        except Exception as e:
            print(f"Failed download hour {hour}: {e}")
    
    process_day_to_silver(date_str)
    delete_bronze_day(date_str)
    
    current += timedelta(days=1)

print("\nBackfill complete!")
