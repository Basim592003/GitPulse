import gzip
import json
from ingest.config import get_s3_client, R2_BUCKET

s3 = get_s3_client()

response = s3.get_object(
    Bucket=R2_BUCKET,
    Key="bronze/year=2025/month=09/day=01/hour=00/events.json.gz"
)

compressed_data = response["Body"].read()
decompressed_data = gzip.decompress(compressed_data)

first_line = decompressed_data.split(b"\n")
count = len(first_line)
print(count)

event = json.loads(first_line)

print(json.dumps(event))
