# https://coh3stats.com/other/open-data

#%%

# %%
from datetime import datetime, timezone
import requests
import dlt
# %%
# utc_now = datetime.now(timezone.utc)

# Extract year, month, day
# year = utc_now.year
# month = utc_now.month - 1  # Months are 0-indexed in Python
# day = utc_now.day
# year = 2024
# month = 1
# day = 2
def get_midnight_utc_timestamp(year, month, day):
    # Create a UTC datetime object at midnight
    midnight_utc = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)

    # Convert to timestamp (seconds since Epoch)
    timestamp = midnight_utc.timestamp()

    return int(timestamp)
# %%
# from datetime import datetime, timezone
# from datetime import UTC
ts = int('1688256000')
# ts = int('1685577600')
print(datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
# %%

# %%
ts_in = get_midnight_utc_timestamp(year=2024, month=1, day=4)
url = f'https://storage.coh3stats.com/matches/matches-{ts_in}.json'
response = requests.get(url,timeout = 60)
response.raise_for_status()
# %%
response_json = response.json()
# %%
ts = int(response_json['timeStamp'])
print(datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
# %%
matches = response_json['matches']
# %%
len(matches)
# %%
pipeline = dlt.pipeline(pipeline_name='coh3', destination="duckdb", dataset_name="matches")
info = pipeline.run(matches, table_name="matches", write_disposition= 'append')
# %%
