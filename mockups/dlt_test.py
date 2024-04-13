# https://coh3stats.com/other/open-data

# %%

# %%
import datetime  # import datetime, timezone
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
def get_midnight_utc_timestamp(date_in: str):

    date_ts = datetime.datetime.strptime(date_in, "%Y-%m-%d")
    year_ = int(datetime.datetime.strftime(date_ts, "%Y"))
    month_ = int(datetime.datetime.strftime(date_ts, "%m"))
    day_ = int(datetime.datetime.strftime(date_ts, "%d"))
    # Create a UTC datetime object at midnight
    midnight_utc = datetime.datetime(
        year_, month_, day_, 0, 0, 0, tzinfo=datetime.timezone.utc
    )

    # Convert to timestamp (seconds since Epoch)
    timestamp = midnight_utc.timestamp()

    return int(timestamp)


# %%
# from datetime import datetime, timezone
# from datetime import UTC
ts = int("1711843218")
# ts = int('1685577600')
print(
    datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
)
# %%
def get_yesterday () -> str:
    date_before = datetime.datetime.now() - datetime.timedelta(
    days=1
        )
    return date_before.strftime("%Y-%m-%d")
get_yesterday ()
# %%

# date_in = "2024-03-31"
date_in = get_yesterday ()

# date_before = datetime.datetime.strptime(date_in, "%Y-%m-%d") - datetime.timedelta(
#     days=1
# )
# date_before = date_before.strftime("%Y-%m-%d")
# %%
# %%
def download_matches(date_in:str) ->list:
    ts_in = get_midnight_utc_timestamp(date_in)
    url = f"https://storage.coh3stats.com/matches/matches-{ts_in}.json"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    response_json = response.json()
    ts = int(response_json["timeStamp"])
    dl_ts  =   datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S")
    matches = response_json["matches"]
    print( f'Downloaded {len(matches)} matches for {dl_ts}')
    return matches
# %%
def merge_matches (matches_in:list):
    pipeline = dlt.pipeline(
        pipeline_name="coh3", destination="duckdb", dataset_name="matches"
    )
    info = pipeline.run(
        matches_in, table_name="matches", write_disposition="merge", primary_key="id"
    )
    print(info)

# %%
matches = download_matches('2024-04-08')
merge_matches(matches)
# %%
import json

json_file = r"C:\Users\kuroc\Downloads\matches-1711843200.json"

# %%
with open(json_file, encoding="utf-8") as f:
    data = json.load(f)
# %%
data["matches"][0]["matchhistorymember"]

# %%
list(data["matches"][0].keys())
# %%
for k, v in data["matches"][0].items():
    print(f"{k}:{v}")
# %%
import pandas as pd

# %%
df = pd.DataFrame(matches)
# %%
df.head()
# %%
df.tail()
# %%
matches = data["matches"]
# %%
import yaml
# %%
yaml_file = "coh3-raw-data.yaml"
with open(yaml_file, encoding="utf-8") as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
# %%
data.keys()
# %%
[v for k,v in data['factions'].items()]
# %%
