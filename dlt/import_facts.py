# pip install duckdb==v0.9.2, dlt[motherduck]>=0.3.25
# https://coh3stats.com/other/open-data
# You can get the data here
# https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-raw-data.ts
# and here
# https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-data.ts
# For the counters you can find more info here https://github.com/cohstats/coh3-stats/issues/194
# dlt[motherduck]>=0.3.25
# %%
import datetime  # import datetime, timezone
import requests
# import yaml
import dlt
import argparse


# %%
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
def get_yesterday() -> str:
    date_before = datetime.datetime.now() - datetime.timedelta(days=1)
    return date_before.strftime("%Y-%m-%d")


# get_yesterday ()


# %%
def download_matches(date_in: str) -> list:
    ts_in = get_midnight_utc_timestamp(date_in)
    url = f"https://storage.coh3stats.com/matches/matches-{ts_in}.json"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    response_json = response.json()
    ts = int(response_json["timeStamp"])
    dl_ts = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    matches = response_json["matches"]
    print(f"Downloaded {len(matches)} matches for {dl_ts}")
    return matches


# %%
def merge_matches(matches_in: list):
    pipeline = dlt.pipeline(
        pipeline_name="coh3", destination="motherduck", dataset_name="fact"
    )
    info = pipeline.run(
        matches_in, table_name="matches", write_disposition="merge", primary_key="id"
    )
    print(info)


# %%
parser = argparse.ArgumentParser()
parser.add_argument(
    "--match_date",
    type=str,
    default=get_yesterday(),
    help="Enter match date YYYY-MM-DD",
)

args = parser.parse_args()
match_date = args.match_date[:11]
matches = download_matches(args.match_date)
merge_matches(matches)
