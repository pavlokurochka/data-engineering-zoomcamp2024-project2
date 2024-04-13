# https://coh3stats.com/other/open-data
# You can get the data here 
# https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-raw-data.ts
# and here
# https://github.com/cohstats/coh3-stats/blob/master/src/coh3/coh3-data.ts
# For the counters you can find more info here https://github.com/cohstats/coh3-stats/issues/194
# %%
import datetime  # import datetime, timezone
import requests
import yaml
import dlt

# %%

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
def get_yesterday () -> str:
    date_before = datetime.datetime.now() - datetime.timedelta(
    days=1
        )
    return date_before.strftime("%Y-%m-%d")
# get_yesterday ()

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
        pipeline_name="coh3", destination="motherduck", dataset_name="fact"
    )
    info = pipeline.run(
        matches_in, table_name="matches", write_disposition="merge", primary_key="id"
    )
    print(info)

# %%
matches = download_matches('2024-04-08')
merge_matches(matches)
# %%
def import_dims():
    yaml_file = "coh3-raw-data.yaml"
    with open(yaml_file, encoding="utf-8") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    
    factions = [v for _k,v in data['factions'].items()]
    data['factions'] = factions
    data['match_types'] = data['matchTypes']
    del data['matchTypes']
    
    maps = []
    for m in data['maps']:
        for k,v in m.items():
            map_row  = {}
            map_row['id'] = k
            map_row.update(v)
            maps.append(map_row)
    data['maps'] = maps
    return data
    
# %%
def refresh_dims(data_in:dict):
    pipeline = dlt.pipeline(
        pipeline_name="coh3", destination="motherduck", dataset_name="dim"
    )
    for table, contents in data_in.items():
        print(f' Loading {table}')
        info = pipeline.run(
                contents, table_name=table, write_disposition="replace", primary_key="id"
            )
        print(info)

# %%
dims = import_dims()
refresh_dims(dims)
# %%

# %%
# maps = []
# for m in dims['maps']:
#     for k,v in m.items():
#         map_row  = {}
#         map_row['id'] = k
#         map_row.update(v)
#         maps.append(map_row)
# # %%
# maps
# %%
