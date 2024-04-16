# %%
import requests
import yaml
import dlt
# %%
def import_dims():
    # yaml_file = "coh3-raw-data.yaml"
    # with open(yaml_file, encoding="utf-8") as f:
    #     data = yaml.load(f, Loader=yaml.FullLoader)
    url = 'https://github.com/pavlokurochka/data-engineering-zoomcamp2024-project2/raw/main/dlt/coh3-raw-data.yaml'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    response_text = response.text
    data = yaml.load(response_text, Loader=yaml.FullLoader)

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