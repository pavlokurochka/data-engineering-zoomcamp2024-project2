""" 
Backfill facts for github actions .github\workflows\data-pipeline.yml

    export $(grep -v '^#' .env | xargs)

"""
# %%
import os
import duckdb
from dlt_motherduck import import_facts
import pandas as pd

# %%
START_DATE = "2024-02-29"
TIME_DELTA = "1D"
# %%
end_date = import_facts.get_yesterday()
# %%
motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
# %%
con = duckdb.connect(f"md:coh3?motherduck_token={motherduck_token}")
# %%
results = con.sql("""SELECT distinct startgame_date::varchar   startgame_date
            from coh3.facts.matches order by 1""")
# print(results)
df = results.df()
# print(df)
dates_db = [
    str(x)[:10]
    for x in df["startgame_date"]
]
# %%
dates_generated = [
    str(x)[:10] for x in pd.date_range(start=START_DATE, end=end_date, freq=TIME_DELTA)
]
# %%
dates_to_run = [x for x in dates_generated if x not in dates_db]
# %%
os.chdir('dlt_motherduck')
# %%
print (f"{dates_to_run=}")
# %%
if len(dates_to_run)>0:
    for match_date in dates_to_run:
        print(f'{match_date=}')
        matches = import_facts.download_matches(match_date)
        import_facts.merge_matches(matches)

# %%
