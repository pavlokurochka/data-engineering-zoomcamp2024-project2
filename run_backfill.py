""" 
Backfill facts for github actions .github\workflows\data-pipeline.yml

    export $(grep -v '^#' .env | xargs)

"""
# %%
import os
import duckdb
from dlt_motherduck import import_facts
import pandas as pd
import subprocess
# %%
# from dotenv import load_dotenv

# # Load variables from .env file
# load_dotenv(".env")
# %%
# %%
START_DATE = "2023-07-01"
TIME_DELTA = "1D"
# %%
end_date = import_facts.get_yesterday()
# %%
motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
# %%
con = duckdb.connect(f"md:coh3?motherduck_token={motherduck_token}")
# %%
results = con.sql(""" SELECT DISTINCT EPOCH_MS(startgametime * 1000)::DATE AS startgame_date
            FROM coh3.fact.matches order by 1""")
# print(results)
df = results.df()
con.close()
del con
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
# if len(dates_to_run)>0:
#     for match_date in dates_to_run:
#         print(f'{match_date=}')
#         matches = import_facts.download_matches(match_date)
#         if matches:
#             import_facts.merge_matches(matches)

# %% Generate the .sh bash script
sh_filename = "run_backfill.sh"

with open(sh_filename, "w") as f:
    f.write("#!/bin/bash\n")
    f.write("# Automated backfill execution script\n\n")
    for match_date in dates_to_run:
        f.write(f"python import_facts.py --match_date '{match_date}'\n")

print(f"\n[SUCCESS] Created {sh_filename} with {len(dates_to_run)} commands.")

# %% Make the script executable and run it
try:
    print("Making the script executable...")
    subprocess.run(["chmod", "+x", sh_filename], check=True)
    
    print("Running the backfill script...")
    # Use text=True to stream the console output in real-time
    subprocess.run([f"./{sh_filename}"], check=True, text=True)
    print("[SUCCESS] Bash script execution completed.")
    
except subprocess.CalledProcessError as e:
    print(f"[ERROR] Bash command failed with exit code {e.returncode}")