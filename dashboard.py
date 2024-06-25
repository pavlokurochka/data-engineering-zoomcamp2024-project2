# https://motherduck.com/blog/the-future-of-bi-bi-as-code-duckdb-impact/
# pip install duckdb==v0.9.2
# https://cheat-sheet.streamlit.app/
# %%
import streamlit as st
import duckdb
import os
from dotenv import load_dotenv

# %%
# load_dotenv(os.path.join('..','.env'))
motherduck_token = None
load_dotenv()
if "MOTHERDUCK_TOKEN" in os.environ:
	motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
# %%
#  for hosting on https://coh3matches.streamlit.app/ - add secret to the app
if not motherduck_token:
	if "MOTHERDUCK_TOKEN" in st.secrets:
		motherduck_token = st.secrets["MOTHERDUCK_TOKEN"]
# %%
con = duckdb.connect(f"md:coh3?motherduck_token={motherduck_token}")

# con = duckdb.connect('md:_share/coh3/03f5c2f6-bc17-4b66-893f-dd009a3d4b59')
# %%
df = con.sql("""WITH by_date AS (
				SELECT
					*
				FROM
					coh3.facts.matches_by_size)
					PIVOT by_date ON
				match_size
					USING sum(count_)
				ORDER BY
				startgame_date""").df().set_index("startgame_date")
# %%
st.set_page_config(
	page_title="COH3 Data",
	page_icon="🪖",
)
st.header('Dashboard for Lightweight Data Pipeline for Company of Heroes 3 Matches')

st.markdown(""" 
	This is the result of the data pipeline of match statistics 
	from my current favorite game [Company of  Heroes 3](https://community.companyofheroes.com/coh-franchise-home/company-of-heroes-3).


	It turns out the enthusiasts behind [coh3stats](https://coh3stats.com/stats/games) already did the best dashboards I could imagine. 
	However, I thought that there could always be more queries to be run to explore data from some other angle. 
	And while coh3stats guys do store and [expose raw data](https://coh3stats.com/other/open-data), 
	I got the impression that the format is not very data analyst friendly.
			
	Check out the [readme](https://github.com/pavlokurochka/data-engineering-zoomcamp2024-project2/blob/main/README.md) for the GitHub repository. Data is hosted and updated using only free tier resources. This pipeline runs daily on GitHub Actions and stores data in [MotherDuck](https://app.motherduck.com/).
	This app retreives the pivot of match size data with DuckDB to display results in the table and the graph below.			

""")
st.caption("_Count of Matches by Size_")
st.dataframe(df)
st.line_chart(df)
# %%
# set MOTHERDUCK_TOKEN=<tocken>
# streamlit run .\dashboard.py
