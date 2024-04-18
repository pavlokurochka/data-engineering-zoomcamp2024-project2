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
load_dotenv()
motherduck_token = os.environ["MOTHERDUCK_TOKEN"]
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
st.markdown("_Count of Matches by Size_")
st.dataframe(df)

st.line_chart(df)
# %%
# set MOTHERDUCK_TOKEN=<tocken>
# streamlit run .\dashboard.py
