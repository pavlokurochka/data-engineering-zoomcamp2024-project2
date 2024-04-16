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
df = con.sql("""WITH by_map_date AS (
SELECT
	mp.name map_name,
	epoch_ms(startgametime * 1000)::date::CHARACTER  start_date,
		count(1) count_
FROM
		coh3.fact.matches mm
JOIN coh3.dim.match_types mt ON
	mm.matchtype_id = mt.id
LEFT JOIN coh3.dim.maps mp ON
	mm.mapname = mp.id
WHERE
	mt."name" NOT LIKE '%Ai%'
	AND LEFT(mt.name,
	3) = '1V1'
	--	AND epoch_ms(startgametime * 1000)::date = '2024-04-08'
	AND mm.description = 'AUTOMATCH'
GROUP BY
	ALL)
	PIVOT by_map_date ON
map_name
	USING sum(count_)
ORDER BY
start_date""").df()
# %%
st.markdown("_Maps by Match Type_")
st.dataframe(df)

# %%
df = con.sql("""SELECT mp.name map_name,
		count(1) count_
	FROM
		coh3.fact.matches mm
  join coh3.dim.match_types mt on mm.matchtype_id = mt.id
  left join coh3.dim.maps mp on mm.mapname = mp.id
  where mt."name" not like '%Ai%'
             and left(mt.name,3)  = '1V1'
             and epoch_ms(startgametime*1000)::date = '2024-04-08'
  and      mm.description = 'AUTOMATCH'
		GROUP BY ALL 
		ORDER BY 1,2 
        limit 10""").df()
# %%
st.markdown("_1v1 Maps_")
st.bar_chart(df.set_index("map_name"))
# %%
# set MOTHERDUCK_TOKEN=<tocken>
# streamlit run .\dashboard.py
