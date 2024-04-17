-- pip install duckdb==v0.9.2
SELECT mm.id,
		description,
		creator_profile_id,
		mapname,
	mp.name map_name,
	epoch_ms(expire_at___seconds * 1000)::datetime expire_at_datetime,
--		expire_at___seconds,
--		expire_at___nanoseconds,
		platform,
		epoch_ms(completiontime * 1000)::datetime completiontime_datetime,
		epoch_ms(completiontime * 1000)::date completiontime_date,
		matchtype_id,
	mt.name match_type,
	epoch_ms(startgametime * 1000)::datetime startgame_datetime,
	epoch_ms(startgametime * 1000)::date startgame_date,
		--		maxplayers,
		mm."_dlt_load_id",
		mm."_dlt_id"
	--	count(1) count_
FROM
	coh3.fact.matches mm
JOIN coh3.dim.match_types mt ON
	mm.matchtype_id = mt.id
LEFT JOIN coh3.dim.maps mp ON
	mm.mapname = mp.id
	--where epoch_ms(startgametime*1000)::datetime  = '2024-01-01 19:25:59.000'
	--	group by
	--	all
	--order by
	--	couNT_ DESC
	;
with platers_per as (
SELECT
	"_dlt_parent_id",
	count(1) count_
FROM
	coh3.fact.matches__profile_ids
group by
	ALL )
	,
	by_map_date as(
SELECT
	mapname,
	p.count_ players_per_match,
	epoch_ms(startgametime * 1000)::date startgame_date,
	count(1) count_
FROM
	coh3.fact.matches m
join platers_per p on
	m._dlt_id = p._dlt_parent_id
group by
	all
having
	players_per_match = 2)
	pivot by_map_date on
startgame_date
	using sum(count_)
order by
3 DESC
limit 10