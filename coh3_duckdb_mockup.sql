SELECT
		description,
		creator_profile_id,
		mapname,
		expire_at___seconds,
		expire_at___nanoseconds,
		platform,
		completiontime,
		matchtype_id,
	epoch_ms(startgametime * 1000)::datetime startgame_date,
		id,
		maxplayers,
		"_dlt_load_id",
		"_dlt_id"
--	count(1) count_
FROM
	coh3.matches.matches
where epoch_ms(startgametime*1000)::datetime  = '2024-01-01 19:25:59.000'
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
	coh3.matches.matches__profile_ids
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
	coh3.matches.matches m
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