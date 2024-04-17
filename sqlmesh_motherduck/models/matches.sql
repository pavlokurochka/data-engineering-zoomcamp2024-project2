MODEL (
  name facts.matches,
    kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (id)
  )
);
SELECT
	mm.id,
		description,
		creator_profile_id,
		mapname,
	COALESCE (mp.name,
	mapname) map_name,
	epoch_ms(expire_at___seconds * 1000)::datetime expire_at_datetime,
		platform,
		epoch_ms(completiontime * 1000)::datetime completiontime_datetime,
		epoch_ms(completiontime * 1000)::date completiontime_date,
		mm.matchtype_id,
		mt.match_size ,	
		mt.match_type,
	mt.ai_difficulty ,
	epoch_ms(startgametime * 1000)::datetime startgame_datetime,
	epoch_ms(startgametime * 1000)::date startgame_date,
		mm."_dlt_load_id",
		mm."_dlt_id"
FROM
	coh3.fact.matches mm
JOIN coh3.dimensions.match_types mt ON
	mm.matchtype_id = mt.id
LEFT JOIN coh3.dim.maps mp ON
	mm.mapname = mp.id