MODEL (
  name dimensions.races,
  kind FULL
);

SELECT
	r.id,
	r.name race_name,	
	faction_id,
	f.name faction_name,
	r.localized_name,
	f."_dlt_load_id",
	f."_dlt_id"
FROM
	coh3.dim.races r
join coh3.dim.factions f on
	r.faction_id = f.id  
	;