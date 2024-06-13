MODEL (
  name facts.matches,
  kind VIEW
  -- kind INCREMENTAL_BY_UNIQUE_KEY (
  --   unique_key (id)
  -- ),
  -- columns (
  --   id BIGINT,
  --   description TEXT,
  --   creator_profile_id BIGINT,
  --   mapname TEXT,
  --   map_name TEXT,
  --   expire_at_datetime TIMESTAMP,
  --   platform TEXT,
  --   completiontime_datetime TIMESTAMP,
  --   completiontime_date DATE,
  --   matchtype_id BIGINT,
  --   match_size TEXT,
  --   match_type TEXT,
  --   ai_difficulty TEXT,
  --   startgame_datetime TIMESTAMP,
  --   startgame_date DATE,
  --   _dlt_load_id TEXT,
  --   _dlt_id TEXT
  -- )
);

SELECT
  mm.id,
  mm.description,
  creator_profile_id,
  mapname,
  COALESCE(mp.name, mapname) AS map_name,
  EPOCH_MS(expire_at___seconds * 1000)::DATETIME AS expire_at_datetime,
  platform,
  EPOCH_MS(completiontime * 1000)::DATETIME AS completiontime_datetime,
  EPOCH_MS(completiontime * 1000)::DATE AS completiontime_date,
  mm.matchtype_id,
  mt.match_size,
  mt.match_type,
  mt.ai_difficulty,
  EPOCH_MS(startgametime * 1000)::DATETIME AS startgame_datetime,
  EPOCH_MS(startgametime * 1000)::DATE AS startgame_date,
  mm."_dlt_load_id",
  mm."_dlt_id"
FROM coh3.fact.matches AS mm
JOIN coh3.dimensions.match_types AS mt
  ON mm.matchtype_id = mt.id
LEFT JOIN coh3.dim.maps AS mp
  ON mm.mapname = mp.id