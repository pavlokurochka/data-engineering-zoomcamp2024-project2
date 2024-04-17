MODEL (
  name dimensions.match_types,
  kind FULL
);

SELECT
  id,
  name,
  SPLIT_PART(name, '_', 1) AS match_size,
  SPLIT_PART(name, '_', 2) AS match_type,
  SPLIT_PART(name, '_', 3) AS ai_difficulty,
  "_dlt_id",
  localized_name
FROM coh3.dim.match_types