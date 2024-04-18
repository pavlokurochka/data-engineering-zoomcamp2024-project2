MODEL (
  name facts.matches_by_size,
  kind FULL
);

-- WITH by_date AS (
SELECT
	  match_size, 
	 startgame_date,
		count(1) count_
FROM
		coh3.facts.matches mm
WHERE
len(ai_difficulty) =0
	AND description = 'AUTOMATCH'
GROUP BY
	ALL
-- 	)
-- 	PIVOT by_date ON
-- match_size
-- 	USING sum(count_)
-- ORDER BY
-- startgame_date