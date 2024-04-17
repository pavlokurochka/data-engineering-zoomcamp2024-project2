MODEL (
  name facts.match_table_rec_counts,
  kind FULL
);

SELECT
	'matches',
	count(1) count_
from
	coh3.fact.matches
union
SELECT
	'matches__matchhistoryitems',
	count(1) count_
from
	coh3.fact.matches__matchhistoryitems
union
SELECT
	'matches__matchhistoryreportresults',
	count(1) count_
from
	coh3.fact.matches__matchhistoryreportresults
union
SELECT
	'matches__profile_ids',
	count(1) count_
from
	coh3.fact.matches__profile_ids