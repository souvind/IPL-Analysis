-- sql_scripts/ipl_analytics_queries.sql
-- Useful SQL queries for IPL analytics after loading parquet/external tables.

-- 1. Top 10 batsmen by runs
SELECT batsman, SUM(batsman_runs) AS total_runs
FROM deliveries
GROUP BY batsman
ORDER BY total_runs DESC
LIMIT 10;

-- 2. Top 10 bowlers by wickets (using dismissal_kind)
SELECT bowler, COUNT(*) AS wickets
FROM deliveries
WHERE dismissal_kind IS NOT NULL AND dismissal_kind <> ''
GROUP BY bowler
ORDER BY wickets DESC
LIMIT 10;

-- 3. Team wins per season
SELECT season, winner, COUNT(*) AS wins
FROM matches
GROUP BY season, winner
ORDER BY season, wins DESC;

-- 4. Highest scoring match (total runs)
SELECT m.match_id, m.team1, m.team2, SUM(d.total_runs) AS match_runs
FROM matches m JOIN deliveries d ON m.match_id = d.match_id
GROUP BY m.match_id, m.team1, m.team2
ORDER BY match_runs DESC
LIMIT 5;

-- 5. Player of the match counts
SELECT player_of_match, COUNT(*) AS awards
FROM matches
GROUP BY player_of_match
ORDER BY awards DESC;

-- 6. Average runs per over by season
SELECT d.season, d.over, AVG(d.total_runs) AS avg_runs_over
FROM (SELECT m.season, d.* FROM matches m JOIN deliveries d ON m.match_id = d.match_id) d
GROUP BY d.season, d.over
ORDER BY d.season, d.over;

-- 7. Player head-to-head example
SELECT batsman, bowler, SUM(batsman_runs) AS runs
FROM deliveries
WHERE batsman = 'V Kohli' AND bowler = 'S Sharma'
GROUP BY batsman, bowler
ORDER BY runs DESC;

-- 8. Dot ball percentage by bowler
SELECT bowler, SUM(CASE WHEN total_runs = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS dot_percentage
FROM deliveries
GROUP BY bowler
ORDER BY dot_percentage DESC
LIMIT 20;
