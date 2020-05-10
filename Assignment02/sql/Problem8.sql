CREATE OR ALTER PROCEDURE sp_Problem8
AS
BEGIN

WITH avg_hours_g1 AS (
	SELECT G1.appid
	       , AVG(CONVERT(DECIMAL, G1.playtime_forever)) as avg_playtime
	FROM games_1 G1
	GROUP BY G1.appid
),
avg_hours_g2 AS (
	SELECT G2.appid
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2 G2
	GROUP BY G2.appid
)
SELECT AVG(ABS(AHG2.avg_playtime - AHG1.avg_playtime)), 'Released Before Initial Capture'
FROM app_id_info AII
-- left join since some games may have come out between
LEFT JOIN avg_hours_g1 AHG1
	   ON AHG1.appid = AII.appid
INNER JOIN avg_hours_g2 AHG2
		ON AHG2.appid = AII.appid
WHERE AHG1.appid IS NOT NULL
UNION
-- coalesce AHG1 to 0 if null (means that game wasn't around during G1 capture date)
SELECT AVG(AHG2.avg_playtime), 'Released After Initial Capture'
FROM app_id_info AII
-- left join since some games may have come out between
LEFT JOIN avg_hours_g1 AHG1
	   ON AHG1.appid = AII.appid
INNER JOIN avg_hours_g2 AHG2
		ON AHG2.appid = AII.appid
WHERE AHG1.appid IS NULL

END
GO

CREATE OR ALTER PROCEDURE sp_Problem8_r3pg
AS
BEGIN

WITH avg_hours_g1 AS (
	SELECT G1.appid
	       , AVG(CONVERT(DECIMAL, G1.playtime_forever)) as avg_playtime
	FROM games_1_r3pg G1
	GROUP BY G1.appid
),
avg_hours_g2 AS (
	SELECT G2.appid
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2_r3pg G2
	GROUP BY G2.appid
)
SELECT AVG(ABS(AHG2.avg_playtime - AHG1.avg_playtime)), 'Released Before Initial Capture'
FROM app_id_info_r3pg AII
-- left join since some games may have come out between
LEFT JOIN avg_hours_g1 AHG1
	   ON AHG1.appid = AII.appid
INNER JOIN avg_hours_g2 AHG2
		ON AHG2.appid = AII.appid
WHERE AHG1.appid IS NOT NULL
UNION
-- coalesce AHG1 to 0 if null (means that game wasn't around during G1 capture date)
SELECT AVG(AHG2.avg_playtime), 'Released After Initial Capture'
FROM app_id_info_r3pg AII
-- left join since some games may have come out between
LEFT JOIN avg_hours_g1 AHG1
	   ON AHG1.appid = AII.appid
INNER JOIN avg_hours_g2 AHG2
		ON AHG2.appid = AII.appid
WHERE AHG1.appid IS NULL

END
GO

